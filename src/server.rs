use crate::config::{self, Config};
use crate::errors::GenericError;
use crate::metrics;
use crate::pb::{Message, MessageMeta};
use prometheus::core::{AtomicF64, GenericGauge};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_util::task::task_tracker::TaskTracker;
use uuid::Uuid;

#[derive(Debug)]
pub struct Envelop {
    pub message: Message,
    pub meta: MessageMeta,
}

impl Envelop {
    pub fn new(message: Message) -> Self {
        Self {
            message,
            meta: MessageMeta::default(),
        }
    }

    pub fn with_generated_id(mut self) -> Self {
        self.meta.id = Uuid::now_v7().to_string();
        self
    }
}

#[derive(Debug)]
pub enum Response {
    StartConsume(String),
    StopConsume(String),
    GracefulShutdown(String),
}

#[derive(Debug)]
pub struct Subscription {
    broadcast: broadcast::Sender<Arc<Envelop>>,
    subs: Vec<GenericQueue>,
}

impl Default for Subscription {
    fn default() -> Self {
        Self::new()
    }
}

impl Subscription {
    pub fn new() -> Self {
        let (broadcast, _) = broadcast::channel(99);
        let subs = vec![];
        Self { broadcast, subs }
    }

    fn subscribe(&mut self, queue: GenericQueue) {
        self.subs.push(queue);
    }

    pub async fn send(&self, msg: Envelop) {
        let msg = Arc::new(msg);
        for qtx in self.subs.iter() {
            if let Err(e) = qtx.send(QueueCommand::Msg(msg.clone())).await {
                log::error!("Error send command {}", e);
            }
        }
        if self.broadcast.receiver_count() > 0 {
            match self.broadcast.send(msg) {
                Ok(_) => (),
                Err(e) => log::error!("Error send broadcast {:?}", e),
            }
        }
    }
}

#[derive(Debug)]
pub enum ConsumerSendResult {
    Consumer(Uuid),
    Requeue(Arc<Envelop>, Uuid),
    RequeueAck(Arc<Envelop>),
    AckTimeout(Arc<Envelop>),
    GracefulShutdown,
}

pub struct UnAck {
    unacked: HashMap<String, Arc<Envelop>>,
    m_unacked: GenericGauge<AtomicF64>,
    m_ack_after: GenericGauge<AtomicF64>,
    pub timeout: Duration,
}

impl UnAck {
    pub fn new(queue_name: String, timeout: Duration) -> Self {
        let unacked = HashMap::new();
        let m_unacked = metrics::QUEUE_GAUGE.with_label_values(&[&queue_name, "unacked"]);
        let m_ack_after = metrics::QUEUE_GAUGE.with_label_values(&[&queue_name, "ack-after"]);
        Self {
            unacked,
            m_unacked,
            m_ack_after,
            timeout,
        }
    }
    pub fn insert(&mut self, value: Arc<Envelop>) -> String {
        let id = value.meta.id.clone();
        self.unacked.insert(id.clone(), value);
        self.m_unacked.inc();
        id
    }
    pub fn remove(&mut self, id: &String, requeue: bool) -> Option<Arc<Envelop>> {
        if let Some(msg) = self.unacked.remove(id) {
            self.m_unacked.dec();
            Some(msg)
        } else {
            if requeue {
                self.m_unacked.dec();
            } else {
                self.m_ack_after.inc();
            };
            None
        }
    }
}

#[tonic::async_trait]
pub trait Consumer: Debug {
    fn get_id(&self) -> Uuid;
    fn is_ackable(&self) -> bool;
    fn send(
        &mut self,
        msg: Arc<Envelop>,
        tasks: &mut JoinSet<ConsumerSendResult>,
        unack: &mut UnAck,
    ) -> Option<Arc<Envelop>>;
    fn ack(&mut self, msg_id: String, unack: &mut UnAck);
    async fn send_resp(&self, resp: Response) -> Result<(), GenericError>;
    async fn stop(&self);
}

pub type GenericFuture<T> = Pin<Box<(dyn std::future::Future<Output = T> + Send + 'static)>>;
pub type GenericConsumer = Box<dyn Consumer + Send + Sync + 'static>;

pub enum QueueCommand {
    Msg(Arc<Envelop>),
    MsgAck(String, String, Uuid),
    Requeue(Arc<Envelop>),
    ConsumeStart(GenericConsumer),
    ConsumeStop(Uuid),
}

#[tonic::async_trait]
pub trait Queue: Debug {
    fn get_name(&self) -> String;
    async fn send(&self, cmd: QueueCommand) -> Result<(), GenericError>;
}

pub type GenericQueue = Arc<Box<dyn Queue + Send + Sync + 'static>>;

#[derive(Debug)]
pub struct InMemoryQueue {
    pub name: String,
    tx: mpsc::Sender<QueueCommand>,
}

#[tonic::async_trait]
impl Queue for InMemoryQueue {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    async fn send(&self, cmd: QueueCommand) -> Result<(), GenericError> {
        self.tx.send(cmd).await?;
        Ok(())
    }
}

impl InMemoryQueue {
    fn new(cfg: config::InMemoryQueue, task_tracker: TaskTracker) -> Self {
        let (tx, rx) = mpsc::channel(99);
        let name = cfg.name.clone();
        task_tracker.spawn(Self::processing(cfg, rx, task_tracker.clone()));
        Self { name, tx }
    }

    fn new_generic(cfg: config::InMemoryQueue, task_tracker: TaskTracker) -> GenericQueue {
        Arc::new(Box::new(Self::new(cfg, task_tracker)))
    }

    async fn processing(
        cfg: config::InMemoryQueue,
        mut rx: mpsc::Receiver<QueueCommand>,
        task_tracker: TaskTracker,
    ) {
        let name = cfg.name.clone();
        let mut tasks = JoinSet::new();
        let mut consumers: HashMap<Uuid, GenericConsumer> = HashMap::new();
        let mut waiters = VecDeque::new();
        let mut messages = VecDeque::new();
        let mut unack = UnAck::new(name.clone(), cfg.ack_timeout.into());
        let m_received = metrics::QUEUE_COUNTER.with_label_values(&[&name, "received"]);
        let m_sent = metrics::QUEUE_COUNTER.with_label_values(&[&name, "sent"]);
        let m_requeue = metrics::QUEUE_COUNTER.with_label_values(&[&name, "requeue"]);
        let m_ack_timeout = metrics::QUEUE_COUNTER.with_label_values(&[&name, "ack-timeout"]);
        let m_drop_limit = metrics::QUEUE_COUNTER.with_label_values(&[&name, "drop-limit"]);
        let m_consumers = metrics::QUEUE_GAUGE.with_label_values(&[&name, "consumers"]);
        let m_messages = metrics::QUEUE_GAUGE.with_label_values(&[&name, "messages"]);
        loop {
            if tasks.is_empty() {
                tasks.spawn(async {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    ConsumerSendResult::GracefulShutdown
                });
            }
            tokio::select! {
                Some(cmd) = rx.recv() => {
                    match cmd {
                        QueueCommand::Msg(msg) => {
                            log::debug!("Received msg {:?}", &msg);
                            m_received.inc();
                            messages.push_back(msg);
                            if let Some(limit) = cfg.limit {
                                while messages.len() > limit {
                                    messages.pop_front();
                                    m_drop_limit.inc();
                                }
                            }
                            m_messages.set(messages.len() as f64);
                        }
                        QueueCommand::MsgAck(msg_id, _, consumer_id) => {
                            log::debug!("Received ack {:?}", &msg_id);
                            if let Some(consumer) = consumers.get_mut(&consumer_id) {
                                consumer.ack(msg_id, &mut unack);
                            } else {
                                unack.remove(&msg_id, false);
                            }
                        }
                        QueueCommand::Requeue(msg) => {
                            m_requeue.inc();
                            messages.push_front(msg);
                            m_messages.set(messages.len() as f64);
                        }
                        QueueCommand::ConsumeStart(consumer) => {
                            let id = consumer.get_id();
                            waiters.push_back(id);
                            let _ = consumer.send_resp(Response::StartConsume(name.clone())).await;
                            consumers.insert(id, consumer);
                            m_consumers.inc();
                        }
                        QueueCommand::ConsumeStop(consumer_id) => {
                            if let Some(consumer) = consumers.remove(&consumer_id) {
                                m_consumers.dec();
                                consumer.stop().await;
                            }
                        }
                    };
                }
                Some(res) = tasks.join_next() => {
                    match res {
                        Ok(ConsumerSendResult::Requeue(msg, consumer_id)) => {
                            log::debug!("Received requeue msg {:?}", &msg);
                            m_requeue.inc();
                            messages.push_front(msg);
                            m_consumers.dec();
                            consumers.remove(&consumer_id);
                        }
                        Ok(ConsumerSendResult::RequeueAck(msg)) => {
                            log::debug!("Received requeue ack msg {:?}", &msg);
                            m_requeue.inc();
                            unack.remove(&msg.meta.id, true);
                            messages.push_front(msg);
                            m_messages.set(messages.len() as f64);
                        }
                        Ok(ConsumerSendResult::AckTimeout(msg)) => {
                            m_ack_timeout.inc();
                            unack.remove(&msg.meta.id, true);
                            messages.push_front(msg);
                            m_messages.set(messages.len() as f64);
                        }
                        Ok(ConsumerSendResult::Consumer(consumer_id)) => {
                            m_sent.inc();
                            waiters.push_back(consumer_id);
                        }
                        Ok(ConsumerSendResult::GracefulShutdown) => {
                            if task_tracker.is_closed() {
                                if messages.is_empty() {
                                    for consumer_id in waiters.iter() {
                                        if let Some(consumer) = consumers.remove(consumer_id) {
                                            let _ = consumer.send_resp(Response::GracefulShutdown(name.clone())).await;
                                        }
                                    }
                                    if consumers.is_empty() {
                                        log::info!("Shutdown queue {} without messages", &name);
                                        return;
                                    }
                                } else if consumers.is_empty() {
                                    log::error!("Shutdown queue {} with messages {} without consumers", &name, messages.len());
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("Error in task queue {}", e);
                        }
                    };
                }
            }
            while !messages.is_empty() && !waiters.is_empty() {
                if let Some(consumer_id) = waiters.pop_front() {
                    if let Some(consumer) = consumers.get_mut(&consumer_id) {
                        if let Some(msg) = messages.pop_front() {
                            m_messages.set(messages.len() as f64);
                            if let Some(msg) = consumer.send(msg, &mut tasks, &mut unack) {
                                messages.push_front(msg);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Default)]
pub struct HsmqServer {
    pub subscriptions: BTreeMap<String, Subscription>,
    pub queues: HashMap<String, GenericQueue>,
    pub task_tracker: TaskTracker,
}

impl HsmqServer {
    pub fn from(config: Config, task_tracker: TaskTracker) -> Self {
        let mut subscriptions = BTreeMap::new();
        let mut queues: HashMap<String, GenericQueue> = HashMap::new();
        for cfg_queue in config.queues {
            match cfg_queue {
                config::Queue::InMemory(cfg_queue) => {
                    let name = cfg_queue.name.clone();
                    let q = InMemoryQueue::new_generic(cfg_queue.clone(), task_tracker.clone());
                    for topic in cfg_queue.topics {
                        let sub = subscriptions.entry(topic).or_insert_with(Subscription::new);
                        sub.subscribe(q.clone());
                    }
                    queues.insert(name, q);
                }
            };
        }
        log::debug!("Created {:?}", &subscriptions);
        Self {
            subscriptions,
            queues,
            task_tracker,
        }
    }
}
