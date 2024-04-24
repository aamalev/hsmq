use crate::config::{self, Config};
use crate::errors::GenericError;
use crate::metrics;
use crate::pb::Message;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_util::task::task_tracker::TaskTracker;
use tokio_util::time::delay_queue::{self, DelayQueue};
use uuid::Uuid;

#[derive(Debug)]
pub struct Envelop {
    pub message: Message,
}

impl Envelop {
    pub fn new(message: Message) -> Self {
        Self { message }
    }

    pub fn gen_msg_id(&self) -> Uuid {
        Uuid::new_v4()
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
    subs: Vec<mpsc::Sender<QueueCommand>>,
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

    fn subscribe(&mut self, queue: &Queue) {
        self.subs.push(queue.tx.clone());
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
enum ConsumerSendResult {
    Consumer(Arc<GenericConsumer>),
    Requeue(Arc<Envelop>),
    RequeueAck(Arc<Envelop>, String),
    GracefulShutdown,
}

#[tonic::async_trait]
pub trait Consumer: Debug {
    fn get_id(&self) -> Uuid;
    fn with_ack(&self) -> bool;
    async fn send(&self, msg: Arc<Envelop>) -> Result<(), GenericError>;
    async fn send_id(&self, msg: Arc<Envelop>, id: String) -> Result<(), GenericError>;
    async fn send_resp(&self, resp: Response) -> Result<(), GenericError>;
    async fn stop(&self);
}

pub type GenericFuture<T> = Pin<Box<(dyn std::future::Future<Output = T> + Send + 'static)>>;
pub type GenericConsumer = Box<dyn Consumer + Send + Sync + 'static>;

pub enum QueueCommand {
    Msg(Arc<Envelop>),
    MsgAck(String),
    Requeue(Arc<Envelop>),
    ConsumeStart(GenericConsumer),
    ConsumeStop(Uuid),
}

#[derive(Clone, Debug)]
pub struct Queue {
    pub name: String,
    tx: mpsc::Sender<QueueCommand>,
}

impl Queue {
    fn new(cfg: config::Queue, task_tracker: TaskTracker) -> Self {
        let (tx, rx) = mpsc::channel(99);
        let name = cfg.name.clone();
        task_tracker.spawn(Self::processing(cfg, rx, task_tracker.clone()));
        Self { name, tx }
    }

    pub async fn send(&self, cmd: QueueCommand) -> Result<(), GenericError> {
        self.tx.send(cmd).await?;
        Ok(())
    }

    async fn processing(
        cfg: config::Queue,
        mut rx: mpsc::Receiver<QueueCommand>,
        task_tracker: TaskTracker,
    ) {
        let name = cfg.name.clone();
        let mut tasks = JoinSet::new();
        let mut consumers = HashMap::new();
        let mut waiters = VecDeque::new();
        let mut messages = VecDeque::new();
        let mut unacked = DelayQueue::new();
        let mut unacked_map: HashMap<String, delay_queue::Key> = HashMap::new();
        let m_received = metrics::QUEUE_COUNTER.with_label_values(&[&name, "received"]);
        let m_sent = metrics::QUEUE_COUNTER.with_label_values(&[&name, "sent"]);
        let m_requeue = metrics::QUEUE_COUNTER.with_label_values(&[&name, "requeue"]);
        let m_drop_limit = metrics::QUEUE_COUNTER.with_label_values(&[&name, "drop-limit"]);
        let m_consumers = metrics::QUEUE_GAUGE.with_label_values(&[&name, "consumers"]);
        let m_messages = metrics::QUEUE_GAUGE.with_label_values(&[&name, "messages"]);
        let m_unacked = metrics::QUEUE_GAUGE.with_label_values(&[&name, "unacked"]);
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
                        QueueCommand::MsgAck(msg_id) => {
                            log::debug!("Received ack {:?}", &msg_id);
                            if let Some(key) = unacked_map.remove(&msg_id) {
                                if let Some(_) = unacked.try_remove(&key) {
                                    m_unacked.dec();
                                }
                            }
                        }
                        QueueCommand::Requeue(msg) => {
                            m_requeue.inc();
                            messages.push_front(msg);
                            m_messages.set(messages.len() as f64);
                        }
                        QueueCommand::ConsumeStart(consumer) => {
                            let id = consumer.get_id();
                            let consumer = Arc::new(consumer);
                            waiters.push_back(consumer.clone());
                            consumers.insert(id, consumer);
                            m_consumers.inc();
                        }
                        QueueCommand::ConsumeStop(consumer_id) => {
                            m_consumers.dec();
                            if let Some(consumer) = consumers.remove(&consumer_id) {
                                consumer.stop().await;
                            }
                        }
                    };
                }
                Some(expired) = unacked.next() => {
                    if let ConsumerSendResult::RequeueAck(msg, id) = expired.into_inner() {
                        m_requeue.inc();
                        messages.push_front(msg);
                        if let Some(key) = unacked_map.remove(&id) {
                            if let Some(_) = unacked.try_remove(&key) {
                                m_unacked.dec();
                            }
                        }
                    }
                }
                Some(res) = tasks.join_next() => {
                    match res {
                        Ok(ConsumerSendResult::Requeue(msg)) => {
                            log::debug!("Received requeue msg {:?}", &msg);
                            m_requeue.inc();
                            messages.push_front(msg);
                        }
                        Ok(ConsumerSendResult::RequeueAck(msg, id)) => {
                            log::debug!("Received requeue ack msg {:?}", &msg);
                            m_requeue.inc();
                            messages.push_front(msg);
                            if let Some(key) = unacked_map.remove(&id) {
                                unacked.try_remove(&key);
                            }
                        }
                        Ok(ConsumerSendResult::Consumer(consumer)) => {
                            m_sent.inc();
                            waiters.push_back(consumer.clone());
                            let _ = consumer.send_resp(Response::StartConsume(name.clone())).await;
                        }
                        Ok(ConsumerSendResult::GracefulShutdown) => {
                            if task_tracker.is_closed() {
                                if messages.is_empty() {
                                    for consumer in waiters.iter() {
                                        consumers.remove(&consumer.get_id());
                                        let _ = consumer.send_resp(Response::GracefulShutdown(name.clone())).await;
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
                if let Some(consumer) = waiters.pop_front() {
                    if consumers.contains_key(&consumer.get_id()) {
                        if let Some(msg) = messages.pop_front() {
                            m_messages.set(messages.len() as f64);
                            if consumer.with_ack() {
                                let id = Uuid::now_v7().to_string();
                                let item = ConsumerSendResult::RequeueAck(msg.clone(), id.clone());
                                let key = unacked.insert(item, Duration::from_secs(60));
                                unacked_map.insert(id.clone(), key);
                                m_unacked.inc();
                                tasks.spawn(async move {
                                    match consumer.send_id(msg.clone(), id.clone()).await {
                                        Ok(_) => ConsumerSendResult::Consumer(consumer),
                                        Err(_) => ConsumerSendResult::RequeueAck(msg, id),
                                    }
                                });
                            } else {
                                tasks.spawn(async move {
                                    match consumer.send(msg.clone()).await {
                                        Ok(_) => ConsumerSendResult::Consumer(consumer),
                                        Err(_) => ConsumerSendResult::Requeue(msg),
                                    }
                                });
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
    pub queues: HashMap<String, Queue>,
    pub task_tracker: TaskTracker,
}

impl HsmqServer {
    pub fn from(config: Config, task_tracker: TaskTracker) -> Self {
        let mut subscriptions = BTreeMap::new();
        let mut queues = HashMap::new();
        for cfg_queue in config.queues {
            let name = cfg_queue.name.clone();
            let q = Queue::new(cfg_queue.clone(), task_tracker.clone());
            for topic in cfg_queue.topics {
                let sub = subscriptions.entry(topic).or_insert_with(Subscription::new);
                sub.subscribe(&q);
            }
            queues.insert(name, q);
        }
        log::debug!("Created {:?}", &subscriptions);
        Self {
            subscriptions,
            queues,
            task_tracker,
        }
    }
}
