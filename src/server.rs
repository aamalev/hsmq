use crate::config::{self, Config};
use crate::errors::{GenericError, PublishMessageError, SendMessageError};
use crate::metrics;
use crate::pb::{Message, MessageMeta};
use prometheus::core::{AtomicF64, GenericCounter, GenericGauge};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_util::task::task_tracker::TaskTracker;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

#[derive(Debug)]
pub struct Envelop {
    pub message: Message,
    pub meta: MessageMeta,
    pub span: tracing::Span,
}

impl Envelop {
    #[tracing::instrument(
        name = "message",
        fields(
            message.topic = &message.topic,
            message.key = &message.key,
        ),
        skip_all,
        level = "debug",
    )]
    pub fn new(message: Message) -> Self {
        let parent_cx =
            opentelemetry::global::get_text_map_propagator(|prop| prop.extract(&message.headers));
        let span = tracing::Span::current();
        span.set_parent(parent_cx);
        Self {
            message,
            meta: MessageMeta::default(),
            span,
        }
    }

    pub fn with_generated_id(mut self) -> Self {
        let id = Uuid::now_v7().to_string();
        self.meta.id.clone_from(&id);
        self.span.set_attribute("message.id", id);
        self
    }
}

#[derive(Debug)]
pub enum Response {
    StartConsume(String),
    StopConsume(String),
    GracefulShutdown(String),
}

#[derive(Debug, Clone)]
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

    pub(crate) fn subscribe(&mut self, queue: GenericQueue) {
        self.subs.push(queue);
    }

    #[tracing::instrument(name = "subscription.publish", parent = &msg.span, skip_all, level = "debug")]
    pub async fn publish(&self, msg: Envelop) -> anyhow::Result<()> {
        let msg = Arc::new(msg);
        for q in self.subs.iter() {
            q.publish(msg.clone()).await?;
        }
        if self.broadcast.receiver_count() > 0 {
            self.broadcast.send(msg)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum ConsumerSendResult {
    Consumer(Uuid),
    Requeue(Arc<Envelop>, Uuid),
    RequeueAck(Arc<Envelop>),
    AckTimeout(Arc<Envelop>),
    FetchDone,
    GracefulShutdown,
}

pub struct UnAck {
    unacked: HashMap<String, Arc<Envelop>>,
    m_unacked: GenericGauge<AtomicF64>,
    m_ack_after: GenericCounter<AtomicF64>,
    pub timeout: Duration,
}

impl UnAck {
    pub fn new(queue_name: String, timeout: Duration) -> Self {
        let unacked = HashMap::new();
        let m_unacked = metrics::QUEUE_GAUGE.with_label_values(&[&queue_name, "unacked"]);
        let m_ack_after = metrics::QUEUE_COUNTER.with_label_values(&[&queue_name, "ack-after"]);
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
    fn generic_clone(&self) -> GenericConsumer;
    fn get_current_tracing_span(&self) -> tracing::Span;
    fn is_ackable(&self) -> bool;
    fn set_deadline(&mut self, deadline: SystemTime);
    fn is_dead(&self) -> bool;
    fn get_prefetch_count(&self) -> usize;
    fn send(
        &self,
        msg: Arc<Envelop>,
        tasks: &mut JoinSet<ConsumerSendResult>,
        unack: &mut UnAck,
    ) -> Option<Arc<Envelop>>;
    async fn send_timeout(&self, queue: String) -> Result<(), GenericError>;
    async fn send_message(&self, msg: Arc<Envelop>) -> Result<(), SendMessageError>;
    fn ack(&mut self, msg_id: String, unack: &mut UnAck);
    async fn send_resp(&self, resp: Response) -> Result<(), GenericError>;
    async fn stop(&self);
}

pub type GenericFuture<T> = Pin<Box<(dyn std::future::Future<Output = T> + Send + 'static)>>;
pub type GenericConsumer = Box<dyn Consumer + Send + Sync>;

pub enum QueueCommand {
    Msg(Arc<Envelop>),
    FetchMsg(Uuid, SystemTime),
    MsgAck(String, String, Uuid),
    Requeue(Arc<Envelop>),
    ConsumeStart(GenericConsumer),
    ConsumeStop(Uuid),
}

#[tonic::async_trait]
pub trait Queue: Debug {
    fn get_name(&self) -> String;
    fn generic_clone(&self) -> GenericQueue;
    async fn subscribe(&self, consumer: GenericConsumer)
        -> Result<GenericSubscriber, GenericError>;
    async fn publish(&self, msg: Arc<Envelop>) -> Result<(), PublishMessageError>;
}

impl Clone for GenericQueue {
    fn clone(&self) -> Self {
        self.generic_clone()
    }
}

#[tonic::async_trait]
pub trait Subscriber: Debug {
    fn get_name(&self) -> String;
    async fn fetch(&mut self, deadline: SystemTime) -> Result<(), GenericError>;
    async fn ack(&mut self, id: String, shard: String) -> Result<(), GenericError>;
    async fn send(&self, cmd: QueueCommand) -> Result<(), GenericError>;
}

pub type GenericSubscriber = Box<dyn Subscriber + Send + Sync + 'static>;
pub type GenericQueue = Box<dyn Queue + Send + Sync + 'static>;

#[derive(Debug, Clone)]
pub struct InMemoryQueue {
    pub name: String,
    tx: mpsc::Sender<QueueCommand>,
}

#[derive(Debug)]
struct InMemorySubscriber {
    name: String,
    consumer_id: Uuid,
    tx: mpsc::Sender<QueueCommand>,
}

#[tonic::async_trait]
impl Subscriber for InMemorySubscriber {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    async fn fetch(&mut self, deadline: SystemTime) -> Result<(), GenericError> {
        Ok(self
            .tx
            .send(QueueCommand::FetchMsg(self.consumer_id, deadline))
            .await?)
    }

    async fn ack(&mut self, id: String, shard: String) -> Result<(), GenericError> {
        Ok(self
            .tx
            .send(QueueCommand::MsgAck(id, shard, self.consumer_id))
            .await?)
    }

    async fn send(&self, cmd: QueueCommand) -> Result<(), GenericError> {
        self.tx.send(cmd).await?;
        Ok(())
    }
}

#[tonic::async_trait]
impl Queue for InMemoryQueue {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn generic_clone(&self) -> GenericQueue {
        Box::new(self.clone())
    }

    #[tracing::instrument(name = "InMemoryQueue.publish", parent = &msg.span, skip_all, level = "debug")]
    async fn publish(&self, msg: Arc<Envelop>) -> Result<(), PublishMessageError> {
        Ok(self
            .tx
            .send(QueueCommand::Msg(msg))
            .await
            .map_err(|_| PublishMessageError)?)
    }

    async fn subscribe(
        &self,
        consumer: GenericConsumer,
    ) -> Result<GenericSubscriber, GenericError> {
        let result = InMemorySubscriber {
            name: self.name.clone(),
            consumer_id: consumer.get_id(),
            tx: self.tx.clone(),
        };
        let cmd = QueueCommand::ConsumeStart(consumer);
        result.send(cmd).await?;
        Ok(Box::new(result))
    }
}

impl InMemoryQueue {
    fn new(cfg: config::InMemoryQueue, task_tracker: TaskTracker) -> Self {
        let (tx, rx) = mpsc::channel(99);
        let name = cfg.name.clone();
        task_tracker.spawn(Self::processing(cfg, rx, task_tracker.clone()));
        Self { name, tx }
    }

    pub(crate) fn new_generic(cfg: config::InMemoryQueue, task_tracker: TaskTracker) -> GenericQueue {
        Box::new(Self::new(cfg, task_tracker))
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
                        QueueCommand::FetchMsg(consumer_id, deadline) => {
                            if let Some(consumer) = consumers.get_mut(&consumer_id) {
                                consumer.set_deadline(deadline);
                                waiters.push_back(consumer_id);
                            }
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
                            if consumer.get_prefetch_count() > 0 {
                                waiters.push_back(id);
                            }
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
                        Ok(ConsumerSendResult::FetchDone) => m_sent.inc(),
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
                            let span = tracing::span!(parent: &msg.span, tracing::Level::INFO, "queue.task_send");
                            let _ = span.enter();
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
    pub async fn from(config: Config, task_tracker: TaskTracker) -> Result<Self, GenericError> {
        #[cfg(feature = "redis")]
        let mut redis_connectors = crate::redis::Connectors::new(config.redis);

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
                #[cfg(feature = "redis")]
                config::Queue::RedisStream(cfg_queue) => {
                    let connector = redis_connectors
                        .get_connection(&cfg_queue.connector)
                        .await?;
                    let q = crate::redis::stream::RedisStreamQueue::new_generic(
                        cfg_queue.clone(),
                        task_tracker.clone(),
                        connector,
                    );
                    for topic in cfg_queue.topics {
                        let sub = subscriptions.entry(topic).or_insert_with(Subscription::new);
                        sub.subscribe(q.clone());
                    }
                    queues.insert(cfg_queue.name.clone(), q);
                }
            };
        }
        log::debug!("Created {:?}", &subscriptions);
        Ok(Self {
            subscriptions,
            queues,
            task_tracker,
        })
    }
}
