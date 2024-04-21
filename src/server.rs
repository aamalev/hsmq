use crate::config::{self, Config};
use crate::metrics;
use crate::pb::Message;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_util::task::task_tracker::TaskTracker;
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
    Message(QueueMessage),
    MessageAck(String),
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
            qtx.send(QueueCommand::Msg(msg.clone())).await.unwrap();
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
pub struct QueueMessage {
    message: Arc<Envelop>,
    queue: mpsc::Sender<QueueCommand>,
}

impl QueueMessage {
    pub async fn requeue(&self) {
        let m = QueueCommand::Requeue(self.message.clone());
        self.queue.send(m).await.unwrap();
    }

    pub fn message_clone(&self) -> Message {
        self.message.message.clone()
    }

    pub async fn stop_consume(&self, consumer: Consumer) {
        let cmd = QueueCommand::ConsumeStop(consumer);
        self.queue.send(cmd).await.unwrap();
    }
}

#[derive(Clone)]
pub struct Consumer {
    id: Uuid,
    q: mpsc::Sender<Response>,
}

impl Consumer {
    pub fn new(q: mpsc::Sender<Response>) -> Self {
        Self {
            id: Uuid::now_v7(),
            q,
        }
    }
}

pub enum QueueCommand {
    Msg(Arc<Envelop>),
    Requeue(Arc<Envelop>),
    ConsumeStart(Consumer),
    ConsumeStop(Consumer),
    GracefulShutdown,
}

#[derive(Clone)]
pub struct Queue {
    pub tx: mpsc::Sender<QueueCommand>,
}

impl Queue {
    fn new(cfg: config::Queue, task_tracker: TaskTracker) -> Self {
        let (tx, rx) = mpsc::channel(99);
        task_tracker.spawn(Self::processing(cfg, tx.clone(), rx, task_tracker.clone()));
        Self { tx }
    }

    async fn processing(
        cfg: config::Queue,
        qtx: mpsc::Sender<QueueCommand>,
        mut rx: mpsc::Receiver<QueueCommand>,
        task_tracker: TaskTracker,
    ) {
        let name = cfg.name.clone();
        let mut tasks = JoinSet::new();
        let mut consumers = HashSet::new();
        let mut waiters = VecDeque::new();
        let mut messages = VecDeque::new();
        let m_received = metrics::QUEUE_COUNTER.with_label_values(&[&name, "received"]);
        let m_sent = metrics::QUEUE_COUNTER.with_label_values(&[&name, "sent"]);
        let m_requeue = metrics::QUEUE_COUNTER.with_label_values(&[&name, "requeue"]);
        let m_drop_limit = metrics::QUEUE_COUNTER.with_label_values(&[&name, "drop-limit"]);
        let m_consumers = metrics::QUEUE_GAUGE.with_label_values(&[&name, "consumers"]);
        let m_messages = metrics::QUEUE_GAUGE.with_label_values(&[&name, "messages"]);
        loop {
            if tasks.is_empty() {
                tasks.spawn(async {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    QueueCommand::GracefulShutdown
                });
            }
            tokio::select! {
                Some(cmd) = rx.recv() => {
                    match cmd {
                        QueueCommand::Msg(msg) => {
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
                        QueueCommand::Requeue(msg) => {
                            m_requeue.inc();
                            messages.push_front(msg);
                            m_messages.set(messages.len() as f64);
                        }
                        QueueCommand::ConsumeStart(consumer) => {
                            consumers.insert(consumer.id);
                            m_consumers.inc();
                            waiters.push_back(consumer);
                        }
                        QueueCommand::ConsumeStop(consumer) => {
                            m_consumers.dec();
                            consumers.remove(&consumer.id);
                            let _ = consumer.q.send(Response::StopConsume(name.clone())).await.is_ok();
                        }
                        QueueCommand::GracefulShutdown => log::error!("unreachable"),
                    };
                }
                Some(Ok(res)) = tasks.join_next() => {
                    match res {
                        QueueCommand::Requeue(msg) => {
                            m_requeue.inc();
                            messages.push_front(msg);
                        }
                        QueueCommand::ConsumeStart(consumer) => {
                            m_sent.inc();
                            if consumer.q.send(Response::StartConsume(name.clone())).await.is_ok() {
                                waiters.push_back(consumer);
                            };
                        }
                        QueueCommand::GracefulShutdown => {
                            if task_tracker.is_closed() {
                                if messages.is_empty() {
                                    for w in waiters.iter() {
                                        consumers.remove(&w.id);
                                        let _ = w.q.send(Response::GracefulShutdown(name.clone())).await;
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
                        _ => log::error!("unreachable"),
                    };
                }
            }
            while !messages.is_empty() && !waiters.is_empty() {
                if let Some(consumer) = waiters.pop_front() {
                    if consumers.contains(&consumer.id) {
                        if let Some(msg) = messages.pop_front() {
                            m_messages.set(messages.len() as f64);
                            let qtx = qtx.clone();
                            tasks.spawn(async move {
                                let qmsg = QueueMessage {
                                    message: msg.clone(),
                                    queue: qtx,
                                };
                                let resp = Response::Message(qmsg);
                                match consumer.q.send(resp).await {
                                    Ok(_) => QueueCommand::ConsumeStart(consumer),
                                    Err(_) => QueueCommand::Requeue(msg),
                                }
                            });
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
