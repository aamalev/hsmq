use std::{
    collections::{BinaryHeap, HashMap, VecDeque},
    fmt::Display,
    sync::Arc,
    time::Duration,
};

use prometheus::core::{AtomicF64, GenericCounter};
use redis::aio::ConnectionLike;
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::{
    config::{RedisStreamConfig, Shard},
    errors::GenericError,
    metrics, pb,
    server::{
        self, ConsumerSendResult, GenericConsumer, GenericQueue, QueueCommand, Response, UnAck,
    },
};

use super::create_client;

fn cmd_to_string(cmd: redis::Cmd) -> String {
    let s = cmd.args_iter().map(|a| match a {
        redis::Arg::Simple(a) => String::from_utf8(a.to_vec()).unwrap_or_default(),
        _ => String::default(),
    });
    let v: Vec<_> = s.collect();
    v.join(" ")
}

#[derive(Clone)]
struct RedisStream {
    order_id: String,
    connection: redis::cluster_async::ClusterConnection,
    cfg: RedisStreamConfig,
    name: String,
    fail: usize,
    m_sleep: GenericCounter<AtomicF64>,
}

impl PartialEq for RedisStream {
    fn eq(&self, other: &Self) -> bool {
        self.order_id == other.order_id
    }
}

impl Eq for RedisStream {}

impl PartialOrd for RedisStream {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.order_id
            .partial_cmp(&other.order_id)
            .map(|o| o.reverse())
    }
}

impl Ord for RedisStream {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_id.cmp(&other.order_id).reverse()
    }
}

impl Display for RedisStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "<RedisStream {} group={}>",
            &self.name, &self.cfg.group
        ))
    }
}

impl RedisStream {
    fn new(
        connection: redis::cluster_async::ClusterConnection,
        cfg: RedisStreamConfig,
        name: String,
    ) -> Self {
        let m_sleep = metrics::REDIS_COUNTER.with_label_values(&[&name, "stream-sleep"]);
        Self {
            order_id: String::default(),
            connection,
            cfg,
            name,
            fail: 0,
            m_sleep,
        }
    }

    async fn execute<T>(&mut self, cmd: redis::Cmd) -> redis::RedisResult<T>
    where
        T: redis::FromRedisValue,
    {
        let result = self.connection.req_packed_command(&cmd).await?;
        redis::FromRedisValue::from_redis_value(&result)
    }

    async fn insert(mut self, msg: Arc<server::Envelop>) -> RedisResult {
        let mut cmd = redis::cmd("XADD").arg(&self.name).to_owned();
        if self.cfg.nomkstream {
            cmd.arg(b"NOMKSTREAM");
        }
        if let Some(maxlen) = self.cfg.maxlen {
            cmd.arg(b"MAXLEN").arg(b"~").arg(maxlen);
            if let Some(limit) = self.cfg.limit {
                cmd.arg(b"LIMIT").arg(limit);
            }
        }
        cmd.arg(b"*");
        for (k, v) in msg.message.headers.iter() {
            cmd.arg(k).arg(v);
        }
        cmd.arg(b"topic").arg(&msg.message.topic);
        if !msg.message.key.is_empty() {
            cmd.arg(b"key").arg(&msg.message.key);
        }
        if let Some(ref data) = msg.message.data {
            cmd.arg(b"body").arg(&data.value);
            cmd.arg(b"content-type").arg(&data.type_url);
        }
        match self.execute::<String>(cmd).await {
            Ok(msg_id) => {
                self.order_id = msg_id.clone();
                RedisResult::Inserted {
                    stream: self,
                    msg_id,
                }
            }
            Err(error) => RedisResult::ErrorInsert {
                stream: self,
                error,
            },
        }
    }

    async fn fetch(mut self, noack: bool, consumer_id: Uuid) -> RedisResult {
        let mut cmd = redis::cmd("XREADGROUP")
            .arg(b"GROUP")
            .arg(&self.cfg.group)
            .arg(consumer_id.to_string())
            .arg(b"COUNT")
            .arg(b"1")
            .to_owned();
        if noack {
            cmd.arg(b"NOACK");
        }
        cmd.arg(b"STREAMS").arg(&self.name).arg(b">");
        match self.connection.req_packed_command(&cmd).await {
            Ok(redis::Value::Bulk(mut b)) => {
                let mut msg_id = String::default();
                let mut msg = pb::Message::default();
                if let Some(redis::Value::Bulk(mut b)) = b.pop() {
                    if let Some(redis::Value::Bulk(mut b)) = b.pop() {
                        if let Some(redis::Value::Bulk(mut b)) = b.pop() {
                            if let Some(m) = b.pop() {
                                let mut headers: HashMap<String, Vec<u8>> =
                                    redis::FromRedisValue::from_redis_value(&m).unwrap_or_default();
                                if let Some(topic) = headers.remove("topic") {
                                    msg.topic = String::from_utf8(topic).unwrap_or_default();
                                }
                                if let Some(key) = headers.remove("key") {
                                    msg.key = String::from_utf8(key).unwrap_or_default();
                                }
                                let mut data = prost_types::Any::default();
                                if let Some(ct) = headers.remove("content-type") {
                                    data.type_url = String::from_utf8(ct).unwrap_or_default();
                                }
                                if let Some(body) = headers.remove("body") {
                                    data.value = body;
                                }
                                msg.data = Some(data);
                                for (k, v) in headers.into_iter() {
                                    msg.headers
                                        .insert(k, String::from_utf8(v).unwrap_or_default());
                                }
                            }
                            if let Some(redis::Value::Data(id)) = b.pop() {
                                msg_id = String::from_utf8(id).unwrap_or_default();
                                self.order_id = msg_id.clone();
                                self.fail = 0;
                            }
                        }
                    }
                }
                RedisResult::Message {
                    stream: self,
                    msg,
                    msg_id,
                    consumer_id,
                }
            }
            Ok(redis::Value::Nil) => {
                self.fail += 1;
                RedisResult::NoMessage {
                    stream: self,
                    consumer_id,
                }
            }
            Ok(other) => RedisResult::Unexpected {
                stream: self,
                value: other,
                consumer_id,
            },
            Err(error) => RedisResult::ErrorConsumer {
                stream: self,
                error,
                consumer_id,
            },
        }
    }

    async fn wait(self) -> RedisResult {
        if self.fail > 3 {
            self.m_sleep.inc();
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        RedisResult::Ready(self)
    }

    async fn ack(mut self, stream: String, id: String, consumer_id: Uuid) -> RedisResult {
        let cmd = redis::cmd("XACK")
            .arg(&stream)
            .arg(&self.cfg.group)
            .arg(&id)
            .to_owned();
        log::debug!("Ack with cmd: {}", cmd_to_string(cmd.clone()));
        match self.execute(cmd).await {
            Ok(true | false) => RedisResult::Acked {
                stream: self,
                msg_id: id,
                consumer_id,
            },
            Err(error) => RedisResult::ErrorAcker {
                stream: self,
                error,
                msg_id: id,
                consumer_id,
            },
        }
    }
}

enum RedisResult {
    Inserted {
        stream: RedisStream,
        msg_id: String,
    },
    ErrorInsert {
        stream: RedisStream,
        error: redis::RedisError,
    },
    Message {
        stream: RedisStream,
        msg: pb::Message,
        msg_id: String,
        consumer_id: Uuid,
    },
    StreamNotFound(RedisStream),
    NoMessage {
        stream: RedisStream,
        consumer_id: Uuid,
    },
    Ready(RedisStream),
    Unexpected {
        stream: RedisStream,
        value: redis::Value,
        consumer_id: Uuid,
    },
    ErrorConsumer {
        stream: RedisStream,
        error: redis::RedisError,
        consumer_id: Uuid,
    },
    Acked {
        stream: RedisStream,
        msg_id: String,
        consumer_id: Uuid,
    },
    ErrorAcker {
        stream: RedisStream,
        error: redis::RedisError,
        msg_id: String,
        consumer_id: Uuid,
    },
}

#[derive(Debug)]
pub struct RedisStreamQueue {
    pub name: String,
    tx: mpsc::Sender<QueueCommand>,
}

#[tonic::async_trait]
impl server::Queue for RedisStreamQueue {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    async fn send(&self, cmd: QueueCommand) -> Result<(), GenericError> {
        self.tx.send(cmd).await?;
        Ok(())
    }
}

impl RedisStreamQueue {
    fn new(cfg: RedisStreamConfig, task_tracker: TaskTracker) -> Self {
        let (tx, rx) = mpsc::channel(99);
        let name = cfg.name.clone();
        task_tracker.spawn(Self::processing(cfg, rx, task_tracker.clone()));
        Self { name, tx }
    }

    pub fn new_generic(cfg: RedisStreamConfig, task_tracker: TaskTracker) -> GenericQueue {
        Arc::new(Box::new(Self::new(cfg, task_tracker)))
    }

    async fn processing(
        cfg: RedisStreamConfig,
        mut rx: mpsc::Receiver<QueueCommand>,
        task_tracker: TaskTracker,
    ) {
        let client = match create_client(&cfg.nodes, &cfg.username, cfg.password.clone()) {
            Ok(c) => c,
            _ => {
                task_tracker.close();
                return;
            }
        };
        let connection = client
            .get_async_connection()
            .await
            .inspect_err(|e| log::error!("Error connection {}", e))
            .unwrap();
        let name = cfg.name.clone();
        let acker = RedisStream::new(connection.clone(), cfg.clone(), String::default());
        let mut writers = VecDeque::new();
        let mut readers = BinaryHeap::new();
        for shard in cfg.shards.iter() {
            match shard {
                Shard::String(s) => {
                    let stream = RedisStream::new(connection.clone(), cfg.clone(), s.clone());
                    writers.push_back(stream.clone());
                    readers.push(stream);
                }
            };
        }
        let mut tasks = JoinSet::new();
        let mut commands = JoinSet::new();
        let mut consumers: HashMap<Uuid, GenericConsumer> = HashMap::new();
        let mut waiters = VecDeque::new();
        let mut messages = VecDeque::new();
        let mut unack = UnAck::new(name.clone(), cfg.ack_timeout.clone().into());
        let m_received = metrics::QUEUE_COUNTER.with_label_values(&[&name, "received"]);
        let m_sent = metrics::QUEUE_COUNTER.with_label_values(&[&name, "sent"]);
        let m_requeue = metrics::QUEUE_COUNTER.with_label_values(&[&name, "requeue"]);
        // let m_ack_timeout = metrics::QUEUE_COUNTER.with_label_values(&[&name, "ack-timeout"]);
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
                            if let Some(stream) = writers.pop_front() {
                                commands.spawn(stream.clone().insert(msg));
                                writers.push_back(stream);
                            }
                        }
                        QueueCommand::MsgAck(msg_id, shard_id, consumer_id) => {
                            log::debug!("Received ack {:?}", &msg_id);
                            commands.spawn(acker.clone().ack(shard_id, msg_id, consumer_id));
                        }
                        QueueCommand::Requeue(msg) => {
                            m_requeue.inc();
                            messages.push_front(msg);
                            m_messages.set(messages.len() as f64);
                        }
                        QueueCommand::ConsumeStart(consumer) => {
                            let id = consumer.get_id();
                            log::error!("ConsumerStart {}", &id);
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
                Some(res) = commands.join_next() => match res {
                    Ok(rr) => Self::redis_result(
                        rr,
                        &mut readers,
                        &mut writers,
                        &mut tasks,
                        &mut consumers,
                        &mut unack,
                        &mut waiters,
                        &mut commands,
                    ),
                    Err(e) => log::error!("Commands waiter error {:?}", e),
                },
                Some(res) = tasks.join_next() => {
                    match res {
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
                        Ok(r) => log::error!("Not impl for {:?}", r),
                        Err(e) => {
                            log::error!("Error in task queue {}", e);
                        }
                    };
                }
            }
            // log::error!("Readers {} waiters {}", readers.len(), waiters.len());
            if let Some(consumer_id) = waiters.pop_front() {
                if let Some(consumer) = consumers.get_mut(&consumer_id) {
                    let noack = !consumer.is_ackable();
                    if let Some(stream) = readers.pop() {
                        commands.spawn(stream.fetch(noack, consumer_id));
                    } else {
                        waiters.push_front(consumer_id);
                    }
                }
            }
        }
    }

    fn redis_result(
        rr: RedisResult,
        readers: &mut BinaryHeap<RedisStream>,
        writers: &mut VecDeque<RedisStream>,
        tasks: &mut JoinSet<ConsumerSendResult>,
        consumers: &mut HashMap<Uuid, GenericConsumer>,
        unack: &mut UnAck,
        waiters: &mut VecDeque<Uuid>,
        commands: &mut JoinSet<RedisResult>,
    ) {
        match rr {
            RedisResult::Inserted {
                stream: _,
                msg_id: _,
            } => {}
            RedisResult::ErrorInsert {
                stream: _,
                error: _,
            } => {}
            RedisResult::Message {
                stream,
                msg,
                msg_id,
                consumer_id,
            } => {
                let shard = stream.name.clone();
                readers.push(stream);
                let msg = Arc::new(server::Envelop {
                    message: msg,
                    id: msg_id,
                    shard,
                });
                waiters.push_front(consumer_id);
                while let Some(consumer_id) = waiters.pop_front() {
                    if let Some(consumer) = consumers.get_mut(&consumer_id) {
                        if let Some(msg) = consumer.send(msg.clone(), tasks, unack) {
                            return;
                        }
                    }
                }
            }
            RedisResult::NoMessage {
                stream,
                consumer_id,
            } => {
                log::debug!("NoMessages {}", &stream.name);
                commands.spawn(stream.wait());
                waiters.push_front(consumer_id);
            }
            RedisResult::StreamNotFound(stream) => todo!(),
            RedisResult::Ready(stream) => {
                readers.push(stream);
            }
            RedisResult::Unexpected {
                stream: _,
                value: _,
                consumer_id: _,
            } => todo!(),
            RedisResult::ErrorConsumer {
                stream: _,
                error: _,
                consumer_id: _,
            } => todo!(),
            RedisResult::Acked {
                stream: _,
                msg_id,
                consumer_id,
            } => {
                if let Some(consumer) = consumers.get_mut(&consumer_id) {
                    consumer.ack(msg_id, unack);
                } else {
                    unack.remove(&msg_id, false);
                }
            }
            RedisResult::ErrorAcker {
                stream: _,
                error: _,
                msg_id: _,
                consumer_id: _,
            } => todo!(),
        }
    }
}
