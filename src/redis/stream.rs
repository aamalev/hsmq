use opentelemetry::trace::FutureExt;
use prometheus::core::{AtomicF64, GenericCounter, GenericGauge};
use rand::{seq::SliceRandom, Rng};
use redis::{aio::ConnectionLike, RedisResult};
use std::{
    collections::{BinaryHeap, HashMap, VecDeque},
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_stream::StreamExt;
use tokio_util::{task::TaskTracker, time::DelayQueue};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::{
    config::{RedisStreamConfig, RedisStreamGroupCleanConfig, RedisStreamGroupConfig, Stream},
    errors::{GenericError, PublishMessageError},
    metrics, pb,
    server::{
        self, ConsumerSendResult, Envelop, GenericConsumer, GenericQueue, GenericSubscriber,
        QueueCommand,
    },
    utils,
};

use super::RedisConnection;

#[allow(dead_code)]
fn cmd_to_string(cmd: redis::Cmd) -> String {
    let s = cmd.args_iter().map(|a| match a {
        redis::Arg::Simple(a) => String::from_utf8(a.to_vec()).unwrap_or_default(),
        _ => String::default(),
    });
    let v: Vec<_> = s.collect();
    v.join(" ")
}

#[allow(dead_code)]
#[derive(Default, Debug)]
struct RedisConsumer {
    name: String,
    pending: u64,
    idle: u64,
    inactive: Option<u64>,
}

impl redis::FromRedisValue for RedisConsumer {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let c: HashMap<String, redis::Value> = redis::FromRedisValue::from_redis_value(v)?;

        fn r<T>(m: &HashMap<String, redis::Value>, field: &str) -> redis::RedisResult<T>
        where
            T: redis::FromRedisValue + Default,
        {
            let result = if let Some(v) = m.get(field) {
                redis::FromRedisValue::from_redis_value(v)?
            } else {
                Default::default()
            };
            redis::RedisResult::Ok(result)
        }

        redis::RedisResult::Ok(RedisConsumer {
            name: r(&c, "name")?,
            idle: r(&c, "idle")?,
            pending: r(&c, "pending")?,
            inactive: r(&c, "inactive")?,
        })
    }
}

#[derive(Clone)]
struct RedisStream<T> {
    order_id: T,
    connection: crate::redis::RedisConnection,
    cfg: RedisStreamConfig,
    name: String,
    group: String,
    fail: Arc<AtomicU64>,
    m_sleep: GenericCounter<AtomicF64>,
    m_xadd: GenericCounter<AtomicF64>,
    m_xread: GenericCounter<AtomicF64>,
    m_xread_nil: GenericCounter<AtomicF64>,
    m_xread_err: GenericCounter<AtomicF64>,
    m_xack: GenericCounter<AtomicF64>,
}

impl<T> Debug for RedisStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStream")
            // .field("order_id", &self.order_id)
            .field("name", &self.name)
            .field("fail", &self.fail)
            .finish()
    }
}

impl PartialEq for RedisStream<Arc<AtomicU64>> {
    fn eq(&self, other: &Self) -> bool {
        self.order_id.load(Ordering::Relaxed) == other.order_id.load(Ordering::Relaxed)
    }
}

impl Eq for RedisStream<Arc<AtomicU64>> {}

impl PartialOrd for RedisStream<Arc<AtomicU64>> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RedisStream<Arc<AtomicU64>> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_id
            .load(Ordering::Relaxed)
            .cmp(&other.order_id.load(Ordering::Relaxed))
            .reverse()
    }
}

impl PartialEq for RedisStream<String> {
    fn eq(&self, other: &Self) -> bool {
        self.order_id == other.order_id
    }
}

impl Eq for RedisStream<String> {}

impl PartialOrd for RedisStream<String> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RedisStream<String> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_id.cmp(&other.order_id).reverse()
    }
}

impl<T> Display for RedisStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "<RedisStream {} group={}>",
            &self.name, &self.group,
        ))
    }
}

trait OrderId {
    fn set_order_u64(&mut self, ms: u64);
    fn set_order_str(&mut self, msg_id: &str);
}

impl OrderId for RedisStream<String> {
    fn set_order_u64(&mut self, ms: u64) {
        self.order_id = ms.to_string();
    }
    fn set_order_str(&mut self, msg_id: &str) {
        self.order_id = msg_id.to_string();
    }
}

impl OrderId for RedisStream<Arc<AtomicU64>> {
    fn set_order_u64(&mut self, ms: u64) {
        self.order_id.store(ms, Ordering::Relaxed);
    }
    fn set_order_str(&mut self, msg_id: &str) {
        if let Some(ts) = msg_id
            .split_once('-')
            .and_then(|(l, _)| l.parse::<u64>().ok())
        {
            self.set_order_u64(ts);
        }
    }
}

#[cfg(feature = "redis-stream-order-atomic")]
type RedisStreamR = RedisStream<Arc<AtomicU64>>;

#[cfg(not(feature = "redis-stream-order-atomic"))]
type RedisStreamR = RedisStream<String>;

impl RedisStreamR {
    fn new(
        connection: crate::redis::RedisConnection,
        cfg: RedisStreamConfig,
        name: String,
    ) -> Self {
        let m_sleep = metrics::REDIS_COUNTER.with_label_values(&[&name, "stream-sleep"]);
        let m_xadd = metrics::REDIS_COUNTER.with_label_values(&[&name, "XADD"]);
        let m_xread = metrics::REDIS_COUNTER.with_label_values(&[&name, "XREADGROUP"]);
        let m_xread_nil = metrics::REDIS_COUNTER.with_label_values(&[&name, "XREADGROUP-NIL"]);
        let m_xread_err = metrics::REDIS_COUNTER.with_label_values(&[&name, "XREADGROUP-ERR"]);
        let m_xack = metrics::REDIS_COUNTER.with_label_values(&[&name, "XACK"]);
        let queue = name.clone();
        let group = match cfg.group {
            RedisStreamGroupConfig::Name(ref name) => name.to_string(),
            RedisStreamGroupConfig::Group {
                ref name,
                init,
                ref clear,
            } => {
                if init {
                    tokio::spawn(Self::init_group(
                        connection.clone(),
                        queue.clone(),
                        name.to_string(),
                    ));
                };
                if let Some(RedisStreamGroupCleanConfig { every, max_idle }) = clear {
                    let group = name.to_string();
                    let every = std::time::Duration::from(every.clone());
                    let max_idle = std::time::Duration::from(max_idle.clone()).as_secs();
                    let mut c = connection.clone();
                    tokio::spawn(async move {
                        let sleep_start = rand::thread_rng().gen_range(0..every.as_secs());
                        tokio::time::sleep(std::time::Duration::from_secs(sleep_start)).await;
                        loop {
                            if let Err(e) =
                                Self::clear_consumers(&mut c, &queue, &group, max_idle).await
                            {
                                tracing::error!("Clear consumers with error {:?}", e);
                            }
                            tokio::time::sleep(every).await;
                        }
                    });
                };
                name.to_string()
            }
        };
        Self {
            order_id: Default::default(),
            connection,
            cfg,
            name,
            group,
            fail: Default::default(),
            m_sleep,
            m_xadd,
            m_xread,
            m_xread_nil,
            m_xread_err,
            m_xack,
        }
    }

    fn up(&mut self) {
        self.set_order_u64(utils::current_time().as_millis() as u64);
    }

    #[tracing::instrument(name = "stream.execute", skip_all, level = "trace")]
    async fn execute<R>(&mut self, cmd: redis::Cmd) -> redis::RedisResult<R>
    where
        R: redis::FromRedisValue,
    {
        let result = self.connection.req_packed_command(&cmd).await?;
        redis::FromRedisValue::from_redis_value(&result)
    }

    async fn init_group(mut connection: RedisConnection, name: String, group: String) {
        let cmd = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&name)
            .arg(&group)
            .arg("$")
            .arg("MKSTREAM")
            .to_owned();
        if let Err(e) = connection.req_packed_command(&cmd).await {
            log::debug!("Init group {} for {} error {:?}", group, name, e);
        }
    }

    async fn get_consumers(
        connection: &mut RedisConnection,
        name: &String,
        group: &String,
    ) -> RedisResult<Vec<RedisConsumer>> {
        let cmd = redis::cmd("XINFO")
            .arg("CONSUMERS")
            .arg(name)
            .arg(group)
            .to_owned();
        let result = connection.req_packed_command(&cmd).await?;
        redis::FromRedisValue::from_redis_value(&result)
    }

    async fn clear_consumers(
        connection: &mut RedisConnection,
        name: &String,
        group: &String,
        max_idle: u64,
    ) -> redis::RedisResult<()> {
        let cmd = redis::cmd("XGROUP")
            .arg("DELCONSUMER")
            .arg(name)
            .arg(group)
            .to_owned();

        let consumers = Self::get_consumers(connection, name, group).await?;
        let idles: Vec<_> = consumers
            .into_iter()
            .filter(|rc| rc.idle > max_idle)
            .collect();

        for rc in idles.iter() {
            tracing::trace!("Delete idle consumer {:?}", rc);
            let cmd = cmd.clone().arg(&rc.name).to_owned();
            connection.req_packed_command(&cmd).await?;
        }
        if idles.is_empty() {
            tracing::info!("[{}] No idle consumers", name);
        } else {
            tracing::info!("[{}] Deleted {} idle consumers", name, idles.len());
        }
        Ok(())
    }

    #[tracing::instrument(
        name = "stream.insert",
        fields(msg_id = &msg.meta.id, stream = &self.name),
        skip_all,
        level = "trace",
    )]
    async fn insert(&mut self, msg: Arc<server::Envelop>) -> Result<String, redis::RedisError> {
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
            cmd.arg(&self.cfg.body_fieldname).arg(&data.value);
            cmd.arg(b"content-type").arg(&data.type_url);
        }
        self.m_xadd.inc();
        let msg_id = self.execute::<String>(cmd).await?;
        self.set_order_str(&msg_id);
        Ok(msg_id)
    }

    #[tracing::instrument(name = "stream.fetch", fields(msg_id, stream = &self.name), skip_all, level = "trace")]
    async fn fetch(
        &mut self,
        noack: bool,
        consumer_id: String,
    ) -> Result<Envelop, redis::RedisError> {
        let mut cmd = redis::cmd("XREADGROUP")
            .arg(b"GROUP")
            .arg(&self.group)
            .arg(consumer_id.to_string())
            .arg(b"COUNT")
            .arg(b"1")
            .to_owned();
        if noack {
            cmd.arg(b"NOACK");
        }
        cmd.arg(b"STREAMS").arg(&self.name).arg(b">");
        self.m_xread.inc();
        let span = tracing::trace_span!("redis::XREADGROUP");
        match self
            .connection
            .req_packed_command(&cmd)
            .instrument(span)
            .await
            .inspect_err(|_| self.m_xread_err.inc())?
        {
            redis::Value::Bulk(mut b) => {
                let mut msg_id = String::default();
                let mut msg = pb::Message::default();
                if let Some(redis::Value::Bulk(mut b)) = b.pop() {
                    if let Some(redis::Value::Bulk(mut b)) = b.pop() {
                        if let Some(redis::Value::Bulk(mut b)) = b.pop() {
                            let span = tracing::Span::current();
                            if let Some(m) = b.pop() {
                                let mut headers: HashMap<String, Vec<u8>> =
                                    redis::FromRedisValue::from_redis_value(&m).unwrap_or_default();
                                if let Some(topic) = headers.remove("topic") {
                                    msg.topic = String::from_utf8_lossy(&topic).to_string();
                                }
                                if let Some(key) = headers.remove("key") {
                                    msg.key = String::from_utf8_lossy(&key).to_string();
                                }
                                let mut data = prost_types::Any::default();
                                if let Some(ct) = headers.remove("content-type") {
                                    data.type_url = String::from_utf8_lossy(&ct).to_string();
                                }
                                if let Some(body) = headers.remove(&self.cfg.body_fieldname) {
                                    data.value = body;
                                }
                                msg.data = Some(data);
                                for (k, v) in headers.into_iter() {
                                    msg.headers
                                        .insert(k, String::from_utf8_lossy(&v).to_string());
                                }
                                self.fail.store(0, Ordering::Relaxed);
                            }
                            if let Some(redis::Value::Data(id)) = b.pop() {
                                msg_id = String::from_utf8_lossy(&id).to_string();
                                self.set_order_str(&msg_id);
                                span.record("msg_id", &msg_id);
                            }
                        }
                    }
                }
                let parent_cx = opentelemetry::global::get_text_map_propagator(|prop| {
                    prop.extract(&msg.headers)
                });
                let mut envelop = Envelop::new(msg);
                envelop.span = tracing::trace_span!(parent: None, "message");
                envelop.span.set_parent(parent_cx);
                envelop.meta.id = msg_id;
                envelop.meta.shard.clone_from(&self.name);
                envelop.meta.queue.clone_from(&self.cfg.name);
                Ok(envelop)
            }
            redis::Value::Nil => {
                self.m_xread_nil.inc();
                self.fail.fetch_add(1, Ordering::Relaxed);
                Err(redis::RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Empty",
                )))
            }
            other => {
                self.m_xread_err.inc();
                self.fail.fetch_add(10, Ordering::Relaxed);
                Err(redis::RedisError::from((
                    redis::ErrorKind::ParseError,
                    "Unexpected value",
                    format!("{other:?}"),
                )))
            }
        }
    }

    #[tracing::instrument(
        name = "stream.ack",
        fields(msg_id = id, stream = &self.name),
        skip_all,
        level = "trace",
    )]
    async fn ack(
        &mut self,
        stream: String,
        id: String,
        _consumer_id: Uuid,
    ) -> Result<bool, redis::RedisError> {
        let cmd = redis::cmd("XACK")
            .arg(&stream)
            .arg(&self.group)
            .arg(&id)
            .to_owned();
        self.m_xack.inc();
        self.execute(cmd).await
    }
}

#[derive(Debug)]
struct SubscriberRedisStream {
    name: String,
    consumer: GenericConsumer,
    consumer_id: String,
    delay: tokio_util::time::DelayQueue<RedisStreamR>,
    fetchers: BinaryHeap<RedisStreamR>,
    streams: HashMap<String, RedisStreamR>,
    m_sent: GenericCounter<AtomicF64>,
    m_consumers: GenericGauge<AtomicF64>,
    tx_fetch: mpsc::Sender<FetchCommand>,
    read_limit: usize,
}

impl Drop for SubscriberRedisStream {
    fn drop(&mut self) {
        self.m_consumers.dec();
    }
}

impl SubscriberRedisStream {
    fn new(q: &RedisStreamQueue, consumer: GenericConsumer) -> Self {
        let mut streams = HashMap::new();
        let mut fetchers = BinaryHeap::new();
        let delay = tokio_util::time::DelayQueue::new();
        let name = q.name.clone();
        for stream in q.streams.iter() {
            streams.insert(stream.name.clone(), stream.clone());
            fetchers.push(stream.clone());
        }
        q.m_consumers.inc();
        Self {
            name,
            consumer,
            consumer_id: q.consumer_id.clone(),
            delay,
            fetchers,
            streams,
            m_sent: q.m_sent.clone(),
            m_consumers: q.m_consumers.clone(),
            tx_fetch: q.tx_fetch.clone(),
            read_limit: q.read_limit,
        }
    }

    async fn fetch_msg(
        &mut self,
        noack: bool,
        root_span: tracing::Span,
    ) -> Result<(), GenericError> {
        if self.consumer.is_dead() {
            self.consumer.send_timeout(self.name.clone()).await?;
            return Ok(());
        }
        for _ in 0..self.read_limit {
            let stream = if let s @ Some(_) = self.fetchers.pop() {
                s
            } else {
                self.delay
                    .next()
                    .instrument(tracing::trace_span!("subscriber.wait_stream"))
                    .await
                    .map(|s| s.into_inner())
            };
            if let Some(mut stream) = stream {
                let result = stream.fetch(noack, self.consumer_id.clone()).await;
                match result {
                    Ok(msg) => {
                        let cx = msg.span.context();
                        root_span.set_parent(cx.clone());
                        self.fetchers.push(stream);
                        self.consumer
                            .send_message(Arc::new(msg))
                            .with_context(cx)
                            .await?;
                        self.m_sent.inc();
                        return Ok(());
                    }
                    Err(_) => {
                        stream.up();
                        if self.consumer.is_dead() {
                            self.consumer.send_timeout(self.name.clone()).await?;
                            return Ok(());
                        }
                        if stream.fail.load(Ordering::Relaxed) < 20 {
                            self.fetchers.push(stream);
                        } else {
                            let timeout = if stream.fail.load(Ordering::Relaxed) < 50 {
                                Duration::from_millis(10)
                            } else {
                                Duration::from_millis(20)
                            };
                            stream.m_sleep.inc();
                            self.delay.insert(stream, timeout);
                        }
                    }
                }
            }
        }
        let consumer = self.consumer.generic_clone();
        Ok(self.tx_fetch.send(FetchCommand::Fetch(consumer)).await?)
    }
}

#[tonic::async_trait]
impl server::Subscriber for SubscriberRedisStream {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    #[tracing::instrument(name = "subscriber.fetch", skip_all, level = "trace")]
    async fn fetch(&mut self, deadline: SystemTime) -> Result<(), GenericError> {
        self.consumer.set_deadline(deadline);
        self.fetch_msg(true, tracing::Span::current()).await
    }

    #[tracing::instrument(name = "subscriber.ack", skip_all, level = "trace")]
    async fn ack(&mut self, id: String, shard: String) -> Result<(), GenericError> {
        if let Some(s) = self.streams.get_mut(&shard) {
            s.ack(shard, id, self.consumer.get_id()).await?;
        }
        let root = tracing::trace_span!(parent: None, "subscriber.fetch");
        self.fetch_msg(!self.consumer.is_ackable(), root).await
    }

    async fn send(&self, _: QueueCommand) -> Result<(), GenericError> {
        Ok(())
    }
}

#[allow(clippy::large_enum_variant)]
enum FetchCommand {
    Fetch(GenericConsumer),
    Consumer(GenericConsumer),
    ErrStreamConsumer(RedisStreamR, GenericConsumer),
    ValueStreamConsumer(Envelop, RedisStreamR, GenericConsumer),
    GracefulShutdown,
}

#[derive(Debug, Clone)]
pub struct RedisStreamQueue {
    pub name: String,
    tx_pub: mpsc::Sender<Arc<Envelop>>,
    tx_fetch: mpsc::Sender<FetchCommand>,
    streams: Vec<RedisStreamR>,
    consumer_id: String,
    m_received: GenericCounter<AtomicF64>,
    m_sent: GenericCounter<AtomicF64>,
    m_consumers: GenericGauge<AtomicF64>,
    read_limit: usize,
}

#[tonic::async_trait]
impl server::Queue for RedisStreamQueue {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn generic_clone(&self) -> GenericQueue {
        Box::new(self.clone())
    }

    #[tracing::instrument(
        fields(
            message.key = &msg.message.key,
        ),
        skip_all,
        level = "trace",
    )]
    async fn publish(&self, msg: Arc<server::Envelop>) -> Result<(), PublishMessageError> {
        let mut streams: Vec<_> = self.streams.iter().collect();
        streams.shuffle(&mut rand::thread_rng());
        for stream in streams {
            if let Err(e) = stream.clone().insert(msg.clone()).await {
                tracing::error!("Error while insert message to stream {}", e);
                continue;
            } else {
                self.m_received.inc();
                return Ok(());
            };
        }
        self.tx_pub.send(msg).await.map_err(|_| PublishMessageError)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    async fn subscribe(
        &self,
        consumer: GenericConsumer,
    ) -> Result<GenericSubscriber, GenericError> {
        let prefetch_count = consumer.get_prefetch_count();
        let noack = !consumer.is_ackable();
        let mut subscriber = SubscriberRedisStream::new(self, consumer);
        let root = tracing::Span::current();
        for _ in 0..prefetch_count {
            subscriber.fetch_msg(noack, root.clone()).await?;
        }
        Ok(Box::new(subscriber))
    }
}

impl RedisStreamQueue {
    fn new(
        cfg: RedisStreamConfig,
        task_tracker: TaskTracker,
        connection: crate::redis::RedisConnection,
    ) -> Self {
        let streams: Vec<_> = cfg
            .streams
            .iter()
            .map(|stream_cfg| match stream_cfg {
                Stream::String(s) => RedisStream::new(connection.clone(), cfg.clone(), s.clone()),
            })
            .collect();
        let (tx_pub, rx) = mpsc::channel(99);
        let name = cfg.name.clone();
        let m_received = metrics::QUEUE_COUNTER.with_label_values(&[&name, "received"]);
        task_tracker.spawn(Self::publishing(
            rx,
            task_tracker.clone(),
            streams.iter().cloned().collect(),
            m_received.clone(),
        ));

        let (tx_fetch, rx) = mpsc::channel(99);
        let read_limit = cfg.read_limit.unwrap_or(streams.len() / 2);
        let name = cfg.name.clone();
        let m_consumers = metrics::QUEUE_GAUGE.with_label_values(&[&name, "consumers"]);
        let m_sent = metrics::QUEUE_COUNTER.with_label_values(&[&name, "sent"]);
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        tokio::spawn(Self::fetching(
            name.clone(),
            rx,
            streams.iter().cloned().collect(),
            task_tracker,
            m_sent.clone(),
            hostname.clone(),
        ));
        Self {
            name,
            tx_pub,
            tx_fetch,
            streams,
            consumer_id: hostname,
            m_received,
            m_sent,
            m_consumers,
            read_limit,
        }
    }

    pub fn new_generic(
        cfg: RedisStreamConfig,
        task_tracker: TaskTracker,
        client: crate::redis::RedisConnection,
    ) -> GenericQueue {
        Box::new(Self::new(cfg, task_tracker, client))
    }

    async fn publishing(
        mut rx: mpsc::Receiver<Arc<Envelop>>,
        task_tracker: TaskTracker,
        mut writers: VecDeque<RedisStreamR>,
        m_received: GenericCounter<AtomicF64>,
    ) {
        let mut tasks = JoinSet::new();
        loop {
            if tasks.is_empty() {
                tasks.spawn(async {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    ConsumerSendResult::GracefulShutdown
                });
            }
            tokio::select! {
                Some(msg) = rx.recv() => {
                    log::debug!("Received msg {:?}", &msg);
                    m_received.inc();
                    if let Some(mut stream) = writers.pop_front() {
                        if let Err(e) = stream.insert(msg).await {
                            log::error!("Error insert {e}");
                        }
                        writers.push_back(stream);
                    }
                }
                Some(res) = tasks.join_next() => {
                    match res {
                        Ok(ConsumerSendResult::GracefulShutdown) => {
                            if task_tracker.is_closed() {
                                break;
                            }
                        }
                        Ok(r) => log::error!("Not impl for {:?}", r),
                        Err(e) => {
                            log::error!("Error in task queue {}", e);
                        }
                    };
                }
            }
        }
    }

    async fn fetching(
        name: String,
        mut rx: mpsc::Receiver<FetchCommand>,
        mut readers: BinaryHeap<RedisStreamR>,
        task_tracker: TaskTracker,
        m_sent: GenericCounter<AtomicF64>,
        queue_consumer_id: String,
    ) {
        let mut delay = DelayQueue::new();
        let mut tasks = JoinSet::new();
        let mut waiters = VecDeque::new();
        loop {
            if tasks.is_empty() {
                tasks.spawn(async {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    FetchCommand::GracefulShutdown
                });
            }
            let fc = tokio::select! {
                Some(fc) = rx.recv() => fc,
                Some(x) = tasks.join_next() => match x {
                    Ok(fc) => fc,
                    Err(e) => {
                        log::error!("Fetching error {e:?}");
                        continue;
                    },
                },
            };
            match fc {
                FetchCommand::Fetch(consumer) => {
                    waiters.push_back(consumer);
                }
                FetchCommand::ValueStreamConsumer(envelop, stream, consumer) => {
                    readers.push(stream);
                    tasks.spawn(async move {
                        let msg = Arc::new(envelop);
                        let _ = consumer.send_message(msg).await;
                        FetchCommand::Consumer(consumer)
                    });
                }
                FetchCommand::ErrStreamConsumer(stream, consumer) => {
                    let timeout = Duration::from_micros(20);
                    delay.insert(stream, timeout);
                    waiters.push_front(consumer);
                }
                FetchCommand::Consumer(_consumer) => {
                    m_sent.inc();
                }
                FetchCommand::GracefulShutdown => {
                    if task_tracker.is_closed() && tasks.is_empty() && waiters.is_empty() {
                        break;
                    }
                }
            };

            while let Some(consumer) = waiters.pop_front() {
                if consumer.is_dead() {
                    let _ = consumer.send_timeout(name.clone()).await;
                    continue;
                }

                let parent = consumer.get_current_tracing_span();

                let stream = if let s @ Some(_) = readers.pop() {
                    s
                } else if delay.is_empty() {
                    None
                } else {
                    delay
                        .next()
                        .instrument(tracing::trace_span!(parent: &parent, "queue.wait_stream"))
                        .await
                        .map(|s| s.into_inner())
                };

                if let Some(mut stream) = stream {
                    let span = tracing::trace_span!(parent: &parent, "queue.task_fetch");
                    let consumer_id = queue_consumer_id.clone();
                    tasks.spawn(async move {
                        let noack = !consumer.is_ackable();
                        match stream.fetch(noack, consumer_id).instrument(span).await {
                            Ok(v) => FetchCommand::ValueStreamConsumer(v, stream, consumer),
                            Err(_e) => FetchCommand::ErrStreamConsumer(stream, consumer),
                        }
                    });
                } else {
                    waiters.push_front(consumer);
                    break;
                }
            }
        }
    }
}
