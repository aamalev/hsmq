use opentelemetry::trace::FutureExt;
use prometheus::core::{AtomicF64, GenericCounter, GenericGauge};
use rand::Rng;
use redis::{aio::ConnectionLike, RedisResult};
use std::{
    collections::{BTreeMap, BinaryHeap, HashMap, VecDeque},
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use tokio::{
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};
use tokio_stream::StreamExt;
use tokio_util::{task::TaskTracker, time::DelayQueue};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    config::{RedisStreamConfig, RedisStreamGroupCleanConfig, RedisStreamGroupConfig, Stream},
    deque::Deque,
    errors::{GenericError, PublishMessageError},
    metrics, pb,
    server::{self, Envelop, GenericConsumer, GenericQueue, GenericSubscriber, QueueCommand},
    utils,
};

use super::RedisConnection;

const TYPE_URL_STRING: &str = "type.googleapis.com/google.protobuf.StringValue";

fn cmd_to_string(cmd: &redis::Cmd) -> String {
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
    pending: f64,
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

#[derive(Debug)]
struct RedisStreamInner {
    cfg: RedisStreamConfig,
    name: String,
    group: String,
    fail: AtomicU64,
    readonly: bool,
    m_xadd: GenericCounter<AtomicF64>,
    m_xread: GenericCounter<AtomicF64>,
    m_xread_nil: GenericCounter<AtomicF64>,
    m_xread_err: GenericCounter<AtomicF64>,
    m_xack: GenericCounter<AtomicF64>,
}

impl RedisStreamInner {
    fn new(cfg: RedisStreamConfig, name: String, group: String, readonly: bool) -> Self {
        let m_xadd = metrics::REDIS_COUNTER.with_label_values(&[&name, "XADD"]);
        let m_xread = metrics::REDIS_COUNTER.with_label_values(&[&name, "XREADGROUP"]);
        let m_xread_nil = metrics::REDIS_COUNTER.with_label_values(&[&name, "XREADGROUP-NIL"]);
        let m_xread_err = metrics::REDIS_COUNTER.with_label_values(&[&name, "XREADGROUP-ERR"]);
        let m_xack = metrics::REDIS_COUNTER.with_label_values(&[&name, "XACK"]);
        Self {
            cfg,
            name,
            group,
            fail: Default::default(),
            readonly,
            m_xadd,
            m_xread,
            m_xread_nil,
            m_xread_err,
            m_xack,
        }
    }
}

#[derive(Clone)]
struct RedisStream<T, C> {
    order_id: T,
    connection: C,
    inner: Arc<RedisStreamInner>,
}

impl<T: std::fmt::Debug, C> Debug for RedisStream<T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStream")
            .field("name", &self.inner.name)
            .field("order_id", &self.order_id)
            .field("fail", &self.inner.fail)
            .finish()
    }
}

impl<C> PartialEq for RedisStream<Arc<AtomicU64>, C> {
    fn eq(&self, other: &Self) -> bool {
        self.order_id.load(Ordering::Relaxed) == other.order_id.load(Ordering::Relaxed)
    }
}

impl<C> Eq for RedisStream<Arc<AtomicU64>, C> {}

impl<C> PartialOrd for RedisStream<Arc<AtomicU64>, C> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<C> Ord for RedisStream<Arc<AtomicU64>, C> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_id
            .load(Ordering::Relaxed)
            .cmp(&other.order_id.load(Ordering::Relaxed))
    }
}

impl<C> PartialEq for RedisStream<String, C> {
    fn eq(&self, other: &Self) -> bool {
        self.order_id == other.order_id
    }
}

impl<C> Eq for RedisStream<String, C> {}

impl<C> PartialOrd for RedisStream<String, C> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<C> Ord for RedisStream<String, C> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_id.cmp(&other.order_id)
    }
}

impl<T, C> Display for RedisStream<T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "<RedisStream {} group={}>",
            &self.inner.name, &self.inner.group,
        ))
    }
}

trait OrderId {
    fn set_order_u64(&mut self, ms: u64);
    fn set_order_str(&mut self, msg_id: &str);
}

impl<C> OrderId for RedisStream<String, C> {
    fn set_order_u64(&mut self, ms: u64) {
        self.order_id = ms.to_string();
    }
    fn set_order_str(&mut self, msg_id: &str) {
        self.order_id = msg_id.to_string();
    }
}

impl<C> OrderId for RedisStream<Arc<AtomicU64>, C> {
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
type RedisStreamR = RedisStream<Arc<AtomicU64>, crate::redis::RedisConnection>;

#[cfg(not(feature = "redis-stream-order-atomic"))]
type RedisStreamR = RedisStream<String, crate::redis::RedisConnection>;

impl RedisStreamR {
    fn new(
        connection: crate::redis::RedisConnection,
        cfg: RedisStreamConfig,
        name: String,
        readonly: bool,
    ) -> Self {
        let queue = name.clone();
        if let Some(ref ttl) = cfg.ttl_key {
            tokio::spawn(Self::bumper_ttl(
                connection.clone(),
                name.clone(),
                ttl.clone().into(),
            ));
        }
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
                    let max_idle = std::time::Duration::from(max_idle.clone());
                    let connection = connection.clone();
                    tokio::spawn(Self::cleaner_consumers(
                        connection,
                        queue.clone(),
                        group,
                        max_idle,
                        every,
                    ));
                };
                name.to_string()
            }
        };
        Self {
            order_id: Default::default(),
            connection,
            inner: Arc::new(RedisStreamInner::new(cfg, name, group, readonly)),
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
            tracing::debug!(
                error = &e as &dyn std::error::Error,
                group,
                name,
                "Init error",
            );
        }
    }

    #[tracing::instrument(name = "stream.consumers", skip(connection))]
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

    #[tracing::instrument(name = "stream.clear", skip(connection, m_pending))]
    async fn clear_consumers(
        connection: &mut RedisConnection,
        name: &String,
        group: &String,
        max_idle: &std::time::Duration,
        m_pending: &GenericGauge<AtomicF64>,
    ) -> anyhow::Result<()> {
        let cmd = redis::cmd("XGROUP")
            .arg("DELCONSUMER")
            .arg(name)
            .arg(group)
            .to_owned();

        let max_idle = max_idle.as_secs();

        let consumers = Self::get_consumers(connection, name, group).await?;
        let mut pending = 0.0;
        let idles: Vec<_> = consumers
            .into_iter()
            .inspect(|c| pending += c.pending)
            .filter(|rc| rc.idle > max_idle)
            .collect();
        m_pending.set(pending);

        for rc in idles.iter() {
            tracing::trace!("Delete idle consumer {:?}", rc);
            let cmd = cmd.clone().arg(&rc.name).to_owned();
            connection.req_packed_command(&cmd).await?;
        }
        if idles.is_empty() {
            tracing::debug!("[{}] No idle consumers", name);
        } else {
            tracing::info!("[{}] Deleted {} idle consumers", name, idles.len());
        }
        Ok(())
    }

    async fn cleaner_consumers(
        mut connection: RedisConnection,
        name: String,
        group: String,
        max_idle: std::time::Duration,
        every: std::time::Duration,
    ) {
        let sleep_start = rand::rng().random_range(0.0..every.as_secs_f64());
        tokio::time::sleep(std::time::Duration::from_secs_f64(sleep_start)).await;

        let m_pending = metrics::REDIS_GAUGE.with_label_values(&[&name, "pending"]);

        loop {
            if let Err(e) =
                Self::clear_consumers(&mut connection, &name, &group, &max_idle, &m_pending).await
            {
                tracing::error!(
                    error = e.as_ref() as &dyn std::error::Error,
                    "Clear consumers with error",
                );
            }
            tokio::time::sleep(every).await;
        }
    }

    #[tracing::instrument(name = "stream.expire", skip(connection))]
    async fn bump_ttl(
        connection: &mut RedisConnection,
        name: &String,
        ttl: &std::time::Duration,
    ) -> anyhow::Result<()> {
        let cmd = redis::cmd("EXPIRE").arg(name).arg(ttl.as_secs()).to_owned();
        tracing::info!(cmd = cmd_to_string(&cmd), "Bump ttl");
        connection.req_packed_command(&cmd).await?;
        Ok(())
    }

    async fn bumper_ttl(mut connection: RedisConnection, name: String, ttl: std::time::Duration) {
        let sleep_start = rand::rng().random_range(0.0..9.0);
        tokio::time::sleep(std::time::Duration::from_secs_f64(sleep_start)).await;
        loop {
            if let Err(e) = Self::bump_ttl(&mut connection, &name, &ttl).await {
                tracing::error!(
                    error = e.as_ref() as &dyn std::error::Error,
                    stream = &name,
                    "Error while bump ttl",
                );
            }
            tokio::time::sleep(ttl / 2).await;
        }
    }

    #[tracing::instrument(
        name = "stream.insert",
        fields(msg_id = &msg.meta.id, stream = &self.inner.name),
        skip_all,
        level = "trace",
    )]
    async fn insert(&mut self, msg: Arc<server::Envelop>) -> Result<String, redis::RedisError> {
        let mut cmd = redis::cmd("XADD").arg(&self.inner.name).to_owned();
        if self.inner.cfg.nomkstream {
            cmd.arg(b"NOMKSTREAM");
        }
        if let Some(maxlen) = self.inner.cfg.maxlen {
            cmd.arg(b"MAXLEN").arg(b"~").arg(maxlen);
            if let Some(limit) = self.inner.cfg.limit {
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
            cmd.arg(&self.inner.cfg.body_fieldname).arg(&data.value);
            if !(data.type_url.is_empty() || data.type_url.eq(TYPE_URL_STRING)) {
                cmd.arg(&self.inner.cfg.body_type_fieldname)
                    .arg(&data.type_url);
            }
        }
        self.inner.m_xadd.inc();
        let msg_id = self.execute::<String>(cmd).await?;
        self.set_order_str(&msg_id);
        Ok(msg_id)
    }

    #[tracing::instrument(
        name = "stream.fetch",
        fields(msg_id, stream = &self.inner.name),
        skip_all,
        level = "trace",
    )]
    async fn fetch(
        &mut self,
        noack: bool,
        consumer_id: String,
    ) -> Result<Envelop, redis::RedisError> {
        let mut cmd = redis::cmd("XREADGROUP")
            .arg(b"GROUP")
            .arg(&self.inner.group)
            .arg(consumer_id.to_string())
            .arg(b"COUNT")
            .arg(b"1")
            .to_owned();
        if noack {
            cmd.arg(b"NOACK");
        }
        cmd.arg(b"STREAMS").arg(&self.inner.name).arg(b">");
        self.inner.m_xread.inc();
        let span = tracing::trace_span!("redis::XREADGROUP");
        match self
            .connection
            .req_packed_command(&cmd)
            .instrument(span)
            .await
            .inspect_err(|_| self.inner.m_xread_err.inc())?
        {
            redis::Value::Array(mut b) => {
                let mut msg_id = String::default();
                let mut msg = pb::Message::default();
                if let Some(redis::Value::Array(mut b)) = b.pop() {
                    if let Some(redis::Value::Array(mut b)) = b.pop() {
                        if let Some(redis::Value::Array(mut b)) = b.pop() {
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
                                if let Some(ct) =
                                    headers.remove(&self.inner.cfg.body_type_fieldname)
                                {
                                    data.type_url = String::from_utf8_lossy(&ct).to_string();
                                } else {
                                    data.type_url = TYPE_URL_STRING.to_string();
                                }
                                if let Some(body) = headers.remove(&self.inner.cfg.body_fieldname) {
                                    data.value = body;
                                }
                                msg.data = Some(data);
                                for (k, v) in headers.into_iter() {
                                    msg.headers
                                        .insert(k, String::from_utf8_lossy(&v).to_string());
                                }
                                self.inner.fail.store(0, Ordering::Relaxed);
                            }
                            if let Some(redis::Value::BulkString(id)) = b.pop() {
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
                envelop.meta.shard.clone_from(&self.inner.name);
                envelop.meta.queue.clone_from(&self.inner.cfg.name);
                Ok(envelop)
            }
            redis::Value::Nil => {
                self.inner.m_xread_nil.inc();
                self.inner.fail.fetch_add(1, Ordering::Relaxed);
                Err(redis::RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Empty",
                )))
            }
            other => {
                self.inner.m_xread_err.inc();
                self.inner.fail.fetch_add(10, Ordering::Relaxed);
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
        fields(stream = &self.inner.name),
        skip_all,
        level = "trace",
    )]
    async fn ack(&mut self, stream: &str, ids: &[String]) -> Result<u32, redis::RedisError> {
        let cmd = redis::cmd("XACK")
            .arg(stream)
            .arg(&self.inner.group)
            .arg(ids)
            .to_owned();
        let result = self.execute(cmd.clone()).await?;
        self.inner.m_xack.inc_by(result as f64);
        Ok(result)
    }
}

#[derive(Debug)]
struct SubscriberRedisStream {
    name: String,
    consumer: GenericConsumer,
    consumer_id: String,
    streams: Vec<RedisStreamR>,
    m_sent: GenericCounter<AtomicF64>,
    m_consumers: GenericGauge<AtomicF64>,
    tx_fetch: mpsc::Sender<FetchCommand>,
    read_limit: usize,
    messages: Arc<Deque<Arc<Envelop>>>,
}

impl Drop for SubscriberRedisStream {
    fn drop(&mut self) {
        self.m_consumers.dec();
    }
}

impl SubscriberRedisStream {
    fn new(q: &RedisStreamQueue, consumer: GenericConsumer) -> Self {
        let streams = q.streams.clone();
        let name = q.name.clone();
        q.m_consumers.inc();
        Self {
            name,
            consumer,
            streams,
            consumer_id: q.consumer_id.clone(),
            m_sent: q.m_sent.clone(),
            m_consumers: q.m_consumers.clone(),
            tx_fetch: q.tx_fetch.clone(),
            read_limit: q.read_limit,
            messages: q.messages.clone(),
        }
    }

    async fn fetch_msg(
        &mut self,
        noack: bool,
        root_span: tracing::Span,
    ) -> Result<(), GenericError> {
        let mut message = None;
        let mut read_limit = self.read_limit;
        self.streams.sort();
        for stream in self.streams.iter_mut() {
            if self.consumer.is_dead() {
                self.consumer.send_timeout(self.name.clone()).await?;
                return Ok(());
            }
            if !self.messages.is_empty() {
                if let Some(msg) = self.messages.pop_front().await {
                    tracing::info!(
                        count = self.messages.len(),
                        queue = self.name,
                        consumer.id = self.consumer_id,
                        "Subscriber extract message from buffer"
                    );
                    message = Some(msg);
                    break;
                }
            }
            if read_limit == 0 {
                break;
            }
            let result = stream.fetch(noack, self.consumer_id.clone()).await;
            match result {
                Ok(msg) => {
                    message = Some(Arc::new(msg));
                    break;
                }
                Err(_) => stream.up(),
            }
            read_limit -= 1;
        }
        if let Some(msg) = message {
            let cx = msg.span.context();
            root_span.set_parent(cx.clone());
            let sent_result = self
                .consumer
                .send_message(msg.clone())
                .with_context(cx)
                .await;
            if let Err(e) = sent_result {
                self.messages.push_back(msg).await;
                Err(e)?
            } else {
                self.m_sent.inc();
                Ok(())
            }
        } else {
            let consumer = self.consumer.generic_clone();
            self.tx_fetch.send(FetchCommand::Fetch(consumer)).await?;
            Ok(())
        }
    }
}

#[derive(Debug)]
struct Subscriber {
    name: String,
    prefetch_count: usize,
    tx_fetch: mpsc::Sender<(tracing::Span, Option<SystemTime>)>,
    task_fetch: JoinHandle<()>,
    tx_ack: mpsc::Sender<Option<(String, String)>>,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        let _ = self.tx_ack.try_send(None);
        self.task_fetch.abort();
    }
}

impl Subscriber {
    fn new(q: &RedisStreamQueue, consumer: GenericConsumer, task_tracker: TaskTracker) -> Self {
        tracing::debug!("New subscriber");
        let consumer_id = consumer.get_id();
        let mut streams = HashMap::new();
        for stream in q.streams.iter() {
            streams.insert(stream.inner.name.clone(), stream.clone());
        }
        let (tx_fetch, rx) = mpsc::channel(99);
        let name = q.name.clone();
        let prefetch_count = consumer.get_prefetch_count();
        let inner = SubscriberRedisStream::new(q, consumer);
        let root = tracing::Span::current();
        let task_fetch = tokio::spawn(async {
            if let Err(e) = Self::consume(inner, rx, root).await {
                tracing::error!(error = e, "Subscriber fetch disabled");
            }
        });
        let (tx_ack, rx_ack) = mpsc::channel(99);
        tokio::spawn(async move {
            if let Err(e) = Self::ack(streams, rx_ack, task_tracker).await {
                tracing::error!(
                    error = e,
                    consumer.id = consumer_id.to_string(),
                    "Subscriber ack disabled",
                );
            }
        });
        Self {
            name,
            prefetch_count,
            tx_fetch,
            task_fetch,
            tx_ack,
        }
    }
    async fn consume(
        mut subscriber: SubscriberRedisStream,
        mut rx: mpsc::Receiver<(tracing::Span, Option<SystemTime>)>,
        prefetch_span: tracing::Span,
    ) -> Result<(), GenericError> {
        tracing::debug!("Start consume");
        let autoack = !subscriber.consumer.is_ackable();
        let prefetch_count = subscriber.consumer.get_prefetch_count();

        for _ in 0..prefetch_count {
            subscriber.fetch_msg(autoack, prefetch_span.clone()).await?;
        }
        while let Some((root_span, deadline)) = rx.recv().await {
            if let Some(deadline) = deadline {
                subscriber.consumer.set_deadline(deadline);
            }
            subscriber.fetch_msg(autoack, root_span).await?;
        }
        Ok(())
    }
    async fn ack(
        streams: HashMap<String, RedisStreamR>,
        mut rx: mpsc::Receiver<Option<(String, String)>>,
        task_tracker: TaskTracker,
    ) -> Result<(), GenericError> {
        let mut tasks = JoinSet::new();
        loop {
            if tasks.is_empty() {
                tasks.spawn(async {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    true // check shutdown
                });
            }
            let item = tokio::select! {
                Some(x) = rx.recv() => {
                    match x {
                        Some((shard, id)) => {
                            tracing::debug!(
                                message.id = &id,
                                message.stream = &shard,
                                "Received ack",
                            );
                            Some((shard, id))
                        }
                        None => break,
                    }
                }
                Some(res) = tasks.join_next() => {
                    match res {
                        Ok(true) => {
                            if task_tracker.is_closed() && rx.is_empty() {
                                rx.close();
                                break;
                            }
                        }
                        Ok(false) => (),
                        Err(e) => tracing::error!("Error in task queue {}", e),
                    };
                    None
                }
            };
            if let Some((shard, id)) = item {
                if let Some(stream) = streams.get(&shard) {
                    let mut stream = stream.clone();
                    tasks.spawn(async move {
                        let ids = [id];
                        while let Err(e) = stream.ack(&shard, &ids).await {
                            tracing::error!(error = &e as &dyn std::error::Error, "Error ack",);
                        }
                        false
                    });
                }
            }
        }
        tasks.join_all().await;
        Ok(())
    }
}

#[tonic::async_trait]
impl server::Subscriber for Subscriber {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    #[tracing::instrument(name = "subscriber.fetch", skip_all, level = "trace")]
    async fn fetch(&mut self, deadline: SystemTime) -> Result<(), GenericError> {
        let span = tracing::Span::current();
        self.tx_fetch.send((span, Some(deadline))).await?;
        Ok(())
    }

    #[tracing::instrument(name = "subscriber.ack", skip_all, level = "trace")]
    async fn ack(&mut self, id: String, shard: String) -> Result<(), GenericError> {
        self.tx_ack.send(Some((shard, id))).await?;
        if self.prefetch_count > 0 {
            let root = tracing::trace_span!(parent: None, "subscriber.fetch");
            self.tx_fetch.send((root, None)).await?;
        }
        Ok(())
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

enum PublishResult {
    Stream(RedisStreamR),
    GracefulShutdown,
}

#[derive(Debug, Clone)]
pub struct RedisStreamQueue {
    pub name: String,
    tx_pub: mpsc::Sender<Arc<Envelop>>,
    tx_fetch: mpsc::Sender<FetchCommand>,
    tx_ack: mpsc::Sender<(String, String)>,
    messages: Arc<Deque<Arc<Envelop>>>,
    streams: Vec<RedisStreamR>,
    consumer_id: String,
    m_sent: GenericCounter<AtomicF64>,
    m_consumers: GenericGauge<AtomicF64>,
    read_limit: usize,
    task_tracker: TaskTracker,
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
        self.tx_pub.send(msg).await.map_err(|_| PublishMessageError)
    }

    #[tracing::instrument(skip_all, level = "trace")]
    async fn subscribe(
        &self,
        consumer: GenericConsumer,
    ) -> Result<GenericSubscriber, GenericError> {
        let subscriber = Subscriber::new(self, consumer, self.task_tracker.clone());
        Ok(Box::new(subscriber))
    }

    async fn ack(
        &self,
        id: String,
        shard: String,
        _consumer_id: uuid::Uuid,
    ) -> Result<(), GenericError> {
        Ok(self.tx_ack.send((id, shard)).await?)
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
                Stream::String(name) => {
                    RedisStream::new(connection.clone(), cfg.clone(), name.clone(), false)
                }
                Stream::Params { name, readonly } => {
                    RedisStream::new(connection.clone(), cfg.clone(), name.clone(), *readonly)
                }
            })
            .collect();

        {
            let queue = cfg.name.clone();
            let connection = connection.clone();
            let keys = streams.iter().map(|s| s.inner.name.clone()).collect();
            tokio::spawn(async move {
                if let Ok(m) = Self::get_keys_addresses(connection, keys).await {
                    for (addr, keys) in m {
                        tracing::info!(
                            queue = queue,
                            addr = addr,
                            streams = keys.join(","),
                            "Queue nodes",
                        );
                    }
                }
            });
        }

        let (tx_pub, rx) = mpsc::channel(99);
        let name = cfg.name.clone();
        let m_received = metrics::QUEUE_COUNTER.with_label_values(&[&name, "received"]);
        let m_in_buffer = metrics::QUEUE_GAUGE.with_label_values(&[&name, "in-buffer"]);
        task_tracker.spawn(Self::publishing(
            cfg.name.clone(),
            rx,
            task_tracker.clone(),
            streams
                .iter()
                .filter(|s| !s.inner.readonly)
                .cloned()
                .collect(),
            m_received,
            m_in_buffer,
            cfg.maxlen,
        ));

        let (tx_fetch, rx_fetch) = mpsc::channel(99);
        let read_limit = cfg.read_limit.unwrap_or(streams.len());
        let name = cfg.name.clone();
        let m_consumers = metrics::QUEUE_GAUGE.with_label_values(&[&name, "consumers"]);
        let m_sent = metrics::QUEUE_COUNTER.with_label_values(&[&name, "sent"]);
        let m_out_buffer = metrics::QUEUE_GAUGE.with_label_values(&[&name, "out-buffer"]);
        let messages = Arc::new(Deque::new(m_out_buffer));
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        tokio::spawn(Self::fetching(
            name.clone(),
            rx_fetch,
            streams.iter().cloned().collect(),
            task_tracker.clone(),
            m_sent.clone(),
            hostname.clone(),
            messages.clone(),
        ));

        let mstreams = streams
            .iter()
            .map(|s| (s.inner.name.clone(), s.clone()))
            .collect();
        let (tx_ack, rx_ack) = mpsc::channel(99);
        let task_tracker_ack = task_tracker.clone();
        task_tracker.spawn(async move {
            if let Err(e) = Self::acker(mstreams, rx_ack, task_tracker_ack).await {
                tracing::error!(error = e, "Subscriber ack disabled");
            }
        });
        Self {
            name,
            tx_pub,
            tx_fetch,
            tx_ack,
            streams,
            messages,
            consumer_id: hostname,
            m_sent,
            m_consumers,
            read_limit,
            task_tracker,
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
        name: String,
        mut rx: mpsc::Receiver<Arc<Envelop>>,
        task_tracker: TaskTracker,
        mut writers: VecDeque<RedisStreamR>,
        m_received: GenericCounter<AtomicF64>,
        m_in_buffer: GenericGauge<AtomicF64>,
        maxlen: Option<usize>,
    ) {
        let m_drop_limit = metrics::QUEUE_COUNTER.with_label_values(&[&name, "drop-limit"]);
        let mut tasks = JoinSet::new();
        let mut messages = VecDeque::new();
        loop {
            if tasks.is_empty() {
                tasks.spawn(async {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    PublishResult::GracefulShutdown
                });
            }
            tokio::select! {
                Some(msg) = rx.recv() => {
                    tracing::debug!("Received {} msg {:?}", messages.len(), &msg);
                    messages.push_back(msg);
                    if let Some(maxlen) = maxlen {
                        if messages.len() > maxlen {
                            m_drop_limit.inc();
                            messages.pop_front();
                        } else {
                            m_in_buffer.inc();
                        }
                    } else {
                        m_in_buffer.inc();
                    }
                }
                Some(res) = tasks.join_next() => {
                    match res {
                        Ok(PublishResult::GracefulShutdown) => {
                            if task_tracker.is_closed() && tasks.is_empty() && rx.is_empty() {
                                rx.close();
                                break;
                            }
                        }
                        Ok(PublishResult::Stream(s)) => {
                            writers.push_back(s);
                        }
                        Err(e) => {
                            tracing::error!("Error in task queue {}", e);
                        }
                    };
                }
            }
            while !messages.is_empty() && !writers.is_empty() {
                if let Some(msg) = messages.pop_front() {
                    m_in_buffer.dec();
                    if let Some(mut stream) = writers.pop_front() {
                        tracing::debug!("Process msg, writers {}", writers.len());
                        let m_received = m_received.clone();
                        tasks.spawn(async move {
                            if let Err(e) = stream.insert(msg).await {
                                tracing::error!(
                                    error = &e as &dyn std::error::Error,
                                    "Error insert"
                                );
                            } else {
                                m_received.inc();
                            }
                            tracing::debug!("Send msg");
                            PublishResult::Stream(stream)
                        });
                    }
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
        messages: Arc<Deque<Arc<Envelop>>>,
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
                    let messages = messages.clone();
                    tasks.spawn(async move {
                        let msg = Arc::new(envelop);
                        if let Err(e) = consumer.send_message(msg.clone()).await {
                            tracing::warn!(
                                error = &e as &dyn std::error::Error,
                                "Error send message to consumer from stream"
                            );
                            messages.push_back(msg).await;
                        }
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
                    if task_tracker.is_closed() && tasks.is_empty() {
                        rx.close();
                        break;
                    }
                }
            };

            while let Some(consumer) = waiters.pop_front() {
                if consumer.is_dead() {
                    let _ = consumer.send_timeout(name.clone()).await;
                    continue;
                }

                if !messages.is_empty() {
                    let messages = messages.clone();
                    tasks.spawn(async move {
                        if let Some(msg) = messages.pop_front().await {
                            tracing::info!(
                                count = messages.len(),
                                "Fetching extract message from buffer"
                            );
                            if let Err(e) = consumer.send_message(msg.clone()).await {
                                tracing::warn!(
                                    error = &e as &dyn std::error::Error,
                                    "Error send message to consumer from buffer"
                                );
                                messages.push_back(msg).await;
                            }
                            FetchCommand::Consumer(consumer)
                        } else {
                            FetchCommand::Fetch(consumer)
                        }
                    });
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

    async fn acker(
        mut streams: HashMap<String, RedisStreamR>,
        mut rx: mpsc::Receiver<(String, String)>,
        task_tracker: TaskTracker,
    ) -> Result<(), GenericError> {
        let mut tasks: JoinSet<Option<(String, RedisStreamR)>> = JoinSet::new();
        let mut ids = HashMap::new();
        loop {
            if tasks.is_empty() {
                tasks.spawn(async {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    None // check shutdown
                });
            }
            tokio::select! {
                Some((id, shard)) = rx.recv() => {
                    tracing::debug!(
                        message.id = &id,
                        message.stream = &shard,
                        "Received ack",
                    );
                    ids.entry(shard).or_insert_with(Vec::new).push(id);

                    while let Ok((id, shard)) = rx.try_recv() {
                        let v = ids.entry(shard).or_insert_with(Vec::new);
                        v.push(id);
                        if v.len() > 10 {
                            break;
                        }
                    }
                }
                Some(res) = tasks.join_next() => {
                    match res {
                        Ok(None) => {
                            if task_tracker.is_closed() && rx.is_empty() {
                                rx.close();
                                break;
                            }
                        }
                        Ok(Some((name, stream))) => {streams.insert(name, stream);}
                        Err(e) => tracing::error!("Error in task queue {}", e),
                    };
                }
            };
            for (shard, shard_ids) in ids.iter_mut() {
                if !shard_ids.is_empty() && streams.contains_key(shard) {
                    if let Some(mut stream) = streams.remove(shard) {
                        let mut ids = vec![];
                        ids.append(shard_ids);
                        let name = shard.clone();
                        tasks.spawn(async move {
                            while let Err(e) = stream.ack(&name, &ids).await {
                                tracing::error!(error = &e as &dyn std::error::Error, "Error ack",);
                            }
                            Some((name, stream))
                        });
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn get_cluster_slots(
        connection: &mut RedisConnection,
    ) -> anyhow::Result<BTreeMap<u16, String>> {
        let result: Vec<Vec<redis::Value>> = redis::cmd("CLUSTER")
            .arg("SLOTS")
            .query_async(connection)
            .await?;

        let mut slot_map = BTreeMap::new();

        for slot_info in result {
            if slot_info.len() < 3 {
                continue;
            }

            let start_slot: u16 = match slot_info[0] {
                redis::Value::Int(i) => i as u16,
                _ => continue,
            };

            let node_info = match &slot_info[2] {
                redis::Value::Array(v) => v,
                _ => continue,
            };

            if node_info.len() < 2 {
                continue;
            }

            let ip = match &node_info[0] {
                redis::Value::BulkString(d) => String::from_utf8_lossy(d),
                _ => continue,
            };

            let port = match node_info[1] {
                redis::Value::Int(p) => p,
                _ => continue,
            };

            let node_address = format!("{}:{}", ip, port);

            slot_map.insert(start_slot, node_address);
        }

        Ok(slot_map)
    }

    pub async fn get_keys_addresses(
        mut connection: RedisConnection,
        keys: Vec<String>,
    ) -> anyhow::Result<HashMap<String, Vec<String>>> {
        let slot_map = Self::get_cluster_slots(&mut connection).await?;
        tracing::info!("CLUSTER SLOTS {}", serde_json::to_string(&slot_map)?);

        let mut key_addresses = HashMap::new();

        let mut pipeline = redis::pipe();
        for key in keys.iter() {
            pipeline.cmd("CLUSTER").arg("KEYSLOT").arg(key);
        }
        let key_slots: Vec<u16> = pipeline.query_async(&mut connection).await?;

        for (key, slot) in keys.iter().zip(key_slots.iter()) {
            let address = slot_map
                .range(..=slot)
                .next_back()
                .map(|(_, addr)| addr.clone())
                .unwrap_or_else(|| "Unknown".to_string());
            key_addresses
                .entry(address)
                .or_insert_with(Vec::new)
                .push(key.clone());
        }

        Ok(key_addresses)
    }
}

#[cfg(test)]
mod tests {
    use super::{RedisStream, RedisStreamInner};
    use crate::config::{RedisStreamConfig, RedisStreamGroupConfig};
    use std::sync::Arc;

    impl Default for RedisStreamConfig {
        fn default() -> Self {
            Self {
                name: Default::default(),
                connector: Default::default(),
                topics: Default::default(),
                maxlen: Default::default(),
                limit: Default::default(),
                group: RedisStreamGroupConfig::Name(Default::default()),
                nomkstream: Default::default(),
                ttl_key: Default::default(),
                streams: Default::default(),
                body_fieldname: RedisStreamConfig::default_body_fieldname(),
                body_type_fieldname: RedisStreamConfig::default_body_type_fieldname(),
                read_limit: Default::default(),
            }
        }
    }

    impl RedisStream<String, String> {
        fn new_test(order_id: &str) -> Self {
            Self {
                order_id: order_id.to_string(),
                connection: String::default(),
                inner: Arc::new(RedisStreamInner::new(
                    RedisStreamConfig::default(),
                    String::default(),
                    String::default(),
                    false,
                )),
            }
        }
    }

    #[tokio::test]
    async fn stream_order() {
        let mut streams = [RedisStream::new_test("2"), RedisStream::new_test("1")];
        streams.sort();

        let orders: Vec<_> = streams.iter().map(|x| x.order_id.as_str()).collect();
        assert_eq!(orders, vec!["1", "2"]);
    }
}
