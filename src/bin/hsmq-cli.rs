use base64::{engine::general_purpose::STANDARD, Engine};
use http::uri::Uri;
use lazy_static::lazy_static;
use opentelemetry::global;
use prometheus::proto::LabelPair;
use prometheus::{register_histogram_vec, Encoder, HistogramVec, TextEncoder};
use rand::Rng;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::mpsc;
use tokio_util::task::task_tracker::TaskTracker;
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::Status;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub mod pb {
    tonic::include_proto!("hsmq.v1.hsmq");
}

#[path = "../config.rs"]
pub mod config;
use crate::cluster::{JwtPackage, Package};
use crate::config::Config;

#[path = "../utils.rs"]
pub mod utils;

#[path = "../errors.rs"]
pub mod errors;

#[path = "../metrics.rs"]
pub mod metrics;

#[path = "../jwt.rs"]
pub mod jwt;

#[path = "../cluster.rs"]
pub mod cluster;

use clap::{command, Parser, Subcommand};
use jsonwebtoken::{encode, EncodingKey, Header};
use pb::{hsmq_client::HsmqClient, subscription_response, Message, SubscribeQueueRequest};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;
use tonic::{transport::Channel, Request};

lazy_static! {
    pub static ref LATENCY_HIST: HistogramVec =
        register_histogram_vec!("hsmq_latency", "HSMQ client latency", &["queue"]).unwrap();
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
struct Claims {
    sub: String,
    exp: usize,
}

#[derive(Clone, Debug)]
struct ClientFactory {
    config: Config,
    grpc_uri: String,
    username: String,
}

impl ClientFactory {
    fn new(config: Config, grpc_uri: String, username: String) -> Self {
        Self {
            config,
            grpc_uri,
            username,
        }
    }

    async fn create_client(
        &self,
    ) -> Result<
        HsmqClient<
            InterceptedService<Channel, impl Fn(Request<()>) -> Result<Request<()>, Status>>,
        >,
        Box<dyn std::error::Error>,
    > {
        let channel = Channel::from_shared(self.grpc_uri.to_string())?
            .connect()
            .await?;

        let users = if let Some(user) = self.config.users.get(&self.username) {
            let mut result = HashMap::new();
            result.insert(self.username.clone(), user.clone());
            result
        } else {
            self.config.users.clone()
        };

        let mut token = None;
        for user in users.values() {
            for t in user.tokens.iter() {
                if let Some(t) = t.resolve() {
                    token = Some(t);
                };
            }
        }
        for secret in self
            .config
            .auth
            .jwt
            .clone()
            .unwrap_or_default()
            .secrets
            .iter()
        {
            if let Some(secret) = secret.resolve() {
                let claims = Claims {
                    sub: self.username.clone(),
                    exp: (utils::current_time() + Duration::from_secs(3600)).as_secs() as usize,
                };

                token = encode(
                    &Header::default(),
                    &claims,
                    &EncodingKey::from_secret(secret.as_ref()),
                )
                .ok();
                break;
            }
        }
        let header: MetadataValue<_> = if let Some(token) = token {
            format!("Bearer {}", token)
        } else {
            let up = format!("{}:", &self.username);
            format!("Basic {}", STANDARD.encode(up))
        }
        .parse()?;
        let client = HsmqClient::with_interceptor(channel, move |mut req: Request<()>| {
            req.metadata_mut().insert("authorization", header.clone());
            Ok(req)
        });
        Ok(client)
    }
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Username
    #[arg(short, long)]
    username: Option<String>,

    /// grpc uri
    #[arg(short, long)]
    grpc_uri: Option<Uri>,

    #[command(subcommand)]
    command: Command,
}

impl Cli {
    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let cfg = if let Some(config_path) = self.config.as_deref() {
            Config::from_file(config_path)?
        } else {
            Config::default()
        };
        let mut grpc_addr = if let Some(ref grpc_uri) = self.grpc_uri {
            grpc_uri.to_string()
        } else {
            cfg.node.grpc_address.map(|v| v.to_string()).unwrap()
        };
        if !grpc_addr.starts_with("http") {
            grpc_addr = format!("http://{}", grpc_addr);
        };

        let username = self
            .username
            .clone()
            .unwrap_or_else(|| std::env::var("USER").unwrap_or_default());

        if let Some(ref p) = cfg.prometheus {
            if let Some(ref addr) = p.http_address {
                let mut addr = addr.clone();
                addr.set_port(8081);
                tokio::spawn(Self::prometheus(addr, p.url.to_string()));
            }
        }

        let factory = ClientFactory::new(cfg.clone(), grpc_addr, username);
        self.command.run(factory, cfg).await?;

        Ok(())
    }

    async fn prometheus(addr: std::net::SocketAddr, url: String) {
        let mut role = LabelPair::new();
        role.set_name("role".to_string());
        role.set_value("hsmq-cli".to_string());
        let router = axum::Router::new().route(
            &url,
            axum::routing::get(|| async move {
                let encoder = TextEncoder::new();
                let mut metric_families = prometheus::gather();
                for mf in metric_families.iter_mut() {
                    let rm = mf.mut_metric();
                    for i in rm.iter_mut() {
                        let l = i.mut_label();
                        l.push(role.clone());
                    }
                }
                let mut buffer = vec![];
                encoder.encode(&metric_families, &mut buffer).unwrap();
                ([("content-type", "text/plain")], buffer)
            }),
        );

        log::info!("Listening on http://{}", &addr);
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, router).await.unwrap();
    }
}

#[derive(Subcommand)]
enum Command {
    /// Publish message
    Publish {
        /// Count
        #[arg(short, long, default_value_t = 1)]
        count: u64,
        /// Topic
        #[command()]
        topic: String,
        /// Data
        #[command()]
        data: String,
    },
    /// Subscribe to queue
    SubscribeQueue {
        /// Queues
        #[command()]
        queues: Vec<String>,
    },
    /// Streaming mode
    Streaming {
        #[command(subcommand)]
        command: StreaminCommand,
    },
    /// Cluster commands
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },
}

impl Command {
    async fn run(
        &self,
        client_factory: ClientFactory,
        cfg: Config,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Command::Publish { topic, data, count } => {
                self.publish(client_factory, topic.clone(), data.clone(), *count)
                    .await?
            }
            Command::SubscribeQueue { queues } => {
                self.subscribe_queue(client_factory, queues.clone()).await?
            }
            Command::Streaming { command } => command.run(client_factory).await?,
            Command::Cluster { command } => command.run(client_factory, cfg).await?,
        }
        Ok(())
    }

    async fn publish(
        &self,
        client_factory: ClientFactory,
        topic: String,
        data: String,
        count: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let any = prost_types::Any {
            type_url: "string".to_string(),
            value: data.clone().into_bytes(),
        };

        fn make_msg(ctx: HashMap<String, String>, topic: String, any: prost_types::Any) -> Message {
            let mut msg = Message::default();
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64();
            msg.headers
                .insert(utils::CREATED_AT.to_string(), now.to_string());
            for (k, v) in ctx.into_iter() {
                msg.headers.insert(k, v);
            }
            msg.topic = topic;
            msg.data = Some(any);
            msg
        }
        let mut ctx = HashMap::new();
        ctx.insert("uuid".to_string(), uuid::Uuid::now_v7().to_string());

        let mut client = client_factory.create_client().await?;
        let response = if count == 1 {
            let msg = make_msg(ctx, topic.clone(), any);
            let request = tonic::Request::new(msg);
            client.publish(request).await?
        } else {
            let in_stream = tokio_stream::iter(0..count).map(move |n| {
                let mut ctx = ctx.clone();
                ctx.insert("counter".to_string(), n.to_string());
                make_msg(ctx, topic.clone(), any.clone())
            });
            client.publish_qos0(in_stream).await?
        };

        log::info!("RESPONSE={:?}", response);
        Ok(())
    }

    async fn subscribe_queue(
        &self,
        client_factory: ClientFactory,
        queues: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut client = client_factory.create_client().await?;
        let s = SubscribeQueueRequest { queues };
        let mut stream = client.subscribe_queue(s).await?.into_inner();
        let mut count = 0u64;

        while let Some(item) = stream.next().await {
            match item?.kind {
                Some(subscription_response::Kind::Message(msg)) => {
                    log::info!("Received: {:?}", utils::repr(&msg));
                    count += 1;
                }
                Some(subscription_response::Kind::Redirect(uri)) => {
                    log::info!("Redirect to: {:?}", uri);
                }
                None => (),
            }
        }
        println!("Received {}", count);
        Ok(())
    }
}

#[derive(Subcommand, Debug, Clone)]
enum StreaminCommand {
    /// Subscribe to queue
    SubscribeQueue {
        /// Queues
        #[command()]
        queues: Vec<String>,
        /// Prefetch count
        #[arg(short, long, default_value_t = 1)]
        prefetch_count: i32,
        /// Limit
        #[arg(short, long, default_value_t = 1)]
        limit: u64,
    },
    /// Bench queue
    BenchQueue {
        /// Queues
        #[command()]
        queue: String,
        /// Prefetch count
        #[arg(short, long, default_value_t = 0)]
        prefetch_count: i32,
        /// Limit
        #[arg(short, long, default_value_t = 1)]
        limit: u64,
        /// Publishers
        #[arg(long, default_value_t = 1)]
        publishers: usize,
        /// Consumers
        #[arg(short, long, default_value_t = 1)]
        consumers: usize,
    },
}

impl StreaminCommand {
    async fn run(&self, client_factory: ClientFactory) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            StreaminCommand::SubscribeQueue {
                queues,
                prefetch_count,
                limit,
            } => {
                let count = self
                    .subscribe_queue(
                        client_factory,
                        queues.clone(),
                        *prefetch_count,
                        Arc::new(AtomicI64::new(*limit as i64)),
                        Duration::from_millis(300),
                    )
                    .await?;
                println!("Received {}", count);
            }
            StreaminCommand::BenchQueue {
                queue,
                prefetch_count,
                limit,
                publishers,
                consumers,
            } => {
                self.bench_queue(
                    client_factory,
                    queue.to_string(),
                    *prefetch_count,
                    *limit as i64,
                    *publishers,
                    *consumers,
                    Duration::from_secs(10),
                )
                .await?;
            }
        };
        Ok(())
    }

    async fn publish(
        &self,
        client_factory: ClientFactory,
        topic: String,
        qos: u32,
        limit: Arc<AtomicI64>,
        timeout: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let (tx, rx) = mpsc::channel(1);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut client = client_factory.create_client().await?;
        let mut stream = client.streaming(stream).await?.into_inner();

        let mut counter = 0u64;
        let mut timeout_serias = 0;
        loop {
            let span = tracing::trace_span!(parent: None, "message");
            let espan = span.enter();
            let cx = span.context();
            let mut msg = pb::Message::default();
            global::get_text_map_propagator(|propagator| {
                propagator.inject_context(&cx, &mut msg.headers)
            });
            let now = utils::current_time().as_secs_f64().to_string();
            let msg_id = uuid::Uuid::now_v7();
            msg.topic = topic.clone();
            msg.headers.insert("uuid".to_string(), msg_id.to_string());
            msg.headers.insert("ts".to_string(), now);
            let cmd = pb::PublishMessage {
                message: Some(msg),
                qos,
            };
            let kind = Some(pb::request::Kind::PublishMessage(cmd));
            let req = pb::Request { kind };
            if let Err(e) = tx
                .send(req)
                .instrument(tracing::trace_span!(parent: &span, "publish"))
                .await
            {
                log::error!("Publusher error {e:?}");
                break;
            } else if limit.fetch_sub(1, Ordering::Acquire) <= 1 {
                counter += 1;
                break;
            } else {
                counter += 1;
            };
            if qos == 1 {
                let oitem = tokio::select! {
                    x = stream.next() => x,
                    _ = tokio::time::sleep(timeout) => if timeout_serias > 100 {
                        break;
                    } else if limit.load(Ordering::Relaxed) <= 1 {
                        break;
                    } else {
                        timeout_serias += 1;
                        None
                    }
                };

                if let Some(item) = oitem {
                    match item?.kind {
                        Some(pb::response::Kind::PubAck(_)) => {
                            timeout_serias = 0;
                        }
                        Some(pb::response::Kind::Redirect(uri)) => {
                            log::info!("Redirect to: {:?}", uri);
                        }
                        _ => (),
                    }
                }
            }
            drop(espan);

            let timeout = rand::thread_rng().gen_range(2..20);
            tokio::time::sleep(Duration::from_millis(timeout)).await;
        }
        Ok(counter)
    }

    async fn subscribe_queue(
        &self,
        client_factory: ClientFactory,
        queues: Vec<String>,
        prefetch_count: i32,
        limit: Arc<AtomicI64>,
        timeout: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let m_latency_hist: HashMap<_, _> = queues
            .iter()
            .map(|q| (q.to_string(), LATENCY_HIST.with_label_values(&[q])))
            .collect();
        let (tx, rx) = mpsc::channel(1);
        let cmd = pb::SubscribeQueue {
            queues,
            prefetch_count,
        };
        let kind = Some(pb::request::Kind::SubscribeQueue(cmd));
        let req = pb::Request { kind };
        tx.send(req).await?;
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut client = client_factory.create_client().await?;
        let mut stream = client.streaming(stream).await?.into_inner();

        let mut counter = 0u64;
        let mut timeout_serias = 0;
        loop {
            let oitem = tokio::select! {
                x = stream.next() => x,
                _ = tokio::time::sleep(timeout) => if timeout_serias > 100 {
                    break;
                } else if limit.load(Ordering::Relaxed) <= 1 {
                    break;
                } else {
                    timeout_serias += 1;
                    None
                }
            };

            if let Some(item) = oitem {
                match item?.kind {
                    Some(pb::response::Kind::Message(msg)) => {
                        let q = msg
                            .meta
                            .as_ref()
                            .map(|m| m.queue.clone())
                            .unwrap_or_default();
                        let cmd = pb::MessageAck { meta: msg.meta };
                        let kind = Some(pb::request::Kind::MessageAck(cmd));
                        let req = pb::Request { kind };
                        tx.send(req).await?;
                        timeout_serias = 0;
                        let msg = msg.message.unwrap_or_default();
                        log::info!("Received: {:?}", utils::repr(&msg));
                        counter += 1;
                        if let Some(ts) = msg.headers.get("ts") {
                            let now = utils::current_time().as_secs_f64();
                            let ts: f64 = ts.parse()?;
                            if let Some(m) = m_latency_hist.get(&q) {
                                m.observe(now - ts);
                            }
                        }
                        let x = limit.fetch_sub(1, Ordering::AcqRel);
                        if x <= 1 {
                            break;
                        }
                    }
                    Some(pb::response::Kind::Redirect(uri)) => {
                        log::info!("Redirect to: {:?}", uri);
                    }
                    _ => (),
                }
            }
        }
        Ok(counter)
    }

    async fn fetch_queue(
        &self,
        client_factory: ClientFactory,
        queue: String,
        limit: Arc<AtomicI64>,
        timeout: Duration,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let (tx, rx) = mpsc::channel(1);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut client = client_factory.create_client().await?;
        let mut stream = client.streaming(stream).await?.into_inner();
        let m_latency_hist = LATENCY_HIST.with_label_values(&[&queue]);

        let mut count = 0u64;
        let mut timeout_serias = 0;
        loop {
            let span = tracing::trace_span!(parent: None, "fetch");
            let espan = span.enter();
            let cmd = pb::FetchMessage {
                queue: queue.clone(),
                timeout: 5.0,
            };
            let kind = Some(pb::request::Kind::FetchMessage(cmd));
            let req = pb::Request { kind };
            tx.send(req).await?;

            let oitem = tokio::select! {
                x = stream.next() => x,
                _ = tokio::time::sleep(timeout) => if timeout_serias > 100 {
                    break;
                } else if limit.load(Ordering::Relaxed) <= 1 {
                    break;
                } else {
                    timeout_serias += 1;
                    None
                }
            };

            if let Some(item) = oitem {
                match item?.kind {
                    Some(pb::response::Kind::Message(msg)) => {
                        let msg = msg.message.unwrap_or_default();
                        let parent_cx =
                            global::get_text_map_propagator(|prop| prop.extract(&msg.headers));
                        span.set_parent(parent_cx);
                        log::info!("Received: {:?}", utils::repr(&msg));
                        count += 1;
                        timeout_serias = 0;
                        if let Some(ts) =
                            msg.headers.get("ts").and_then(|ts| ts.parse::<f64>().ok())
                        {
                            let now = utils::current_time().as_secs_f64();
                            m_latency_hist.observe(now - ts);
                        }
                        let x = limit.fetch_sub(1, Ordering::AcqRel);
                        if x <= 1 {
                            break;
                        }
                    }
                    Some(pb::response::Kind::Redirect(uri)) => {
                        log::info!("Redirect to: {:?}", uri);
                    }
                    _ => (),
                }
            }
            drop(espan);
        }
        Ok(count)
    }

    async fn bench_queue(
        &self,
        client_factory: ClientFactory,
        queue: String,
        prefetch_count: i32,
        limit: i64,
        publishers: usize,
        consumers: usize,
        timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = std::time::SystemTime::now();
        let publish_limit = Arc::new(AtomicI64::new(limit));
        let consume_limit = Arc::new(AtomicI64::new(limit));
        {
            let task_tracker = TaskTracker::new();

            for _ in 0..consumers {
                let queue = queue.clone();
                let cmd = self.clone();
                let client_factory = client_factory.clone();
                let limit = consume_limit.clone();
                task_tracker.spawn(async move {
                    let r = if prefetch_count > 0 {
                        cmd.subscribe_queue(
                            client_factory,
                            vec![queue],
                            prefetch_count,
                            limit.clone(),
                            timeout,
                        )
                        .await
                    } else {
                        cmd.fetch_queue(client_factory, queue, limit.clone(), timeout)
                            .await
                    };
                    log::error!("Finish consumer {r:?}");
                });
            }

            for _ in 0..publishers {
                let cmd = self.clone();
                let client_factory = client_factory.clone();
                let topic = queue.clone();
                let limit = publish_limit.clone();
                task_tracker.spawn(async move {
                    let counter = cmd.publish(client_factory, topic, 1, limit, timeout).await;
                    log::error!("Finish publisher {counter:?}");
                });
            }

            task_tracker.close();
            tokio::select! {
                _ = task_tracker.wait() => (),
                _ = tokio::signal::ctrl_c() => println!(" Stop by signal"),
            }
        }

        let stop = std::time::SystemTime::now();
        let duration = stop.duration_since(start)?.as_secs_f64();
        let send = limit - publish_limit.load(Ordering::Relaxed);
        let recv = limit - consume_limit.load(Ordering::Relaxed);
        let rate = recv as f64 / duration;
        println!(
            "Messages {limit}, send {send}, recv {recv}, \
            seconds {duration:.3}, rate {rate:.3} rps"
        );
        Ok(())
    }
}

#[derive(Subcommand)]
enum ClusterCommand {
    /// Join node
    Join {
        /// Node address
        #[command()]
        address: String,
    },
}

impl ClusterCommand {
    async fn run(
        &self,
        _client_factory: ClientFactory,
        cfg: Config,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cluster_cfg = cfg.cluster.clone().unwrap_or_default();
        let jwt_cfg = cfg.cluster_jwt();
        let jwt = crate::jwt::JWT::new(jwt_cfg);
        let sock = crate::cluster::JwtSocket::bind("0.0.0.0:0", jwt).await?;

        match self {
            ClusterCommand::Join { address } => {
                let pack = crate::cluster::Package::Join {
                    cluster: cluster_cfg.name.clone(),
                    addr: address.to_string(),
                };
                sock.send_to(pack, address).await?;

                if let Some(JwtPackage {
                    pack: Package::Response { lines },
                    ..
                }) = sock.wait_package(Duration::from_secs(5)).await
                {
                    for line in lines {
                        println!("{}", line);
                    }
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli = Cli::parse();

    cli.run().await?;

    Ok(())
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
