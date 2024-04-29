use base64::{engine::general_purpose::STANDARD, Engine};
use http::uri::Uri;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::SystemTime;
use tokio::sync::mpsc;
use tonic::{
    codegen::{Body, Bytes, StdError},
    metadata::MetadataValue,
};

pub mod pb {
    tonic::include_proto!("hsmq.v1");
}

#[path = "../config.rs"]
pub mod config;
use crate::config::Config;

#[path = "../utils.rs"]
pub mod utils;

use clap::{command, Parser, Subcommand};
use jsonwebtoken::{encode, EncodingKey, Header};
use pb::{hsmq_client::HsmqClient, subscription_response, Message, SubscribeQueueRequest};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;
use tonic::{transport::Channel, Request};

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
struct Claims {
    sub: String,
    exp: usize,
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

        let channel = Channel::from_shared(grpc_addr)?.connect().await?;

        let username = self
            .username
            .clone()
            .unwrap_or_else(|| env!("USER").to_string());
        let users = if let Some(user) = cfg.user.get(&username) {
            let mut result = HashMap::new();
            result.insert(username.clone(), user.clone());
            result
        } else {
            cfg.user.clone()
        };

        let mut token = None;
        for user in users.values() {
            for t in user.tokens.iter() {
                if let Some(t) = t.resolve() {
                    token = Some(t);
                };
            }
        }
        for secret in cfg.auth.jwt.secrets {
            if let Some(secret) = secret.resolve() {
                let claims = Claims {
                    sub: username.clone(),
                    exp: 100000000000000000,
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
            let up = format!("{}:", username);
            format!("Basic {}", STANDARD.encode(up))
        }
        .parse()
        .unwrap();
        let client = HsmqClient::with_interceptor(channel, move |mut req: Request<()>| {
            req.metadata_mut().insert("authorization", header.clone());
            Ok(req)
        });

        self.command.run(client).await?;

        Ok(())
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
}

impl Command {
    async fn run<T>(&self, client: HsmqClient<T>) -> Result<(), Box<dyn std::error::Error>>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        match self {
            Command::Publish { topic, data, count } => {
                self.publish(client, topic.clone(), data.clone(), *count)
                    .await?
            }
            Command::SubscribeQueue { queues } => {
                self.subscribe_queue(client, queues.clone()).await?
            }
            Command::Streaming { command } => command.run(client).await?,
        }
        Ok(())
    }
    async fn publish<T>(
        &self,
        mut client: HsmqClient<T>,
        topic: String,
        data: String,
        count: u64,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
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

    async fn subscribe_queue<T>(
        &self,
        mut client: HsmqClient<T>,
        queues: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        let s = SubscribeQueueRequest { queue: queues };
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

#[derive(Subcommand)]
enum StreaminCommand {
    /// Subscribe to queue
    SubscribeQueue {
        /// Queues
        #[command()]
        queues: Vec<String>,
    },
}

impl StreaminCommand {
    async fn run<T>(&self, client: HsmqClient<T>) -> Result<(), Box<dyn std::error::Error>>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        match self {
            StreaminCommand::SubscribeQueue { queues } => {
                self.subscribe_queue(client, queues.clone()).await?
            }
        };
        Ok(())
    }

    async fn subscribe_queue<T>(
        &self,
        mut client: HsmqClient<T>,
        queues: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        let (tx, rx) = mpsc::channel(1);
        let cmd = SubscribeQueueRequest {
            queue: queues.clone(),
        };
        let kind = Some(pb::request::Kind::SubscribeQueue(cmd));
        let req = pb::Request { kind };
        tx.send(req).await.unwrap();
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut stream = client.streaming(stream).await?.into_inner();

        let mut count = 0u64;
        while let Some(item) = stream.next().await {
            match item?.kind {
                Some(pb::response::Kind::Message(msg)) => {
                    let cmd = pb::MessageAck {
                        msg_id: msg.id,
                        queue: msg.queue,
                    };
                    let kind = Some(pb::request::Kind::MessageAck(cmd));
                    let req = pb::Request { kind };
                    tx.send(req).await.unwrap();
                    log::info!("Received: {:?}", utils::repr(&msg.message.unwrap()));
                    count += 1;
                }
                Some(pb::response::Kind::Redirect(uri)) => {
                    log::info!("Redirect to: {:?}", uri);
                }
                None => (),
            }
        }
        println!("Received {}", count);
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
