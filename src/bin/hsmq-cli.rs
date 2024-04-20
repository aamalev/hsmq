use http::uri::Uri;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::SystemTime;

pub mod pb {
    tonic::include_proto!("hsmq.v1");
}

#[path = "../config.rs"]
pub mod config;
use crate::config::Config;

#[path = "../utils.rs"]
pub mod utils;

use clap::{command, Parser, Subcommand};
use pb::{hsmq_client::HsmqClient, subscription_response, Message, SubscribeQueueRequest};
use tokio_stream::StreamExt;
use tonic::{metadata::MetadataValue, transport::Channel, Request};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// grpc uri
    #[arg(short, long)]
    grpc_uri: Option<Uri>,

    #[command(subcommand)]
    command: Command,
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli = Cli::parse();

    let mut grpc_addr = "[::1]:4848".to_string();

    if let Some(config_path) = cli.config.as_deref() {
        let cfg = Config::from_file(config_path);

        if let Some(grpc_uri) = cli.grpc_uri {
            grpc_addr = grpc_uri.to_string();
        } else if let Some(ref node) = cfg.node {
            if let Some(addr) = &node.grpc_address {
                grpc_addr = addr.to_string();
            }
        };
        if !grpc_addr.starts_with("http") {
            grpc_addr = format!("http://{}", grpc_addr);
        }
    }

    let channel = Channel::from_shared(grpc_addr)?.connect().await?;

    let token: MetadataValue<_> = "Bearer some-secret-token".parse()?;

    let mut client = HsmqClient::with_interceptor(channel, move |mut req: Request<()>| {
        req.metadata_mut().insert("authorization", token.clone());
        Ok(req)
    });

    match cli.command {
        Command::Publish { topic, data, count } => {
            let any = prost_types::Any {
                type_url: "string".to_string(),
                value: data.clone().into_bytes(),
            };
            fn make_msg(
                ctx: HashMap<String, String>,
                topic: String,
                any: prost_types::Any,
            ) -> Message {
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
        }
        Command::SubscribeQueue { queues } => {
            let s = SubscribeQueueRequest { queue: queues };
            let mut stream = client.subscribe_queue(s).await?.into_inner();

            while let Some(item) = stream.next().await {
                match item?.kind {
                    Some(subscription_response::Kind::Message(msg)) => {
                        log::info!("Received: {:?}", utils::repr(&msg));
                    }
                    Some(subscription_response::Kind::Redirect(uri)) => {
                        log::info!("Redirect to: {:?}", uri);
                    }
                    None => (),
                }
            }
        }
    }

    Ok(())
}
