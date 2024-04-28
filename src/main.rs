pub mod pb {
    tonic::include_proto!("hsmq.v1");
}
pub mod cluster;
pub mod config;
pub mod errors;
pub mod grpc;
pub mod metrics;
pub mod server;
pub mod web;

use clap::{command, Parser};
use std::path::PathBuf;

use config::Config;
use server::HsmqServer;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,
}

async fn ctrl_c(graceful: bool) {
    match tokio::signal::ctrl_c().await {
        Ok(_) => {
            if graceful {
                println!(" Graceful shutdown by CTRL+C");
            } else {
                println!(" Force shutdown by CTRL+C");
            }
        }
        Err(_) => {
            log::error!("Failed to install Ctrl+C handler");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli = Cli::parse();

    let cfg = if let Some(config_path) = cli.config.as_deref() {
        Config::from_file(config_path)?
    } else {
        Config::default()
    };

    let task_tracker = tokio_util::task::task_tracker::TaskTracker::new();

    let mut grpc_addr = "[::1]:4848".parse().unwrap();

    if let Some(ref node) = cfg.node {
        if let Some(addr) = &node.grpc_address {
            grpc_addr = *addr;
        }
    };

    let hsmq = HsmqServer::from(cfg.clone(), task_tracker.clone());

    let mut tasks = tokio::task::JoinSet::new();

    let grpc_srv = grpc::GrpcService::new(grpc_addr, task_tracker.clone());
    tasks.spawn(async move {
        grpc_srv.run(hsmq).await;
    });

    if let Some(ref prometheus) = cfg.prometheus {
        if let Some(addr) = &prometheus.http_address {
            let mut w = web::WebServer::new(*addr, task_tracker.clone());
            w = w.serve_metrics(prometheus.clone());
            tasks.spawn(async move {
                w.run().await;
            });
        }
    };

    if let Some(ref cluster) = cfg.cluster {
        let w = cluster::Cluster::new(cluster.clone(), task_tracker.clone());
        tasks.spawn(async move {
            w.run().await;
        });
    };

    #[cfg(not(unix))]
    let shutdown = ctrl_c(true);

    #[cfg(unix)]
    let shutdown = async {
        let s = tokio::signal::unix::SignalKind::terminate();
        match tokio::signal::unix::signal(s) {
            Ok(mut res) => {
                tokio::select! {
                    _ = ctrl_c(true) => (),
                    _ = res.recv() => {
                        log::info!("Graceful shutdown by signal TERM");
                    }
                    _ = task_tracker.wait() => (),
                }
            }
            Err(_) => {
                log::error!("Failed to install signal handler");
                tokio::select! {
                    _ = ctrl_c(true) => (),
                    _ = task_tracker.wait() => (),
                }
            }
        }
    };

    shutdown.await;
    task_tracker.close();

    loop {
        tokio::select! {
            _ = tasks.join_next() => {
                if tasks.is_empty() {
                    break;
                };
            }
            _ = ctrl_c(false) => {
                break;
            }
        }
    }

    Ok(())
}
