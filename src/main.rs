pub mod pb {
    tonic::include_proto!("hsmq.v1");
}
pub mod cluster;
pub mod config;
pub mod grpc;
pub mod metrics;
pub mod server;
pub mod web;

use config::Config;
use server::HsmqServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let cfg = Config::from_file(std::path::Path::new("hsmq.toml"));

    let task_tracker = tokio_util::task::task_tracker::TaskTracker::new();

    let mut grpc_addr = "[::1]:4848".parse().unwrap();

    if let Some(ref node) = cfg.node {
        if let Some(addr) = &node.grpc_address {
            grpc_addr = *addr;
        }
    };

    let hsmq = HsmqServer::from(cfg.clone(), task_tracker.clone());

    let mut tasks = tokio::task::JoinSet::new();

    let grpc_srv = grpc::ServiceV1::new(grpc_addr, task_tracker.clone());
    tasks.spawn(async move {
        grpc_srv.run(hsmq).await;
        "grpc"
    });

    if let Some(ref prometheus) = cfg.prometheus {
        if let Some(addr) = &prometheus.http_address {
            let mut w = web::WebServer::new(*addr, task_tracker.clone());
            w = w.serve_metrics(prometheus.clone());
            tasks.spawn(async move {
                w.run().await;
                "web"
            });
        }
    };

    if let Some(ref cluster) = cfg.cluster {
        let w = cluster::Cluster::new(cluster.clone(), task_tracker.clone());
        tasks.spawn(async move {
            w.run().await;
            "cluster"
        });
    };

    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => println!(" Graceful shutdown by CTRL+C"),
        _ = terminate => log::info!("Graceful shutdown by signal TERM"),
    };
    task_tracker.close();

    loop {
        tokio::select! {
            _ = tasks.join_next() => {
                if tasks.is_empty() {
                    break;
                };
            }
            _ = async {
                tokio::signal::ctrl_c()
                    .await
                    .expect("failed to install Ctrl+C handler");
            } => {
                println!(" Force shutdown by CTRL+C");
                break;
            }
        }
    }

    Ok(())
}
