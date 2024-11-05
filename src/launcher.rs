use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

use crate::auth::Auth;
use crate::config::Config;
use crate::errors::GenericError;
use crate::grpc::GrpcService;
use crate::server::HsmqServer;
use crate::{cluster, web};

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

#[allow(unused_mut)]
pub async fn run(mut cfg: Config, listener: Option<TcpListener>) -> Result<(), GenericError> {
    crate::tracing::init_subscriber(&cfg)?;

    #[cfg(feature = "vault")]
    if let Some(ref cfgv) = cfg.vault {
        let mut v = crate::vault::Client::new(cfgv.clone())?;
        v.login().await?;
        while let Err(e) = cfg.resolve(&v).await {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            tracing::warn!(error = e.as_ref() as &dyn std::error::Error, "Retry vault");
        }
    }

    let hostname = gethostname::gethostname().to_string_lossy().to_string();

    let task_tracker = tokio_util::task::task_tracker::TaskTracker::new();

    let hsmq = HsmqServer::from(cfg.clone(), task_tracker.clone()).await?;
    let auth = Arc::new(Auth::new(cfg.auth.clone()).with_users(cfg.users.clone()));

    let mut tasks = JoinSet::new();

    if let Some(grpc_addr) = cfg.node.grpc_address {
        let grpc_srv = GrpcService::new(grpc_addr, task_tracker.clone());
        let auth = auth.clone();
        tasks.spawn(async move {
            grpc_srv.run(hsmq, auth, listener).await;
        });
    }

    if let Some(ref prometheus) = cfg.prometheus {
        if let Some(addr) = &prometheus.http_address {
            let w = web::WebServer::new(*addr, task_tracker.clone());
            let mut pcfg = prometheus.clone();
            if let Some(cluster) = cfg.cluster_name() {
                pcfg.labels.insert("cluster".to_string(), cluster);
            }
            tasks.spawn(async move {
                w.serve_metrics(pcfg).run().await;
            });
        }
    };

    if let Some(ref cluster) = cfg.cluster {
        let mut cluster_cfg = cluster.clone();
        cluster_cfg.jwt = Some(cfg.cluster_jwt());
        let node_name = format!("{}:{}", hostname, cluster.udp_port);
        let w = cluster::Cluster::new(cluster_cfg, node_name, task_tracker.clone());
        tasks.spawn(w.run());
    };

    #[cfg(feature = "consul")]
    let srv_consul = if let Some(cfg) = cfg.consul {
        let c = crate::consul::Consul::new(cfg);
        if let Err(e) = c.start().await {
            ::tracing::error!("Error consul start {:?}", e);
        }
        Some(c)
    } else {
        None
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

    #[cfg(feature = "consul")]
    if let Some(c) = srv_consul {
        if let Err(e) = c.stop().await {
            ::tracing::error!("Error consul stop {:?}", e);
        }
    }

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
