use axum::{response::IntoResponse, routing, Router};
use prometheus::{Encoder, TextEncoder};
use std::net::SocketAddr;
use tokio_util::task::task_tracker::TaskTracker;

use crate::config;

async fn internal_metrics() -> impl IntoResponse {
    let encoder = TextEncoder::new();

    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();

    ([("content-type", "text/plain")], buffer)
}

pub struct WebServer {
    pub addr: SocketAddr,
    pub router: Router,
    task_tracker: TaskTracker,
}
impl WebServer {
    pub fn new(addr: SocketAddr, task_tracker: TaskTracker) -> Self {
        let router = Router::new();
        Self {
            addr,
            router,
            task_tracker,
        }
    }

    pub fn serve_metrics(mut self, cfg: config::Prometheus) -> Self {
        self.router = self.router.route(&cfg.url, routing::get(internal_metrics));
        self
    }

    pub async fn run(self) {
        log::info!("Listening on http://{}", &self.addr);
        let listener = tokio::net::TcpListener::bind(self.addr).await.unwrap();
        let task_tracker = self.task_tracker.clone();
        axum::serve(listener, self.router)
            .with_graceful_shutdown(async move { task_tracker.wait().await })
            .await
            .unwrap();
        log::info!("Stopped");
    }
}
