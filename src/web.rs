use axum::{response::IntoResponse, routing, Router};
use prometheus::proto::LabelPair;
use prometheus::{Encoder, TextEncoder};
use std::net::SocketAddr;
use tokio_util::task::task_tracker::TaskTracker;

use crate::config;

fn internal_metrics(labels: Vec<LabelPair>) -> impl IntoResponse {
    let encoder = TextEncoder::new();

    let mut metric_families = prometheus::gather();
    for mf in metric_families.iter_mut() {
        let rm = mf.mut_metric();
        for i in rm.iter_mut() {
            let l = i.mut_label();
            for lp in labels.iter() {
                l.push(lp.to_owned());
            }
        }
    }
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
        let mut labels = vec![];
        for (k, v) in cfg.labels.into_iter() {
            let mut pair = LabelPair::new();
            pair.set_name(k);
            pair.set_value(v);
            labels.push(pair);
        }
        self.router = self.router.route(
            &cfg.url,
            routing::get(|| async move { internal_metrics(labels) }),
        );
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
