use http::uri::Uri;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use tokio::{net::UdpSocket, select};
use tokio_util::task::task_tracker::TaskTracker;

use crate::config;

struct Node {
    _grpc_uri: Uri,
    _queue_consumers: HashMap<String, u64>,
    _queue_producers: HashMap<String, u64>,
}

pub struct Cluster {
    cfg: config::Cluster,
    _nodes: HashMap<SocketAddr, Node>,
    task_tracker: TaskTracker,
}

impl Cluster {
    pub fn new(cfg: config::Cluster, task_tracker: TaskTracker) -> Self {
        Self {
            cfg,
            _nodes: HashMap::new(),
            task_tracker,
        }
    }
    pub async fn run(self) {
        log::info!("Run {} on UDP:{:?}", &self.cfg.name, &self.cfg.udp_address);
        let socket = UdpSocket::bind(self.cfg.udp_address).await.unwrap();
        loop {
            select! {
                _ = self.task_tracker.wait() => {break;}
                _ = socket.readable() => {}
            }

            let mut buf = Vec::with_capacity(1024);

            match socket.try_recv_buf_from(&mut buf) {
                Ok((n, _addr)) => {
                    println!("GOT {:?}", &buf[..n]);
                    break;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    log::error!("UDP receive error {:?}", e);
                }
            }
        }
        log::info!("Stopped");
    }
}
