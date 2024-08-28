use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::{io::ErrorKind, net::UdpSocket, select};
use tokio_util::task::task_tracker::TaskTracker;

use crate::errors::GenericError;
use crate::jwt::JWT;
use crate::{config, utils};

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct Queue {
    #[serde(default)]
    consumers: usize,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct Node {
    pub addr: String,
    grpc_uri: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    queue: HashMap<String, Queue>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct JwtPackage {
    pub exp: u64,
    pub pack: Package,
    #[serde(skip)]
    pub addr: Option<SocketAddr>,
}

impl JwtPackage {
    pub fn new(pack: Package) -> Self {
        Self {
            pack,
            exp: utils::current_time().as_secs() + 99,
            addr: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(tag = "type")]
pub enum Package {
    Join { cluster: String, addr: String },
    Node { name: String, node: Node },
    Response { lines: Vec<String> },
}

impl From<Package> for JwtPackage {
    fn from(value: Package) -> Self {
        Self::new(value)
    }
}

pub struct JwtSocket {
    socket: UdpSocket,
    jwt: JWT,
}

impl JwtSocket {
    pub async fn bind(addr: &str, jwt: JWT) -> Result<Self, GenericError> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self { socket, jwt })
    }

    fn decode(&self, b: &[u8]) -> Result<JwtPackage, GenericError> {
        let s = String::from_utf8_lossy(b);
        let p = self.jwt.decode::<JwtPackage>(&s)?;
        Ok(p)
    }

    pub async fn readable(&self) -> std::io::Result<()> {
        self.socket.readable().await
    }

    #[allow(dead_code)]
    pub async fn wait_package(&self, timeout: Duration) -> Option<JwtPackage> {
        tokio::select! {
            _ = self.readable() => self.read_package(),
            _ = tokio::time::sleep(timeout) => {
                log::error!("Timeout message");
                None
            }
        }
    }

    pub fn read_package(&self) -> Option<JwtPackage> {
        let mut buf = Vec::with_capacity(1024);

        match self.socket.try_recv_buf_from(&mut buf) {
            Ok((n, addr)) => match self.decode(&buf[..n]) {
                Ok(mut p) => {
                    p.addr = Some(addr);
                    Some(p)
                }
                Err(e) => {
                    log::error!("Error decode {}", e);
                    None
                }
            },
            Err(e) if e.kind() == ErrorKind::WouldBlock => None,
            Err(e) => {
                log::error!("UDP receive error {:?}", e);
                None
            }
        }
    }

    fn encode(&self, pack: Package) -> Result<String, GenericError> {
        Ok(self.jwt.encode(JwtPackage::new(pack))?)
    }

    pub async fn send_to(&self, pack: Package, addr: &str) -> Result<(), GenericError> {
        let s = self.encode(pack)?;
        self.socket.send_to(s.as_bytes(), addr).await?;
        Ok(())
    }
}

pub struct Cluster {
    cfg: config::Cluster,
    node_name: String,
    nodes: HashMap<String, Node>,
    jwt: JWT,
    task_tracker: TaskTracker,
}

impl Cluster {
    pub fn new(cfg: config::Cluster, node_name: String, task_tracker: TaskTracker) -> Self {
        let jwt = JWT::new(cfg.jwt.clone().unwrap_or_default());
        Self {
            cfg,
            node_name,
            nodes: HashMap::new(),
            jwt,
            task_tracker,
        }
    }

    pub async fn run(mut self) {
        let address = format!("0.0.0.0:{}", self.cfg.udp_port);
        let socket = JwtSocket::bind(&address, self.jwt.clone()).await.unwrap();
        log::info!("Run {} on UDP:{:?}", &self.cfg.name, address);
        loop {
            select! {
                _ = self.task_tracker.wait() => {break;}
                _ = socket.readable() => {}
            }

            if let Some(pack) = socket.read_package() {
                match self.process_package(&socket, pack).await {
                    Ok(_) => (),
                    Err(e) => log::error!("Error processing {}", e),
                }
            }
        }
        log::info!("Stopped");
    }

    async fn process_package(
        &mut self,
        sock: &JwtSocket,
        jwt_pack: JwtPackage,
    ) -> Result<(), GenericError> {
        match &jwt_pack.pack {
            Package::Node { name, node } => {
                let mut node = node.clone();
                node.addr = jwt_pack.addr.map(|a| a.to_string()).unwrap_or_default();
                log::info!("Node {:?} {:?}", jwt_pack.addr, &node);
                if self.node_name.ne(name) {
                    self.nodes.insert(name.clone(), node);
                }
            }
            join @ Package::Join { addr, cluster } => {
                log::info!("Join {}", &addr);
                if self.cfg.name.eq(cluster) {
                    let mut lines = vec![];
                    for node in self.nodes.values() {
                        sock.send_to(join.clone(), node.addr.as_str()).await?;
                        lines.push(format!(
                            "[{}:{}] ReSend to {}",
                            &self.cfg.name,
                            &self.node_name,
                            node.addr.as_str(),
                        ));
                    }
                    let node = Node::default();
                    let pack = Package::Node {
                        name: self.node_name.clone(),
                        node,
                    };
                    sock.send_to(pack, addr).await?;
                    lines.push(format!(
                        "[{}:{}] Apply {}",
                        &self.cfg.name, &self.node_name, addr,
                    ));

                    let pack = Package::Response { lines };
                    let addr = jwt_pack.addr.map(|x| x.to_string()).unwrap_or_default();
                    sock.send_to(pack, &addr).await?;
                }
            }
            _ => (),
        }
        Ok(())
    }
}
