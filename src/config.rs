use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::Path;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Node {
    pub name: Option<String>,
    pub grpc_address: Option<SocketAddr>,
    pub http_address: Option<SocketAddr>,
}

fn default_metrics_path() -> String {
    "/metrics".to_string()
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Prometheus {
    #[serde(default = "default_metrics_path")]
    pub url: String,
    pub http_address: Option<SocketAddr>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Cluster {
    pub name: String,
    pub udp_address: SocketAddr,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct User {
    #[serde(default)]
    pub tokens: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Queue {
    pub name: String,
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct Config {
    pub node: Option<Node>,
    pub prometheus: Option<Prometheus>,
    pub cluster: Option<Cluster>,
    #[serde(default)]
    pub queues: Vec<Queue>,
    #[serde(default)]
    pub users: HashMap<String, User>,
}

impl Config {
    pub fn from_file(path: &Path) -> Self {
        let mut f = File::open(path).unwrap_or_else(|_| panic!("File not found {:?}", path));
        let mut s = vec![];
        f.read_to_end(&mut s).expect("Corrupted file");
        let s = String::from_utf8(s).unwrap();
        let result = toml::from_str(&s).expect("Toml is incorrect");
        log::debug!("Loaded: {:?}", &result);
        result
    }
}
