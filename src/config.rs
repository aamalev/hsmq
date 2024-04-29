use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::Path;

fn default_grpc_addr() -> Option<SocketAddr> {
    "0.0.0.0:4848".parse().ok()
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Node {
    pub name: Option<String>,
    #[serde(default = "default_grpc_addr")]
    pub grpc_address: Option<SocketAddr>,
    pub http_address: Option<SocketAddr>,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            grpc_address: default_grpc_addr(),
            name: None,
            http_address: None,
        }
    }
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

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct JWT {
    pub secrets: Vec<ResolvableValue>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct Auth {
    pub jwt: JWT,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct User {
    #[serde(default)]
    pub tokens: Vec<ResolvableValue>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(untagged)]
pub enum Duration {
    Days { d: f32 },
    Hours { h: f32 },
    Minutes { m: f32 },
    SecondsFloat(f32),
    Seconds { s: f32 },
    MilliSeconds { ms: u64 },
}

impl Default for Duration {
    fn default() -> Self {
        Duration::Seconds { s: 60.0 }
    }
}

impl From<Duration> for std::time::Duration {
    fn from(d: Duration) -> std::time::Duration {
        match d {
            Duration::Days { d } => std::time::Duration::from_secs_f32(d * 24.0 * 60.0 * 60.0),
            Duration::Hours { h } => std::time::Duration::from_secs_f32(h * 60.0 * 60.0),
            Duration::Minutes { m } => std::time::Duration::from_secs_f32(m * 60.0),
            Duration::SecondsFloat(s) => std::time::Duration::from_secs_f32(s),
            Duration::Seconds { s } => std::time::Duration::from_secs_f32(s),
            Duration::MilliSeconds { ms } => std::time::Duration::from_millis(ms),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Queue {
    pub name: String,
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub ack_timeout: Duration,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct Config {
    pub node: Node,
    pub prometheus: Option<Prometheus>,
    pub cluster: Option<Cluster>,
    #[serde(default)]
    pub queues: Vec<Queue>,
    #[serde(default)]
    pub auth: Auth,
    #[serde(default)]
    pub user: HashMap<String, User>,
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        match Self::load_file(path) {
            Ok(cfg) => Ok(cfg),
            Err(e) => {
                log::error!("Error while load config {:?}", path);
                Err(e)
            }
        }
    }
    fn load_file(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let mut f = File::open(path)?;
        let mut s = vec![];
        f.read_to_end(&mut s)?;
        let s = String::from_utf8(s)?;
        let result = toml::from_str(&s)?;
        log::debug!("Loaded: {:?}", &result);
        Ok(result)
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(untagged)]
pub enum ResolvableValue {
    Value(String),
    Env {
        #[serde(default)]
        name: Option<String>,
        env: String,
        #[serde(default)]
        default: Option<String>,
        #[serde(default)]
        disable: bool,
    },
}

impl ResolvableValue {
    pub fn get_name(&self) -> Option<String> {
        match self {
            ResolvableValue::Env { name, .. } => name.clone(),
            _ => None,
        }
    }

    pub fn resolve(&self) -> Option<String> {
        match self {
            ResolvableValue::Value(val) => Some(val.to_string()),
            ResolvableValue::Env {
                name,
                env,
                default,
                disable,
            } => {
                if *disable {
                    return None;
                }
                match std::env::var(env) {
                    Ok(result) => Some(result),
                    Err(_) => {
                        if let Some(default) = default {
                            Some(default.clone())
                        } else {
                            match name {
                                Some(name) => log::error!("Expect value in {} for {}", env, name),
                                _ => log::error!("Expect value in {}", env),
                            };
                            None
                        }
                    }
                }
            }
        }
    }
}

impl From<ResolvableValue> for String {
    fn from(value: ResolvableValue) -> String {
        value.resolve().unwrap_or_default()
    }
}
