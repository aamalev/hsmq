#[cfg(feature = "sentry")]
use sentry::IntoDsn;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Client {
    pub grpc_uri: Option<String>,
    pub username: Option<String>,
    pub http_port: Option<u16>,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            grpc_uri: None,
            username: None,
            http_port: Some(8081),
        }
    }
}

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
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

impl Default for Prometheus {
    fn default() -> Self {
        Self {
            url: default_metrics_path(),
            http_address: None,
            labels: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct Tracing {
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default)]
    pub with_ansi: Option<bool>,
}

#[cfg(feature = "consul")]
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct ConsulServiceCheck {
    pub name: String,
    pub interval: String,
    pub http: ResolvableValue,
    pub grpc: ResolvableValue,
}

#[cfg(feature = "consul")]
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct ConsulService {
    pub name: String,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub check: Option<ConsulServiceCheck>,
}

#[cfg(feature = "consul")]
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Consul {
    pub address: String,
    #[serde(default)]
    pub service: Option<ConsulService>,
}

#[cfg(feature = "sentry")]
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct Sentry {
    #[serde(default)]
    pub dsn: Option<std::borrow::Cow<'static, str>>,
    #[serde(default)]
    pub env: Option<std::borrow::Cow<'static, str>>,
    #[serde(default)]
    pub sample_rate: Option<f32>,
    #[serde(default)]
    pub traces_sample_rate: f32,
    #[serde(default)]
    pub max_breadcrumbs: Option<usize>,
}

#[cfg(feature = "sentry")]
impl From<Sentry> for sentry::ClientOptions {
    fn from(value: Sentry) -> Self {
        let mut result = Self {
            dsn: value.dsn.and_then(|dsn| dsn.into_dsn().unwrap_or_default()),
            release: sentry::release_name!(),
            environment: value.env,
            ..Default::default()
        };
        result.sample_rate = value.sample_rate.unwrap_or(result.sample_rate);
        result.traces_sample_rate = value.traces_sample_rate;
        result.max_breadcrumbs = value.max_breadcrumbs.unwrap_or(result.max_breadcrumbs);
        result
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct Cluster {
    pub name: String,
    pub udp_port: u16,
    pub jwt: Option<JWT>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct JWT {
    pub secrets: Vec<ResolvableValue>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct Auth {
    pub jwt: Option<JWT>,
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

fn default_prefetch_count() -> usize {
    1
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(tag = "type")]
pub enum Queue {
    InMemory(InMemoryQueue),
    #[cfg(feature = "redis")]
    RedisStream(RedisStreamConfig),
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct InMemoryQueue {
    pub name: String,
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub ack_timeout: Duration,
    #[serde(default = "default_prefetch_count")]
    pub prefetch_count: usize,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(untagged)]
pub enum Stream {
    String(String),
    Params {
        name: String,
        #[serde(default)]
        readonly: bool,
    },
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct RedisConfig {
    #[cfg(feature = "redis-cluster")]
    pub nodes: Vec<String>,
    #[cfg(not(feature = "redis-cluster"))]
    pub uri: String,
    #[serde(default)]
    pub username: Option<ResolvableValue>,
    #[serde(default)]
    pub password: Option<ResolvableValue>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct RedisStreamGroupCleanConfig {
    #[serde(default)]
    pub every: Duration,
    pub max_idle: Duration,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(untagged)]
pub enum RedisStreamGroupConfig {
    Name(String),
    Group {
        name: String,
        #[serde(default)]
        init: bool,
        #[serde(default)]
        clear: Option<RedisStreamGroupCleanConfig>,
    },
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct RedisStreamConfig {
    pub name: String,
    #[serde(default)]
    pub connector: String,
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default)]
    pub maxlen: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    pub group: RedisStreamGroupConfig,
    #[serde(default)]
    pub nomkstream: bool,
    pub streams: Vec<Stream>,
    #[serde(default)]
    pub ttl_key: Option<Duration>,
    #[serde(default = "RedisStreamConfig::default_body_fieldname")]
    pub body_fieldname: String,
    #[serde(default = "RedisStreamConfig::default_body_type_fieldname")]
    pub body_type_fieldname: String,
    #[serde(default)]
    pub read_limit: Option<usize>,
}

impl RedisStreamConfig {
    pub fn default_body_type_fieldname() -> String {
        "type".to_string()
    }
    pub fn default_body_fieldname() -> String {
        "body".to_string()
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct Config {
    pub client: Client,
    #[serde(default)]
    pub node: Node,
    pub tracing: Option<Tracing>,
    pub prometheus: Option<Prometheus>,
    pub cluster: Option<Cluster>,
    #[serde(default)]
    pub queues: Vec<Queue>,
    #[serde(default)]
    pub auth: Auth,
    #[serde(default)]
    pub users: HashMap<String, User>,
    #[cfg(feature = "sentry")]
    #[serde(default)]
    pub sentry: Sentry,
    #[cfg(feature = "consul")]
    pub consul: Option<Consul>,
    #[serde(default)]
    pub redis: HashMap<String, RedisConfig>,
}

impl Config {
    pub fn from_file(path: &Path) -> anyhow::Result<Self> {
        match Self::load_file(path) {
            Ok(cfg) => Ok(cfg),
            Err(e) => {
                log::error!("Error while load config {:?}", path);
                Err(e)
            }
        }
    }
    fn load_file(path: &Path) -> anyhow::Result<Self> {
        let mut f = File::open(path)?;
        let mut s = vec![];
        f.read_to_end(&mut s)?;
        let s = String::from_utf8(s)?;
        let result = toml::from_str(&s)?;
        log::debug!("Loaded: {:?}", &result);
        Ok(result)
    }

    pub fn cluster_jwt(&self) -> JWT {
        self.cluster
            .clone()
            .unwrap_or_default()
            .jwt
            .or(self.auth.jwt.clone())
            .unwrap_or_default()
    }

    pub fn cluster_name(&self) -> Option<String> {
        self.cluster.as_ref().map(|c| c.name.clone())
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(untagged)]
pub enum ResolvableValue {
    Value(String),
    Resolvable {
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        env: Option<String>,
        #[serde(default)]
        file: Option<PathBuf>,
        #[serde(default)]
        json_field: Option<String>,
        #[serde(default)]
        default: Option<String>,
        #[serde(default)]
        disable: bool,
    },
}

impl ResolvableValue {
    pub fn get_name(&self) -> Option<String> {
        match self {
            ResolvableValue::Resolvable { name, .. } => name.clone(),
            _ => None,
        }
    }

    fn read_file(path: PathBuf) -> anyhow::Result<String> {
        let mut f = File::open(path)?;
        let mut v = vec![];
        f.read_to_end(&mut v)?;
        let s = String::from_utf8_lossy(&v);
        Ok(s.to_string())
    }

    pub fn resolve(&self) -> Option<String> {
        match self {
            ResolvableValue::Value(val) => Some(val.to_string()),
            ResolvableValue::Resolvable {
                name,
                env,
                file,
                default,
                json_field,
                disable,
            } => {
                if *disable {
                    return None;
                }
                let mut result = env
                    .clone()
                    .and_then(|env| {
                        std::env::var(env.clone())
                            .inspect_err(|_| {
                                if default.is_none() {
                                    tracing::error!(name = name, "Expect value in env {}", env,);
                                };
                            })
                            .ok()
                    })
                    .or_else(|| {
                        file.clone().and_then(|f| {
                            Self::read_file(f.clone())
                                .inspect_err(|_| {
                                    if default.is_none() {
                                        tracing::error!(
                                            name = name,
                                            "Expect value in file {:?}",
                                            f
                                        );
                                    };
                                })
                                .ok()
                        })
                    });
                if let Some(field) = json_field {
                    result = result
                        .and_then(|r| {
                            serde_json::from_str(&r)
                                .inspect_err(|e| {
                                    tracing::error!(
                                        error = e as &dyn std::error::Error,
                                        name = name,
                                        "Error decode json",
                                    )
                                })
                                .ok()
                        })
                        .and_then(|r| match r {
                            serde_json::Value::Object(m) => {
                                if let Some(v) = m.get(field) {
                                    match v {
                                        serde_json::Value::String(s) => Some(s.to_string()),
                                        serde_json::Value::Number(n) => Some(n.to_string()),
                                        _ => {
                                            tracing::error!(
                                                name = name,
                                                json.field = field,
                                                "Not string in json field",
                                            );
                                            None
                                        }
                                    }
                                } else {
                                    tracing::error!(
                                        name = name,
                                        json.field = field,
                                        "Not found field in json",
                                    );
                                    None
                                }
                            }
                            _ => {
                                tracing::error!(
                                    name = name,
                                    json.field = field,
                                    "Not object in json",
                                );
                                None
                            }
                        });
                }
                result.filter(|s| !s.is_empty()).or(default.clone())
            }
        }
    }
}

impl From<ResolvableValue> for String {
    fn from(value: ResolvableValue) -> String {
        value.resolve().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::ResolvableValue;

    #[tokio::test]
    async fn resolve_env_empty() {
        const ENV: &str = "resolve_env_empty";
        std::env::remove_var(ENV);
        let v = ResolvableValue::Resolvable {
            env: Some(ENV.to_string()),
            file: None,
            name: None,
            json_field: None,
            default: None,
            disable: false,
        };
        let result = v.resolve();
        tracing::info!("Result {:?}", result);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_env_empty_default() {
        const ENV: &str = "resolve_env_empty_default";
        std::env::remove_var(ENV);
        let v = ResolvableValue::Resolvable {
            env: Some(ENV.to_string()),
            file: None,
            name: None,
            json_field: None,
            default: Some("default".to_string()),
            disable: false,
        };
        let result = v.resolve();
        tracing::info!("Result {:?}", result);
        assert_eq!(result, Some("default".to_string()));
    }

    #[tokio::test]
    async fn resolve_env_exist_empty() {
        const ENV: &str = "resolve_env_exist_empty";
        std::env::set_var(ENV, "");
        let v = ResolvableValue::Resolvable {
            env: Some(ENV.to_string()),
            file: None,
            name: None,
            json_field: None,
            default: None,
            disable: false,
        };
        let result = v.resolve();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_env_json() {
        const ENV: &str = "resolve_env_json";
        std::env::set_var(ENV, "{\"W\": 123}");
        let v = ResolvableValue::Resolvable {
            env: Some(ENV.to_string()),
            file: None,
            name: None,
            json_field: Some("W".to_string()),
            default: None,
            disable: false,
        };
        let result = v.resolve();
        assert_eq!(result, Some("123".to_string()));
    }
}
