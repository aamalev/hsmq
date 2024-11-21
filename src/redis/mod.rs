pub mod errors;
pub mod stream;
use std::collections::HashMap;

use crate::config::RedisConfig;
use redis::IntoConnectionInfo;

#[cfg(feature = "redis-cluster")]
type Client = redis::cluster::ClusterClient;
#[cfg(not(feature = "redis-cluster"))]
type Client = redis::Client;

#[cfg(feature = "redis-cluster")]
type RedisConnection = redis::cluster_async::ClusterConnection;
#[cfg(not(feature = "redis-cluster"))]
type RedisConnection = redis::aio::MultiplexedConnection;

#[cfg(feature = "redis-cluster")]
pub fn create_client(config: &RedisConfig) -> anyhow::Result<Client> {
    let password = config
        .password
        .clone()
        .map(String::from)
        .filter(|s| !s.is_empty());
    let username = config
        .username
        .clone()
        .map(String::from)
        .filter(|s| !s.is_empty());
    let nodes = config
        .nodes
        .iter()
        .filter_map(|n| {
            let result = n.clone().into_connection_info();
            if let Err(ref err) = result {
                log::error!("Error parse string '{}' as redis uri: {:?}", n, err);
            }
            result.ok()
        })
        .map(|mut i| {
            i.redis.username.clone_from(&username);
            i.redis.password.clone_from(&password);
            i
        });
    let redis = Client::new(nodes).inspect_err(|e| {
        log::error!("Critical error with redis: {}", e);
    })?;
    Ok(redis)
}

#[cfg(not(feature = "redis-cluster"))]
pub fn create_client(config: &RedisConfig) -> Result<Client, GenericError> {
    let password = config.password.clone().map(String::from);
    let info = config.uri.clone().into_connection_info().map(|mut i| {
        i.redis.username.clone_from(&config.username);
        i.redis.password.clone_from(&password);
        i
    })?;

    let redis = Client::open(info).inspect_err(|e| {
        log::error!("Critical error with redis: {}", e);
    })?;
    Ok(redis)
}

pub struct Connectors {
    cfg: HashMap<String, RedisConfig>,
    m: HashMap<String, Client>,
}

impl Connectors {
    pub fn new(cfg: HashMap<String, RedisConfig>) -> Self {
        let m = HashMap::new();
        Self { m, cfg }
    }

    pub async fn get_connection(&mut self, name: &str) -> anyhow::Result<RedisConnection> {
        let c = if let Some(c) = self.m.get(name) {
            c
        } else if let Some(cfg_connector) = self.cfg.get(name) {
            let c = create_client(cfg_connector)?;
            self.m.insert(name.to_string(), c);
            &self.m[name]
        } else {
            panic!("Not found redis-connector {}", name);
        };
        #[cfg(feature = "redis-cluster")]
        let connector = c.get_async_connection().await?;
        #[cfg(not(feature = "redis-cluster"))]
        let connector = c.get_multiplexed_async_connection().await?;

        Ok(connector)
    }
}
