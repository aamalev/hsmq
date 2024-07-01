pub mod errors;
pub mod stream;
use std::collections::HashMap;

use crate::{config::RedisConfig, errors::GenericError};
use redis::{cluster::ClusterClient, IntoConnectionInfo};

type RedisConnection = redis::cluster_async::ClusterConnection;

pub fn create_client(config: &RedisConfig) -> Result<ClusterClient, GenericError> {
    let password = config.password.clone().map(String::from);
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
            i.redis.username.clone_from(&config.username);
            i.redis.password.clone_from(&password);
            i
        });
    let redis = ClusterClient::new(nodes).inspect_err(|e| {
        log::error!("Critical error with redis: {}", e);
    })?;
    Ok(redis)
}

pub struct Connectors {
    cfg: HashMap<String, RedisConfig>,
    m: HashMap<String, ClusterClient>,
}

impl Connectors {
    pub fn new(cfg: HashMap<String, RedisConfig>) -> Self {
        let m = HashMap::new();
        Self { m, cfg }
    }

    pub async fn get_connection(&mut self, name: &str) -> Result<RedisConnection, GenericError> {
        let connector = if let Some(c) = self.m.get(name) {
            c.get_async_connection().await?
        } else if let Some(cfg_connector) = self.cfg.get(name) {
            let c = crate::redis::create_client(cfg_connector).unwrap();
            self.m.insert(name.to_string(), c);
            self.m[name].get_async_connection().await?
        } else {
            panic!("Not found redis-connector {}", name);
        };
        Ok(connector)
    }
}
