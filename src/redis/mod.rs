pub mod stream;

use redis::{cluster::ClusterClient, IntoConnectionInfo};

use crate::config;

pub fn create_client(
    nodes: &[String],
    username: &Option<String>,
    password: Option<config::ResolvableValue>,
) -> Result<ClusterClient, Box<dyn std::error::Error>> {
    let password = password.map(String::from);
    let nodes = nodes
        .iter()
        .filter_map(|n| {
            let result = n.clone().into_connection_info();
            if let Err(ref err) = result {
                log::error!("Error parse string '{}' as redis uri: {:?}", n, err);
            }
            result.ok()
        })
        .map(|mut i| {
            i.redis.username = username.clone();
            i.redis.password = password.clone();
            i
        });
    let redis = ClusterClient::new(nodes).inspect_err(|e| {
        log::error!("Critical error with redis: {}", e);
    })?;
    Ok(redis)
}
