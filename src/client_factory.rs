use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use jsonwebtoken::{encode, EncodingKey, Header};
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tonic::codegen::InterceptedService;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::auth::Claims;
use crate::config::Config;
use crate::pb::hsmq_client::HsmqClient;
use crate::utils;

#[derive(Clone, Debug)]
pub struct ClientFactory {
    config: Config,
}

impl ClientFactory {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn gen_jwt<T>(&self, claims: T) -> Option<String>
    where
        T: Serialize,
    {
        let mut result = None;
        for secret in self
            .config
            .auth
            .jwt
            .clone()
            .expect("Not found jwt secrets")
            .secrets
            .iter()
        {
            if let Some(secret) = secret.resolve() {
                result = encode(
                    &Header::default(),
                    &claims,
                    &EncodingKey::from_secret(secret.as_ref()),
                )
                .ok();
                break;
            }
        }
        result
    }

    pub async fn create_client(
        &self,
    ) -> anyhow::Result<
        HsmqClient<
            InterceptedService<Channel, impl Fn(Request<()>) -> Result<Request<()>, Status>>,
        >,
    > {
        let grpc_uri = self.config.client.grpc_uri.clone().unwrap();
        let channel = Channel::from_shared(grpc_uri)?.connect().await?;

        let username = self.config.client.username.as_ref().unwrap();
        let users = if let Some(user) = self.config.users.get(username) {
            let mut result = HashMap::new();
            result.insert(username.clone(), user.clone());
            result
        } else {
            self.config.users.clone()
        };

        let claims = Claims {
            sub: username.clone(),
            exp: Some((utils::current_time() + Duration::from_secs(3600)).as_secs() as usize),
        };
        let token = if let Some(token) = self.gen_jwt(claims) {
            Some(token)
        } else {
            let mut token = None;
            for user in users.values() {
                for t in user.tokens.iter() {
                    if let Some(t) = t.resolve() {
                        token = Some(t);
                    };
                }
            }
            token
        };
        let header: MetadataValue<_> = if let Some(token) = token {
            format!("Bearer {}", token)
        } else {
            let up = format!("{}:", username);
            format!("Basic {}", STANDARD.encode(up))
        }
        .parse()?;
        let client = HsmqClient::with_interceptor(channel, move |mut req: Request<()>| {
            req.metadata_mut().insert("authorization", header.clone());
            Ok(req)
        });
        Ok(client)
    }
}
