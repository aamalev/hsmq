use consulrs::api::check::common::AgentServiceCheckBuilder;
use consulrs::api::service::requests::RegisterServiceRequest;
use consulrs::client::{ConsulClient, ConsulClientSettingsBuilder};
use consulrs::{error::ClientError, service};

use crate::{config, errors::GenericError};

pub struct Consul {
    cfg: config::Consul,
    client: ConsulClient,
}

impl Consul {
    pub fn new(cfg: config::Consul) -> Self {
        let client = ConsulClient::new(
            ConsulClientSettingsBuilder::default()
                .address(cfg.address.as_str())
                .build()
                .unwrap(),
        )
        .unwrap();
        Self { cfg, client }
    }

    async fn service_register(&self, cfg: &config::ConsulService) -> Result<(), GenericError> {
        let mut builder = RegisterServiceRequest::builder();
        if !cfg.tags.is_empty() {
            builder.tags(cfg.tags.clone());
        }
        if let Some(ref addr) = cfg.address {
            builder.address(addr);
        }
        if let Some(port) = cfg.port {
            builder.port(port);
        }
        if let Some(ref check) = cfg.check {
            let mut checker = AgentServiceCheckBuilder::default();
            if let Some(grpc) = check.grpc.resolve() {
                checker.grpc(String::from(grpc.clone()));
            } else {
                checker.http(String::from(check.http.clone()));
            }
            builder.check(
                checker
                    .name(check.name.as_str())
                    .interval(check.interval.as_str())
                    .status("passing")
                    .build()?,
            );
        }
        service::register(&self.client, &cfg.name, Some(&mut builder)).await?;
        Ok(())
    }

    pub async fn start(&self) -> Result<(), GenericError> {
        if let Some(ref cfg) = self.cfg.service {
            self.service_register(cfg).await?;
        }
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), ClientError> {
        if let Some(ref cfg) = self.cfg.service {
            service::deregister(&self.client, &cfg.name, None).await?;
        }
        Ok(())
    }
}
