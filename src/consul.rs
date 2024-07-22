use consulrs::api::check::common::AgentServiceCheckBuilder;
use consulrs::api::service::requests::RegisterServiceRequest;
use consulrs::client::{ConsulClient, ConsulClientSettingsBuilder};
use consulrs::{error::ClientError, service};

use crate::config;

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

    async fn service_register(&self, cfg: &config::ConsulService) -> Result<(), ClientError> {
        let mut builder = RegisterServiceRequest::builder();
        if let Some(ref addr) = cfg.address {
            builder.address(addr);
        }
        if let Some(port) = cfg.port {
            builder.port(port);
        }
        if let Some(ref check) = cfg.check {
            let checker = AgentServiceCheckBuilder::default()
                .name(check.name.as_str())
                .interval(check.interval.as_str())
                .http(String::from(check.http.clone()))
                .status("passing")
                .build()
                .unwrap();
            builder.check(checker);
        }
        service::register(&self.client, &cfg.name, Some(&mut builder)).await?;
        Ok(())
    }

    pub async fn start(&self) -> Result<(), ClientError> {
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
