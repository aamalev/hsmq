use std::collections::HashMap;
use tokio::sync::RwLock;
use vaultrs::{auth, client::Client as _};

use crate::config;

pub struct Client {
    cfg: config::Vault,
    client: vaultrs::client::VaultClient,
    kv: RwLock<HashMap<String, HashMap<String, String>>>,
}

impl Client {
    pub fn new(cfg: config::Vault) -> anyhow::Result<Self> {
        let mut builder = vaultrs::client::VaultClientSettingsBuilder::default();

        if let Some(uri) = cfg.uri.clone() {
            builder.address(uri);
        }

        if let Some(token) = cfg.token.clone().and_then(|r| r.resolve()) {
            builder.token(token);
        }

        if let Some(verify) = cfg.verify {
            builder.verify(verify);
        }
        let mut settings = builder.build()?;
        if let Some(cert) = cfg.ca_cert.clone() {
            settings.ca_certs.push(cert);
        }

        let client = vaultrs::client::VaultClient::new(settings)?;

        Ok(Client {
            cfg,
            client,
            kv: Default::default(),
        })
    }

    pub async fn login(&mut self) -> anyhow::Result<()> {
        match self.cfg.auth {
            Some(config::VaultAuth::JWT { ref jwt, ref role }) => {
                let jwt = jwt.resolve().unwrap_or_default();
                let result = auth::oidc::login(&self.client, "jwt", &jwt, role.clone()).await?;
                self.client.set_token(&result.client_token);
            }
            None => (),
        };
        Ok(())
    }

    #[tracing::instrument(name = "vault.get_kv", skip(self), err)]
    async fn get_kv(
        &self,
        mount: &str,
        path: &str,
        v: u8,
    ) -> anyhow::Result<HashMap<String, String>> {
        if let Some(result) = self.kv.read().await.get(path) {
            Ok(result.clone())
        } else {
            let result: Result<HashMap<_, _>, _> = if v == 1 {
                vaultrs::kv1::get(&self.client, mount, path).await
            } else {
                vaultrs::kv2::read(&self.client, mount, path).await
            };
            let result = result?;
            self.kv
                .write()
                .await
                .insert(path.to_string(), result.clone());
            Ok(result)
        }
    }
}

#[tonic::async_trait]
impl config::Resolver for Client {
    async fn resolve<'a>(&self, rv: &'a mut config::ResolvableValue) -> anyhow::Result<()> {
        if let config::ResolvableValue::Resolvable {
            json_field,
            vault_mount,
            vault_path: Some(path),
            mut vault_kv_version,
            value,
            disable: false,
            ..
        } = rv
        {
            let mount = vault_mount.as_ref().map(String::as_str).unwrap_or("secret");
            if vault_kv_version != 1 {
                vault_kv_version = 2;
            };
            let m = self.get_kv(mount, path, vault_kv_version).await?;
            if let Some(field) = json_field {
                if let Some(v) = m.get(field) {
                    *value = Some(v.to_string());
                };
            }
        }
        Ok(())
    }
}
