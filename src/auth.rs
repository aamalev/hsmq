use std::{collections::HashMap, sync::Arc};

use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{config, errors::AuthError, metrics::JWT_COUNTER};

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
struct Claims {
    sub: String,
    exp: usize,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct User {
    username: String,
}

impl From<Claims> for User {
    fn from(value: Claims) -> Self {
        Self {
            username: value.sub,
        }
    }
}

struct JwtSecret {
    name: String,
    secret: DecodingKey,
}

#[derive(Default)]
pub struct Auth {
    pub tokens: HashMap<String, Arc<User>>,
    jwt_secrets: Vec<JwtSecret>,
}

impl Auth {
    pub fn new(cfg: config::Auth, users: HashMap<String, config::User>) -> Self {
        let mut result = Self::default();
        for (username, cfg) in users {
            let u = Arc::new(User { username });
            for token in cfg.tokens {
                if let Some(token) = token.resolve() {
                    result.tokens.insert(token, u.clone());
                }
            }
        }
        for (n, secret) in cfg.jwt.secrets.iter().enumerate() {
            let name = secret.get_name().unwrap_or_else(|| n.to_string());
            if let Some(secret) = secret.resolve() {
                let secret = DecodingKey::from_secret(secret.as_ref());
                let secret = JwtSecret { name, secret };
                result.jwt_secrets.push(secret);
            }
        }
        result
    }

    pub fn authorize(&self, authorization: &str) -> Result<Arc<User>, AuthError> {
        if let Some((prefix, token)) = authorization.split_once(' ') {
            match prefix {
                "Bearer" => {
                    if let Some(user) = self.tokens.get(token) {
                        Ok(user.clone())
                    } else {
                        let claims: Claims = self.jwt_decode(token)?;
                        Ok(Arc::new(claims.into()))
                    }
                }
                _ => Err(AuthError),
            }
        } else {
            Err(AuthError)
        }
    }

    pub fn jwt_decode<T>(&self, token: &str) -> Result<T, AuthError>
    where
        T: DeserializeOwned,
    {
        for secret in self.jwt_secrets.iter() {
            if let Ok(token) = decode(token, &secret.secret, &Validation::default()) {
                JWT_COUNTER.with_label_values(&[&secret.name]).inc();
                return Ok(token.claims);
            }
        }
        Err(AuthError)
    }
    pub fn grpc_check_auth(
        &self,
        req: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        match req.metadata().get("authorization") {
            Some(t) if self.authorize(t.to_str().unwrap_or_default()).is_ok() => Ok(req),
            a => Err(tonic::Status::unauthenticated(format!(
                "No valid auth token {:?}",
                a
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, vec};

    use crate::config::{self, ResolvableValue};

    use super::{Auth, Claims, User};
    use jsonwebtoken::{encode, EncodingKey, Header};

    #[tokio::test]
    async fn authorize_fail() {
        let header = "Bearer 123";
        let auth = Auth::default();
        let result = auth.authorize(header);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn authorize_by_token() {
        let token = "123".to_string();
        let user = config::User {
            tokens: vec![ResolvableValue::Value(token.clone())],
        };
        let mut users = HashMap::new();
        users.insert("user".to_string(), user);
        let cfg = config::Auth::default();
        let auth = Auth::new(cfg, users);
        let result = auth.authorize(&format!("Bearer {}", token));
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn jwt_decode_ok() {
        let secret = "secret".to_string();
        let mut claims = Claims::default();
        claims.exp = 1000000000000000000;

        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_ref()),
        )
        .unwrap();
        let mut cfg = config::Auth::default();
        cfg.jwt
            .secrets
            .push(crate::config::ResolvableValue::Value(secret));
        let auth = Auth::new(cfg, HashMap::new());
        let result = auth.jwt_decode::<Claims>(&token);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn jwt_decode_expired() {
        let secret = "secret".to_string();
        let mut claims = Claims::default();
        claims.exp = 1;

        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_ref()),
        )
        .unwrap();
        let mut cfg = config::Auth::default();
        cfg.jwt
            .secrets
            .push(crate::config::ResolvableValue::Value(secret));
        let auth = Auth::new(cfg, HashMap::new());
        let result = auth.jwt_decode::<Claims>(&token);
        log::error!("Error {:?}", &result);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn grpc_check_auth_deny() {
        let req = tonic::Request::new(());
        let auth = Auth::default();
        let result = auth.grpc_check_auth(req);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn grpc_check_auth_allow() {
        let token = "123".to_string();
        let header: tonic::metadata::MetadataValue<_> =
            format!("Bearer {}", token).parse().unwrap();
        let mut req = tonic::Request::new(());
        req.metadata_mut().insert("authorization", header);
        let mut auth = Auth::default();
        let user = Arc::new(User::default());
        auth.tokens.insert(token, user);
        let result = auth.grpc_check_auth(req);
        assert!(result.is_ok());
    }
}
