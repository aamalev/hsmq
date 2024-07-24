use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{config, errors::AuthError, jwt::JWT};

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct Claims {
    pub sub: String,
    #[serde(default)]
    pub exp: Option<usize>,
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

#[derive(Default)]
pub struct Auth {
    tokens: HashMap<String, Arc<User>>,
    pub jwt: Option<JWT>,
}

impl Auth {
    pub fn new(cfg: config::Auth) -> Self {
        Self {
            jwt: cfg.jwt.map(JWT::new),
            ..Default::default()
        }
    }

    pub fn with_users(mut self, users: HashMap<String, config::User>) -> Self {
        for (username, cfg) in users {
            let u = Arc::new(User { username });
            for token in cfg.tokens {
                if let Some(token) = token.resolve() {
                    self.tokens.insert(token, u.clone());
                }
            }
        }
        self
    }

    pub fn authorize(&self, authorization: &str) -> Result<Arc<User>, AuthError> {
        if let Some((prefix, token)) = authorization.split_once(' ') {
            match prefix {
                "Bearer" => {
                    if let Some(user) = self.tokens.get(token) {
                        Ok(user.clone())
                    } else if let Some(ref jwt) = self.jwt {
                        let claims: Claims = jwt.decode(token)?;
                        Ok(Arc::new(claims.into()))
                    } else {
                        Err(AuthError)
                    }
                }
                _ => Err(AuthError),
            }
        } else {
            Err(AuthError)
        }
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
    use std::{collections::HashMap, sync::Arc, time::Duration, vec};

    use crate::{
        config::{self, ResolvableValue},
        utils,
    };

    use super::{Auth, Claims, User};

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
        let auth = Auth::new(cfg).with_users(users);
        let result = auth.authorize(&format!("Bearer {}", token));
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn authorize_by_jwt() {
        let mut cfg = config::Auth::default();
        let mut jwt = config::JWT::default();
        jwt.secrets
            .push(crate::config::ResolvableValue::Value("123".to_string()));
        cfg.jwt = Some(jwt);
        let auth = Auth::new(cfg);
        let mut claims = Claims::default();
        claims.exp = Some((utils::current_time() + Duration::from_secs(3600)).as_secs() as usize);
        let token = auth
            .jwt
            .as_ref()
            .map(|j| j.encode(claims).unwrap())
            .unwrap();
        let result = auth.authorize(&format!("Bearer {}", token));
        assert!(result.is_ok());
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
