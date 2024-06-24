use std::fmt::Display;

use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{de::DeserializeOwned, Serialize};

use crate::{config, metrics::JWT_COUNTER};

#[derive(Debug, PartialEq, Eq)]
pub enum JwtError {
    InitError,
    ValidationError(jsonwebtoken::errors::Error),
    EncodeError(jsonwebtoken::errors::Error),
}

impl Display for JwtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JwtError")
    }
}

impl std::error::Error for JwtError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            JwtError::InitError => None,
            JwtError::ValidationError(e) => Some(e),
            JwtError::EncodeError(e) => Some(e),
        }
    }
}

#[derive(Clone)]
struct Secret {
    name: String,
    secret: DecodingKey,
    encoding_key: EncodingKey,
}

#[derive(Default, Clone)]
pub struct JWT {
    secrets: Vec<Secret>,
}

impl JWT {
    pub fn new(cfg: config::JWT) -> Self {
        let mut result = Self::default();
        for (n, secret) in cfg.secrets.iter().enumerate() {
            let name = secret.get_name().unwrap_or_else(|| n.to_string());
            if let Some(secret) = secret.resolve() {
                let encoding_key = EncodingKey::from_secret(secret.as_ref());
                let secret = DecodingKey::from_secret(secret.as_ref());
                let secret = Secret {
                    name,
                    secret,
                    encoding_key,
                };
                result.secrets.push(secret);
            }
        }
        result
    }

    pub fn encode<T>(&self, claims: T) -> Result<String, JwtError>
    where
        T: Serialize,
    {
        self.secrets
            .first()
            .map(|secret| {
                encode(&Header::default(), &claims, &secret.encoding_key)
                    .map_err(JwtError::EncodeError)
            })
            .unwrap_or(Err(JwtError::InitError))
    }

    pub fn decode<T>(&self, token: &str) -> Result<T, JwtError>
    where
        T: DeserializeOwned,
    {
        let mut e = JwtError::InitError;
        for secret in self.secrets.iter() {
            match decode(token, &secret.secret, &Validation::default()) {
                Ok(token) => {
                    JWT_COUNTER.with_label_values(&[&secret.name]).inc();
                    return Ok(token.claims);
                }
                Err(err) => {
                    log::error!("Validation error: {:?}", err);
                    e = JwtError::ValidationError(err);
                }
            }
        }
        Err(e)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    use super::JWT;
    use crate::{config, utils};

    #[derive(Serialize, Deserialize, Default, Debug)]
    struct Claims {
        exp: f64,
    }

    const SECRET: &str = "secret";

    #[tokio::test]
    async fn jwt_decode_ok() {
        let mut claims = Claims::default();
        claims.exp = (utils::current_time() + Duration::from_secs(3600)).as_secs_f64();

        let mut cfg = config::JWT::default();
        cfg.secrets
            .push(crate::config::ResolvableValue::Value(SECRET.to_string()));
        let jwt = JWT::new(cfg);

        let token = jwt.encode(claims).unwrap();
        let result = jwt.decode::<Claims>(&token);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn jwt_decode_expired() {
        let claims = Claims::default();
        let mut cfg = config::JWT::default();
        cfg.secrets
            .push(crate::config::ResolvableValue::Value(SECRET.to_string()));
        let jwt = JWT::new(cfg);

        let token = jwt.encode(claims).unwrap();
        let result = jwt.decode::<Claims>(&token);
        log::error!("Error {:?}", &result);
        assert!(result.is_err());
    }
}
