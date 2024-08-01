use std::str::FromStr;

use tracing_subscriber::{filter, prelude::*};

use crate::config;
use crate::errors::GenericError;

pub fn init_subscriber(cfg: &config::Config) -> Result<(), GenericError> {
    let level = cfg
        .tracing
        .clone()
        .and_then(|t| t.level)
        .unwrap_or("INFO".to_string());
    let level = tracing::Level::from_str(&level)?;
    let fltr = filter::Targets::new().with_target("hsmq", level);

    let registry = tracing_subscriber::registry()
        .with(fltr)
        .with(tracing_subscriber::fmt::layer());

    #[cfg(feature = "sentry")]
    let registry = registry.with(sentry_tracing::layer());

    registry.init();
    Ok(())
}
