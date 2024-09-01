use std::str::FromStr;

use tracing_subscriber::{filter, prelude::*};

use crate::config;

pub fn init_subscriber(cfg: &config::Config) -> anyhow::Result<()> {
    let level = cfg
        .tracing
        .clone()
        .and_then(|t| t.level)
        .unwrap_or("INFO".to_string());
    let level = tracing::Level::from_str(&level)?;

    let registry = tracing_subscriber::registry();

    #[cfg(feature = "console")]
    let registry = registry.with(console_subscriber::spawn());

    let registry = registry.with(
        tracing_subscriber::fmt::layer()
            .with_filter(filter::Targets::new().with_target("hsmq", level)),
    );

    #[cfg(feature = "sentry")]
    let registry = registry.with(sentry_tracing::layer());

    registry.init();
    Ok(())
}
