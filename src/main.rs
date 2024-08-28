use clap::{command, Parser};
use std::path::PathBuf;

use hsmq::config::Config;
use hsmq::errors::GenericError;
use hsmq::launcher::run;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,
}

fn main() -> Result<(), GenericError> {
    let cli = Cli::parse();

    let cfg = if let Some(config_path) = cli.config.as_deref() {
        Config::from_file(config_path)?
    } else {
        Config::default()
    };

    #[cfg(feature = "sentry")]
    let _guard = sentry::init(cfg.sentry.clone());

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(cfg, None))?;

    opentelemetry::global::shutdown_tracer_provider();
    Ok(())
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}
