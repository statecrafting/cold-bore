//! coldbore-ingest: the consumer side of the pipeline.
//!
//! Classic mode (phase 1): manual-ack consumer on the durable frames queue,
//! batched idempotent inserts into TimescaleDB, ack strictly after commit.
//! Stream mode lands in phase 3. See docs/design/architecture.md §5;
//! spec 005-ingest-consumer.

mod consume;
mod consume_stream;
mod control;
mod gap;
mod sink;
mod telemetry;

use std::time::Duration;

use coldbore_proto::config::{IngestConfig, Mode};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = IngestConfig::from_env()?;
    info!(
        mode = cfg.common.mode.as_str(),
        prefetch = cfg.prefetch,
        batch_max_frames = cfg.batch_max_frames,
        batch_max_ms = cfg.batch_max_ms,
        "coldbore-ingest starting"
    );

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(cfg))
}

async fn run(cfg: IngestConfig) -> anyhow::Result<()> {
    // Accounting outlives connections: a reconnect is not an amnesty.
    let mut counters = telemetry::Counters::default();
    let mut gaps = gap::GapTracker::new();
    let mut backoff = Duration::from_millis(500);

    let supervisor = async {
        loop {
            let session = match cfg.common.mode {
                Mode::Classic => consume::run_classic(&cfg, &mut counters, &mut gaps).await,
                Mode::Stream => consume_stream::run_stream(&cfg, &mut counters, &mut gaps).await,
            };
            match session {
                Ok(()) => return Ok::<(), anyhow::Error>(()),
                Err(e) => {
                    warn!(error = %e, backoff_ms = backoff.as_millis() as u64, "consume session ended")
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(10));
        }
    };

    tokio::select! {
        result = supervisor => result,
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down");
            Ok(())
        }
    }
}
