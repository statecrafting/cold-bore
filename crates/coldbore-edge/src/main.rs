//! coldbore-edge: the field side of the pipeline.
//!
//! Simulates N pads x M wells of frac telemetry and publishes it to the
//! frames exchange with publisher confirms. Owns the store-and-forward
//! buffer (severed uplinks), the confirm/retransmit window, and every
//! injectable fault in the publish path (dup, reorder, rate). See
//! docs/design/architecture.md §5 and §7; spec 004-edge-producer.

mod control;
mod faults;
mod sim;
mod telemetry;
mod uplink;
mod uplink_stream;

use std::sync::Arc;

use coldbore_proto::config::{EdgeConfig, Mode};
use coldbore_proto::now_ms;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = EdgeConfig::from_env()?;
    info!(
        pads = cfg.pads,
        wells_per_pad = cfg.wells_per_pad,
        rate_hz = cfg.rate_hz,
        mode = cfg.common.mode.as_str(),
        "coldbore-edge starting"
    );

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(cfg))
}

async fn run(cfg: EdgeConfig) -> anyhow::Result<()> {
    let faults = faults::FaultBox::new(cfg.pads, cfg.wells_per_pad);
    let counters = Arc::new(telemetry::Counters::default());
    let (tx, rx) = tokio::sync::mpsc::channel(8192);
    // The producer generation: frame identity and the stream producer's
    // dedup name both embed it.
    let epoch = now_ms();

    let mut generator = tokio::spawn(sim::generator(
        cfg.clone(),
        faults.clone(),
        counters.clone(),
        tx,
        epoch,
    ));
    let mut uplink = match cfg.common.mode {
        Mode::Classic => tokio::spawn(uplink::run(cfg, faults, counters, rx)),
        Mode::Stream => tokio::spawn(uplink_stream::run(cfg, faults, counters, rx, epoch)),
    };

    // Either core task ending outside a ctrl-c is a failure, and the process
    // must die loudly so the supervisor restarts it: a silent partial death
    // is how a pipeline zombifies.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down");
            generator.abort();
            uplink.abort();
            Ok(())
        }
        result = &mut generator => {
            uplink.abort();
            Err(anyhow::anyhow!("generator task ended unexpectedly: {result:?}"))
        }
        result = &mut uplink => {
            generator.abort();
            Err(anyhow::anyhow!("uplink task ended unexpectedly: {result:?}"))
        }
    }
}
