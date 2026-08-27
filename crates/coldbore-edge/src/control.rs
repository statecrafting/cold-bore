//! Control-plane consumer: an exclusive queue on the fanout control
//! exchange. Applies commands addressed to the edge and reports each one via
//! a `fault_applied` event.

use coldbore_proto::control::{ControlCommand, ServiceId};
use coldbore_proto::metrics::event_kind;
use coldbore_proto::topology::CONTROL_EXCHANGE;
use futures_util::StreamExt;
use lapin::Channel;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use tracing::{info, warn};

use crate::faults::FaultBox;
use crate::telemetry;

pub async fn run(channel: Channel, faults: FaultBox) {
    if let Err(e) = serve(channel, faults).await {
        warn!(error = %e, "control consumer stopped");
    }
}

async fn serve(channel: Channel, faults: FaultBox) -> anyhow::Result<()> {
    let queue = channel
        .queue_declare(
            "".into(), // server-named
            QueueDeclareOptions {
                exclusive: true,
                auto_delete: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_bind(
            queue.name().as_str().into(),
            CONTROL_EXCHANGE.into(),
            "".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    let mut consumer = channel
        .basic_consume(
            queue.name().as_str().into(),
            "edge-control".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        match serde_json::from_slice::<ControlCommand>(&delivery.data) {
            Ok(cmd) => {
                if let Err(e) = cmd.validate() {
                    warn!(error = %e, "rejecting out-of-bounds control command");
                } else if let ControlCommand::Kill {
                    service: ServiceId::Edge,
                } = cmd
                {
                    warn!("kill command received; exiting for the supervisor to restart");
                    let _ = telemetry::publish_event(
                        &channel,
                        event_kind::SERVICE_STOPPING,
                        "edge",
                        serde_json::json!({"reason": "kill"}),
                    )
                    .await;
                    let _ = delivery.ack(BasicAckOptions::default()).await;
                    std::process::exit(3);
                } else if let Some(payload) = faults.apply(&cmd) {
                    info!(?cmd, "fault applied");
                    let _ = telemetry::publish_event(
                        &channel,
                        event_kind::FAULT_APPLIED,
                        "edge",
                        payload,
                    )
                    .await;
                }
            }
            Err(e) => warn!(error = %e, "unparseable control message"),
        }
        delivery.ack(BasicAckOptions::default()).await?;
    }
    Ok(())
}
