//! Control-plane consumer for the ingest: the only command addressed to this
//! service is `kill` (crash drill). Faults never touch the data path here.

use coldbore_proto::control::{ControlCommand, ServiceId};
use coldbore_proto::topology::CONTROL_EXCHANGE;
use futures_util::StreamExt;
use lapin::Channel;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use tracing::warn;

pub async fn run(channel: Channel) {
    if let Err(e) = serve(channel).await {
        warn!(error = %e, "control consumer stopped");
    }
}

async fn serve(channel: Channel) -> anyhow::Result<()> {
    let queue = channel
        .queue_declare(
            "".into(),
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
            "ingest-control".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        if let Ok(ControlCommand::Kill {
            service: ServiceId::Ingest,
        }) = serde_json::from_slice::<ControlCommand>(&delivery.data)
        {
            warn!("kill command received; exiting for the supervisor to restart");
            let _ = delivery.ack(BasicAckOptions::default()).await;
            std::process::exit(3);
        }
        delivery.ack(BasicAckOptions::default()).await?;
    }
    Ok(())
}
