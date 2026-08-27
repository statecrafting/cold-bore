//! The idempotent sink: batched multi-row inserts into the frames
//! hypertable. `ON CONFLICT DO NOTHING` against the identity index is what
//! turns the pipeline's at-least-once delivery into effectively-exactly-once
//! storage; the conflict count is the observable measure of duplicate
//! absorption (architecture doc §5.5).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coldbore_proto::Frame;
use tokio_postgres::{Client, NoTls, Statement};
use tracing::warn;

const INSERT_SQL: &str = "\
INSERT INTO frames (time, pad_id, well_id, epoch, seq, pressure_psi, rate_bpm, proppant_ppa, temp_f)
SELECT * FROM unnest(
    $1::timestamptz[], $2::int2[], $3::int2[], $4::int8[], $5::int8[],
    $6::real[], $7::real[], $8::real[], $9::real[]
)
ON CONFLICT (pad_id, well_id, epoch, seq, time) DO NOTHING";

/// Per-well high-water marks from the durable store: the newest epoch and
/// its max seq, used to seed gap accounting at startup.
const WATERMARKS_SQL: &str = "\
SELECT DISTINCT ON (pad_id, well_id) pad_id, well_id, epoch, max_seq
FROM (
    SELECT pad_id, well_id, epoch, max(seq) AS max_seq
    FROM frames GROUP BY pad_id, well_id, epoch
) per_epoch
ORDER BY pad_id, well_id, epoch DESC";

pub struct Sink {
    client: Client,
    insert: Statement,
}

impl Sink {
    pub async fn connect(dsn: &str) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                warn!(error = %e, "postgres connection task ended");
            }
        });
        let insert = client.prepare(INSERT_SQL).await?;
        Ok(Self { client, insert })
    }

    /// Insert a batch; returns the number of rows actually inserted. The
    /// difference against `frames.len()` is the duplicate count.
    pub async fn insert_batch(&self, frames: &[Frame]) -> anyhow::Result<u64> {
        let n = frames.len();
        let mut times: Vec<SystemTime> = Vec::with_capacity(n);
        let mut pads: Vec<i16> = Vec::with_capacity(n);
        let mut wells: Vec<i16> = Vec::with_capacity(n);
        let mut epochs: Vec<i64> = Vec::with_capacity(n);
        let mut seqs: Vec<i64> = Vec::with_capacity(n);
        let mut pressures: Vec<f32> = Vec::with_capacity(n);
        let mut rates: Vec<f32> = Vec::with_capacity(n);
        let mut proppants: Vec<f32> = Vec::with_capacity(n);
        let mut temps: Vec<f32> = Vec::with_capacity(n);
        for f in frames {
            times.push(UNIX_EPOCH + Duration::from_millis(f.t_ms));
            pads.push(f.pad as i16);
            wells.push(f.well as i16);
            epochs.push(f.epoch as i64);
            seqs.push(f.seq as i64);
            pressures.push(f.pressure_psi);
            rates.push(f.rate_bpm);
            proppants.push(f.proppant_ppa);
            temps.push(f.temp_f);
        }
        let inserted = self
            .client
            .execute(
                &self.insert,
                &[
                    &times, &pads, &wells, &epochs, &seqs, &pressures, &rates, &proppants, &temps,
                ],
            )
            .await?;
        Ok(inserted)
    }

    /// `(pad, well, epoch, max_seq)` for each well's newest epoch.
    pub async fn watermarks(&self) -> anyhow::Result<Vec<(u16, u16, u64, u64)>> {
        let rows = self.client.query(WATERMARKS_SQL, &[]).await?;
        Ok(rows
            .iter()
            .map(|r| {
                let pad: i16 = r.get(0);
                let well: i16 = r.get(1);
                let epoch: i64 = r.get(2);
                let max_seq: i64 = r.get(3);
                (pad as u16, well as u16, epoch as u64, max_seq as u64)
            })
            .collect())
    }
}
