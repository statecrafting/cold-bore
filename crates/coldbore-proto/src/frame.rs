//! The sensor frame: one sample from one well, the unit the whole pipeline
//! moves, deduplicates, and accounts for.

use serde::{Deserialize, Serialize};

/// Wire version of the frame contract. Bump on breaking change; the ingest
/// treats an unknown version as poison (dead-letter, never crash).
pub const FRAME_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Frame {
    pub v: u8,
    pub pad: u16,
    pub well: u16,
    /// Producer generation: the edge process's start wall-clock (ms). A
    /// restarted edge starts a new epoch, so seq reuse across restarts can
    /// never collide: pipeline identity is `(pad, well, epoch, seq)`.
    pub epoch: u64,
    /// Assigned only by the edge, monotonically increasing per `(pad, well)`
    /// within an epoch, never reused. With `epoch`, the idempotency and
    /// gap-detection key for the pipeline.
    pub seq: u64,
    /// Event time: producer wall clock (ms since epoch) at sample generation.
    pub t_ms: u64,
    pub pressure_psi: f32,
    pub rate_bpm: f32,
    pub proppant_ppa: f32,
    pub temp_f: f32,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("unsupported frame version {0} (expected {FRAME_VERSION})")]
    Version(u8),
    #[error("non-finite value in channel {0}")]
    NonFinite(&'static str),
}

impl Frame {
    /// Topic routing key on the frames exchange.
    pub fn routing_key(&self) -> String {
        format!("frames.pad{}.well{}", self.pad, self.well)
    }

    /// AMQP `message_id`: the human-readable identity
    /// `(pad, well, epoch, seq)`.
    pub fn message_id(&self) -> String {
        format!("{}-{}-{}-{}", self.pad, self.well, self.epoch, self.seq)
    }

    /// Contract validation. A frame that fails here is poison: the consumer
    /// dead-letters it and moves on.
    pub fn validate(&self) -> Result<(), FrameError> {
        if self.v != FRAME_VERSION {
            return Err(FrameError::Version(self.v));
        }
        for (name, value) in [
            ("pressure_psi", self.pressure_psi),
            ("rate_bpm", self.rate_bpm),
            ("proppant_ppa", self.proppant_ppa),
            ("temp_f", self.temp_f),
        ] {
            if !value.is_finite() {
                return Err(FrameError::NonFinite(name));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame() -> Frame {
        Frame {
            v: FRAME_VERSION,
            pad: 2,
            well: 5,
            epoch: 1_724_790_000_000,
            seq: 184_467,
            t_ms: 1_724_790_000_123,
            pressure_psi: 8543.2,
            rate_bpm: 92.4,
            proppant_ppa: 1.85,
            temp_f: 74.3,
        }
    }

    #[test]
    fn serde_round_trip() {
        let f = frame();
        let json = serde_json::to_string(&f).expect("serialize");
        let back: Frame = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(f, back);
    }

    #[test]
    fn wire_shape_matches_architecture_doc() {
        // The exact field names are the cross-language contract (§4 of the
        // architecture doc); renaming any of them breaks the Python side.
        let value = serde_json::to_value(frame()).expect("to_value");
        let obj = value.as_object().expect("object");
        let mut keys: Vec<_> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "epoch",
                "pad",
                "pressure_psi",
                "proppant_ppa",
                "rate_bpm",
                "seq",
                "t_ms",
                "temp_f",
                "v",
                "well"
            ]
        );
    }

    #[test]
    fn routing_and_identity() {
        let f = frame();
        assert_eq!(f.routing_key(), "frames.pad2.well5");
        assert_eq!(f.message_id(), "2-5-1724790000000-184467");
    }

    #[test]
    fn validation_rejects_poison() {
        let mut f = frame();
        f.v = 9;
        assert_eq!(f.validate(), Err(FrameError::Version(9)));

        let mut f = frame();
        f.pressure_psi = f32::NAN;
        assert_eq!(f.validate(), Err(FrameError::NonFinite("pressure_psi")));

        assert_eq!(frame().validate(), Ok(()));
    }
}
