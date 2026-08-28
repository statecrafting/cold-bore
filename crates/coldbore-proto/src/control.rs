//! Control-plane commands: fault injection and runtime configuration.
//!
//! Published by the api to the fanout control exchange; every service sees
//! every command and applies the ones addressed to it. Faults live only in
//! the edge's publish path and the process supervisor (architecture doc §7):
//! the ingest data path has no test-only branches.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LinkState {
    Up,
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceId {
    Edge,
    Ingest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum ControlCommand {
    /// Sever or restore one pad's uplink (edge store-and-forward).
    Link { pad: u16, state: LinkState },
    /// Re-publish that fraction of confirmed frames (duplicate injection).
    Dup { rate: f64 },
    /// Emit frames in shuffled windows of this size; 0 disables.
    Reorder { window: u32 },
    /// Scale telemetry generation frequency (volume surge).
    Rate { multiplier: f64 },
    /// Resize the simulated field at runtime: how many pads exist and how
    /// many wells (signal sources) each carries. A setting, not a fault:
    /// `reset` does not touch it.
    Topology { pads: u16, wells_per_pad: u16 },
    /// Named service exits non-zero; its supervisor restarts it.
    Kill { service: ServiceId },
    /// Clear all injected faults back to defaults.
    Reset,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ControlError {
    #[error("dup rate {0} outside [0.0, 1.0]")]
    DupRate(f64),
    #[error("reorder window {0} exceeds {MAX_REORDER_WINDOW}")]
    ReorderWindow(u32),
    #[error("rate multiplier {0} outside ({MIN_RATE_MULTIPLIER}, {MAX_RATE_MULTIPLIER}]")]
    RateMultiplier(f64),
    #[error(
        "topology {pads} pads x {wells_per_pad} wells outside 1..={MAX_PADS} x 1..={MAX_WELLS_PER_PAD} with at most {MAX_TOTAL_WELLS} wells total"
    )]
    Topology { pads: u16, wells_per_pad: u16 },
}

pub const MAX_REORDER_WINDOW: u32 = 4096;
pub const MIN_RATE_MULTIPLIER: f64 = 0.0;
pub const MAX_RATE_MULTIPLIER: f64 = 100.0;
/// Topology bounds: generous enough to stress the pipeline (64x64 would be
/// 4096 wells), capped in total so one command cannot melt the lab host.
pub const MAX_PADS: u16 = 64;
pub const MAX_WELLS_PER_PAD: u16 = 64;
pub const MAX_TOTAL_WELLS: u32 = 2048;

impl ControlCommand {
    /// Bounds validation, applied by the api before publish and by services
    /// on receipt (defense in depth: the broker is not a trust boundary we
    /// rely on).
    pub fn validate(&self) -> Result<(), ControlError> {
        match *self {
            ControlCommand::Dup { rate } => {
                if !(0.0..=1.0).contains(&rate) || !rate.is_finite() {
                    return Err(ControlError::DupRate(rate));
                }
            }
            ControlCommand::Reorder { window } => {
                if window > MAX_REORDER_WINDOW {
                    return Err(ControlError::ReorderWindow(window));
                }
            }
            ControlCommand::Rate { multiplier } => {
                if !multiplier.is_finite()
                    || multiplier <= MIN_RATE_MULTIPLIER
                    || multiplier > MAX_RATE_MULTIPLIER
                {
                    return Err(ControlError::RateMultiplier(multiplier));
                }
            }
            ControlCommand::Topology {
                pads,
                wells_per_pad,
            } => {
                let total = u32::from(pads) * u32::from(wells_per_pad);
                if pads == 0
                    || wells_per_pad == 0
                    || pads > MAX_PADS
                    || wells_per_pad > MAX_WELLS_PER_PAD
                    || total > MAX_TOTAL_WELLS
                {
                    return Err(ControlError::Topology {
                        pads,
                        wells_per_pad,
                    });
                }
            }
            ControlCommand::Link { .. } | ControlCommand::Kill { .. } | ControlCommand::Reset => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_shape() {
        let cmd = ControlCommand::Link {
            pad: 2,
            state: LinkState::Down,
        };
        let json = serde_json::to_string(&cmd).expect("serialize");
        assert_eq!(json, r#"{"cmd":"link","pad":2,"state":"down"}"#);

        let back: ControlCommand =
            serde_json::from_str(r#"{"cmd":"rate","multiplier":20.0}"#).expect("deserialize");
        assert_eq!(back, ControlCommand::Rate { multiplier: 20.0 });
    }

    #[test]
    fn validation_bounds() {
        assert!(ControlCommand::Dup { rate: 0.05 }.validate().is_ok());
        assert!(ControlCommand::Dup { rate: 1.5 }.validate().is_err());
        assert!(ControlCommand::Dup { rate: f64::NAN }.validate().is_err());
        assert!(ControlCommand::Reorder { window: 64 }.validate().is_ok());
        assert!(ControlCommand::Reorder { window: 5000 }.validate().is_err());
        assert!(ControlCommand::Rate { multiplier: 0.0 }.validate().is_err());
        assert!(
            ControlCommand::Rate { multiplier: 100.0 }
                .validate()
                .is_ok()
        );
        assert!(ControlCommand::Reset.validate().is_ok());
    }

    #[test]
    fn topology_wire_shape_and_bounds() {
        let cmd = ControlCommand::Topology {
            pads: 6,
            wells_per_pad: 12,
        };
        let json = serde_json::to_string(&cmd).expect("serialize");
        assert_eq!(json, r#"{"cmd":"topology","pads":6,"wells_per_pad":12}"#);
        assert!(cmd.validate().is_ok());

        for (pads, wells) in [
            (0, 8),
            (8, 0),
            (MAX_PADS + 1, 1),
            (1, MAX_WELLS_PER_PAD + 1),
        ] {
            assert!(
                ControlCommand::Topology {
                    pads,
                    wells_per_pad: wells
                }
                .validate()
                .is_err(),
                "{pads}x{wells} should be rejected"
            );
        }
        // Within per-axis bounds but over the total-well cap.
        assert!(
            ControlCommand::Topology {
                pads: 64,
                wells_per_pad: 64
            }
            .validate()
            .is_err()
        );
    }
}
