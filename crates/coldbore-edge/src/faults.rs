//! Injectable fault state, mutated by the control consumer, read by the
//! generator and the uplink. Faults exist only on this side of the broker
//! (architecture doc §7): the consumer earns its guarantees against real
//! disorder, not simulated flags.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use coldbore_proto::control::{ControlCommand, LinkState};
use serde_json::json;

#[derive(Debug, Clone)]
pub struct FaultState {
    pub dup_rate: f64,
    pub reorder_window: u32,
    pub rate_multiplier: f64,
    /// Pad id -> uplink up?
    pub links: BTreeMap<u16, bool>,
}

impl FaultState {
    fn healthy(pads: u16) -> Self {
        Self {
            dup_rate: 0.0,
            reorder_window: 0,
            rate_multiplier: 1.0,
            links: (1..=pads).map(|p| (p, true)).collect(),
        }
    }
}

#[derive(Clone)]
pub struct FaultBox {
    inner: Arc<Mutex<FaultState>>,
    pads: u16,
}

impl FaultBox {
    pub fn new(pads: u16) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FaultState::healthy(pads))),
            pads,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FaultState> {
        // A poisoned lock means a panic while holding it; the state itself
        // is plain data, so continuing with it is sound.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn snapshot(&self) -> FaultState {
        self.lock().clone()
    }

    pub fn rate_multiplier(&self) -> f64 {
        self.lock().rate_multiplier
    }

    pub fn dup_rate(&self) -> f64 {
        self.lock().dup_rate
    }

    pub fn reorder_window(&self) -> u32 {
        self.lock().reorder_window
    }

    pub fn link_up(&self, pad: u16) -> bool {
        *self.lock().links.get(&pad).unwrap_or(&true)
    }

    /// Apply a command addressed to the edge. Returns the `fault_applied`
    /// event payload, or `None` when the command is not for this service.
    pub fn apply(&self, cmd: &ControlCommand) -> Option<serde_json::Value> {
        let mut st = self.lock();
        match *cmd {
            ControlCommand::Link { pad, state } => {
                st.links.insert(pad, state == LinkState::Up);
                Some(json!({"cmd": "link", "pad": pad, "state": state}))
            }
            ControlCommand::Dup { rate } => {
                st.dup_rate = rate;
                Some(json!({"cmd": "dup", "rate": rate}))
            }
            ControlCommand::Reorder { window } => {
                st.reorder_window = window;
                Some(json!({"cmd": "reorder", "window": window}))
            }
            ControlCommand::Rate { multiplier } => {
                st.rate_multiplier = multiplier;
                Some(json!({"cmd": "rate", "multiplier": multiplier}))
            }
            ControlCommand::Reset => {
                *st = FaultState::healthy(self.pads);
                Some(json!({"cmd": "reset"}))
            }
            ControlCommand::Kill { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coldbore_proto::control::ServiceId;

    #[test]
    fn apply_and_reset() {
        let fb = FaultBox::new(2);
        assert!(fb.link_up(1));

        fb.apply(&ControlCommand::Link {
            pad: 1,
            state: LinkState::Down,
        });
        fb.apply(&ControlCommand::Dup { rate: 0.1 });
        fb.apply(&ControlCommand::Rate { multiplier: 5.0 });
        assert!(!fb.link_up(1));
        assert!(fb.link_up(2));
        assert_eq!(fb.dup_rate(), 0.1);
        assert_eq!(fb.rate_multiplier(), 5.0);

        assert!(
            fb.apply(&ControlCommand::Kill {
                service: ServiceId::Ingest
            })
            .is_none()
        );

        fb.apply(&ControlCommand::Reset);
        assert!(fb.link_up(1));
        assert_eq!(fb.dup_rate(), 0.0);
        assert_eq!(fb.rate_multiplier(), 1.0);
    }
}
