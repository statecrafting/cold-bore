//! Telemetry generation: plausible frac-pad waveforms at a configurable
//! frequency. Realism of the waveforms is a non-goal; realism of the data
//! rates is the point (architecture doc §4).

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use coldbore_proto::config::EdgeConfig;
use coldbore_proto::{Frame, frame::FRAME_VERSION, now_ms};
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use tokio::sync::mpsc::Sender;

use crate::faults::FaultBox;
use crate::telemetry::Counters;

pub struct WellSim {
    pad: u16,
    well: u16,
    epoch: u64,
    seq: u64,
    rng: SmallRng,
    pressure_set: f32,
    pressure_phase: f64,
    rate_phase: f64,
    stage_t: f64,
    temp_base: f32,
}

impl WellSim {
    pub fn new(pad: u16, well: u16, epoch: u64) -> Self {
        let mut rng = SmallRng::seed_from_u64(u64::from(pad) << 16 | u64::from(well));
        let pressure_set = 8200.0 + rng.random_range(-300.0..300.0);
        let temp_base = 68.0 + rng.random_range(0.0..12.0);
        let stage_t = rng.random_range(0.0..1200.0);
        Self {
            pad,
            well,
            epoch,
            seq: 0,
            rng,
            pressure_set,
            pressure_phase: 0.0,
            rate_phase: 0.0,
            stage_t,
            temp_base,
        }
    }

    /// Advance one sample interval `dt` seconds and emit the next frame.
    pub fn step(&mut self, t_ms: u64, dt: f64) -> Frame {
        self.seq += 1;
        self.pressure_phase += dt * 0.8;
        self.rate_phase += dt * 0.13;
        self.stage_t += dt;

        // Occasional setpoint step: roughly once per 45 s of simulated time.
        if self.rng.random::<f64>() < dt / 45.0 {
            self.pressure_set =
                (self.pressure_set + self.rng.random_range(-300.0..300.0)).clamp(7600.0, 9400.0);
        }

        let noise = |rng: &mut SmallRng, scale: f32| rng.random_range(-scale..scale);
        let pressure = self.pressure_set
            + 140.0 * (self.pressure_phase.sin() as f32)
            + noise(&mut self.rng, 25.0);
        let rate = 93.0 + 7.0 * (self.rate_phase.sin() as f32) + noise(&mut self.rng, 0.6);
        // Proppant ramps 0 -> 3 ppa over a ~20 minute stage, then resets.
        let stage_frac = (self.stage_t % 1200.0) / 1200.0;
        let proppant = (3.0 * stage_frac as f32 + noise(&mut self.rng, 0.05)).max(0.0);
        let temp = self.temp_base
            + 4.0 * ((self.stage_t / 600.0).sin() as f32)
            + noise(&mut self.rng, 0.2);

        Frame {
            v: FRAME_VERSION,
            pad: self.pad,
            well: self.well,
            epoch: self.epoch,
            seq: self.seq,
            t_ms,
            pressure_psi: pressure,
            rate_bpm: rate,
            proppant_ppa: proppant,
            temp_f: temp,
        }
    }
}

/// The generation loop. Sensors do not stop sampling when the uplink is
/// down: frames are always produced and handed to the uplink, which decides
/// whether they publish or buffer.
///
/// The field size is read from the control state every tick, so a
/// `topology` command takes effect on the next sample. Wells are created on
/// demand and never discarded: a well resized away and back within one
/// epoch resumes its own seq counter, so seqs are never reused.
pub async fn generator(
    cfg: EdgeConfig,
    faults: FaultBox,
    counters: Arc<Counters>,
    tx: Sender<Frame>,
    epoch: u64,
) {
    // One epoch per process run (assigned in main): seq accounting
    // downstream stays sound across edge restarts because identity
    // includes the epoch, and the stream producer's dedup name embeds it.
    let mut wells: std::collections::BTreeMap<(u16, u16), WellSim> =
        std::collections::BTreeMap::new();

    let mut acc = 0.0_f64;
    let mut last = Instant::now();
    loop {
        let hz = (cfg.rate_hz * faults.rate_multiplier()).max(0.01);
        let tick = Duration::from_secs_f64((1.0 / hz).clamp(0.001, 0.1));
        tokio::time::sleep(tick).await;

        let now = Instant::now();
        let dt = (now - last).as_secs_f64();
        last = now;

        acc += hz * dt;
        let samples = acc.floor() as u64;
        acc -= samples as f64;

        let (pads, wells_per_pad) = faults.topology();
        for _ in 0..samples {
            let t_ms = now_ms();
            for p in 1..=pads {
                for w in 1..=wells_per_pad {
                    let well = wells
                        .entry((p, w))
                        .or_insert_with(|| WellSim::new(p, w, epoch));
                    let frame = well.step(t_ms, 1.0 / hz);
                    counters.generated.fetch_add(1, Relaxed);
                    if tx.send(frame).await.is_err() {
                        return; // uplink gone; shutting down
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_is_monotonic_and_channels_valid() {
        let mut sim = WellSim::new(1, 1, 42);
        let mut prev = 0;
        for _ in 0..1000 {
            let f = sim.step(now_ms(), 0.1);
            assert_eq!(f.seq, prev + 1);
            prev = f.seq;
            assert!(f.validate().is_ok());
            assert!(
                (7000.0..10500.0).contains(&f.pressure_psi),
                "{}",
                f.pressure_psi
            );
            assert!((80.0..106.0).contains(&f.rate_bpm));
            assert!((0.0..3.5).contains(&f.proppant_ppa));
        }
    }
}
