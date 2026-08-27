//! Per-(pad, well) sequence accounting: the machinery that turns "messages
//! arrived" into "every frame is accounted for". Tracks the highest
//! contiguous seq per well and a bounded set of open gap ranges; late
//! arrivals (store-and-forward drains, redeliveries, reordering) shrink and
//! heal them. Architecture doc §5, "Gap tracking".

use std::collections::{BTreeMap, HashMap};

/// Open ranges are bounded per well; beyond this the tracker collapses new
/// gaps into a summary counter instead of growing without limit.
const MAX_OPEN_PER_WELL: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GapChange {
    Opened {
        pad: u16,
        well: u16,
        from: u64,
        to: u64,
    },
    Healed {
        pad: u16,
        well: u16,
        from: u64,
        to: u64,
        after_ms: u64,
    },
}

#[derive(Debug, Clone, Copy)]
struct GapRange {
    end: u64,
    opened_at_ms: u64,
}

#[derive(Debug, Default)]
struct WellGaps {
    /// Producer generation this accounting belongs to. A newer epoch resets
    /// the well (the old producer's in-memory buffer died with it); frames
    /// from an older epoch are stale stragglers and are ignored.
    epoch: u64,
    /// Next seq we expect if arrivals were perfectly contiguous.
    next_expected: u64,
    /// start -> range. All ranges lie strictly below `next_expected`.
    open: BTreeMap<u64, GapRange>,
    /// Gaps not individually tracked because the range table was full.
    untracked: u64,
}

#[derive(Debug, Default)]
pub struct GapTracker {
    wells: HashMap<(u16, u16), WellGaps>,
    opened: u64,
    healed: u64,
}

impl GapTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Cumulative count of gap ranges ever opened.
    pub fn opened(&self) -> u64 {
        self.opened
    }

    /// Cumulative count of gap ranges fully healed.
    pub fn healed(&self) -> u64 {
        self.healed
    }

    /// Currently open ranges across all wells.
    pub fn open_now(&self) -> u64 {
        self.wells
            .values()
            .map(|w| w.open.len() as u64 + w.untracked)
            .sum()
    }

    /// Seed a well's watermark from the durable store (used at startup so a
    /// restarted consumer does not report already-landed history as gaps).
    /// Only applies to wells with no in-memory state yet.
    pub fn seed(&mut self, pad: u16, well: u16, epoch: u64, max_seq: u64) {
        self.wells.entry((pad, well)).or_insert_with(|| WellGaps {
            epoch,
            next_expected: max_seq + 1,
            ..WellGaps::default()
        });
    }

    /// Record an arrival. Returns the gap changes it caused (usually none).
    pub fn observe(
        &mut self,
        pad: u16,
        well: u16,
        epoch: u64,
        seq: u64,
        now_ms: u64,
    ) -> Vec<GapChange> {
        let entry = self.wells.entry((pad, well)).or_insert_with(|| WellGaps {
            epoch,
            next_expected: 1,
            ..WellGaps::default()
        });
        if epoch < entry.epoch {
            return Vec::new(); // straggler from a dead producer generation
        }
        if epoch > entry.epoch {
            // New producer generation: the old epoch can never heal (its
            // buffers died with the process); start fresh accounting.
            *entry = WellGaps {
                epoch,
                next_expected: 1,
                ..WellGaps::default()
            };
        }
        let mut changes = Vec::new();

        if seq == entry.next_expected {
            entry.next_expected += 1;
        } else if seq > entry.next_expected {
            let (from, to) = (entry.next_expected, seq - 1);
            if entry.open.len() < MAX_OPEN_PER_WELL {
                entry.open.insert(
                    from,
                    GapRange {
                        end: to,
                        opened_at_ms: now_ms,
                    },
                );
            } else {
                entry.untracked += 1;
            }
            self.opened += 1;
            changes.push(GapChange::Opened {
                pad,
                well,
                from,
                to,
            });
            entry.next_expected = seq + 1;
        } else {
            // Late arrival: seq < next_expected. Either it fills an open gap
            // or it is a duplicate (the sink counts those; nothing to do).
            let containing = entry
                .open
                .range(..=seq)
                .next_back()
                .filter(|(_, r)| r.end >= seq)
                .map(|(&start, &r)| (start, r));
            if let Some((start, range)) = containing {
                entry.open.remove(&start);
                if start < seq {
                    entry.open.insert(
                        start,
                        GapRange {
                            end: seq - 1,
                            opened_at_ms: range.opened_at_ms,
                        },
                    );
                }
                if seq < range.end {
                    entry.open.insert(
                        seq + 1,
                        GapRange {
                            end: range.end,
                            opened_at_ms: range.opened_at_ms,
                        },
                    );
                }
                if start == seq && range.end == seq {
                    self.healed += 1;
                    changes.push(GapChange::Healed {
                        pad,
                        well,
                        from: start,
                        to: range.end,
                        after_ms: now_ms.saturating_sub(range.opened_at_ms),
                    });
                }
            }
        }
        changes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contiguous_arrivals_open_nothing() {
        let mut t = GapTracker::new();
        for seq in 1..=100 {
            assert!(t.observe(1, 1, 7, seq, 0).is_empty());
        }
        assert_eq!(t.open_now(), 0);
    }

    #[test]
    fn jump_opens_gap_and_late_fill_heals_it() {
        let mut t = GapTracker::new();
        assert!(t.observe(1, 1, 7, 1, 1000).is_empty());
        // seq 2..=4 lost in flight; 5 arrives.
        let changes = t.observe(1, 1, 7, 5, 1000);
        assert_eq!(
            changes,
            vec![GapChange::Opened {
                pad: 1,
                well: 1,
                from: 2,
                to: 4
            }]
        );
        assert_eq!(t.open_now(), 1);

        // Late arrivals fill the range out of order.
        assert!(t.observe(1, 1, 7, 3, 2000).is_empty()); // splits into [2,2] and [4,4]
        assert_eq!(t.open_now(), 2);
        assert!(t.observe(1, 1, 7, 2, 2500).iter().any(|c| matches!(
            c,
            GapChange::Healed {
                from: 2,
                to: 2,
                after_ms: 1500,
                ..
            }
        )));
        assert!(
            t.observe(1, 1, 7, 4, 3000)
                .iter()
                .any(|c| matches!(c, GapChange::Healed { from: 4, to: 4, .. }))
        );
        assert_eq!(t.open_now(), 0);
        assert_eq!(t.opened(), 1);
        assert_eq!(t.healed(), 2); // two sub-ranges healed
    }

    #[test]
    fn duplicates_change_nothing() {
        let mut t = GapTracker::new();
        t.observe(1, 1, 7, 1, 0);
        t.observe(1, 1, 7, 2, 0);
        assert!(t.observe(1, 1, 7, 2, 0).is_empty());
        assert!(t.observe(1, 1, 7, 1, 0).is_empty());
        assert_eq!(t.open_now(), 0);
    }

    #[test]
    fn epoch_change_resets_without_phantom_gaps() {
        let mut t = GapTracker::new();
        for seq in 1..=50 {
            t.observe(1, 1, 100, seq, 0);
        }
        // Producer restarted: new epoch, seq restarts at 1. No gap opens.
        assert!(t.observe(1, 1, 200, 1, 0).is_empty());
        assert!(t.observe(1, 1, 200, 2, 0).is_empty());
        // A straggler from the dead epoch changes nothing.
        assert!(t.observe(1, 1, 100, 51, 0).is_empty());
        assert_eq!(t.open_now(), 0);
    }

    #[test]
    fn seeded_watermark_prevents_phantom_gaps_after_consumer_restart() {
        let mut t = GapTracker::new();
        t.seed(1, 1, 100, 4000); // durable store already holds seq 1..=4000
        assert!(t.observe(1, 1, 100, 4001, 0).is_empty());
        assert_eq!(t.open_now(), 0);
        // Seeding never overwrites live in-memory state.
        t.seed(1, 1, 100, 10);
        assert!(t.observe(1, 1, 100, 4002, 0).is_empty());
    }

    #[test]
    fn wells_are_independent() {
        let mut t = GapTracker::new();
        t.observe(1, 1, 7, 1, 0);
        let changes = t.observe(2, 7, 7, 10, 0);
        assert_eq!(
            changes,
            vec![GapChange::Opened {
                pad: 2,
                well: 7,
                from: 1,
                to: 9
            }]
        );
    }
}
