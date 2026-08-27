//! Ingest accounting. The consume loop is single-threaded, so plain fields
//! suffice; the loop itself publishes the 1 Hz snapshot (see consume.rs).

#[derive(Debug, Default)]
pub struct Counters {
    pub consumed: u64,
    pub inserted: u64,
    pub dup_dropped: u64,
    pub poison: u64,
    pub redeliveries: u64,
    pub batches: u64,
}
