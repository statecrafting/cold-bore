//! Environment-driven configuration (`CB_*`), shared across services.
//!
//! Panic-free by contract: malformed values yield a clean `ConfigError`
//! naming the variable, never a panic.

use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid value {value:?} for {var}: {reason}")]
    Invalid {
        var: &'static str,
        value: String,
        reason: String,
    },
}

fn env_parse<T>(var: &'static str, default: T) -> Result<T, ConfigError>
where
    T: FromStr,
    T::Err: Display,
{
    match std::env::var(var) {
        Ok(raw) => raw
            .trim()
            .parse()
            .map_err(|e: T::Err| ConfigError::Invalid {
                var,
                value: raw,
                reason: e.to_string(),
            }),
        Err(_) => Ok(default),
    }
}

fn env_string(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Classic,
    Stream,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Classic => "classic",
            Mode::Stream => "stream",
        }
    }
}

impl FromStr for Mode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "classic" => Ok(Mode::Classic),
            "stream" => Ok(Mode::Stream),
            other => Err(format!("expected classic|stream, got {other:?}")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommonConfig {
    pub amqp_url: String,
    pub mode: Mode,
    pub metrics_interval_ms: u64,
    /// Native stream protocol endpoint (port 5552) and credentials.
    pub stream_host: String,
    pub stream_port: u16,
    pub stream_user: String,
    pub stream_pass: String,
}

impl CommonConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            amqp_url: env_string("CB_AMQP_URL", "amqp://coldbore:coldbore@localhost:5672/%2f"),
            mode: env_parse("CB_MODE", Mode::Classic)?,
            metrics_interval_ms: env_parse("CB_METRICS_INTERVAL_MS", 1000)?,
            stream_host: env_string("CB_STREAM_HOST", "localhost"),
            stream_port: env_parse("CB_STREAM_PORT", 5552)?,
            stream_user: env_string("CB_STREAM_USER", "coldbore"),
            stream_pass: env_string("CB_STREAM_PASS", "coldbore"),
        })
    }
}

#[derive(Debug, Clone)]
pub struct EdgeConfig {
    pub common: CommonConfig,
    pub pads: u16,
    pub wells_per_pad: u16,
    /// Base generation frequency per well, before the rate multiplier.
    pub rate_hz: f64,
    /// Store-and-forward capacity in frames, per pad.
    pub buffer_cap: usize,
    /// Maximum unconfirmed publishes in flight before the publisher awaits.
    pub confirm_window: usize,
}

impl EdgeConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            common: CommonConfig::from_env()?,
            pads: env_parse("CB_PADS", 4)?,
            wells_per_pad: env_parse("CB_WELLS_PER_PAD", 8)?,
            rate_hz: env_parse("CB_RATE_HZ", 10.0)?,
            buffer_cap: env_parse("CB_BUFFER_CAP", 1_000_000)?,
            confirm_window: env_parse("CB_CONFIRM_WINDOW", 256)?,
        })
    }
}

/// Where a stream-mode consumer starts when no transactionally stored
/// offset exists (a stored offset always wins: `resume` semantics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamFrom {
    /// The stream's first retained record (full replay). The default.
    First,
    /// Only records published after subscription.
    Next,
    /// An explicit offset.
    Offset(u64),
}

impl FromStr for StreamFrom {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "first" => Ok(StreamFrom::First),
            "next" => Ok(StreamFrom::Next),
            other => match other.strip_prefix("offset:") {
                Some(n) => n
                    .parse()
                    .map(StreamFrom::Offset)
                    .map_err(|e| format!("bad offset in {other:?}: {e}")),
                None => Err(format!("expected first|next|offset:N, got {other:?}")),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct IngestConfig {
    pub common: CommonConfig,
    pub pg_dsn: String,
    pub prefetch: u16,
    pub batch_max_frames: usize,
    pub batch_max_ms: u64,
    /// Stream mode: start position when no stored offset exists.
    pub stream_from: StreamFrom,
    /// Stream mode: ignore the stored offset and force `stream_from`
    /// (the replay drill switch).
    pub stream_force_from: bool,
}

impl IngestConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            common: CommonConfig::from_env()?,
            pg_dsn: env_string(
                "CB_PG_DSN",
                "host=localhost port=5433 user=coldbore password=coldbore dbname=coldbore",
            ),
            prefetch: env_parse("CB_PREFETCH", 512)?,
            batch_max_frames: env_parse("CB_BATCH_MAX_FRAMES", 500)?,
            batch_max_ms: env_parse("CB_BATCH_MAX_MS", 200)?,
            stream_from: env_parse("CB_STREAM_FROM", StreamFrom::First)?,
            stream_force_from: env_parse("CB_STREAM_FORCE_FROM", false)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_from_parses() {
        assert_eq!("first".parse::<StreamFrom>(), Ok(StreamFrom::First));
        assert_eq!("next".parse::<StreamFrom>(), Ok(StreamFrom::Next));
        assert_eq!(
            "offset:42".parse::<StreamFrom>(),
            Ok(StreamFrom::Offset(42))
        );
        assert!("last".parse::<StreamFrom>().is_err());
    }

    #[test]
    fn mode_parses() {
        assert_eq!("classic".parse::<Mode>(), Ok(Mode::Classic));
        assert_eq!("stream".parse::<Mode>(), Ok(Mode::Stream));
        assert!("kafka".parse::<Mode>().is_err());
    }
}
