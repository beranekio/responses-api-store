use std::env;

use anyhow::{bail, Context, Result};

#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub redis_url: String,
    pub key_prefix: String,
    pub ttl_seconds: u64,
    pub stream_key: String,
    pub stale_seconds: i64,
    pub stream_maxlen: usize,
}

impl StoreConfig {
    pub fn from_env() -> Result<Self> {
        let redis_url =
            env::var("RESPONSE_ID_STORE_URL").unwrap_or_else(|_| "redis://valkey:6379".to_string());
        let key_prefix = env::var("RESPONSE_ID_STORE_KEY_PREFIX")
            .unwrap_or_else(|_| "responses-api-store:responses".to_string());
        let ttl_seconds = parse_env_u64("RESPONSE_ID_STORE_TTL_SECONDS", 86_400)?;
        let stream_key = env::var("BACKGROUND_QUEUE_STREAM_KEY")
            .unwrap_or_else(|_| "responses-api-store:background".to_string());
        let stale_seconds = parse_env_i64("BACKGROUND_RESPONSE_STALE_SECONDS", 3600)?;
        let stream_maxlen = parse_env_usize("BACKGROUND_QUEUE_STREAM_MAXLEN", 10_000)?;

        let config = Self {
            redis_url,
            key_prefix,
            ttl_seconds,
            stream_key,
            stale_seconds,
            stream_maxlen,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.ttl_seconds == 0 {
            bail!("RESPONSE_ID_STORE_TTL_SECONDS must be greater than 0");
        }
        if self.stale_seconds <= 0 {
            bail!("BACKGROUND_RESPONSE_STALE_SECONDS must be greater than 0");
        }
        Ok(())
    }
}

pub fn grpc_listen_addr_from_env() -> Result<String> {
    Ok(env::var("GRPC_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".to_string()))
}

pub fn service_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub fn validate_redis_url(url: &str) -> Result<()> {
    redis::Client::open(url).with_context(|| format!("invalid RESPONSE_ID_STORE_URL {url}"))?;
    Ok(())
}

fn parse_env_u64(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("invalid {name} value {value:?}")),
        Err(_) => Ok(default),
    }
}

fn parse_env_i64(name: &str, default: i64) -> Result<i64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("invalid {name} value {value:?}")),
        Err(_) => Ok(default),
    }
}

fn parse_env_usize(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("invalid {name} value {value:?}")),
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_positive_stale_seconds() {
        let config = StoreConfig {
            redis_url: "redis://localhost".to_string(),
            key_prefix: "test".to_string(),
            ttl_seconds: 60,
            stream_key: "stream".to_string(),
            stale_seconds: 0,
            stream_maxlen: 100,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_zero_ttl_seconds() {
        let config = StoreConfig {
            redis_url: "redis://localhost".to_string(),
            key_prefix: "test".to_string(),
            ttl_seconds: 0,
            stream_key: "stream".to_string(),
            stale_seconds: 60,
            stream_maxlen: 100,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn accepts_valid_config() {
        let config = StoreConfig {
            redis_url: "redis://localhost".to_string(),
            key_prefix: "test".to_string(),
            ttl_seconds: 60,
            stream_key: "stream".to_string(),
            stale_seconds: 60,
            stream_maxlen: 100,
        };
        assert!(config.validate().is_ok());
    }
}
