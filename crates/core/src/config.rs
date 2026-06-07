use std::env;

use anyhow::{bail, Context, Result};

#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub redis_url: String,
    pub key_prefix: String,
    pub ttl_seconds: u64,
    pub stream_key: String,
    pub stale_seconds: i64,
}

impl StoreConfig {
    pub fn from_env() -> Result<Self> {
        let redis_url =
            env::var("RESPONSE_ID_STORE_URL").unwrap_or_else(|_| "redis://valkey:6379".to_string());
        let key_prefix = env::var("RESPONSE_ID_STORE_KEY_PREFIX")
            .unwrap_or_else(|_| "responses-api-store:responses".to_string());
        let ttl_seconds = env::var("RESPONSE_ID_STORE_TTL_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(86_400);
        let stream_key = env::var("BACKGROUND_QUEUE_STREAM_KEY")
            .unwrap_or_else(|_| "responses-api-store:background".to_string());
        let stale_seconds = env::var("BACKGROUND_RESPONSE_STALE_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(3600);

        if ttl_seconds == 0 {
            bail!("RESPONSE_ID_STORE_TTL_SECONDS must be greater than 0");
        }

        Ok(Self {
            redis_url,
            key_prefix,
            ttl_seconds,
            stream_key,
            stale_seconds,
        })
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
