mod convert;
mod metrics;
mod service;

use anyhow::Context;
use responses_api_store_core::{
    grpc_listen_addr_from_env, grpc_max_message_bytes_from_env, metrics_http_enabled_from_env,
    metrics_http_listen_addr_from_env, StoreConfig,
};
use responses_api_store_proto::ResponsesApiStoreServer;
use service::ResponsesApiStoreService;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let config = StoreConfig::from_env().context("load store configuration")?;
    let listen_addr = grpc_listen_addr_from_env().context("resolve gRPC listen address")?;
    let max_message_bytes =
        grpc_max_message_bytes_from_env().context("resolve gRPC max message size")?;
    let addr = listen_addr
        .parse()
        .with_context(|| format!("invalid GRPC_LISTEN_ADDR {listen_addr}"))?;

    let service = ResponsesApiStoreService::new(config)
        .await
        .context("initialize Responses API store service")?;

    info!(%listen_addr, max_message_bytes, "starting Responses API store gRPC server");

    let queue = service.queue().clone();
    let grpc_service = ResponsesApiStoreServer::new(service)
        .max_decoding_message_size(max_message_bytes)
        .max_encoding_message_size(max_message_bytes);

    if metrics_http_enabled_from_env() {
        let metrics_addr =
            metrics_http_listen_addr_from_env().context("resolve metrics HTTP listen address")?;
        let metrics_listener = tokio::net::TcpListener::bind(&metrics_addr)
            .await
            .with_context(|| format!("bind metrics HTTP listener on {metrics_addr}"))?;
        info!(%metrics_addr, "starting background queue metrics HTTP server");
        tokio::spawn(async move {
            if let Err(err) = metrics::serve(queue, metrics_listener).await {
                tracing::error!(error = %err, "metrics HTTP server exited");
            }
        });
    }

    Server::builder()
        .add_service(grpc_service)
        .serve(addr)
        .await
        .context("serve gRPC requests")?;

    Ok(())
}
