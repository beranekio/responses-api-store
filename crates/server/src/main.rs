mod convert;
mod service;

use anyhow::Context;
use responses_api_store_core::{grpc_listen_addr_from_env, StoreConfig};
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
    let addr = listen_addr
        .parse()
        .with_context(|| format!("invalid GRPC_LISTEN_ADDR {listen_addr}"))?;

    let service = ResponsesApiStoreService::new(config)
        .await
        .context("initialize Responses API store service")?;

    info!(%listen_addr, "starting Responses API store gRPC server");

    Server::builder()
        .add_service(ResponsesApiStoreServer::new(service))
        .serve(addr)
        .await
        .context("serve gRPC requests")?;

    Ok(())
}
