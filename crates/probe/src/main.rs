use responses_api_store_client::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = probe_endpoint_from_env()?;
    let mut client = Client::connect(endpoint).await?;
    let health = client.health().await?;
    if !health.redis_ok {
        std::process::exit(1);
    }
    Ok(())
}

fn probe_endpoint_from_env() -> anyhow::Result<String> {
    let listen_addr =
        std::env::var("GRPC_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let dial_addr = listen_addr.replace("0.0.0.0:", "127.0.0.1:");
    if dial_addr.starts_with("http://") || dial_addr.starts_with("https://") {
        Ok(dial_addr)
    } else {
        Ok(format!("http://{dial_addr}"))
    }
}
