mod operations;
mod server;
mod signaling;
mod types;
mod webrtc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    server::run().await?;
    Ok(())
}
