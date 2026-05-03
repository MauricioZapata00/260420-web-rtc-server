mod operations;
mod server;
mod signaling;
mod types;
mod webrtc;

#[tokio::main]
async fn main() {
    server::run().await.unwrap();
}
