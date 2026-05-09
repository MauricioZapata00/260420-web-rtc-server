use std::{net::SocketAddr, sync::Arc};

use crate::{
    session::SessionRegistry,
    signaling::{AppState, router as signaling_router},
    types::AppError,
    webrtc::peer::WebRtcPeer,
};

pub struct AppConfig {
    pub bind_addr: SocketAddr,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let bind_addr = std::env::var("BIND_ADDR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| "0.0.0.0:3000".parse().unwrap());
        Self { bind_addr }
    }
}

pub async fn run() -> Result<(), AppError> {
    let config = AppConfig::from_env();
    let peer = WebRtcPeer::new().await?;
    let state = AppState {
        peer: Arc::new(peer),
        registry: Arc::new(SessionRegistry::new()),
    };
    let router = signaling_router::<WebRtcPeer>().with_state(state);
    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .map_err(|e| AppError::SignalingError(e.to_string()))?;
    tracing::info!("listening on {}", config.bind_addr);
    axum::serve(listener, router)
        .await
        .map_err(|e| AppError::SignalingError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn default_bind_addr() {
        unsafe { std::env::remove_var("BIND_ADDR") };
        let config = AppConfig::from_env();
        assert_eq!(
            config.bind_addr,
            "0.0.0.0:3000".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    #[serial]
    fn custom_bind_addr() {
        unsafe { std::env::set_var("BIND_ADDR", "127.0.0.1:8080") };
        let config = AppConfig::from_env();
        assert_eq!(
            config.bind_addr,
            "127.0.0.1:8080".parse::<SocketAddr>().unwrap()
        );
        unsafe { std::env::remove_var("BIND_ADDR") };
    }
}
