use std::{net::SocketAddr, sync::Arc, time::Duration};

use crate::{
    session::SessionRegistry,
    signaling::{AppState, router as signaling_router},
    types::AppError,
    webrtc::peer::WebRtcPeer,
};

pub struct AppConfig {
    pub bind_addr: SocketAddr,
    pub shutdown_timeout: Duration,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let bind_addr = std::env::var("BIND_ADDR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| "0.0.0.0:3000".parse().unwrap());
        let shutdown_timeout = std::env::var("SHUTDOWN_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(30));
        Self { bind_addr, shutdown_timeout }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }

    tracing::info!("received shutdown signal, draining connections");
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
        .with_graceful_shutdown(shutdown_signal())
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

    #[test]
    #[serial]
    fn default_shutdown_timeout() {
        unsafe { std::env::remove_var("SHUTDOWN_TIMEOUT_SECS") };
        let config = AppConfig::from_env();
        assert_eq!(config.shutdown_timeout, Duration::from_secs(30));
    }

    #[test]
    #[serial]
    fn custom_shutdown_timeout() {
        unsafe { std::env::set_var("SHUTDOWN_TIMEOUT_SECS", "10") };
        let config = AppConfig::from_env();
        assert_eq!(config.shutdown_timeout, Duration::from_secs(10));
        unsafe { std::env::remove_var("SHUTDOWN_TIMEOUT_SECS") };
    }
}
