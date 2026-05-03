#![allow(dead_code)]

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("peer connection failed: {0}")]
    PeerConnectionFailed(String),
    #[error("SDP parse failed: {0}")]
    SdpParseFailed(String),
    #[error("ICE candidate failed: {0}")]
    IceCandidateFailed(String),
    #[error("signaling error: {0}")]
    SignalingError(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()).into_response()
    }
}
