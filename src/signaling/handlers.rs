use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use tokio::sync::oneshot;

use crate::{
    operations::{parse_sdp_offer, validate_ice_candidate},
    session::{PeerHandle, SessionRegistry},
    types::{AppError, IceCandidate, OfferResponse, PeerId, SdpOffer},
    webrtc::peer::PeerOps,
};

pub struct AppState<P> {
    pub peer: Arc<P>,
    pub registry: Arc<SessionRegistry>,
}

impl<P> Clone for AppState<P> {
    fn clone(&self) -> Self {
        Self {
            peer: Arc::clone(&self.peer),
            registry: Arc::clone(&self.registry),
        }
    }
}

pub fn router<P: PeerOps + 'static>() -> Router<AppState<P>> {
    Router::new()
        .route("/offer", post(offer::<P>))
        .route("/ws/ice", get(ws_ice::<P>))
}

pub async fn offer<P: PeerOps + 'static>(
    State(state): State<AppState<P>>,
    Json(body): Json<SdpOffer>,
) -> Result<Json<OfferResponse>, AppError> {
    let validated = parse_sdp_offer(&body.sdp)?;
    state.peer.set_remote_description(validated).await?;
    let answer = state.peer.create_answer().await?;

    let peer_id = PeerId::new();
    let (disconnect_tx, _) = oneshot::channel::<()>();
    state.registry.register(peer_id, PeerHandle { data_channel: None, disconnect_tx });
    state.peer.on_data_channel(peer_id, Arc::clone(&state.registry)).await?;

    Ok(Json(OfferResponse { peer_id, sdp: answer.sdp }))
}

pub async fn ws_ice<P: PeerOps + 'static>(
    ws: WebSocketUpgrade,
    State(state): State<AppState<P>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ice_socket(socket, state.peer))
}

async fn handle_ice_socket<P: PeerOps>(mut socket: WebSocket, peer: Arc<P>) {
    while let Some(msg) = socket.recv().await {
        let msg = match msg {
            Ok(m) => m,
            Err(_) => break,
        };
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };
        let Ok(candidate) = serde_json::from_str::<IceCandidate>(&text) else {
            continue;
        };
        if validate_ice_candidate(&candidate).is_err() {
            continue;
        }
        if peer.add_ice_candidate(candidate).await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use serial_test::serial;
    use tower::ServiceExt;

    use crate::{types::SdpAnswer, webrtc::peer::MockPeerOps};

    const VALID_SDP: &str = "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n";

    fn build_router(mock: MockPeerOps) -> Router {
        let state = AppState {
            peer: Arc::new(mock),
            registry: Arc::new(SessionRegistry::new()),
        };
        router::<MockPeerOps>().with_state(state)
    }

    fn offer_request(sdp: &str) -> Request<Body> {
        let body = serde_json::to_string(&SdpOffer { sdp: sdp.to_string() }).unwrap();
        Request::builder()
            .method("POST")
            .uri("/offer")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn offer_returns_peer_id_and_sdp() {
        let mut mock = MockPeerOps::new();
        mock.expect_set_remote_description().returning(|_| Ok(()));
        mock.expect_create_answer()
            .returning(|| Ok(SdpAnswer { sdp: VALID_SDP.to_string() }));
        mock.expect_on_data_channel().returning(|_, _| Ok(()));

        let response = build_router(mock).oneshot(offer_request(VALID_SDP)).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: OfferResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.sdp, VALID_SDP);
        assert_eq!(body.peer_id.0.get_version(), Some(uuid::Version::Random));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn offer_peer_error() {
        let mut mock = MockPeerOps::new();
        mock.expect_set_remote_description()
            .returning(|_| Err(AppError::PeerConnectionFailed("fail".to_string())));

        let response = build_router(mock).oneshot(offer_request(VALID_SDP)).await.unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn offer_empty_sdp_rejected() {
        let mock = MockPeerOps::new();

        let response = build_router(mock).oneshot(offer_request("")).await.unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
