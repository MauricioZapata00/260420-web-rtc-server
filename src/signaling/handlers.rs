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

use crate::{
    operations::{parse_sdp_offer, validate_ice_candidate},
    types::{AppError, IceCandidate, SdpAnswer, SdpOffer},
    webrtc::peer::PeerOps,
};

pub fn router<P: PeerOps + 'static>() -> Router<Arc<P>> {
    Router::new()
        .route("/offer", post(offer::<P>))
        .route("/ws/ice", get(ws_ice::<P>))
}

pub async fn offer<P: PeerOps + 'static>(
    State(peer): State<Arc<P>>,
    Json(body): Json<SdpOffer>,
) -> Result<Json<SdpAnswer>, AppError> {
    let validated = parse_sdp_offer(&body.sdp)?;
    peer.set_remote_description(validated).await?;
    let answer = peer.create_answer().await?;
    Ok(Json(answer))
}

pub async fn ws_ice<P: PeerOps + 'static>(
    ws: WebSocketUpgrade,
    State(peer): State<Arc<P>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ice_socket(socket, peer))
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

    use crate::webrtc::peer::MockPeerOps;

    fn build_router(mock: MockPeerOps) -> Router {
        router::<MockPeerOps>().with_state(Arc::new(mock))
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
    async fn offer_happy_path() {
        let mut mock = MockPeerOps::new();
        mock.expect_set_remote_description().returning(|_| Ok(()));
        mock.expect_create_answer()
            .returning(|| Ok(SdpAnswer { sdp: "v=0\r\n".to_string() }));

        let response = build_router(mock).oneshot(offer_request("v=0\r\n")).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let answer: SdpAnswer = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(answer.sdp, "v=0\r\n");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn offer_peer_error() {
        let mut mock = MockPeerOps::new();
        mock.expect_set_remote_description()
            .returning(|_| Err(AppError::PeerConnectionFailed("fail".to_string())));

        let response = build_router(mock).oneshot(offer_request("v=0\r\n")).await.unwrap();

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
