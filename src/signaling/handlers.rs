use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::{
    operations::{parse_sdp_offer, validate_ice_candidate},
    session::{PeerHandle, SessionRegistry},
    types::{AppError, IceCandidate, IceWsMessage, OfferResponse, PeerId, SdpOffer},
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
    state.registry.register(
        peer_id,
        PeerHandle {
            data_channel: None,
            peer_connection: Some(state.peer.peer_connection()),
            ws_tx: None,
            disconnect_tx,
        },
    );
    state
        .peer
        .on_data_channel(peer_id, Arc::clone(&state.registry))
        .await?;
    state
        .peer
        .on_track(peer_id, Arc::clone(&state.registry))
        .await?;

    Ok(Json(OfferResponse {
        peer_id,
        sdp: answer.sdp,
    }))
}

#[derive(serde::Deserialize)]
pub struct WsQuery {
    peer_id: Option<PeerId>,
}

pub async fn ws_ice<P: PeerOps + 'static>(
    ws: WebSocketUpgrade,
    Query(query): Query<WsQuery>,
    State(state): State<AppState<P>>,
) -> impl IntoResponse {
    let ice_rx = state.peer.subscribe_ice_candidates().await;
    ws.on_upgrade(move |socket| {
        handle_ice_socket(socket, state.peer, state.registry, query.peer_id, ice_rx)
    })
}

async fn handle_ice_socket<P: PeerOps>(
    socket: WebSocket,
    peer: Arc<P>,
    registry: Arc<SessionRegistry>,
    peer_id: Option<PeerId>,
    mut ice_rx: mpsc::Receiver<Option<IceCandidate>>,
) {
    let (mut sink, mut stream) = socket.split();
    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<IceWsMessage>();

    if let Some(id) = peer_id {
        registry.set_ws_sender(id, ws_tx);
    }

    loop {
        tokio::select! {
            msg = stream.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    _ => break,
                };
                let text = match msg {
                    Message::Text(t) => t,
                    Message::Close(_) => break,
                    _ => continue,
                };

                // Try full IceWsMessage envelope first, fall back to bare IceCandidate.
                if let Ok(envelope) = serde_json::from_str::<IceWsMessage>(&text) {
                    match envelope {
                        IceWsMessage::Candidate(c) => {
                            if validate_ice_candidate(&c).is_ok() {
                                if peer.add_ice_candidate(c).await.is_err() {
                                    break;
                                }
                            }
                        }
                        IceWsMessage::Answer(a) => {
                            if let Some(id) = peer_id {
                                if registry.apply_remote_answer(id, a).await.is_err() {
                                    break;
                                }
                            }
                        }
                        IceWsMessage::Offer(_) | IceWsMessage::Done => {}
                    }
                } else if let Ok(candidate) = serde_json::from_str::<IceCandidate>(&text) {
                    if validate_ice_candidate(&candidate).is_ok() {
                        if peer.add_ice_candidate(candidate).await.is_err() {
                            break;
                        }
                    }
                }
            }
            outgoing = ws_rx.recv() => {
                let Some(msg) = outgoing else { break };
                let Ok(json) = serde_json::to_string(&msg) else { continue };
                if sink.send(Message::Text(json.into())).await.is_err() {
                    break;
                }
            }
            candidate = ice_rx.recv() => {
                match candidate {
                    Some(Some(c)) => {
                        let msg = IceWsMessage::Candidate(c);
                        let Ok(json) = serde_json::to_string(&msg) else { continue };
                        if sink.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(None) | None => {
                        let Ok(json) = serde_json::to_string(&IceWsMessage::Done) else { break };
                        let _ = sink.send(Message::Text(json.into())).await;
                        break;
                    }
                }
            }
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

    use std::time::Duration;

    use ::webrtc::{api::APIBuilder, peer_connection::configuration::RTCConfiguration};
    use tokio_tungstenite::tungstenite::Message as WsMsg;

    use crate::{types::SdpAnswer, webrtc::peer::MockPeerOps};

    async fn spawn_ws_server(mock: MockPeerOps) -> std::net::SocketAddr {
        let state = AppState {
            peer: Arc::new(mock),
            registry: Arc::new(SessionRegistry::new()),
        };
        let app = router::<MockPeerOps>().with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok() });
        addr
    }

    const VALID_SDP: &str = "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n";

    fn build_router(mock: MockPeerOps) -> Router {
        let state = AppState {
            peer: Arc::new(mock),
            registry: Arc::new(SessionRegistry::new()),
        };
        router::<MockPeerOps>().with_state(state)
    }

    fn offer_request(sdp: &str) -> Request<Body> {
        let body = serde_json::to_string(&SdpOffer {
            sdp: sdp.to_string(),
        })
        .unwrap();
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
        let api = APIBuilder::new().build();
        let pc = Arc::new(api.new_peer_connection(RTCConfiguration::default()).await.unwrap());
        let mut mock = MockPeerOps::new();
        mock.expect_set_remote_description().returning(|_| Ok(()));
        mock.expect_create_answer().returning(|| {
            Ok(SdpAnswer {
                sdp: VALID_SDP.to_string(),
            })
        });
        mock.expect_on_data_channel().returning(|_, _| Ok(()));
        mock.expect_on_track().returning(|_, _| Ok(()));
        mock.expect_peer_connection().returning(move || Arc::clone(&pc));

        let response = build_router(mock)
            .oneshot(offer_request(VALID_SDP))
            .await
            .unwrap();

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

        let response = build_router(mock)
            .oneshot(offer_request(VALID_SDP))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn offer_empty_sdp_rejected() {
        let mock = MockPeerOps::new();

        let response = build_router(mock).oneshot(offer_request("")).await.unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn offer_on_track_error_propagates() {
        let api = APIBuilder::new().build();
        let pc = Arc::new(api.new_peer_connection(RTCConfiguration::default()).await.unwrap());
        let mut mock = MockPeerOps::new();
        mock.expect_set_remote_description().returning(|_| Ok(()));
        mock.expect_create_answer().returning(|| {
            Ok(SdpAnswer {
                sdp: VALID_SDP.to_string(),
            })
        });
        mock.expect_on_data_channel().returning(|_, _| Ok(()));
        mock.expect_on_track()
            .returning(|_, _| Err(AppError::SignalingError("fail".to_string())));
        mock.expect_peer_connection().returning(move || Arc::clone(&pc));

        let response = build_router(mock)
            .oneshot(offer_request(VALID_SDP))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn ws_ice_forwards_server_candidate_to_browser() {
        let (tx, rx) = mpsc::channel::<Option<IceCandidate>>(4);
        let shared_rx = Arc::new(std::sync::Mutex::new(Some(rx)));

        let cand = IceCandidate {
            candidate: "candidate:1 1 UDP 2130706431 10.0.0.1 12345 typ host".to_string(),
            sdp_mid: Some("0".to_string()),
            sdp_mline_index: Some(0),
        };
        tx.send(Some(cand.clone())).await.unwrap();

        let mut mock = MockPeerOps::new();
        mock.expect_subscribe_ice_candidates()
            .returning(move || shared_rx.lock().unwrap().take().unwrap());

        let addr = spawn_ws_server(mock).await;
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws/ice"))
            .await
            .unwrap();
        let (_, mut ws_rx) = ws.split();

        let msg = tokio::time::timeout(Duration::from_secs(2), ws_rx.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let text = match msg {
            WsMsg::Text(t) => t.to_string(),
            other => panic!("expected text frame, got {other:?}"),
        };
        let envelope: IceWsMessage = serde_json::from_str(&text).unwrap();
        assert!(
            matches!(&envelope, IceWsMessage::Candidate(c) if c.candidate == cand.candidate),
            "expected Candidate with the same string, got {envelope:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn ws_ice_sends_done_when_gathering_ends() {
        let (tx, rx) = mpsc::channel::<Option<IceCandidate>>(4);
        let shared_rx = Arc::new(std::sync::Mutex::new(Some(rx)));
        tx.send(None).await.unwrap(); // None = ICE gathering complete
        drop(tx);

        let mut mock = MockPeerOps::new();
        mock.expect_subscribe_ice_candidates()
            .returning(move || shared_rx.lock().unwrap().take().unwrap());

        let addr = spawn_ws_server(mock).await;
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws/ice"))
            .await
            .unwrap();
        let (_, mut ws_rx) = ws.split();

        let msg = tokio::time::timeout(Duration::from_secs(2), ws_rx.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let text = match msg {
            WsMsg::Text(t) => t.to_string(),
            other => panic!("expected text frame, got {other:?}"),
        };
        let envelope: IceWsMessage = serde_json::from_str(&text).unwrap();
        assert!(
            matches!(envelope, IceWsMessage::Done),
            "expected Done, got {envelope:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn ws_ice_accepts_candidate_from_browser() {
        // Keep _ice_tx alive (named binding) so the ice_rx channel stays open and the
        // server loop does not exit via the Done path before the browser message arrives.
        let (_ice_tx, rx) = mpsc::channel::<Option<IceCandidate>>(1);
        let shared_rx = Arc::new(std::sync::Mutex::new(Some(rx)));

        let (verified_tx, verified_rx) = oneshot::channel::<IceCandidate>();
        let verified_tx = Arc::new(std::sync::Mutex::new(Some(verified_tx)));

        let mut mock = MockPeerOps::new();
        mock.expect_subscribe_ice_candidates()
            .returning(move || shared_rx.lock().unwrap().take().unwrap());
        mock.expect_add_ice_candidate()
            .return_once(move |c| {
                let _ = verified_tx.lock().unwrap().take().unwrap().send(c);
                Ok(())
            });

        let addr = spawn_ws_server(mock).await;
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws/ice"))
            .await
            .unwrap();
        let (mut ws_tx, _) = ws.split();

        let outgoing = IceWsMessage::Candidate(IceCandidate {
            candidate: "candidate:1 1 UDP 2130706431 10.0.0.1 9999 typ host".to_string(),
            sdp_mid: Some("0".to_string()),
            sdp_mline_index: Some(0),
        });
        ws_tx
            .send(WsMsg::Text(serde_json::to_string(&outgoing).unwrap().into()))
            .await
            .unwrap();

        let received = tokio::time::timeout(Duration::from_secs(2), verified_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(received.candidate.contains("10.0.0.1"));
    }
}
