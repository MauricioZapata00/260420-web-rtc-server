#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use ::webrtc::{
    api::APIBuilder,
    data_channel::{data_channel_message::DataChannelMessage, RTCDataChannel},
    ice_transport::{
        ice_candidate::{RTCIceCandidate, RTCIceCandidateInit},
        ice_server::RTCIceServer,
    },
    peer_connection::{
        configuration::RTCConfiguration, sdp::session_description::RTCSessionDescription,
        RTCPeerConnection,
    },
    track::track_local::{track_local_static_rtp::TrackLocalStaticRTP, TrackLocalWriter},
};

use crate::{
    session::SessionRegistry,
    types::{AppError, ChatMessage, IceCandidate, PeerId, SdpAnswer, SdpOffer},
};

#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait PeerOps: Send + Sync {
    async fn set_remote_description(&self, sdp: SdpOffer) -> Result<(), AppError>;
    async fn set_remote_answer(&self, sdp: SdpAnswer) -> Result<(), AppError>;
    async fn create_answer(&self) -> Result<SdpAnswer, AppError>;
    async fn add_ice_candidate(&self, candidate: IceCandidate) -> Result<(), AppError>;
    async fn on_connection_state_change(&self) -> Result<(), AppError>;
    async fn on_data_channel(
        &self,
        peer_id: PeerId,
        registry: Arc<SessionRegistry>,
    ) -> Result<(), AppError>;
    async fn on_track(
        &self,
        peer_id: PeerId,
        registry: Arc<SessionRegistry>,
    ) -> Result<(), AppError>;
    fn peer_connection(&self) -> Arc<RTCPeerConnection>;
    async fn subscribe_ice_candidates(&self) -> mpsc::Receiver<Option<IceCandidate>>;
}

fn make_broadcast_payload(peer_id: PeerId, text: String) -> String {
    serde_json::to_string(&ChatMessage { from: peer_id, text }).unwrap_or_default()
}

async fn handle_data_channel_open(
    peer_id: PeerId,
    dc: Arc<RTCDataChannel>,
    registry: Arc<SessionRegistry>,
) {
    registry.set_data_channel(peer_id, Arc::clone(&dc));

    let registry_msg = Arc::clone(&registry);
    dc.on_message(Box::new(move |msg: DataChannelMessage| {
        let registry = Arc::clone(&registry_msg);
        Box::pin(async move {
            if msg.is_string {
                let text = String::from_utf8_lossy(&msg.data).to_string();
                let json = make_broadcast_payload(peer_id, text);
                registry.broadcast_text(peer_id, &json);
            }
        })
    }));

    let registry_close = Arc::clone(&registry);
    dc.on_close(Box::new(move || {
        let registry = Arc::clone(&registry_close);
        Box::pin(async move {
            registry.deregister(peer_id);
        })
    }));
}

pub struct WebRtcPeer {
    pc: Arc<RTCPeerConnection>,
    ice_tx: Arc<Mutex<Option<mpsc::Sender<Option<IceCandidate>>>>>,
}

impl WebRtcPeer {
    pub async fn new() -> Result<Self, AppError> {
        let config = RTCConfiguration {
            ice_servers: vec![RTCIceServer {
                urls: vec![
                    "stun:stun.l.google.com:19302".to_string(),
                    "stun:stun1.l.google.com:19302".to_string(),
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let api = APIBuilder::new().build();
        let pc = api
            .new_peer_connection(config)
            .await
            .map_err(|e| AppError::PeerConnectionFailed(e.to_string()))?;
        Ok(Self {
            pc: Arc::new(pc),
            ice_tx: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait::async_trait]
impl PeerOps for WebRtcPeer {
    async fn set_remote_description(&self, sdp: SdpOffer) -> Result<(), AppError> {
        let offer = RTCSessionDescription::offer(sdp.sdp)
            .map_err(|e| AppError::SdpParseFailed(e.to_string()))?;
        self.pc
            .set_remote_description(offer)
            .await
            .map_err(|e| AppError::SdpParseFailed(e.to_string()))
    }

    async fn set_remote_answer(&self, sdp: SdpAnswer) -> Result<(), AppError> {
        let answer = RTCSessionDescription::answer(sdp.sdp)
            .map_err(|e| AppError::SdpParseFailed(e.to_string()))?;
        self.pc
            .set_remote_description(answer)
            .await
            .map_err(|e| AppError::SdpParseFailed(e.to_string()))
    }

    async fn create_answer(&self) -> Result<SdpAnswer, AppError> {
        let answer = self
            .pc
            .create_answer(None)
            .await
            .map_err(|e| AppError::SdpParseFailed(e.to_string()))?;
        self.pc
            .set_local_description(answer.clone())
            .await
            .map_err(|e| AppError::SdpParseFailed(e.to_string()))?;
        Ok(SdpAnswer { sdp: answer.sdp })
    }

    async fn add_ice_candidate(&self, candidate: IceCandidate) -> Result<(), AppError> {
        let init = RTCIceCandidateInit {
            candidate: candidate.candidate,
            sdp_mid: candidate.sdp_mid,
            sdp_mline_index: candidate.sdp_mline_index,
            ..Default::default()
        };
        self.pc
            .add_ice_candidate(init)
            .await
            .map_err(|e| AppError::IceCandidateFailed(e.to_string()))
    }

    async fn on_connection_state_change(&self) -> Result<(), AppError> {
        self.pc
            .on_peer_connection_state_change(Box::new(|state| {
                tracing::info!("connection state: {state}");
                Box::pin(async {})
            }));
        Ok(())
    }

    async fn on_data_channel(
        &self,
        peer_id: PeerId,
        registry: Arc<SessionRegistry>,
    ) -> Result<(), AppError> {
        self.pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
            let registry = Arc::clone(&registry);
            tracing::info!("data channel opened: {}", dc.label());
            Box::pin(handle_data_channel_open(peer_id, dc, registry))
        }));
        Ok(())
    }

    async fn on_track(
        &self,
        peer_id: PeerId,
        registry: Arc<SessionRegistry>,
    ) -> Result<(), AppError> {
        self.pc.on_track(Box::new(move |track, _receiver, _transceiver| {
            let registry = Arc::clone(&registry);
            Box::pin(async move {
                tracing::info!(
                    "track received: {}/{}",
                    track.kind(),
                    track.codec().capability.mime_type
                );

                let local_track = Arc::new(TrackLocalStaticRTP::new(
                    track.codec().capability.clone(),
                    track.id(),
                    track.stream_id(),
                ));

                let senders = match registry
                    .add_track_to_all(peer_id, Arc::clone(&local_track))
                    .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!("add_track_to_all failed: {e}");
                        return;
                    }
                };

                tokio::spawn(async move {
                    loop {
                        match track.read_rtp().await {
                            Ok((pkt, _attr)) => {
                                if local_track.write_rtp(&pkt).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    registry.remove_tracks(senders).await;
                });
            })
        }));
        Ok(())
    }

    fn peer_connection(&self) -> Arc<RTCPeerConnection> {
        Arc::clone(&self.pc)
    }

    async fn subscribe_ice_candidates(&self) -> mpsc::Receiver<Option<IceCandidate>> {
        let (tx, rx) = mpsc::channel(32);
        *self.ice_tx.lock().unwrap() = Some(tx);

        let ice_tx = Arc::clone(&self.ice_tx);
        self.pc.on_ice_candidate(Box::new(move |candidate: Option<RTCIceCandidate>| {
            let ice_tx = Arc::clone(&ice_tx);
            Box::pin(async move {
                let mapped = candidate
                    .and_then(|c| c.to_json().ok())
                    .map(|init| IceCandidate {
                        candidate: init.candidate,
                        sdp_mid: init.sdp_mid,
                        sdp_mline_index: init.sdp_mline_index,
                    });
                let done = mapped.is_none();
                let tx = ice_tx.lock().unwrap().clone();
                if let Some(tx) = tx {
                    let _ = tx.send(mapped).await;
                    if done {
                        ice_tx.lock().unwrap().take();
                    }
                }
            })
        }));

        rx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn on_connection_state_change_registers_ok() {
        let peer = WebRtcPeer::new().await.unwrap();
        assert!(peer.on_connection_state_change().await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn on_track_registers_ok() {
        let peer = WebRtcPeer::new().await.unwrap();
        let registry = Arc::new(SessionRegistry::new());
        assert!(peer.on_track(PeerId::new(), registry).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn on_data_channel_registers_ok() {
        let peer = WebRtcPeer::new().await.unwrap();
        let registry = Arc::new(SessionRegistry::new());
        assert!(peer.on_data_channel(PeerId::new(), registry).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn set_remote_description_error_via_mock() {
        let mut mock = MockPeerOps::new();
        mock.expect_set_remote_description()
            .returning(|_| Err(AppError::SdpParseFailed("fail".to_string())));
        let result = mock
            .set_remote_description(SdpOffer { sdp: "bad".to_string() })
            .await;
        assert!(matches!(result, Err(AppError::SdpParseFailed(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn set_remote_answer_error_via_mock() {
        let mut mock = MockPeerOps::new();
        mock.expect_set_remote_answer()
            .returning(|_| Err(AppError::SdpParseFailed("fail".to_string())));
        let result = mock
            .set_remote_answer(SdpAnswer { sdp: "bad".to_string() })
            .await;
        assert!(matches!(result, Err(AppError::SdpParseFailed(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn create_answer_error_via_mock() {
        let mut mock = MockPeerOps::new();
        mock.expect_create_answer()
            .returning(|| Err(AppError::SdpParseFailed("fail".to_string())));
        let result = mock.create_answer().await;
        assert!(matches!(result, Err(AppError::SdpParseFailed(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn add_ice_candidate_error_via_mock() {
        let mut mock = MockPeerOps::new();
        mock.expect_add_ice_candidate()
            .returning(|_| Err(AppError::IceCandidateFailed("fail".to_string())));
        let result = mock
            .add_ice_candidate(IceCandidate {
                candidate: "x".to_string(),
                sdp_mid: None,
                sdp_mline_index: None,
            })
            .await;
        assert!(matches!(result, Err(AppError::IceCandidateFailed(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn on_data_channel_error_via_mock() {
        let mut mock = MockPeerOps::new();
        mock.expect_on_data_channel()
            .returning(|_, _| Err(AppError::SignalingError("fail".to_string())));
        let result = mock
            .on_data_channel(PeerId::new(), Arc::new(SessionRegistry::new()))
            .await;
        assert!(matches!(result, Err(AppError::SignalingError(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn on_track_error_via_mock() {
        let mut mock = MockPeerOps::new();
        mock.expect_on_track()
            .returning(|_, _| Err(AppError::SignalingError("fail".to_string())));
        let result = mock
            .on_track(PeerId::new(), Arc::new(SessionRegistry::new()))
            .await;
        assert!(matches!(result, Err(AppError::SignalingError(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn subscribe_ice_candidates_delivers_candidate_to_receiver() {
        let (tx, rx) = mpsc::channel::<Option<IceCandidate>>(4);
        let shared_rx = Arc::new(std::sync::Mutex::new(Some(rx)));

        let mut mock = MockPeerOps::new();
        mock.expect_subscribe_ice_candidates()
            .returning(move || shared_rx.lock().unwrap().take().unwrap());

        let mut receiver = mock.subscribe_ice_candidates().await;

        let cand = IceCandidate {
            candidate: "candidate:1 1 UDP 2130706431 192.168.1.1 54321 typ host".to_string(),
            sdp_mid: Some("0".to_string()),
            sdp_mline_index: Some(0),
        };
        tx.send(Some(cand.clone())).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert!(received.is_some());
        assert_eq!(received.unwrap().candidate, cand.candidate);
    }

    #[test]
    fn message_handler_encodes_from_peer_id_in_json() {
        let peer_id = PeerId::new();
        let json = make_broadcast_payload(peer_id, "hello".to_string());
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["text"], "hello");
        let from: PeerId = serde_json::from_value(parsed["from"].clone()).unwrap();
        assert_eq!(from, peer_id, "from field must match the sending peer's id");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn on_data_channel_open_registers_dc_in_registry() {
        use crate::session::PeerHandle;
        use tokio::sync::oneshot;
        use ::webrtc::{api::APIBuilder, peer_connection::configuration::RTCConfiguration};

        let api = APIBuilder::new().build();
        let pc = Arc::new(api.new_peer_connection(RTCConfiguration::default()).await.unwrap());
        let dc = pc.create_data_channel("test", None).await.unwrap();

        let registry = Arc::new(SessionRegistry::new());
        let peer_id = PeerId::new();

        let (disconnect_tx, _) = oneshot::channel();
        registry.register(
            peer_id,
            PeerHandle {
                data_channel: None,
                peer_connection: Some(Arc::clone(&pc)),
                ws_tx: None,
                disconnect_tx,
            },
        );

        handle_data_channel_open(peer_id, dc, Arc::clone(&registry)).await;

        assert!(
            registry.get_data_channel(peer_id).is_some(),
            "set_data_channel must be invoked once when the data channel opens"
        );
    }
}
