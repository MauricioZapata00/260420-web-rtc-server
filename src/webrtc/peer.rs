#![allow(dead_code)]

use std::sync::Arc;

use ::webrtc::{
    api::APIBuilder,
    data_channel::{data_channel_message::DataChannelMessage, RTCDataChannel},
    ice_transport::{ice_candidate::RTCIceCandidateInit, ice_server::RTCIceServer},
    peer_connection::{
        configuration::RTCConfiguration, sdp::session_description::RTCSessionDescription,
        RTCPeerConnection,
    },
};

use crate::{
    session::SessionRegistry,
    types::{AppError, ChatMessage, IceCandidate, PeerId, SdpAnswer, SdpOffer},
};

#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait PeerOps: Send + Sync {
    async fn set_remote_description(&self, sdp: SdpOffer) -> Result<(), AppError>;
    async fn create_answer(&self) -> Result<SdpAnswer, AppError>;
    async fn add_ice_candidate(&self, candidate: IceCandidate) -> Result<(), AppError>;
    async fn on_connection_state_change(&self) -> Result<(), AppError>;
    async fn on_data_channel(
        &self,
        peer_id: PeerId,
        registry: Arc<SessionRegistry>,
    ) -> Result<(), AppError>;
    async fn on_track(&self) -> Result<(), AppError>;
}

pub struct WebRtcPeer {
    pc: Arc<RTCPeerConnection>,
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
        Ok(Self { pc: Arc::new(pc) })
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
            Box::pin(async move {
                registry.set_data_channel(peer_id, Arc::clone(&dc));

                let registry_msg = Arc::clone(&registry);
                dc.on_message(Box::new(move |msg: DataChannelMessage| {
                    let registry = Arc::clone(&registry_msg);
                    Box::pin(async move {
                        if msg.is_string {
                            let text = String::from_utf8_lossy(&msg.data).to_string();
                            let json =
                                serde_json::to_string(&ChatMessage { from: peer_id, text })
                                    .unwrap_or_default();
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
            })
        }));
        Ok(())
    }

    async fn on_track(&self) -> Result<(), AppError> {
        self.pc.on_track(Box::new(|_track, _receiver, _transceiver| {
            tracing::info!("track received");
            Box::pin(async {})
        }));
        Ok(())
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
        assert!(peer.on_track().await.is_ok());
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
}
