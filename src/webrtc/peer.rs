#![allow(dead_code)]

use std::sync::Arc;

use ::webrtc::{
    api::APIBuilder,
    data_channel::{data_channel_message::DataChannelMessage, RTCDataChannel},
    ice_transport::ice_candidate::RTCIceCandidateInit,
    peer_connection::{
        configuration::RTCConfiguration, sdp::session_description::RTCSessionDescription,
        RTCPeerConnection,
    },
};

use crate::types::{AppError, IceCandidate, SdpAnswer, SdpOffer};

#[cfg_attr(test, mockall::automock)]
pub trait PeerOps: Send + Sync {
    async fn set_remote_description(&self, sdp: SdpOffer) -> Result<(), AppError>;
    async fn create_answer(&self) -> Result<SdpAnswer, AppError>;
    async fn add_ice_candidate(&self, candidate: IceCandidate) -> Result<(), AppError>;
    async fn on_connection_state_change(&self) -> Result<(), AppError>;
    async fn on_data_channel(&self) -> Result<(), AppError>;
    async fn on_message(&self) -> Result<(), AppError>;
    async fn on_close(&self) -> Result<(), AppError>;
    async fn on_track(&self) -> Result<(), AppError>;
}

pub struct WebRtcPeer {
    pc: Arc<RTCPeerConnection>,
}

impl WebRtcPeer {
    pub async fn new() -> Result<Self, AppError> {
        let api = APIBuilder::new().build();
        let pc = api
            .new_peer_connection(RTCConfiguration::default())
            .await
            .map_err(|e| AppError::PeerConnectionFailed(e.to_string()))?;
        Ok(Self { pc: Arc::new(pc) })
    }
}

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

    async fn on_data_channel(&self) -> Result<(), AppError> {
        self.pc
            .on_data_channel(Box::new(|dc: Arc<RTCDataChannel>| {
                let label = dc.label().to_string();
                tracing::info!("data channel opened: {label}");
                Box::pin(async move {
                    dc.on_message(Box::new(|msg: DataChannelMessage| {
                        tracing::info!("message received ({} bytes)", msg.data.len());
                        Box::pin(async {})
                    }));
                    dc.on_close(Box::new(|| {
                        tracing::info!("data channel closed");
                        Box::pin(async {})
                    }));
                })
            }));
        Ok(())
    }

    async fn on_message(&self) -> Result<(), AppError> {
        Ok(())
    }

    async fn on_close(&self) -> Result<(), AppError> {
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
}
