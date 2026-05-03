#![allow(dead_code)]

pub mod error;
pub use error::AppError;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SdpOffer {
    pub sdp: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SdpAnswer {
    pub sdp: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IceCandidate {
    pub candidate: String,
    pub sdp_mid: Option<String>,
    pub sdp_mline_index: Option<u16>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sdp_offer_json_round_trip() {
        let offer = SdpOffer { sdp: "v=0\r\n".to_string() };
        let json = serde_json::to_string(&offer).unwrap();
        let decoded: SdpOffer = serde_json::from_str(&json).unwrap();
        assert_eq!(offer.sdp, decoded.sdp);
    }

    #[test]
    fn sdp_answer_json_round_trip() {
        let answer = SdpAnswer { sdp: "v=0\r\n".to_string() };
        let json = serde_json::to_string(&answer).unwrap();
        let decoded: SdpAnswer = serde_json::from_str(&json).unwrap();
        assert_eq!(answer.sdp, decoded.sdp);
    }

    #[test]
    fn ice_candidate_json_round_trip() {
        let candidate = IceCandidate {
            candidate: "candidate:1 1 UDP 2130706431 192.168.1.1 54321 typ host".to_string(),
            sdp_mid: Some("0".to_string()),
            sdp_mline_index: Some(0),
        };
        let json = serde_json::to_string(&candidate).unwrap();
        let decoded: IceCandidate = serde_json::from_str(&json).unwrap();
        assert_eq!(candidate.candidate, decoded.candidate);
        assert_eq!(candidate.sdp_mid, decoded.sdp_mid);
        assert_eq!(candidate.sdp_mline_index, decoded.sdp_mline_index);
    }

    #[test]
    fn app_error_display_non_empty() {
        let errors = [
            AppError::PeerConnectionFailed("test".to_string()),
            AppError::SdpParseFailed("test".to_string()),
            AppError::IceCandidateFailed("test".to_string()),
            AppError::SignalingError("test".to_string()),
        ];
        for err in &errors {
            assert!(!err.to_string().is_empty());
        }
    }
}
