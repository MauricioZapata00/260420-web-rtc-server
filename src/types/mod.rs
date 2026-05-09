#![allow(dead_code)]

pub mod error;
pub use error::AppError;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PeerId(pub Uuid);

impl PeerId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OfferResponse {
    pub peer_id: PeerId,
    pub sdp: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatMessage {
    pub from: PeerId,
    pub text: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SdpOffer {
    pub sdp: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SdpAnswer {
    pub sdp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IceCandidate {
    pub candidate: String,
    pub sdp_mid: Option<String>,
    pub sdp_mline_index: Option<u16>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum IceWsMessage {
    Candidate(IceCandidate),
    Done,
    Offer(SdpOffer),
    Answer(SdpAnswer),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sdp_offer_json_round_trip() {
        let offer = SdpOffer {
            sdp: "v=0\r\n".to_string(),
        };
        let json = serde_json::to_string(&offer).unwrap();
        let decoded: SdpOffer = serde_json::from_str(&json).unwrap();
        assert_eq!(offer.sdp, decoded.sdp);
    }

    #[test]
    fn sdp_answer_json_round_trip() {
        let answer = SdpAnswer {
            sdp: "v=0\r\n".to_string(),
        };
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

    #[test]
    fn peer_id_new_produces_unique_ids() {
        let a = PeerId::new();
        let b = PeerId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn peer_id_copy_and_eq() {
        let id = PeerId::new();
        let copy = id;
        assert_eq!(id, copy);
    }

    #[test]
    fn peer_id_serialises_as_plain_uuid_string() {
        let id = PeerId::new();
        let json = serde_json::to_string(&id).unwrap();
        let decoded: PeerId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, decoded);
        assert!(!json.contains('{'));
    }

    #[test]
    fn offer_response_json_round_trip() {
        let id = PeerId::new();
        let resp = OfferResponse {
            peer_id: id,
            sdp: "v=0\r\n".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: OfferResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.peer_id, id);
        assert_eq!(decoded.sdp, "v=0\r\n");
    }

    #[test]
    fn ice_ws_message_candidate_round_trip() {
        let c = IceCandidate {
            candidate: "candidate:1 1 UDP 1 192.168.1.1 1234 typ host".to_string(),
            sdp_mid: Some("0".to_string()),
            sdp_mline_index: Some(0),
        };
        let msg = IceWsMessage::Candidate(c);
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&msg).unwrap()).unwrap();
        assert_eq!(json["type"], "candidate");
    }

    #[test]
    fn ice_ws_message_offer_round_trip() {
        let msg = IceWsMessage::Offer(SdpOffer {
            sdp: "v=0\r\n".to_string(),
        });
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&msg).unwrap()).unwrap();
        assert_eq!(json["type"], "offer");
    }

    #[test]
    fn ice_ws_message_answer_round_trip() {
        let msg = IceWsMessage::Answer(SdpAnswer {
            sdp: "v=0\r\n".to_string(),
        });
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&msg).unwrap()).unwrap();
        assert_eq!(json["type"], "answer");
    }

    #[test]
    fn ice_ws_message_done_serializes_to_type_done() {
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&IceWsMessage::Done).unwrap()).unwrap();
        assert_eq!(json["type"], "done");
        assert!(json.get("data").is_none(), "Done must not have a data field");
        let decoded: IceWsMessage = serde_json::from_value(json).unwrap();
        assert!(matches!(decoded, IceWsMessage::Done));
    }

    #[test]
    fn chat_message_json_includes_from_and_text() {
        let id = PeerId::new();
        let msg = ChatMessage {
            from: id,
            text: "hello".to_string(),
        };
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&msg).unwrap()).unwrap();
        assert_eq!(json["text"], "hello");
        assert!(json.get("from").is_some());
    }
}
