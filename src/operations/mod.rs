use crate::types::{AppError, IceCandidate, SdpOffer};

pub fn parse_sdp_offer(raw: &str) -> Result<SdpOffer, AppError> {
    if raw.is_empty() {
        return Err(AppError::SdpParseFailed("SDP must not be empty".to_string()));
    }
    Ok(SdpOffer { sdp: raw.to_string() })
}

pub fn validate_ice_candidate(candidate: &IceCandidate) -> Result<(), AppError> {
    if candidate.candidate.is_empty() {
        return Err(AppError::IceCandidateFailed(
            "candidate string must not be empty".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::IceCandidate;

    #[test]
    fn parse_sdp_offer_valid() {
        let result = parse_sdp_offer("v=0\r\n");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().sdp, "v=0\r\n");
    }

    #[test]
    fn parse_sdp_offer_empty() {
        let result = parse_sdp_offer("");
        assert!(matches!(result, Err(AppError::SdpParseFailed(_))));
    }

    #[test]
    fn validate_ice_candidate_valid() {
        let candidate = IceCandidate {
            candidate: "candidate:1 1 UDP 2130706431 192.168.1.1 54321 typ host".to_string(),
            sdp_mid: Some("0".to_string()),
            sdp_mline_index: Some(0),
        };
        assert!(validate_ice_candidate(&candidate).is_ok());
    }

    #[test]
    fn validate_ice_candidate_empty() {
        let candidate = IceCandidate {
            candidate: String::new(),
            sdp_mid: None,
            sdp_mline_index: None,
        };
        assert!(matches!(
            validate_ice_candidate(&candidate),
            Err(AppError::IceCandidateFailed(_))
        ));
    }
}
