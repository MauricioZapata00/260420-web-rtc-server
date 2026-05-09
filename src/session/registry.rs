#![allow(dead_code)]

use std::sync::Arc;

use ::webrtc::{
    data_channel::RTCDataChannel,
    peer_connection::{sdp::session_description::RTCSessionDescription, RTCPeerConnection},
    rtp_transceiver::rtp_sender::RTCRtpSender,
    track::track_local::{TrackLocal, track_local_static_rtp::TrackLocalStaticRTP},
};
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};

use crate::types::{AppError, IceWsMessage, PeerId, SdpAnswer, SdpOffer};

pub struct PeerHandle {
    pub data_channel: Option<Arc<RTCDataChannel>>,
    pub peer_connection: Option<Arc<RTCPeerConnection>>,
    pub ws_tx: Option<mpsc::UnboundedSender<IceWsMessage>>,
    pub disconnect_tx: oneshot::Sender<()>,
}

pub struct SessionRegistry {
    peers: DashMap<PeerId, PeerHandle>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self {
            peers: DashMap::new(),
        }
    }

    pub fn register(&self, id: PeerId, handle: PeerHandle) {
        self.peers.insert(id, handle);
    }

    pub fn deregister(&self, id: PeerId) {
        if let Some((_, handle)) = self.peers.remove(&id) {
            let _ = handle.disconnect_tx.send(());
        }
    }

    pub fn set_data_channel(&self, id: PeerId, dc: Arc<RTCDataChannel>) {
        if let Some(mut entry) = self.peers.get_mut(&id) {
            entry.data_channel = Some(dc);
        }
    }

    pub fn get_data_channel(&self, id: PeerId) -> Option<Arc<RTCDataChannel>> {
        self.peers.get(&id).and_then(|e| e.data_channel.as_ref().map(Arc::clone))
    }

    pub fn set_ws_sender(&self, id: PeerId, tx: mpsc::UnboundedSender<IceWsMessage>) {
        if let Some(mut entry) = self.peers.get_mut(&id) {
            entry.ws_tx = Some(tx);
        }
    }

    pub fn broadcast_text(&self, from: PeerId, text: &str) {
        for entry in self.peers.iter() {
            if *entry.key() == from {
                continue;
            }
            if let Some(dc) = entry.data_channel.as_ref() {
                let dc = Arc::clone(dc);
                let text = text.to_string();
                tokio::spawn(async move {
                    let _ = dc.send_text(text).await;
                });
            }
        }
    }

    // Collect snapshot before any await to avoid holding shard locks across await points.
    pub async fn add_track_to_all(
        &self,
        except: PeerId,
        track: Arc<TrackLocalStaticRTP>,
    ) -> Result<Vec<(PeerId, Arc<RTCRtpSender>)>, AppError> {
        let targets: Vec<(
            PeerId,
            Arc<RTCPeerConnection>,
            Option<mpsc::UnboundedSender<IceWsMessage>>,
        )> = self
            .peers
            .iter()
            .filter(|e| *e.key() != except)
            .filter_map(|e| {
                e.peer_connection
                    .as_ref()
                    .map(|pc| (*e.key(), Arc::clone(pc), e.ws_tx.clone()))
            })
            .collect();

        let mut senders = Vec::new();
        for (peer_id, pc, ws_tx) in targets {
            let track_clone = Arc::clone(&track);
            let track_dyn: Arc<dyn TrackLocal + Send + Sync> = track_clone;
            let sender = pc
                .add_track(track_dyn)
                .await
                .map_err(|e| AppError::SignalingError(e.to_string()))?;

            if let Some(tx) = ws_tx {
                let offer = pc
                    .create_offer(None)
                    .await
                    .map_err(|e| AppError::SdpParseFailed(e.to_string()))?;
                pc.set_local_description(offer.clone())
                    .await
                    .map_err(|e| AppError::SdpParseFailed(e.to_string()))?;
                let _ = tx.send(IceWsMessage::Offer(SdpOffer { sdp: offer.sdp }));
            }

            senders.push((peer_id, sender));
        }
        Ok(senders)
    }

    pub async fn remove_tracks(&self, senders: Vec<(PeerId, Arc<RTCRtpSender>)>) {
        let targets: Vec<(Arc<RTCPeerConnection>, Arc<RTCRtpSender>)> = senders
            .into_iter()
            .filter_map(|(peer_id, sender)| {
                self.peers.get(&peer_id).and_then(|e| {
                    e.peer_connection
                        .as_ref()
                        .map(|pc| (Arc::clone(pc), sender))
                })
            })
            .collect();

        for (pc, sender) in targets {
            let _ = pc.remove_track(&sender).await;
        }
    }

    pub async fn apply_remote_answer(&self, id: PeerId, sdp: SdpAnswer) -> Result<(), AppError> {
        let pc: Option<Arc<RTCPeerConnection>> = self
            .peers
            .get(&id)
            .and_then(|e| e.peer_connection.as_ref().map(Arc::clone));

        let pc = pc.ok_or_else(|| {
            AppError::SignalingError("peer not found or missing connection".to_string())
        })?;

        let answer = RTCSessionDescription::answer(sdp.sdp)
            .map_err(|e| AppError::SdpParseFailed(e.to_string()))?;

        pc.set_remote_description(answer)
            .await
            .map_err(|e| AppError::SdpParseFailed(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn make_handle() -> PeerHandle {
        let (disconnect_tx, _) = oneshot::channel();
        PeerHandle {
            data_channel: None,
            peer_connection: None,
            ws_tx: None,
            disconnect_tx,
        }
    }

    #[test]
    fn register_then_deregister_removes_entry() {
        let registry = SessionRegistry::new();
        let id = PeerId::new();
        registry.register(id, make_handle());
        assert!(registry.peers.contains_key(&id));
        registry.deregister(id);
        assert!(!registry.peers.contains_key(&id));
    }

    #[test]
    fn deregister_fires_disconnect_tx() {
        let registry = SessionRegistry::new();
        let id = PeerId::new();
        let (tx, mut rx) = oneshot::channel::<()>();
        registry.register(
            id,
            PeerHandle {
                data_channel: None,
                peer_connection: None,
                ws_tx: None,
                disconnect_tx: tx,
            },
        );
        registry.deregister(id);
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn deregister_unknown_peer_is_noop() {
        let registry = SessionRegistry::new();
        registry.deregister(PeerId::new());
    }

    #[test]
    fn set_ws_sender_stores_value() {
        let registry = SessionRegistry::new();
        let id = PeerId::new();
        registry.register(id, make_handle());
        let (tx, _rx) = mpsc::unbounded_channel::<IceWsMessage>();
        registry.set_ws_sender(id, tx);
        assert!(registry.peers.get(&id).unwrap().ws_tx.is_some());
    }

    #[tokio::test]
    async fn broadcast_text_skips_sender() {
        let registry = SessionRegistry::new();
        let sender = PeerId::new();
        registry.register(sender, make_handle());
        registry.broadcast_text(sender, "hello");
        assert!(registry.peers.contains_key(&sender));
    }

    #[tokio::test]
    async fn broadcast_text_silently_skips_peers_without_dc() {
        let registry = SessionRegistry::new();
        let sender = PeerId::new();
        let other = PeerId::new();
        registry.register(sender, make_handle());
        registry.register(other, make_handle());
        registry.broadcast_text(sender, "hello");
        assert!(registry.peers.contains_key(&other));
    }

    #[tokio::test]
    async fn add_track_to_all_skips_peers_without_pc() {
        use ::webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
        use ::webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;

        let registry = SessionRegistry::new();
        let publisher = PeerId::new();
        let listener = PeerId::new();
        registry.register(publisher, make_handle());
        registry.register(listener, make_handle());

        let track = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
            "audio".to_string(),
            "stream".to_string(),
        ));

        let senders = registry.add_track_to_all(publisher, track).await.unwrap();
        assert!(senders.is_empty());
    }

    #[tokio::test]
    async fn remove_tracks_with_empty_list_is_noop() {
        let registry = SessionRegistry::new();
        registry.remove_tracks(vec![]).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn add_track_to_all_fans_out_to_receivers_skips_publisher() {
        use ::webrtc::{
            api::APIBuilder,
            peer_connection::configuration::RTCConfiguration,
            rtp_transceiver::rtp_codec::RTCRtpCodecCapability,
            track::track_local::track_local_static_rtp::TrackLocalStaticRTP,
        };

        let api = APIBuilder::new().build();
        let config = RTCConfiguration::default();
        let publisher_pc = Arc::new(api.new_peer_connection(config.clone()).await.unwrap());
        let receiver_pc = Arc::new(api.new_peer_connection(config).await.unwrap());

        let registry = SessionRegistry::new();
        let publisher = PeerId::new();
        let receiver = PeerId::new();

        let (tx1, _) = oneshot::channel();
        registry.register(
            publisher,
            PeerHandle {
                data_channel: None,
                peer_connection: Some(Arc::clone(&publisher_pc)),
                ws_tx: None,
                disconnect_tx: tx1,
            },
        );
        let (tx2, _) = oneshot::channel();
        registry.register(
            receiver,
            PeerHandle {
                data_channel: None,
                peer_connection: Some(Arc::clone(&receiver_pc)),
                ws_tx: None,
                disconnect_tx: tx2,
            },
        );

        let track = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
            "audio".to_string(),
            "stream".to_string(),
        ));

        let senders = registry.add_track_to_all(publisher, track).await.unwrap();
        assert_eq!(senders.len(), 1, "only the receiver should receive the track");
        assert_eq!(senders[0].0, receiver, "sender entry must reference the receiver peer");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn remove_tracks_removes_senders_from_correct_connections() {
        use ::webrtc::{
            api::APIBuilder,
            peer_connection::configuration::RTCConfiguration,
            rtp_transceiver::rtp_codec::RTCRtpCodecCapability,
            track::track_local::track_local_static_rtp::TrackLocalStaticRTP,
        };

        let api = APIBuilder::new().build();
        let config = RTCConfiguration::default();
        let publisher_pc = Arc::new(api.new_peer_connection(config.clone()).await.unwrap());
        let receiver_pc = Arc::new(api.new_peer_connection(config).await.unwrap());

        let registry = SessionRegistry::new();
        let publisher = PeerId::new();
        let receiver = PeerId::new();

        let (tx1, _) = oneshot::channel();
        registry.register(
            publisher,
            PeerHandle {
                data_channel: None,
                peer_connection: Some(Arc::clone(&publisher_pc)),
                ws_tx: None,
                disconnect_tx: tx1,
            },
        );
        let (tx2, _) = oneshot::channel();
        registry.register(
            receiver,
            PeerHandle {
                data_channel: None,
                peer_connection: Some(Arc::clone(&receiver_pc)),
                ws_tx: None,
                disconnect_tx: tx2,
            },
        );

        let track = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
            "audio".to_string(),
            "stream".to_string(),
        ));

        let senders = registry.add_track_to_all(publisher, track).await.unwrap();
        assert!(!senders.is_empty());
        registry.remove_tracks(senders).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn apply_remote_answer_fails_for_unknown_peer() {
        let registry = SessionRegistry::new();
        let result = registry
            .apply_remote_answer(PeerId::new(), SdpAnswer { sdp: "v=0\r\n".to_string() })
            .await;
        assert!(matches!(result, Err(AppError::SignalingError(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn apply_remote_answer_fails_for_peer_without_connection() {
        let registry = SessionRegistry::new();
        let id = PeerId::new();
        registry.register(id, make_handle());
        let result = registry
            .apply_remote_answer(id, SdpAnswer { sdp: "v=0\r\n".to_string() })
            .await;
        assert!(matches!(result, Err(AppError::SignalingError(_))));
    }
}
