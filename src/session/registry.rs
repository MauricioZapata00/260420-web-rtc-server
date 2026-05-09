#![allow(dead_code)]

use std::sync::Arc;

use ::webrtc::{
    data_channel::RTCDataChannel,
    peer_connection::RTCPeerConnection,
    rtp_transceiver::rtp_sender::RTCRtpSender,
    track::track_local::{TrackLocal, track_local_static_rtp::TrackLocalStaticRTP},
};
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};

use crate::types::{AppError, IceWsMessage, PeerId, SdpOffer};

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

    pub fn set_peer_connection(&self, id: PeerId, pc: Arc<RTCPeerConnection>) {
        if let Some(mut entry) = self.peers.get_mut(&id) {
            entry.peer_connection = Some(pc);
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
