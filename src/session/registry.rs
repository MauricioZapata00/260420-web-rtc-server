#![allow(dead_code)]

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::oneshot;
use ::webrtc::data_channel::RTCDataChannel;

use crate::types::PeerId;

pub struct PeerHandle {
    pub data_channel: Option<Arc<RTCDataChannel>>,
    pub disconnect_tx: oneshot::Sender<()>,
}

pub struct SessionRegistry {
    peers: DashMap<PeerId, PeerHandle>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self { peers: DashMap::new() }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handle() -> PeerHandle {
        let (disconnect_tx, _) = oneshot::channel();
        PeerHandle { data_channel: None, disconnect_tx }
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
        registry.register(id, PeerHandle { data_channel: None, disconnect_tx: tx });
        registry.deregister(id);
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn deregister_unknown_peer_is_noop() {
        let registry = SessionRegistry::new();
        registry.deregister(PeerId::new());
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
}
