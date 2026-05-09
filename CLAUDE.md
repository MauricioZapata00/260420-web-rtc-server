# WebRTC Server

## Architecture

```
webrtc-server/
├── types/          # Core domain types with compile-time guarantees (type-driven!)
├── operations/     # Pure business logic (testable without mocks)
├── webrtc/         # WebRTC peer connection, data channels, and track handling
├── signaling/      # SDP offer/answer and ICE candidate exchange (Axum handlers)
└── server/         # Application composition and routing
```

## Design Principles

- **Type-driven**: model domain concepts as types first; invalid states should be unrepresentable
- **Pure operations**: `operations/` contains functions with no I/O — no browser, no network, no async runtime required
  to test them
- **Trait abstraction for I/O**: any struct that performs I/O (WebRTC peer, signaling transport) must be hidden behind a
  trait so tests can inject mocks
- **No comments unless non-obvious**: do not add comments to code unless the logic is genuinely non-obvious

## Module Responsibilities

- `types/` — domain enums, structs, and error types; no async, no I/O, no external crates beyond `serde` and `schemars`
- `operations/` — pure functions that transform types (e.g. parsing SDP, validating ICE candidates); plain `#[test]`
  only
- `webrtc/` — `PeerOps` trait + `WebRtcPeer` real implementation; handles `connectionstatechange`, `datachannel`,
  `message`, `close`, `track` events
- `signaling/` — Axum route handlers for `POST /offer` and WebSocket ICE trickle; drives `PeerOps` and returns SDP
  answers
- `server/` — wires `Router`, `SharedState`, and `AppConfig` together; entry point only, no business logic

## Unit Testing

- Tests live in `#[cfg(test)] mod tests { use super::*; ... }` at the bottom of the **same file** as the code under
  test — never in separate test files
- Cover all lines and edge cases: happy path, every error variant, boundary values
- **Pure functions** (`types/`, `operations/`): use plain `#[test]` — no async, no mocks needed
- **WebRTC / signaling integration** (`webrtc/peer.rs`, `signaling/`): use `#[tokio::test(flavor = "multi_thread")]` +
  `#[serial_test::serial]`
- **Error path coverage for WebRTC ops** (`webrtc/peer.rs`): extract I/O behind the `PeerOps` trait, annotate it with
  `#[cfg_attr(test, mockall::automock)]`, then inject a `MockPeerOps` to force each error variant independently
- Run WebRTC integration tests with `-- --test-threads=1` to avoid peer connection concurrency failures

## WebRTC Event Handling

All five events are handled inside `webrtc/peer.rs` through the `PeerOps` trait:

| Event                                     | Trait method                 |
|-------------------------------------------|------------------------------|
| `connectionstatechange`                   | `on_connection_state_change` |
| `datachannel` (remote opens channel)      | `on_data_channel`            |
| `message` (data channel message received) | `on_message`                 |
| `close` (data channel closed)             | `on_close`                   |
| `track` (audio/video RTP track received)  | `on_track`                   |

## Signaling Flow

1. Client sends `POST /offer` with an SDP offer JSON body
2. Axum handler calls `PeerOps::set_remote_description` then `PeerOps::create_answer`
3. ICE gathering completes (blocking or trickle via WebSocket at `/ws/ice`)
4. Handler returns the SDP answer as JSON
5. Subsequent ICE candidates are exchanged over the WebSocket connection

## Code Style

- Do NOT add comments to code unless the logic is truly non-obvious
- When the user requests a commit message, use imperative mood (e.g. "Add ICE trickle support" not "Added ICE trickle
  support")
- Errors are domain types defined in `types/error.rs` — never use `anyhow` in library code, only in `main.rs` if at all
- All public async functions that perform I/O return `Result<_, AppError>`