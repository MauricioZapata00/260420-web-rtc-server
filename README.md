# WebRTC Server

A WebRTC signaling server written in Rust. Handles SDP offer/answer exchange over HTTP and ICE candidate trickle over WebSocket.

## Running

```bash
cargo run
```

By default the server listens on `0.0.0.0:3000`. Override with the `BIND_ADDR` environment variable:

```bash
BIND_ADDR=0.0.0.0:8080 cargo run
```

Control log verbosity with `RUST_LOG`:

```bash
RUST_LOG=web_rtc_server=info cargo run
```

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/offer` | Submit an SDP offer, receive an SDP answer |
| `GET` | `/ws/ice` | WebSocket — trickle ICE candidates |

## ICE / STUN / TURN configuration

### Current setup

The server configures each peer connection with Google's public STUN servers:

```
stun:stun.l.google.com:19302
stun:stun1.l.google.com:19302
```

STUN is enough for peers on standard NATs (~80–85% of users). Peers behind a **symmetric NAT** require a TURN relay to connect.

### Adding a TURN server

Edit `src/webrtc/peer.rs` → `WebRtcPeer::new()` and extend the `ice_servers` list:

```rust
RTCIceServer {
    urls: vec![
        "turn:your-turn-server.com:3478".to_string(),
        "turns:your-turn-server.com:5349".to_string(), // TLS
    ],
    username: "your_username".to_string(),
    credential: "your_password".to_string(),
    ..Default::default()
},
```

In production, load credentials from environment variables instead of hardcoding:

```rust
let turn_user = std::env::var("TURN_USERNAME").unwrap_or_default();
let turn_pass = std::env::var("TURN_CREDENTIAL").unwrap_or_default();
```

### TURN server options

#### Option 1 — Metered public relay (development / testing)

Free, no account needed, bandwidth-limited:

```
stun:openrelay.metered.ca:80
turn:openrelay.metered.ca:80
turn:openrelay.metered.ca:443
turn:openrelay.metered.ca:443?transport=tcp
```

Username: `openrelayproject` · Credential: `openrelayproject`

Not suitable for production — no SLA and shared bandwidth.

#### Option 2 — Twilio Network Traversal Service (production)

Twilio provides managed STUN/TURN with an SLA and short-lived credentials generated via API (credentials expire, so they are never hardcoded). Suitable for production workloads with a predictable cost model.

#### Option 3 — Self-hosted `coturn` (recommended for production)

`coturn` is the standard open-source STUN/TURN server. Install on any Linux VM:

```bash
apt install coturn
```

Minimal `/etc/turnserver.conf`:

```
realm=yourdomain.com
fingerprint
lt-cred-mech
user=youruser:yourpassword
listening-port=3478
tls-listening-port=5349
cert=/path/to/cert.pem
pkey=/path/to/key.pem
```

Start:

```bash
systemctl enable --now coturn
```

Open firewall ports `3478/udp`, `3478/tcp`, `5349/tcp`, and the relay port range (default `49152–65535/udp`).

**Sizing for 30 users (SFU topology):** a 2 vCPU / 2 GB RAM VM handles the CPU easily. Size the VM's network bandwidth to the expected peak media bitrate × number of users who hit the TURN relay (typically 15–20% of sessions).

## Running tests

```bash
cargo test -- --test-threads=1
```

The `--test-threads=1` flag is required for the WebRTC integration tests, which share a single ICE agent and fail under parallel execution.
