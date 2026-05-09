# WebRTC Server

A Rust SFU (Selective Forwarding Unit) signaling server. Handles SDP offer/answer exchange over HTTP, trickle ICE over
WebSocket, real-time audio/video forwarding, and public text chat over WebRTC data channels.

## Running

```bash
cargo run
```

By default the server binds to `127.0.0.1:3000` (loopback only, intended for use behind Nginx). For local development
without a reverse proxy, bind to all interfaces:

```bash
BIND_ADDR=0.0.0.0:3000 cargo run
```

Control log verbosity with `RUST_LOG`:

```bash
RUST_LOG=web_rtc_server=info cargo run
```

---

## Configuration

All configuration is read from environment variables at startup. No config file is required.

| Variable                | Default          | Description                                                  |
|-------------------------|------------------|--------------------------------------------------------------|
| `BIND_ADDR`             | `127.0.0.1:3000` | TCP address the server listens on                            |
| `SHUTDOWN_TIMEOUT_SECS` | `30`             | Seconds to wait for connections to drain on SIGTERM / SIGINT |
| `RUST_LOG`              | _(none)_         | Log filter, e.g. `web_rtc_server=info` or `debug`            |

---

## API Reference

This section describes every endpoint the server exposes and the exact JSON shapes the browser client must use.

### `POST /offer`

Initiates a new peer connection. The client sends its SDP offer; the server replies with an SDP answer and assigns a
`peer_id` that identifies this connection for the lifetime of the session.

**Request body** (`Content-Type: application/json`):

```json
{
  "sdp": "<SDP offer string from RTCPeerConnection.createOffer()>"
}
```

**Response body** (`200 OK`, `Content-Type: application/json`):

```json
{
  "peer_id": "550e8400-e29b-41d4-a716-446655440000",
  "sdp": "<SDP answer string — pass to RTCPeerConnection.setRemoteDescription()>"
}
```

**Error response** (`500 Internal Server Error`, plain text body with the error message).

**Typical client flow:**

```js
const pc = new RTCPeerConnection({ iceServers: [...] });
const offer = await pc.createOffer();
await pc.setLocalDescription(offer);

const res = await fetch('/offer', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ sdp: offer.sdp }),
});
const { peer_id, sdp } = await res.json();
await pc.setRemoteDescription({ type: 'answer', sdp });
// Store peer_id — needed for the WebSocket connection below.
```

---

### `GET /ws/ice`

WebSocket endpoint for bidirectional ICE candidate exchange and SFU renegotiation. Open this connection immediately
after receiving the SDP answer from `POST /offer`.

**Query parameter:** `peer_id` (UUID returned by `POST /offer`)

```
ws://host/ws/ice?peer_id=550e8400-e29b-41d4-a716-446655440000
```

All messages in both directions are JSON-encoded `IceWsMessage` objects with a `type` discriminator field.

#### Messages the server sends to the browser

| `type`      | `data` shape                                                                        | When                                               |
|-------------|-------------------------------------------------------------------------------------|----------------------------------------------------|
| `candidate` | `{ "candidate": string, "sdp_mid": string\|null, "sdp_mline_index": number\|null }` | Server gathered a new ICE candidate                |
| `done`      | _(no data field)_                                                                   | Server ICE gathering is complete                   |
| `offer`     | `{ "sdp": string }`                                                                 | SFU renegotiation — a new peer joined with a track |

#### Messages the browser sends to the server

| `type`      | `data` shape                                                                        | When                                        |
|-------------|-------------------------------------------------------------------------------------|---------------------------------------------|
| `candidate` | `{ "candidate": string, "sdp_mid": string\|null, "sdp_mline_index": number\|null }` | Browser gathered a new ICE candidate        |
| `answer`    | `{ "sdp": string }`                                                                 | Browser reply to a server-initiated `offer` |

**Typical client flow:**

```js
const ws = new WebSocket(`ws://host/ws/ice?peer_id=${peer_id}`);

// Send browser ICE candidates to the server.
pc.onicecandidate = ({ candidate }) => {
  if (candidate) {
    ws.send(JSON.stringify({
      type: 'candidate',
      data: {
        candidate: candidate.candidate,
        sdp_mid: candidate.sdpMid,
        sdp_mline_index: candidate.sdpMLineIndex,
      },
    }));
  }
};

ws.onmessage = async ({ data }) => {
  const msg = JSON.parse(data);

  if (msg.type === 'candidate') {
    // Server-gathered ICE candidate — add it to the peer connection.
    await pc.addIceCandidate(msg.data);
  }

  if (msg.type === 'done') {
    // Server finished gathering. No more candidates will arrive.
  }

  if (msg.type === 'offer') {
    // SFU renegotiation: a new peer started publishing a track.
    await pc.setRemoteDescription({ type: 'offer', sdp: msg.data.sdp });
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    ws.send(JSON.stringify({ type: 'answer', data: { sdp: answer.sdp } }));
  }
};
```

---

### Data channel — public chat

After the SDP exchange completes, **the browser opens a data channel** (not the server). The server fans out every
text message to all other connected peers, stamping the sender's `peer_id` as `from`.

**Opening the data channel (browser side):**

```js
const dc = pc.createDataChannel('chat');
```

**Message format** (string, JSON-encoded):

```json
{ "from": "550e8400-e29b-41d4-a716-446655440000", "text": "hello everyone" }
```

The `from` field is added by the server — never trust a client-supplied sender identity.

---

## ICE / STUN / TURN configuration

### Current setup

Each peer connection is configured with Google's public STUN servers:

```
stun:stun.l.google.com:19302
stun:stun1.l.google.com:19302
```

STUN is sufficient for ~80–85% of users on standard NATs. Peers behind a **symmetric NAT** need a TURN relay.

### Adding a TURN server

Edit `src/webrtc/peer.rs` → `WebRtcPeer::new()` and extend the `ice_servers` list:

```rust
RTCIceServer {
    urls: vec![
        "turn:your-turn-server.com:3478".to_string(),
        "turns:your-turn-server.com:5349".to_string(),
    ],
    username: std::env::var("TURN_USERNAME").unwrap_or_default(),
    credential: std::env::var("TURN_CREDENTIAL").unwrap_or_default(),
    ..Default::default()
},
```

### TURN options

| Option                                                       | Notes                                                         |
|--------------------------------------------------------------|---------------------------------------------------------------|
| [Metered openrelay](https://www.metered.ca/tools/openrelay/) | Free, bandwidth-limited — development only                    |
| Twilio Network Traversal Service                             | Managed, short-lived credentials, production SLA              |
| Self-hosted `coturn`                                         | Recommended for production; install with `apt install coturn` |

**Sizing for 30 users (SFU):** 2 vCPU / 2 GB RAM handles CPU easily. Size network bandwidth to expected peak bitrate ×
percentage of users hitting TURN (typically 15–20%).

---

## Production deployment

### Nginx reverse proxy + TLS

Browsers require a **secure origin** (`https://` or `localhost`) to access camera and microphone via `getUserMedia`.
TLS is terminated at Nginx; the Rust server stays plain HTTP on the loopback interface.

```
Internet
  │  HTTPS :443 / WSS :443
  ▼
Nginx  (TLS termination, Let's Encrypt cert)
  │  HTTP :3000 / WS :3000
  ▼
Rust server  (127.0.0.1:3000)
```

#### Prerequisites

```bash
apt install nginx certbot python3-certbot-nginx
```

#### Obtain a Let's Encrypt certificate

```bash
certbot --nginx -d your-domain.com
```

Certbot patches the Nginx config and schedules auto-renewal via a systemd timer.

#### `/etc/nginx/sites-available/web-rtc-server`

```nginx
server {
    listen 80;
    server_name your-domain.com;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    server_name your-domain.com;

    ssl_certificate     /etc/letsencrypt/live/your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.com/privkey.pem;
    include             /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam         /etc/letsencrypt/ssl-dhparams.pem;

    # HTTP endpoints (POST /offer)
    location / {
        proxy_pass         http://127.0.0.1:3000;
        proxy_set_header   Host              $host;
        proxy_set_header   X-Real-IP         $remote_addr;
        proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
    }

    # WebSocket endpoint (GET /ws/ice)
    location /ws/ice {
        proxy_pass         http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header   Upgrade    $http_upgrade;
        proxy_set_header   Connection "upgrade";
        proxy_set_header   Host       $host;

        # Keep WebSocket alive during ICE trickle and media sessions.
        proxy_read_timeout  3600s;
        proxy_send_timeout  3600s;
    }
}
```

Enable the site:

```bash
ln -s /etc/nginx/sites-available/web-rtc-server /etc/nginx/sites-enabled/
nginx -t && systemctl reload nginx
```

> **Why the WebSocket location needs special treatment:** a standard HTTP proxy closes the connection after a single
> request/response. WebSocket is an upgraded, long-lived bidirectional connection. Without `Upgrade` / `Connection:
> upgrade`, Nginx closes it immediately after the handshake. Without extended timeouts, Nginx closes idle WebSocket
> connections after 60 s (its default), cutting off ICE trickle mid-flow.

#### Firewall

Expose only what Nginx needs — never port 3000 directly:

```bash
ufw allow 80/tcp    # HTTP → redirected to HTTPS
ufw allow 443/tcp   # HTTPS + WSS
# Port 3000 must NOT be open — only Nginx reaches the Rust process.
```

---

### systemd process supervision

`/etc/systemd/system/web-rtc-server.service`:

```ini
[Unit]
Description=WebRTC Signaling Server
After=network.target

[Service]
ExecStart=/usr/local/bin/web-rtc-server
Restart=on-failure
RestartSec=3s
Environment=RUST_LOG=web_rtc_server=info
Environment=SHUTDOWN_TIMEOUT_SECS=30
KillMode=mixed
TimeoutStopSec=35

[Install]
WantedBy=multi-user.target
```

`TimeoutStopSec=35` gives the process 35 s to drain before systemd force-kills it (5 s buffer over the 30 s drain
timeout).

```bash
systemctl daemon-reload
systemctl enable --now web-rtc-server
```

### Docker

```yaml
services:
  web-rtc-server:
    image: web-rtc-server:latest
    restart: unless-stopped
    environment:
      BIND_ADDR: 127.0.0.1:3000
      RUST_LOG: web_rtc_server=info
      SHUTDOWN_TIMEOUT_SECS: "30"
    stop_grace_period: 35s
```

---

## Running tests

```bash
cargo test -- --test-threads=1
```

The `--test-threads=1` flag is required because the WebRTC integration tests share a single ICE agent and fail under
parallel execution.
