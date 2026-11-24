# signaling-rust

A Rust reimplementation of [Radix's signaling server](https://github.com/radixdlt/signaling-server)
(originally Node.js/TypeScript), built to be a drop-in replacement: same
wire protocol, same Redis data model, same HTTP/metrics contract — lower
memory footprint, lower operating cost, simpler deployment.

## What this is

The signaling server is the piece of infrastructure that lets the official
**Radix Wallet** and **Radix Connector Extension** find each other and
exchange WebRTC session-negotiation messages (SDP offer/answer, ICE
candidates) over a WebSocket connection, so they can then establish a
direct, end-to-end-encrypted WebRTC data channel between themselves. This
server never sees the content of that channel — only the WebRTC negotiation
messages, and it treats their payload as opaque ciphertext.

`signaling-rust` is a protocol-for-protocol reimplementation of that server.
It is not an approximation or a generic WebSocket relay — it reproduces the
original's exact behavior, including its message formats, Redis key layout,
validation order, rate limiting, metrics, and a couple of known original
quirks (kept intentionally, see "Known deviations from the original" below).

## What it does

- Accepts WebSocket connections from Wallet and Connector Extension clients,
  identifying each link by a `connectionId` (a 64-character SHA-256 hex
  string carried in the URL path) and a role (`wallet` or `extension`,
  carried as query parameters).
- Validates and routes WebRTC negotiation messages (`offer`, `answer`,
  `iceCandidate`, `iceCandidates`) between the two ends of a link.
- Tracks which client instances are currently connected for each
  `connectionId`/role pair in Redis (`SADD`/`SREM`/`SISMEMBER`/`SMEMBERS`,
  with a 12-hour TTL renewed on every registration), so multiple server
  instances can coordinate without sticky sessions.
- Delivers messages across instances via Redis Pub/Sub: each physical
  connection subscribes to a channel named after its own connection ID, and
  the server `PUBLISH`es to that channel to deliver a message to it,
  wherever that connection is actually being served.
- Notifies both ends of a link when the other side connects, disconnects,
  or is already present when a new connection opens.
- Enforces a per-connection rate limit (sliding window, default 100
  messages/second) and closes connections that exceed it.
- Actively detects dead connections via WebSocket ping/pong and cleans up
  their Redis registration immediately, instead of only relying on the
  12-hour TTL (see "Known deviations" below — this is the one intentional
  behavioral improvement over the original).
- Exposes `/health` and `/metrics` (Prometheus text format) on a separate
  HTTP port, with connection/message/Redis-timing counters and gauges, plus
  standard process-resource metrics (`process_resident_memory_bytes`,
  `process_cpu_seconds_total`, `process_open_fds`, etc.).

## What it does NOT do

- **It does not inspect, log, decrypt, or modify application payloads.**
  Every message's `encryptedPayload` field is treated as an opaque string,
  exactly like the original.
- **It does not establish the WebRTC connection itself.** It only
  exchanges the negotiation messages; the actual peer-to-peer data channel
  is established directly between Wallet and Extension, outside this
  server's involvement.
- **It does not replace Redis.** Redis remains the source of truth for
  cross-instance coordination — this is a stateless-per-instance service by
  design, so it can be scaled horizontally without sticky sessions.
- **It is not a generic WebSocket server.** It implements one specific
  protocol (Radix Connect signaling) with one specific message schema — it
  is not meant to be repurposed for other kinds of real-time messaging.
- **It does not implement STUN/TURN.** Those are separate, standard pieces
  of WebRTC infrastructure that the clients use independently; this server
  only carries the SDP/ICE negotiation messages.
- **It does not do authentication or access control beyond the protocol
  itself.** Any client that knows (or can brute-force, which is
  infeasible given the 64-char SHA-256 format) a `connectionId` can attempt
  to join that link as either role — this matches the original's security
  model exactly; it relies on the `connectionId` being an unguessable
  secret shared out-of-band (via QR code / deep link) between the two
  legitimate clients.
- **It is not involved in any alternative transport.** See the note on
  `iroh-relay` below — that is a separate, independent service, not a
  mode or fallback implemented inside this codebase.

## Known deviations from the original

Kept intentionally for behavioral/metric parity, documented rather than
silently changed:

- `signaling_server_subscribe_messages` is declared but never incremented
  (always reports `0`), matching the original's dead metric — not
  reimplemented with new logic that the original never had.
- Active ping/pong-based connection cleanup is a deliberate **correction**
  over the original (which only relies on a 12-hour Redis TTL for orphaned
  registrations when a connection dies without a clean close). This does
  not change anything visible in the WebSocket protocol to Wallet/Extension
  — it only makes stale-registration cleanup happen sooner.
- The original's double-increment of `signaling_server_outgoing_messages`
  (an internal-metrics-only bug, invisible to clients) is **not**
  replicated — this implementation counts each outgoing message once.

## Architecture (summary)

- Async runtime: Tokio. Each WebSocket connection runs as its own task.
- WebSocket: `tokio-tungstenite`.
- HTTP (`/health`, `/metrics`): `axum`, on a separate port from the WS
  listener, exactly like the original.
- Redis: `redis` (redis-rs), using one shared multiplexed connection for
  commands and one dedicated Pub/Sub connection/task for message delivery —
  mirroring the original's single-shared-subscriber design rather than
  opening one Redis connection per client.
- Metrics: `prometheus` (TiKV/rust-prometheus), with `ProcessCollector` for
  standard process metrics.
- Message validation is hand-written (not derived automatically) so that an
  invalid/empty message reports every failing field at once, matching the
  original's `zod`-based validation instead of stopping at the first error.

## Configuration

| Variable | Required | Default | Notes |
|---|---|---|---|
| `REDIS_HOST_PUB` | yes | — | |
| `REDIS_HOST_SUBS` | yes | — | may differ from `REDIS_HOST_PUB` |
| `REDIS_PORT` | yes | — | |
| `REDIS_PASSWORD` | no | unauthenticated | |
| `LOG_LEVEL` | yes | — | numeric, mapped to `tracing` levels |
| `PORT` | yes | — | WebSocket listener port |
| `HTTP_PORT` | yes | — | `/health` and `/metrics` port |
| `RATE_LIMIT_MESSAGES` | no | `100` | messages per window before closing the connection |
| `RATE_LIMIT_MS` | no | `1000` | window size in milliseconds |

## Testing

- Protocol/validation tests ported from the original's test suite
  (including the exact "4 validation errors for an empty message" case).
- A cross-instance integration test exercising a full offer/answer/ICE
  exchange between two server instances sharing Redis, matching the
  original's `sdp-ice-exchange.test.ts` scenario.

## Load testing

A companion Rust load generator (`signaling-loadtest`, in a separate
crate/binary) drives realistic traffic — real `connectionId`/role pairs,
real offer/answer/ICE exchanges with correctly resolved `targetClientId`
values — rather than opening empty WebSocket connections.

## Testing end-to-end with the real Wallet and Connector Extension

To validate this server against the actual official clients (not just the
test suite), you need to point a real Radix Wallet and a real Connector
Extension at your own signaling-rust instance instead of Radix's production
one. Both clients support this, but neither exposes it trivially — here's
what's actually required for each, discovered by tracing both clients'
source.

### 1. Connector Extension (browser)

The production build (what you get from the Web Store / `npm run build`)
hides the signaling-server option entirely. In
[`connector-extension`](https://github.com/radixdlt/connector-extension)'s
`src/config.ts`, the Options page's "Signaling Server" section is gated by:

```ts
export const isPublicRelease = githubRefName === 'main' || githubRefName.includes('release/')
export const isDevMode = import.meta.env.VITE_DEV_TOOLS === 'true'
// in options.tsx: {isPublicRelease && !isDevMode ? null : ( ...Signaling Server UI... )}
```

`isPublicRelease` is only `true` in CI (it checks `VITE_GITHUB_REF_NAME`),
so a plain local build already satisfies `!isPublicRelease` and shows the
section — you don't strictly need `VITE_DEV_TOOLS=true`, but the repo's own
`build:dev` script sets it anyway:

```bash
git clone https://github.com/radixdlt/connector-extension
cd connector-extension
npm ci                # do NOT skip this — without it, a stray global `tsc`
                       # (if one is on your PATH) will build against the
                       # wrong TypeScript version and fail with TS5107/TS5101
npm run build:dev
```

Load the resulting `dist/` (or equivalent build output directory) as an
unpacked extension in Chrome, disable/remove any other copy of the
Connector Extension you have installed (only one can be active for a given
dApp/browser pairing), open its **Options** page, and under **Signaling
Server** select **Custom** and enter your signaling-rust instance's WebSocket
URL (`wss://...`). The STUN/TURN servers can be left as whatever the
selected fallback profile (`development`/`production`) provides — a custom
signaling server does not require custom STUN/TURN; `getConnectionConfig()`
only overrides `signalingServerBaseUrl` when `radixConnectConfiguration ===
'custom'`.

### 2. Radix Wallet (Android confirmed; iOS has the equivalent screen)

Both the official iOS and Android Wallet apps have a real,
shipped-to-production **Signaling Servers** settings screen (`Settings →
Preferences → Signaling Servers` on Android; iOS wallet-settings has the
same feature, per the iOS App Store changelog: *"Added ability to configure
Signaling Server and Relay Server"*). It is not developer-only. Two
non-obvious requirements, confirmed against the Wallet's own source
(`babylon-wallet-android`/`babylon-wallet-ios`, both public):

- **Adding a profile does not activate it.** The Wallet keeps a *list* of
  signaling-server profiles (one marked "current"). Tapping **Save** when
  adding/editing a profile only adds it to the list. You must go back into
  that saved profile (edit mode) and tap the separate **"Change as
  current"** button — only then does
  `p2pTransportProfilesClient.changeProfile(...)` actually switch which
  server new links use.

- **The signaling server URL must end with a trailing `/`.** Both clients
  build the final WebSocket URL by straight string concatenation of the
  configured base URL and the connection ID, with no separator inserted —
  e.g. on Android, `WebSocketClient.kt`:

  ```kotlin
  url("${signalingServerUrl}${connectionId.id}?source=wallet&target=extension")
  ```

  `wss://your-host.example.com` (no trailing slash) silently produces an
  unroutable hostname (`your-host.example.comd919b57e...`), which fails
  with `UnresolvedAddressException` client-side — the Wallet never even
  attempts a TCP connection, and nothing shows up at the signaling server or
  any tunnel/proxy in front of it. Use `wss://your-host.example.com/`.

#### Debugging a Wallet that won't connect (Android)

The official Play Store build ships with internal logging compiled out
entirely — `app/build.gradle`'s `release` build type sets both
`DEBUG_MODE` and `FILE_LOGGER_ENABLED` to `false`, so `RadixApplication`
never plants a Timber tree and every `Timber.tag(...)` call in the app
(including the `"LinkingCE"`-tagged linking-flow logs) is a silent no-op —
`adb logcat` will never show them, with or without a tag filter, regardless
of what you grep for.

If a link attempt isn't reaching your server, verify at the network layer
instead of the log layer:

```bash
# resolve your test server's hostname (e.g. your tunnel) to an IP first
getent hosts your-host.example.com

# then watch for an outbound connection to that IP while you scan the QR
# and name the connector in the Wallet
while true; do adb shell ss -tn | grep '<that IP>'; sleep 1; done
```

If that IP never appears, the Wallet isn't even attempting the connection
(check the trailing-slash and "Change as current" items above first). If it
does appear, the problem is downstream (TLS/WS handshake, a reverse proxy,
or the server itself) — check your proxy/tunnel logs and the server's own
`/metrics` (`signaling_server_connected_clients`) next.

### 3. Exposing a local instance for testing on a real device

For testing from an actual phone (not an emulator reachable over
localhost), a quick tunnel works without any DNS/TLS setup of your own:

```bash
cloudflared tunnel --url http://localhost:<PORT>
```

This prints a random `https://<random-words>.trycloudflare.com` hostname;
use `wss://<that-hostname>/` (remember the trailing slash — see above) as
the signaling server URL in both clients. The hostname changes every time
`cloudflared` restarts, so both clients need re-pointing after a restart.

### 4. Redis authentication

If you're also testing with `REDIS_PASSWORD` set (recommended, to exercise
the same path production will use), nothing client-facing changes — the
Wallet and Extension have no knowledge of Redis. Just confirm the server
itself is actually authenticating rather than silently falling back:
signaling-rust builds the connection URL as `redis://:{password}@{host}:{port}`
when `REDIS_PASSWORD` is set, `redis://{host}:{port}` otherwise (see
`main.rs`) — start Redis with `requirepass` set and confirm the server logs
a successful connection (or fails loudly if the password is wrong) rather
than connecting unauthenticated.

## Future / related: `iroh-relay`

The official Radix Wallet and Connector Extension speak **only** this
WebRTC signaling protocol today. A third party has proposed an additional,
optional transport for Radix Connect based on QUIC over
[iroh](https://iroh.computer) relays, intended to run **alongside**
WebRTC, not replace it — once (and if) that work is merged into the
official clients, a client could attempt the iroh path first and
automatically fall back to this signaling-rust/WebRTC service when iroh
isn't available.

That would be served by a separate, independent `iroh-relay` process (the
official relay server from the iroh project) — a different service, on
different ports, with no shared code or state with this repository. It is
not implemented here and this server requires no changes to support it:
signaling-rust keeps functioning as the WebRTC fallback path exactly as it
does today, whether or not an iroh relay is ever deployed alongside it.
