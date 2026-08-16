# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Per-connection HTTP context: what the WebSocket upgrade request knows about a
  caller can now reach the gRPC server context. A browser cannot set an
  `Authorization` header on a WebSocket, so its credential is a cookie — which
  used to be discarded along with the request.
  - `ServerConfig.OnUpgrade func(*http.Request) (any, error)` — runs before the
    upgrade; refuses it by returning an error, and otherwise attaches the value
    it returns to the accepted connection. Optional: leaving it nil keeps the
    previous behaviour exactly.
  - `UpgradeError` — refuse an upgrade with a chosen HTTP status (default 403).
    Only `Code`/`Message` reach the client; `Err` goes to `ServerConfig.Logger`.
  - `ServerCredentials()` — `credentials.TransportCredentials` that publish the
    upgrade value as gRPC peer `AuthInfo`, plus `AuthInfo` and `FromContext` to
    read it back inside a service method.
  - `ConnValue` / `ValueConn` — the same value straight off the accepted
    `net.Conn`, for callers who prefer their own credentials and `AuthInfo`
    type.
  - `HandshakeError` — a native client's typed view of a refused upgrade,
    carrying the HTTP status (browsers hide it from page scripts).
- End-to-end proof on both targets: natively, a session cookie read by an HTTP
  middleware arrives in a service method's stream context; under `js/wasm`, the
  compiled browser client running on Node gets its identity back from the
  server and, without the cookie, is refused.
- Initial WebSocket transport for gRPC.
  - `ListenWebSocket` — standalone `net.Listener` of upgraded WebSocket conns for
    `grpc.Server.Serve`, with optional TLS (`wss://`).
  - `HandlerListener` — `http.Handler` + `net.Listener` pair for mounting the
    gRPC endpoint on an existing mux (serve a wasm client and its gRPC endpoint
    from one origin).
  - `DialOption` — `grpc.DialOption` that tunnels gRPC over a WebSocket. Two
    build-tagged implementations behind one signature: `coder/websocket` on
    native targets, `syscall/js` (zero third-party deps) on `js/wasm`.
- 100% statement coverage of the native code; a Node-driven end-to-end test
  (`TestWasmE2E`) runs the compiled `js/wasm` client through a real
  bidirectional stream against `ListenWebSocket`.
- CI across six architectures (amd64, arm64 native; riscv64, loong64, ppc64le,
  s390x emulated) plus a dedicated `js/wasm` end-to-end job.
