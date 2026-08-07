# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
