<p align="center"><img src="https://raw.githubusercontent.com/grpc-transports/brand/main/social/grpc-transports.png" alt="grpc-transports/websocket" width="720"></p>

# websocket

[![Go Reference](https://pkg.go.dev/badge/github.com/grpc-transports/websocket.svg)](https://pkg.go.dev/github.com/grpc-transports/websocket)
[![CI](https://github.com/grpc-transports/websocket/actions/workflows/ci.yml/badge.svg)](https://github.com/grpc-transports/websocket/actions/workflows/ci.yml)
[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD--3--Clause-blue.svg)](LICENSE)

WebSocket transport layer for gRPC — the carrier that works **inside the browser**. The server exposes a `net.Listener` for a standard `grpc.Server`; the client provides a `grpc.DialOption` that tunnels every gRPC channel over a WebSocket. The same client compiles and runs under `GOOS=js/GOARCH=wasm`, so a Go program in the browser speaks full gRPC — including **client-streaming and bidirectional streaming** — with **no sidecar proxy**.

Two client implementations ship side-by-side behind one signature; the build tag picks which.

| | native (`!js`) | browser (`js && wasm`) |
|---|---|---|
| **Dialer** | `github.com/coder/websocket` → `net.Conn` | `syscall/js` `WebSocket` → `net.Conn` |
| **Dependencies** | one Go module | **zero** third-party (stdlib only) |
| **Transport security** | `wss://` via `ClientConfig.TLSConfig` | `wss://`, owned by the browser |
| **When** | services, CLIs, tests | wasm front-ends, wasm workers |

## Module

```
github.com/grpc-transports/websocket
```

## Why WebSocket — the wasm story

grpc-go runs its HTTP/2 framing **in userspace** over whatever `net.Conn` the dialer hands it; it never needs an OS socket or access to HTTP/2 trailers. A browser can do neither — no raw TCP, no readable HTTP/2 trailers — which is exactly why `grpc-web` is limited to unary and server-streaming and needs an Envoy / `grpcwebproxy` sidecar in front of your server.

Wrapping a browser `WebSocket` as a `net.Conn` sidesteps both limits at once:

- the **unmodified** grpc-go client transport runs under `js/wasm`;
- you get **client-streaming and bidirectional streaming**, not just unary;
- the **server is a plain `grpc.Server`** behind this package's `net.Listener` — no sidecar, no second protocol.

For inter-VM gRPC across hosts, prefer [`wireguard`](https://github.com/grpc-transports/wireguard); for human-driven CLI clients, prefer [`ssh`](https://github.com/grpc-transports/ssh). Reach for `websocket` whenever one end lives in a browser (or any host that only offers a WebSocket).

## When to use

- A Go **wasm** front-end (or web worker) that must call gRPC services directly, with streaming
- Sharing the **same gRPC API** between a native client and a browser client instead of maintaining a parallel REST/JSON portal
- Environments where the only reachable channel is an HTTP(S)/WebSocket endpoint (corporate proxies, edge, PaaS)

## API

### Server

```go
type ServerConfig struct {
    Path           string      // upgrade path (default "/")
    TLSConfig      *tls.Config // non-nil ⇒ serve wss:// (TLS)
    OriginPatterns []string    // allowed browser Origins ("*" to allow all; empty = same-origin)
    Logger         *log.Logger
}

// ListenWebSocket binds addr, serves the upgrade handler, and returns a
// net.Listener of upgraded WebSocket conns for grpc.Server.Serve. Closing it
// stops the server. Addr() reports the real TCP address (resolves ":0").
func ListenWebSocket(addr string, cfg ServerConfig) (net.Listener, error)

// HandlerListener returns an http.Handler + net.Listener pair, so the gRPC
// endpoint can be mounted on an existing mux — e.g. to serve a wasm client and
// its gRPC endpoint from one origin.
func HandlerListener(cfg ServerConfig) (http.Handler, net.Listener)
```

### Client

```go
type ClientConfig struct {
    Subprotocols []string
    TLSConfig    *tls.Config // native only (browser owns TLS)
    HTTPHeader   http.Header // native only
    Logger       *log.Logger
}

// DialOption returns a grpc.DialOption that tunnels gRPC over a WebSocket to
// wsURL (ws:// or wss://). Identical signature on native and js/wasm. Pair it
// with insecure transport credentials — security is provided by wss.
func DialOption(wsURL string, cfg ClientConfig) (grpc.DialOption, error)
```

## Usage

**Server:**

```go
lis, err := wstransport.ListenWebSocket("0.0.0.0:8080", wstransport.ServerConfig{
    OriginPatterns: []string{"app.example"},
    TLSConfig:      myTLS, // wss://
})
if err != nil {
    log.Fatal(err)
}
grpcServer.Serve(lis)
```

**Client (native *and* wasm — the same code):**

```go
opt, err := wstransport.DialOption("wss://app.example:8080", wstransport.ClientConfig{})
if err != nil {
    log.Fatal(err)
}
cc, err := grpc.NewClient("passthrough:///svc",
    grpc.WithTransportCredentials(insecure.NewCredentials()), opt)
```

Build the browser client with:

```
GOOS=js GOARCH=wasm go build -o app.wasm ./cmd/app
```

## Testing

- **100%** statement coverage of the native code (`task test`).
- A Node-driven end-to-end test compiles the real `js/wasm` client and runs a
  full bidirectional stream against `ListenWebSocket` (`task wasm-e2e`) — it
  proves the browser path *runs*, not merely that it compiles.
- CI exercises six architectures (amd64, arm64 native; riscv64, loong64,
  ppc64le, s390x under QEMU) plus the `js/wasm` end-to-end job.

## License

BSD-3-Clause. See [LICENSE](LICENSE).
