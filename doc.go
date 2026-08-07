// Package wstransport tunnels gRPC over a WebSocket, so a standard
// google.golang.org/grpc client and server speak to each other across a
// carrier the browser can actually use.
//
// Like the sibling grpc-transports carriers (ssh, wireguard), the shape is
// "one net.Listener, one client dialer":
//
//   - the server side exposes a [net.Listener] you hand straight to
//     grpc.Server.Serve (see [ListenWebSocket]), or an [http.Handler] +
//     net.Listener pair you mount on an existing mux (see [HandlerListener]);
//   - the client side provides a grpc.DialOption whose context dialer opens a
//     WebSocket and presents it to grpc-go as a [net.Conn] (see [DialOption]).
//
// # Why WebSocket, and why it matters for wasm
//
// grpc-go runs its HTTP/2 framing in userspace over whatever net.Conn the
// dialer returns — it never needs an OS socket or access to HTTP/2 trailers.
// A browser cannot open raw TCP and cannot read HTTP/2 trailers, which is why
// grpc-web is limited to unary and server-streaming and needs an Envoy/
// grpcwebproxy sidecar. Wrapping a browser WebSocket as a net.Conn sidesteps
// both limits: the same grpc-go transport runs unmodified under
// GOOS=js/GOARCH=wasm and gets full client-streaming and bidirectional
// streaming, with no sidecar — the server is just a grpc.Server behind this
// package's net.Listener.
//
// # Build targets
//
// [DialOption] has two implementations selected by build tag. On native
// targets it dials with github.com/coder/websocket; on js/wasm it dials the
// browser's WebSocket via syscall/js with zero third-party dependencies. The
// public signature is identical on both, mirroring wireguard's
// userspace/kernel backend split.
//
// # Transport security
//
// The WebSocket layer carries transport security: use a wss:// URL (set
// [ServerConfig.TLSConfig] on the server; the browser or [ClientConfig.TLSConfig]
// on the client). grpc-go therefore sees a plain net.Conn and should be dialed
// with insecure transport credentials:
//
//	opt, _ := wstransport.DialOption("wss://svc.example/grpc", wstransport.ClientConfig{})
//	cc, _ := grpc.NewClient("passthrough:///svc",
//	    grpc.WithTransportCredentials(insecure.NewCredentials()), opt)
package wstransport
