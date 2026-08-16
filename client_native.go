//go:build !js

package wstransport

import (
	"context"
	"net"
	"net/http"

	"github.com/coder/websocket"
	"google.golang.org/grpc"
)

// dialOpts translates a ClientConfig into coder/websocket dial options.
func dialOpts(cfg ClientConfig) *websocket.DialOptions {
	o := &websocket.DialOptions{
		Subprotocols: cfg.Subprotocols,
		HTTPHeader:   cfg.HTTPHeader,
	}
	if cfg.TLSConfig != nil {
		o.HTTPClient = &http.Client{
			Transport: &http.Transport{TLSClientConfig: cfg.TLSConfig},
		}
	}
	return o
}

// dialConn opens a WebSocket to wsURL and adapts it to a net.Conn.
func dialConn(ctx context.Context, wsURL string, cfg ClientConfig) (net.Conn, error) {
	c, resp, err := websocket.Dial(ctx, wsURL, dialOpts(cfg))
	if err != nil {
		// A response means the server answered the handshake and refused it —
		// e.g. the 403 a ServerConfig.OnUpgrade hook produces. Surface the
		// status so callers can act on it instead of parsing a message.
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			err = &HandshakeError{StatusCode: resp.StatusCode, Status: resp.Status, Err: err}
		}
		logf(cfg.Logger, "wstransport: dial %s: %v", wsURL, err)
		return nil, err
	}
	return websocket.NetConn(context.Background(), c, websocket.MessageBinary), nil
}

// DialOption returns a grpc.DialOption that tunnels every gRPC channel over a
// WebSocket to wsURL (ws:// or wss://). Combine it with insecure transport
// credentials — transport security is provided by the WebSocket layer (wss).
func DialOption(wsURL string, cfg ClientConfig) (grpc.DialOption, error) {
	if wsURL == "" {
		return nil, errEmptyURL
	}
	return grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return dialConn(ctx, wsURL, cfg)
	}), nil
}
