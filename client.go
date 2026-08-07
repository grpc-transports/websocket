package wstransport

import (
	"crypto/tls"
	"errors"
	"log"
	"net/http"
)

// errEmptyURL is returned by DialOption when given an empty WebSocket URL.
var errEmptyURL = errors.New("wstransport: empty WebSocket URL")

// ClientConfig configures the dialing side of the transport. It is shared by
// both the native and js/wasm implementations of [DialOption]; fields noted as
// native-only are ignored on js/wasm, where the browser owns TLS and headers.
type ClientConfig struct {
	// Subprotocols requested during the WebSocket handshake.
	Subprotocols []string
	// TLSConfig customizes TLS for wss:// dials (native only).
	TLSConfig *tls.Config
	// HTTPHeader is sent with the handshake request (native only).
	HTTPHeader http.Header
	// Logger, when non-nil, receives non-fatal dial diagnostics.
	Logger *log.Logger
}
