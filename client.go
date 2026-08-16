package wstransport

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
)

// errEmptyURL is returned by DialOption when given an empty WebSocket URL.
var errEmptyURL = errors.New("wstransport: empty WebSocket URL")

// HandshakeError reports a WebSocket handshake the server answered with an
// HTTP status other than 101 Switching Protocols — what a client sees when a
// [ServerConfig.OnUpgrade] hook refuses the upgrade. Recover the status with
// errors.As to tell "your session expired" (401/403) from "the server is
// down":
//
//	var he *wstransport.HandshakeError
//	if errors.As(err, &he) && he.StatusCode == http.StatusUnauthorized {
//	    reauthenticate()
//	}
//
// Native dials only: a browser deliberately hides a failed handshake's status
// from page scripts, so the js/wasm dialer reports a plain error.
type HandshakeError struct {
	// StatusCode is the HTTP status the server refused the upgrade with.
	StatusCode int
	// Status is that status as text, e.g. "403 Forbidden".
	Status string
	// Err is the underlying dial error.
	Err error
}

func (e *HandshakeError) Error() string {
	return fmt.Sprintf("wstransport: handshake refused with HTTP %s: %v", e.Status, e.Err)
}

// Unwrap exposes the underlying dial error to errors.Is/errors.As.
func (e *HandshakeError) Unwrap() error { return e.Err }

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
