package wstransport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

// errListenerClosed is returned by Accept once the listener has been closed.
var errListenerClosed = errors.New("wstransport: listener closed")

// ServerConfig configures the WebSocket-accepting side of the transport.
type ServerConfig struct {
	// Path is the HTTP path WebSocket upgrades are accepted on. Empty means "/".
	Path string
	// TLSConfig, when non-nil, makes [ListenWebSocket] serve over TLS (wss://).
	TLSConfig *tls.Config
	// OriginPatterns is passed to the WebSocket handshake to authorize browser
	// Origins. Empty means same-origin only; use []string{"*"} to allow all.
	OriginPatterns []string
	// Logger, when non-nil, receives non-fatal handshake diagnostics.
	Logger *log.Logger

	// OnUpgrade, when non-nil, is called with the HTTP request carrying the
	// WebSocket handshake, before the upgrade is performed. It is the seam
	// between the HTTP layer and the gRPC layer: everything the request knows
	// about the caller — cookies, headers, the TLS client certificate, the
	// identity an HTTP middleware just established in r.Context() — is
	// otherwise lost once the connection becomes a stream of gRPC frames.
	//
	// The value it returns is attached to the accepted net.Conn and can be
	// recovered from that conn with [ConnValue]; combined with
	// [ServerCredentials] it reaches a service method as the AuthInfo of
	// peer.FromContext(ctx). The value is opaque to this package: use whatever
	// type the application wants (a session, a user ID, the *http.Request
	// itself).
	//
	// Returning a non-nil error refuses the upgrade: no WebSocket is created
	// and the request is answered with an HTTP status — 403 Forbidden, or the
	// status carried by an [UpgradeError]. The error is reported to Logger.
	//
	// Leaving OnUpgrade nil keeps the previous behaviour exactly: the accepted
	// conn is the bare WebSocket conn and carries no value.
	OnUpgrade func(*http.Request) (any, error)
}

func (c ServerConfig) path() string {
	if c.Path == "" {
		return "/"
	}
	return c.Path
}

// UpgradeError refuses a WebSocket upgrade with a specific HTTP status. Return
// one from [ServerConfig.OnUpgrade] when the default 403 Forbidden is not the
// right answer — 401 for a missing session, 404 to hide the endpoint's
// existence, 503 while draining:
//
//	OnUpgrade: func(r *http.Request) (any, error) {
//	    c, err := r.Cookie("session")
//	    if err != nil {
//	        return nil, &wstransport.UpgradeError{Code: http.StatusUnauthorized}
//	    }
//	    return lookupSession(c.Value)
//	}
//
// Only Code and Message reach the client; Err is for the server's Logger.
type UpgradeError struct {
	// Code is the HTTP status sent to the client. Zero means
	// http.StatusForbidden.
	Code int
	// Message is the HTTP response body. Empty means http.StatusText(Code).
	Message string
	// Err, when non-nil, is the underlying cause. It is logged, never sent.
	Err error
}

// status reports the HTTP status and body this refusal sends to the client.
func (e *UpgradeError) status() (int, string) {
	code := e.Code
	if code == 0 {
		code = http.StatusForbidden
	}
	msg := e.Message
	if msg == "" {
		msg = http.StatusText(code)
	}
	return code, msg
}

func (e *UpgradeError) Error() string {
	code, msg := e.status()
	if e.Err != nil {
		return fmt.Sprintf("wstransport: upgrade refused (%d %s): %v", code, msg, e.Err)
	}
	return fmt.Sprintf("wstransport: upgrade refused (%d %s)", code, msg)
}

// Unwrap exposes the underlying cause to errors.Is/errors.As.
func (e *UpgradeError) Unwrap() error { return e.Err }

// refusal maps an OnUpgrade error to the HTTP status and body sent back. Any
// error that is not (or does not wrap) an [UpgradeError] is a plain 403, and
// its text is never disclosed to the client.
func refusal(err error) (int, string) {
	var ue *UpgradeError
	if errors.As(err, &ue) {
		return ue.status()
	}
	return http.StatusForbidden, http.StatusText(http.StatusForbidden)
}

// ValueConn is implemented by the net.Conns delivered by this package's
// listeners when [ServerConfig.OnUpgrade] is set. A
// credentials.TransportCredentials can type-assert an accepted conn to it in
// its ServerHandshake and turn the value into gRPC peer AuthInfo — which is
// what [ServerCredentials] does.
type ValueConn interface {
	net.Conn
	// UpgradeValue returns the value OnUpgrade attached to this connection.
	UpgradeValue() any
}

// valueConn carries an OnUpgrade value alongside an upgraded WebSocket conn.
type valueConn struct {
	net.Conn
	value any
}

func (c *valueConn) UpgradeValue() any { return c.value }

// ConnValue returns the value [ServerConfig.OnUpgrade] attached to c, if any.
// It reports false for connections accepted without an OnUpgrade hook, and for
// any other net.Conn.
func ConnValue(c net.Conn) (any, bool) {
	if vc, ok := c.(ValueConn); ok {
		return vc.UpgradeValue(), true
	}
	return nil, false
}

// wsAddr is the net.Addr reported by the transport's conns and listener.
type wsAddr string

func (wsAddr) Network() string  { return "websocket" }
func (a wsAddr) String() string { return string(a) }

// chanListener is a net.Listener fed by the upgrade handler: each accepted
// WebSocket becomes one net.Conn delivered over ch.
type chanListener struct {
	ch   chan net.Conn
	done chan struct{}
	once sync.Once
	addr net.Addr
}

func newChanListener(addr net.Addr) *chanListener {
	return &chanListener{ch: make(chan net.Conn), done: make(chan struct{}), addr: addr}
}

// push hands an upgraded conn to a waiting Accept, or closes it if the
// listener has already been closed.
func (l *chanListener) push(c net.Conn) {
	select {
	case l.ch <- c:
	case <-l.done:
		_ = c.Close()
	}
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, errListenerClosed
	}
}

func (l *chanListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *chanListener) Addr() net.Addr { return l.addr }

// HandlerListener returns an http.Handler that upgrades WebSocket requests on
// cfg.Path into net.Conns, together with the net.Listener those conns are
// delivered on. Mount the handler on an existing http.ServeMux (e.g. to serve
// a wasm client and its gRPC endpoint from one origin) and pass the listener
// to grpc.Server.Serve.
func HandlerListener(cfg ServerConfig) (http.Handler, net.Listener) {
	lis := newChanListener(wsAddr(cfg.path()))
	mux := http.NewServeMux()
	mux.HandleFunc(cfg.path(), func(w http.ResponseWriter, r *http.Request) {
		// Run the hook before the upgrade: once websocket.Accept has hijacked
		// the connection there is no HTTP response left to refuse with.
		var value any
		if cfg.OnUpgrade != nil {
			v, err := cfg.OnUpgrade(r)
			if err != nil {
				code, msg := refusal(err)
				logf(cfg.Logger, "wstransport: upgrade refused (%d): %v", code, err)
				http.Error(w, msg, code)
				return
			}
			value = v
		}
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: cfg.OriginPatterns,
		})
		if err != nil {
			logf(cfg.Logger, "wstransport: accept: %v", err)
			return
		}
		nc := websocket.NetConn(context.Background(), c, websocket.MessageBinary)
		if cfg.OnUpgrade == nil {
			lis.push(nc)
			return
		}
		lis.push(&valueConn{Conn: nc, value: value})
	})
	return mux, lis
}

// serverListener adapts the transport for grpc.Server.Serve: Accept yields
// upgraded WebSocket conns (via the inner chanListener), Addr reports the real
// TCP bind address (so callers can discover an ephemeral ":0" port), and Close
// stops the owned http.Server.
type serverListener struct {
	inner *chanListener
	ln    net.Listener
	srv   *http.Server
}

func (s *serverListener) Accept() (net.Conn, error) { return s.inner.Accept() }
func (s *serverListener) Addr() net.Addr            { return s.ln.Addr() }

func (s *serverListener) Close() error {
	err := s.inner.Close()
	_ = s.srv.Close()
	return err
}

// ListenWebSocket binds addr, serves the WebSocket upgrade handler on it, and
// returns a net.Listener that yields one net.Conn per accepted WebSocket —
// ready for grpc.Server.Serve. When cfg.TLSConfig is set the server speaks
// wss:// (TLS). Closing the returned listener stops the server.
func ListenWebSocket(addr string, cfg ServerConfig) (net.Listener, error) {
	h, lis := HandlerListener(cfg)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		_ = lis.Close()
		return nil, err
	}
	srv := &http.Server{Handler: h}
	if cfg.TLSConfig != nil {
		srv.TLSConfig = cfg.TLSConfig
		go func() { _ = srv.ServeTLS(ln, "", "") }()
	} else {
		go func() { _ = srv.Serve(ln) }()
	}
	return &serverListener{inner: lis.(*chanListener), ln: ln, srv: srv}, nil
}

func logf(l *log.Logger, format string, args ...any) {
	if l != nil {
		l.Printf(format, args...)
	}
}
