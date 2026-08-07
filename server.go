package wstransport

import (
	"context"
	"crypto/tls"
	"errors"
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
}

func (c ServerConfig) path() string {
	if c.Path == "" {
		return "/"
	}
	return c.Path
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
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: cfg.OriginPatterns,
		})
		if err != nil {
			logf(cfg.Logger, "wstransport: accept: %v", err)
			return
		}
		lis.push(websocket.NetConn(context.Background(), c, websocket.MessageBinary))
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
