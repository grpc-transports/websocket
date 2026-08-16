//go:build !js

package wstransport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// ---- a realistic cookie-authenticated stack ------------------------------
//
// An HTTP middleware validates a session cookie — the only credential a
// browser can attach to a WebSocket — and puts the resolved session on the
// request context. ServerConfig.OnUpgrade lifts it off the request and onto
// the accepted conn; ServerCredentials turns it into gRPC peer AuthInfo; the
// service method reads it back out of its stream context.

type sessionCtxKey struct{}

type testSession struct{ user string }

// sessionCookies maps cookie values to users, standing in for a session store.
var sessionCookies = map[string]string{"tok-alice": "alice", "tok-bob": "bob"}

// sessionMiddleware is the http middleware in front of the upgrade handler.
func sessionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c, err := r.Cookie("session"); err == nil {
			if user, ok := sessionCookies[c.Value]; ok {
				ctx := context.WithValue(r.Context(), sessionCtxKey{}, &testSession{user: user})
				r = r.WithContext(ctx)
			}
		}
		next.ServeHTTP(w, r)
	})
}

// requireSession is the ServerConfig.OnUpgrade hook: it accepts the upgrade
// only for a request the middleware authenticated, and attaches the session.
func requireSession(r *http.Request) (any, error) {
	s, ok := r.Context().Value(sessionCtxKey{}).(*testSession)
	if !ok {
		return nil, &UpgradeError{
			Code: http.StatusUnauthorized,
			Err:  errors.New("no valid session cookie"),
		}
	}
	return s, nil
}

const whoamiMethod = "/wstransport.Whoami/Stream"

// seenAuthType records the AuthType() grpc-go stored in the peer, proving the
// value travels the idiomatic credentials path and not a side channel.
var seenAuthType atomic.Value

var whoamiDesc = grpc.ServiceDesc{
	ServiceName: "wstransport.Whoami",
	HandlerType: (*any)(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		Handler:       whoamiHandler,
		ServerStreams: true,
		ClientStreams: true,
	}},
}

// whoamiHandler prefixes every echoed message with the user the WebSocket
// upgrade was authenticated as.
func whoamiHandler(_ any, stream grpc.ServerStream) error {
	if p, ok := peer.FromContext(stream.Context()); ok && p.AuthInfo != nil {
		seenAuthType.Store(p.AuthInfo.AuthType())
	}
	v, ok := FromContext(stream.Context())
	if !ok {
		return status.Error(codes.Unauthenticated, "no upgrade value on the connection")
	}
	s := v.(*testSession)
	for {
		var msg []byte
		if err := stream.RecvMsg(&msg); err != nil {
			return err
		}
		reply := []byte(s.user + ":" + string(msg))
		if err := stream.SendMsg(&reply); err != nil {
			return err
		}
	}
}

// serveWhoami starts a gRPC server whose peer AuthInfo carries the upgrade
// value, on a WebSocket endpoint fronted by the cookie middleware. It returns
// the ws:// URL to dial.
func serveWhoami(t *testing.T, cfg ServerConfig) string {
	t.Helper()
	h, lis := HandlerListener(cfg)
	srv := httptest.NewServer(sessionMiddleware(h))
	gs := grpc.NewServer(grpc.Creds(ServerCredentials()))
	gs.RegisterService(&whoamiDesc, nil)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
		srv.Close()
	})
	return "ws" + strings.TrimPrefix(srv.URL, "http") + cfg.path()
}

// whoami dials url with the given cookie header and returns the server's reply
// to one message.
func whoami(t *testing.T, url string, hdr http.Header, msg string) (string, error) {
	t.Helper()
	opt, err := DialOption(url, ClientConfig{HTTPHeader: hdr})
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	cc, err := grpc.NewClient("passthrough:///whoami",
		grpc.WithTransportCredentials(insecure.NewCredentials()), opt)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := cc.NewStream(ctx,
		&grpc.StreamDesc{StreamName: "Stream", ServerStreams: true, ClientStreams: true},
		whoamiMethod, grpc.CallContentSubtype(testCodec))
	if err != nil {
		return "", err
	}
	out := []byte(msg)
	if err := stream.SendMsg(&out); err != nil {
		return "", err
	}
	var in []byte
	if err := stream.RecvMsg(&in); err != nil {
		return "", err
	}
	_ = stream.CloseSend()
	return string(in), nil
}

// ---- tests ---------------------------------------------------------------

// TestUpgradeValueReachesServiceMethod is the end-to-end proof: a cookie set
// by the browser, read by an http middleware, arrives in a gRPC service
// method's stream context.
func TestUpgradeValueReachesServiceMethod(t *testing.T) {
	seenAuthType.Store("")
	url := serveWhoami(t, ServerConfig{
		Path:           "/grpc",
		OriginPatterns: []string{"*"},
		OnUpgrade:      requireSession,
	})

	for _, tc := range []struct{ cookie, user string }{
		{"tok-alice", "alice"},
		{"tok-bob", "bob"},
	} {
		hdr := http.Header{"Cookie": []string{"session=" + tc.cookie}}
		got, err := whoami(t, url, hdr, "hello")
		if err != nil {
			t.Fatalf("cookie %s: %v", tc.cookie, err)
		}
		if want := tc.user + ":hello"; got != want {
			t.Fatalf("cookie %s: got %q want %q", tc.cookie, got, want)
		}
	}
	if got := seenAuthType.Load().(string); got != "wstransport-upgrade" {
		t.Fatalf("peer AuthInfo.AuthType() = %q, want the transport's", got)
	}
}

// TestUpgradeRefusedSeenByClient proves the hook can refuse an upgrade and
// that the client learns of it — as a typed HandshakeError carrying the status
// on a raw dial, and as a failed RPC through the gRPC client.
func TestUpgradeRefusedSeenByClient(t *testing.T) {
	var logbuf strings.Builder
	url := serveWhoami(t, ServerConfig{
		Path:           "/grpc",
		OriginPatterns: []string{"*"},
		OnUpgrade:      requireSession,
		Logger:         log.New(&logbuf, "", 0),
	})

	// 1. Raw dial without the cookie: refused with the hook's own status.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := dialConn(ctx, url, ClientConfig{})
	if err == nil {
		t.Fatal("dial without session cookie succeeded")
	}
	var he *HandshakeError
	if !errors.As(err, &he) {
		t.Fatalf("error is not a *HandshakeError: %v", err)
	}
	if he.StatusCode != http.StatusUnauthorized {
		t.Fatalf("StatusCode = %d, want 401", he.StatusCode)
	}
	if !strings.Contains(he.Error(), "401") || errors.Unwrap(he) == nil {
		t.Fatalf("HandshakeError not descriptive: %q", he.Error())
	}

	// 2. An unknown cookie is refused just the same.
	hdr := http.Header{"Cookie": []string{"session=tok-forged"}}
	if _, err := dialConn(ctx, url, ClientConfig{HTTPHeader: hdr}); !errors.As(err, &he) ||
		he.StatusCode != http.StatusUnauthorized {
		t.Fatalf("forged cookie: got %v", err)
	}

	// 3. Through the gRPC client: the RPC fails rather than reaching a method.
	if got, err := whoami(t, url, nil, "hello"); err == nil {
		t.Fatalf("RPC succeeded without a session: %q", got)
	} else if status.Code(err) != codes.Unavailable {
		t.Fatalf("RPC error code = %s, want Unavailable (%v)", status.Code(err), err)
	}

	// 4. The server logged the refusal for its operator.
	if !strings.Contains(logbuf.String(), "upgrade refused") {
		t.Fatalf("refusal not logged: %q", logbuf.String())
	}
}

// acceptOne dials the handler mounted on srv and returns the conn its listener
// accepted, bypassing gRPC so the conn itself can be inspected. Both ends are
// drained and closed at cleanup, so the WebSocket close handshake completes
// immediately instead of timing out.
func acceptOne(t *testing.T, lis net.Listener, srv *httptest.Server) net.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	cli, errc := make(chan net.Conn, 1), make(chan error, 1)
	go func() {
		c, err := dialConn(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/", ClientConfig{})
		if err != nil {
			errc <- err
			return
		}
		cli <- c
	}()

	srvConn, err := lis.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	var cliConn net.Conn
	select {
	case cliConn = <-cli:
	case err := <-errc:
		t.Fatalf("dial: %v", err)
	}
	go func() { _, _ = io.Copy(io.Discard, srvConn) }()
	go func() { _, _ = io.Copy(io.Discard, cliConn) }()
	t.Cleanup(func() {
		_ = cliConn.Close()
		_ = srvConn.Close()
	})
	return srvConn
}

// TestNoHookLeavesConnUnchanged pins the zero value: without OnUpgrade the
// accepted conn is the bare WebSocket conn, carrying no value.
func TestNoHookLeavesConnUnchanged(t *testing.T) {
	h, lis := HandlerListener(ServerConfig{OriginPatterns: []string{"*"}})
	srv := httptest.NewServer(h)
	t.Cleanup(func() { _ = lis.Close(); srv.Close() })

	conn := acceptOne(t, lis, srv)
	if v, ok := ConnValue(conn); ok {
		t.Fatalf("ConnValue on a hookless conn = (%v, true), want false", v)
	}
	if _, ok := conn.(ValueConn); ok {
		t.Fatal("hookless conn implements ValueConn")
	}
}

// TestOnUpgradeNilValue covers a hook that authorizes without attaching
// anything: the conn is a ValueConn holding nil.
func TestOnUpgradeNilValue(t *testing.T) {
	h, lis := HandlerListener(ServerConfig{
		OriginPatterns: []string{"*"},
		OnUpgrade:      func(*http.Request) (any, error) { return nil, nil },
	})
	srv := httptest.NewServer(h)
	t.Cleanup(func() { _ = lis.Close(); srv.Close() })

	conn := acceptOne(t, lis, srv)
	v, ok := ConnValue(conn)
	if !ok || v != nil {
		t.Fatalf("ConnValue = (%v, %v), want (nil, true)", v, ok)
	}
}

func TestConnValueForeignConn(t *testing.T) {
	if v, ok := ConnValue(&fakeConn{}); ok {
		t.Fatalf("ConnValue on a foreign conn = (%v, true)", v)
	}
	vc := &valueConn{Conn: &fakeConn{}, value: 42}
	if v, ok := ConnValue(vc); !ok || v != 42 {
		t.Fatalf("ConnValue = (%v, %v), want (42, true)", v, ok)
	}
}

func TestUpgradeErrorStatusAndText(t *testing.T) {
	cause := errors.New("boom")
	for _, tc := range []struct {
		name     string
		err      *UpgradeError
		code     int
		msg      string
		contains string
	}{
		{"zero value", &UpgradeError{}, http.StatusForbidden, "Forbidden", "403 Forbidden)"},
		{"cause", &UpgradeError{Err: cause}, http.StatusForbidden, "Forbidden", "boom"},
		{"custom", &UpgradeError{Code: http.StatusServiceUnavailable, Message: "draining"},
			http.StatusServiceUnavailable, "draining", "503 draining"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, msg := tc.err.status()
			if code != tc.code || msg != tc.msg {
				t.Fatalf("status() = (%d, %q), want (%d, %q)", code, msg, tc.code, tc.msg)
			}
			if !strings.Contains(tc.err.Error(), tc.contains) {
				t.Fatalf("Error() = %q, want it to contain %q", tc.err.Error(), tc.contains)
			}
			if code, msg := refusal(tc.err); code != tc.code || msg != tc.msg {
				t.Fatalf("refusal() = (%d, %q), want (%d, %q)", code, msg, tc.code, tc.msg)
			}
		})
	}
	if got := errors.Unwrap(&UpgradeError{Err: cause}); got != cause {
		t.Fatalf("Unwrap = %v, want %v", got, cause)
	}
	// A wrapped UpgradeError still decides the status.
	wrapped := fmt.Errorf("session store: %w", &UpgradeError{Code: http.StatusNotFound})
	if code, _ := refusal(wrapped); code != http.StatusNotFound {
		t.Fatalf("refusal(wrapped) = %d, want 404", code)
	}
}

// TestRefusalPlainErrorIsOpaque: a hook that returns an ordinary error refuses
// with 403 and must not leak its text to the client.
func TestRefusalPlainErrorIsOpaque(t *testing.T) {
	code, msg := refusal(errors.New("user 17 is banned until 2031"))
	if code != http.StatusForbidden || msg != "Forbidden" {
		t.Fatalf("refusal(plain) = (%d, %q), want (403, \"Forbidden\")", code, msg)
	}

	h, lis := HandlerListener(ServerConfig{
		OriginPatterns: []string{"*"},
		OnUpgrade:      func(*http.Request) (any, error) { return nil, errors.New("user 17 is banned") },
	})
	defer lis.Close()
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	body := make([]byte, 256)
	n, _ := resp.Body.Read(body)
	if strings.Contains(string(body[:n]), "banned") {
		t.Fatalf("refusal body leaked the cause: %q", body[:n])
	}
}

// TestHandshakeErrorOnlyForRefusals: a dial that never reaches a server is a
// plain error, not a HandshakeError.
func TestHandshakeErrorOnlyForRefusals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := dialConn(ctx, "ws://127.0.0.1:1/", ClientConfig{})
	if err == nil {
		t.Fatal("expected dial error")
	}
	var he *HandshakeError
	if errors.As(err, &he) {
		t.Fatalf("connection failure reported as a handshake refusal: %v", err)
	}
}

// ---- credentials ---------------------------------------------------------

func TestServerCredentials(t *testing.T) {
	creds := ServerCredentials()

	// ServerHandshake on a conn carrying a value.
	vc := &valueConn{Conn: &fakeConn{}, value: "alice"}
	c, ai, err := creds.ServerHandshake(vc)
	if err != nil || c != net.Conn(vc) {
		t.Fatalf("ServerHandshake = (%v, %v, %v)", c, ai, err)
	}
	if got, ok := ai.(AuthInfo); !ok || got.Value != "alice" {
		t.Fatalf("AuthInfo = %#v", ai)
	}
	if got := ai.AuthType(); got != "wstransport-upgrade" {
		t.Fatalf("AuthType = %q", got)
	}

	// ServerHandshake on a conn without one: AuthInfo with a nil value.
	if _, ai, err := creds.ServerHandshake(&fakeConn{}); err != nil || ai.(AuthInfo).Value != nil {
		t.Fatalf("hookless ServerHandshake = (%v, %v)", ai, err)
	}

	// Client side is refused: these credentials are server-side only.
	if _, _, err := creds.ClientHandshake(context.Background(), "x", &fakeConn{}); !errors.Is(err, errClientHandshake) {
		t.Fatalf("ClientHandshake err = %v", err)
	}

	if got := creds.Info().SecurityProtocol; got != "websocket" {
		t.Fatalf("Info().SecurityProtocol = %q", got)
	}
	if creds.Clone() != credentials.TransportCredentials(upgradeCreds{}) {
		t.Fatal("Clone did not return equivalent credentials")
	}
	if err := creds.OverrideServerName("other"); err != nil {
		t.Fatalf("OverrideServerName: %v", err)
	}
}

// foreignAuthInfo stands for AuthInfo produced by some other credentials.
type foreignAuthInfo struct{}

func (foreignAuthInfo) AuthType() string { return "foreign" }

func TestFromContext(t *testing.T) {
	if v, ok := FromContext(context.Background()); ok {
		t.Fatalf("FromContext(no peer) = (%v, true)", v)
	}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: foreignAuthInfo{}})
	if v, ok := FromContext(ctx); ok {
		t.Fatalf("FromContext(foreign AuthInfo) = (%v, true)", v)
	}
	ctx = peer.NewContext(context.Background(), &peer.Peer{AuthInfo: AuthInfo{Value: "alice"}})
	if v, ok := FromContext(ctx); !ok || v != "alice" {
		t.Fatalf("FromContext = (%v, %v), want (alice, true)", v, ok)
	}
}
