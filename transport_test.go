//go:build !js

package wstransport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

// ---- protoc-free echo service (test scaffolding only) --------------------

const (
	testCodec  = "wstransport-rawbytes"
	testMethod = "/wstransport.Echo/Stream"
)

type rawCodec struct{}

func (rawCodec) Name() string { return testCodec }
func (rawCodec) Marshal(v any) ([]byte, error) {
	return *v.(*[]byte), nil
}
func (rawCodec) Unmarshal(data []byte, v any) error {
	b := v.(*[]byte)
	*b = append((*b)[:0], data...)
	return nil
}

func init() { encoding.RegisterCodec(rawCodec{}) }

var echoDesc = grpc.ServiceDesc{
	ServiceName: "wstransport.Echo",
	HandlerType: (*any)(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		Handler:       echoHandler,
		ServerStreams: true,
		ClientStreams: true,
	}},
}

func echoHandler(_ any, stream grpc.ServerStream) error {
	for {
		var msg []byte
		if err := stream.RecvMsg(&msg); err != nil {
			return err
		}
		reply := append([]byte("echo:"), msg...)
		if err := stream.SendMsg(&reply); err != nil {
			return err
		}
	}
}

// serveEcho registers the echo service on a grpc.Server bound to lis.
func serveEcho(lis net.Listener) *grpc.Server {
	gs := grpc.NewServer()
	gs.RegisterService(&echoDesc, nil)
	go func() { _ = gs.Serve(lis) }()
	return gs
}

// roundTrip dials url with cfg, sends n messages over the bidi stream and
// asserts each is echoed.
func roundTrip(t *testing.T, url string, cfg ClientConfig, n int) {
	t.Helper()
	opt, err := DialOption(url, cfg)
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	cc, err := grpc.NewClient("passthrough:///echo",
		grpc.WithTransportCredentials(insecure.NewCredentials()), opt)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := cc.NewStream(ctx,
		&grpc.StreamDesc{StreamName: "Stream", ServerStreams: true, ClientStreams: true},
		testMethod, grpc.CallContentSubtype(testCodec))
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	for i := 0; i < n; i++ {
		out := []byte(fmt.Sprintf("m%d", i))
		if err := stream.SendMsg(&out); err != nil {
			t.Fatalf("SendMsg: %v", err)
		}
		var in []byte
		if err := stream.RecvMsg(&in); err != nil {
			t.Fatalf("RecvMsg: %v", err)
		}
		if want := "echo:" + string(out); string(in) != want {
			t.Fatalf("got %q want %q", in, want)
		}
	}
	_ = stream.CloseSend()
}

// ---- tests ---------------------------------------------------------------

func TestBidiRoundTripPlain(t *testing.T) {
	lis, err := ListenWebSocket("127.0.0.1:0", ServerConfig{OriginPatterns: []string{"*"}})
	if err != nil {
		t.Fatalf("ListenWebSocket: %v", err)
	}
	gs := serveEcho(lis)
	defer gs.Stop()
	defer lis.Close()

	addr := lis.Addr().String() // real TCP addr (ephemeral :0 resolved)
	roundTrip(t, "ws://"+addr, ClientConfig{}, 5)

	if _, _, err := net.SplitHostPort(addr); err != nil {
		t.Fatalf("Addr() not a host:port: %q (%v)", addr, err)
	}
}

func TestBidiRoundTripTLS(t *testing.T) {
	cert := selfSigned(t)
	lis, err := ListenWebSocket("127.0.0.1:0", ServerConfig{
		OriginPatterns: []string{"*"},
		TLSConfig:      &tls.Config{Certificates: []tls.Certificate{cert}},
	})
	if err != nil {
		t.Fatalf("ListenWebSocket(TLS): %v", err)
	}
	gs := serveEcho(lis)
	defer gs.Stop()
	defer lis.Close()

	addr := lis.Addr().String()
	roundTrip(t, "wss://"+addr, ClientConfig{
		TLSConfig:  &tls.Config{InsecureSkipVerify: true},
		HTTPHeader: http.Header{"X-Test": []string{"1"}},
	}, 3)
}

func TestCustomPathAndHandlerListener(t *testing.T) {
	h, lis := HandlerListener(ServerConfig{Path: "/grpc", OriginPatterns: []string{"*"}})
	srv := httptest.NewServer(h)
	defer srv.Close()
	gs := serveEcho(lis)
	defer gs.Stop()
	defer lis.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/grpc"
	roundTrip(t, url, ClientConfig{}, 2)

	if got := lis.Addr().String(); got != "/grpc" {
		t.Fatalf("custom path Addr = %q", got)
	}
	if got := lis.Addr().Network(); got != "websocket" {
		t.Fatalf("Addr().Network() = %q", got)
	}
}

func TestListenBindError(t *testing.T) {
	if _, err := ListenWebSocket("127.0.0.1:99999999", ServerConfig{}); err == nil {
		t.Fatal("expected bind error for invalid port")
	}
}

func TestDialOptionEmptyURL(t *testing.T) {
	if _, err := DialOption("", ClientConfig{}); err != errEmptyURL {
		t.Fatalf("got %v want errEmptyURL", err)
	}
	if opt, err := DialOption("ws://x", ClientConfig{}); err != nil || opt == nil {
		t.Fatalf("valid url: opt=%v err=%v", opt, err)
	}
}

func TestDialConnError(t *testing.T) {
	var buf strings.Builder
	lg := log.New(&buf, "", 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Nothing listening on this port → dial must fail and be logged.
	_, err := dialConn(ctx, "ws://127.0.0.1:1/", ClientConfig{Logger: lg})
	if err == nil {
		t.Fatal("expected dial error")
	}
	if !strings.Contains(buf.String(), "dial") {
		t.Fatalf("logger did not record dial error: %q", buf.String())
	}
}

func TestChanListenerPushAfterClose(t *testing.T) {
	lis := newChanListener(wsAddr("/"))
	if err := lis.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Idempotent Close.
	_ = lis.Close()

	fc := &fakeConn{}
	lis.push(fc) // closed listener → conn must be closed, not delivered
	if fc.closed.Load() != 1 {
		t.Fatal("push on closed listener did not close the conn")
	}
	if _, err := lis.Accept(); err != errListenerClosed {
		t.Fatalf("Accept after close = %v", err)
	}
}

func TestChanListenerPushDelivers(t *testing.T) {
	lis := newChanListener(wsAddr("/"))
	fc := &fakeConn{}
	go lis.push(fc)
	got, err := lis.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if got != fc {
		t.Fatal("Accept returned a different conn")
	}
	_ = lis.Close()
}

func TestAcceptErrorLogged(t *testing.T) {
	var buf strings.Builder
	lg := log.New(&buf, "", 0)
	h, lis := HandlerListener(ServerConfig{Logger: lg})
	defer lis.Close()
	srv := httptest.NewServer(h)
	defer srv.Close()

	// A plain GET is not a WebSocket handshake → Accept fails and logs.
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = resp.Body.Close()
	if !strings.Contains(buf.String(), "accept") {
		t.Fatalf("accept error not logged: %q", buf.String())
	}
}

func TestLogfNil(t *testing.T) {
	logf(nil, "ignored %d", 1) // must not panic
}

// ---- helpers -------------------------------------------------------------

type fakeConn struct {
	net.Conn
	closed atomic.Int32
}

func (f *fakeConn) Close() error { f.closed.Store(1); return nil }

func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
