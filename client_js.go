//go:build js && wasm

package wstransport

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"syscall/js"
	"time"

	"google.golang.org/grpc"
)

// DialOption returns a grpc.DialOption that tunnels every gRPC channel over a
// browser WebSocket to wsURL (ws:// or wss://), using syscall/js with no
// third-party dependency. Combine it with insecure transport credentials —
// transport security is provided by the browser's wss layer. The TLSConfig and
// HTTPHeader fields of cfg are ignored here (the browser owns them).
func DialOption(wsURL string, cfg ClientConfig) (grpc.DialOption, error) {
	if wsURL == "" {
		return nil, errEmptyURL
	}
	return grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return dialJS(ctx, wsURL, cfg)
	}), nil
}

// dialJS opens a browser WebSocket and returns it as a net.Conn once OPEN.
func dialJS(ctx context.Context, wsURL string, cfg ClientConfig) (net.Conn, error) {
	var ws js.Value
	if len(cfg.Subprotocols) > 0 {
		protos := make([]any, len(cfg.Subprotocols))
		for i, p := range cfg.Subprotocols {
			protos[i] = p
		}
		ws = js.Global().Get("WebSocket").New(wsURL, protos)
	} else {
		ws = js.Global().Get("WebSocket").New(wsURL)
	}
	ws.Set("binaryType", "arraybuffer")

	c := &jsConn{ws: ws, readCh: make(chan []byte, 32), closeCh: make(chan struct{}), logger: cfg.Logger}

	opened := make(chan error, 1)
	c.onOpen = js.FuncOf(func(js.Value, []js.Value) any {
		trySend(opened, nil)
		return nil
	})
	c.onErr = js.FuncOf(func(js.Value, []js.Value) any {
		err := errors.New("wstransport: websocket error")
		trySend(opened, err)
		c.closeWith(err)
		return nil
	})
	c.onClose = js.FuncOf(func(js.Value, []js.Value) any {
		c.closeWith(io.EOF)
		return nil
	})
	c.onMsg = js.FuncOf(func(_ js.Value, args []js.Value) any {
		data := args[0].Get("data")
		u8 := js.Global().Get("Uint8Array").New(data)
		buf := make([]byte, u8.Get("length").Int())
		js.CopyBytesToGo(buf, u8)
		select {
		case c.readCh <- buf:
		case <-c.closeCh:
		}
		return nil
	})
	ws.Call("addEventListener", "open", c.onOpen)
	ws.Call("addEventListener", "error", c.onErr)
	ws.Call("addEventListener", "close", c.onClose)
	ws.Call("addEventListener", "message", c.onMsg)

	select {
	case err := <-opened:
		if err != nil {
			return nil, err
		}
		return c, nil
	case <-ctx.Done():
		_ = c.Close()
		return nil, ctx.Err()
	}
}

func trySend(ch chan error, err error) {
	select {
	case ch <- err:
	default:
	}
}

// jsConn adapts a browser WebSocket to net.Conn for grpc-go's HTTP/2 transport.
type jsConn struct {
	ws       js.Value
	readCh   chan []byte
	rem      []byte
	closeCh  chan struct{}
	closeErr error
	once     sync.Once
	logger   *log.Logger

	onOpen, onErr, onClose, onMsg js.Func
}

func (c *jsConn) Read(p []byte) (int, error) {
	if len(c.rem) > 0 {
		n := copy(p, c.rem)
		c.rem = c.rem[n:]
		return n, nil
	}
	select {
	case b := <-c.readCh:
		return c.fill(p, b), nil
	case <-c.closeCh:
		select {
		case b := <-c.readCh:
			return c.fill(p, b), nil
		default:
		}
		return 0, c.closeErr
	}
}

func (c *jsConn) fill(p, b []byte) int {
	n := copy(p, b)
	if n < len(b) {
		c.rem = b[n:]
	}
	return n
}

func (c *jsConn) Write(p []byte) (int, error) {
	select {
	case <-c.closeCh:
		return 0, c.closeErr
	default:
	}
	u8 := js.Global().Get("Uint8Array").New(len(p))
	js.CopyBytesToJS(u8, p)
	c.ws.Call("send", u8)
	return len(p), nil
}

func (c *jsConn) closeWith(err error) {
	c.once.Do(func() {
		if err == nil {
			err = io.EOF
		}
		c.closeErr = err
		close(c.closeCh)
	})
}

func (c *jsConn) Close() error {
	c.closeWith(errors.New("wstransport: conn closed"))
	c.ws.Call("close")
	c.onOpen.Release()
	c.onErr.Release()
	c.onClose.Release()
	c.onMsg.Release()
	return nil
}

func (c *jsConn) LocalAddr() net.Addr                { return wsAddr("websocket") }
func (c *jsConn) RemoteAddr() net.Addr               { return wsAddr("websocket") }
func (c *jsConn) SetDeadline(t time.Time) error      { return nil }
func (c *jsConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *jsConn) SetWriteDeadline(t time.Time) error { return nil }
