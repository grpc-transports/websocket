//go:build js && wasm

// Command wasmtest is the browser-side half of the wasm end-to-end tests. It
// dials a service through the real wstransport.DialOption (js/wasm
// implementation) and reports the outcome to the JS host via
// globalThis.__grpcwsResult. It is driven by ../wasmtest/run.mjs.
//
// globalThis.__grpcwsMode selects what it proves:
//
//	echo   (default) a bidirectional stream against the echo service;
//	whoami a bidirectional stream against the Whoami service, whose replies
//	       are prefixed with the identity the server derived from the
//	       WebSocket upgrade request — globalThis.__grpcwsUser is the identity
//	       expected back;
//	refuse the server refuses the upgrade, so the RPC must fail.
package main

import (
	"context"
	"fmt"
	"syscall/js"
	"time"

	wstransport "github.com/grpc-transports/websocket"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

// The codec name and methods must match the server started by the Go test.
const (
	codecName    = "wstransport-rawbytes"
	echoMethod   = "/wstransport.Echo/Stream"
	whoamiMethod = "/wstransport.Whoami/Stream"
)

type rawCodec struct{}

func (rawCodec) Name() string                  { return codecName }
func (rawCodec) Marshal(v any) ([]byte, error) { return *v.(*[]byte), nil }
func (rawCodec) Unmarshal(d []byte, v any) error {
	b := v.(*[]byte)
	*b = append((*b)[:0], d...)
	return nil
}

func init() { encoding.RegisterCodec(rawCodec{}) }

// global returns a string global set by the JS host, or def when unset.
func global(name, def string) string {
	v := js.Global().Get(name)
	if v.Type() != js.TypeString {
		return def
	}
	return v.String()
}

func main() {
	report := func(ok bool, msg string) {
		js.Global().Set("__grpcwsResult", map[string]any{"ok": ok, "msg": msg})
	}
	url := js.Global().Get("__grpcwsURL").String()
	mode := global("__grpcwsMode", "echo")

	opt, err := wstransport.DialOption(url, wstransport.ClientConfig{})
	if err != nil {
		report(false, "DialOption: "+err.Error())
		return
	}
	cc, err := grpc.NewClient("passthrough:///echo",
		grpc.WithTransportCredentials(insecure.NewCredentials()), opt)
	if err != nil {
		report(false, "NewClient: "+err.Error())
		return
	}
	defer cc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	method, prefix := echoMethod, "echo:"
	switch mode {
	case "whoami", "refuse":
		method, prefix = whoamiMethod, global("__grpcwsUser", "")+":"
	}

	stream, err := cc.NewStream(ctx,
		&grpc.StreamDesc{StreamName: "Stream", ServerStreams: true, ClientStreams: true},
		method, grpc.CallContentSubtype(codecName))
	if err != nil {
		// The server refused the WebSocket upgrade: the browser is told the
		// socket failed, so the RPC never gets a transport. That is the
		// refusal reaching the browser client.
		if mode == "refuse" {
			report(true, "upgrade refused as expected: "+err.Error())
			return
		}
		report(false, "NewStream: "+err.Error())
		return
	}
	for i := 0; i < 5; i++ {
		out := []byte(fmt.Sprintf("m%d", i))
		if err := stream.SendMsg(&out); err != nil {
			if mode == "refuse" {
				report(true, "upgrade refused as expected: "+err.Error())
				return
			}
			report(false, "Send: "+err.Error())
			return
		}
		var in []byte
		if err := stream.RecvMsg(&in); err != nil {
			if mode == "refuse" {
				report(true, "upgrade refused as expected: "+err.Error())
				return
			}
			report(false, "Recv: "+err.Error())
			return
		}
		if want := prefix + string(out); string(in) != want {
			report(false, fmt.Sprintf("mismatch got %q want %q", in, want))
			return
		}
	}
	_ = stream.CloseSend()
	if mode == "refuse" {
		report(false, "the refused upgrade carried a full stream")
		return
	}
	report(true, mode+" bidi x5 ok (replies prefixed "+prefix+")")
}
