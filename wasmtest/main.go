//go:build js && wasm

// Command wasmtest is the browser-side half of the wasm end-to-end test. It
// dials the echo service through the real wstransport.DialOption (js/wasm
// implementation), runs a bidirectional stream, and reports the outcome to the
// JS host via globalThis.__grpcwsResult. It is driven by ../wasmtest/run.mjs.
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

// The codec name and method must match the server started by TestWasmE2E.
const (
	codecName = "wstransport-rawbytes"
	method    = "/wstransport.Echo/Stream"
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

func main() {
	report := func(ok bool, msg string) {
		js.Global().Set("__grpcwsResult", map[string]any{"ok": ok, "msg": msg})
	}
	url := js.Global().Get("__grpcwsURL").String()

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
	stream, err := cc.NewStream(ctx,
		&grpc.StreamDesc{StreamName: "Stream", ServerStreams: true, ClientStreams: true},
		method, grpc.CallContentSubtype(codecName))
	if err != nil {
		report(false, "NewStream: "+err.Error())
		return
	}
	for i := 0; i < 5; i++ {
		out := []byte(fmt.Sprintf("m%d", i))
		if err := stream.SendMsg(&out); err != nil {
			report(false, "Send: "+err.Error())
			return
		}
		var in []byte
		if err := stream.RecvMsg(&in); err != nil {
			report(false, "Recv: "+err.Error())
			return
		}
		if want := "echo:" + string(out); string(in) != want {
			report(false, fmt.Sprintf("mismatch got %q want %q", in, want))
			return
		}
	}
	_ = stream.CloseSend()
	report(true, "bidi echo x5 ok")
}
