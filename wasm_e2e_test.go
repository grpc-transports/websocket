//go:build !js

package wstransport

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestWasmE2E compiles the js/wasm client, runs it under Node against a real
// echo server reached through this package's ListenWebSocket, and asserts a
// full bidirectional stream completes. It proves the js/wasm DialOption
// actually runs — not merely that it compiles. It skips cleanly when the
// toolchain's wasm glue or Node is unavailable (e.g. under qemu-emulated CI),
// mirroring how the wireguard transport excludes its kernel backend.
func TestWasmE2E(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found; skipping wasm e2e")
	}
	wasmExec := filepath.Join(runtime.GOROOT(), "lib", "wasm", "wasm_exec.js")
	if _, err := os.Stat(wasmExec); err != nil {
		t.Skipf("wasm_exec.js not found at %s; skipping", wasmExec)
	}

	tmp := t.TempDir()
	wasmPath := filepath.Join(tmp, "client.wasm")
	build := exec.Command("go", "build", "-o", wasmPath, "./wasmtest")
	build.Env = append(os.Environ(), "GOOS=js", "GOARCH=wasm", "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("wasm build failed: %v\n%s", err, out)
	}

	lis, err := ListenWebSocket("127.0.0.1:0", ServerConfig{OriginPatterns: []string{"*"}})
	if err != nil {
		t.Fatalf("ListenWebSocket: %v", err)
	}
	gs := serveEcho(lis)
	defer gs.Stop()
	defer lis.Close()

	cmd := exec.Command(node, filepath.Join("wasmtest", "run.mjs"))
	cmd.Env = append(os.Environ(),
		"URL=ws://"+lis.Addr().String(),
		"WASM="+wasmPath,
		"WASM_EXEC="+wasmExec,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node host failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "WASM_OK") {
		t.Fatalf("wasm e2e did not report success:\n%s", out)
	}
	t.Logf("wasm e2e: %s", strings.TrimSpace(string(out)))
}
