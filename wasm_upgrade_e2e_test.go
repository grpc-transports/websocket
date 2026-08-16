//go:build !js

package wstransport

import (
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// wasmToolchain locates node and the toolchain's wasm glue, skipping the test
// when either is unavailable (e.g. under qemu-emulated CI).
func wasmToolchain(t *testing.T) (node, wasmExec string) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found; skipping wasm e2e")
	}
	wasmExec = filepath.Join(runtime.GOROOT(), "lib", "wasm", "wasm_exec.js")
	if _, err := os.Stat(wasmExec); err != nil {
		t.Skipf("wasm_exec.js not found at %s; skipping", wasmExec)
	}
	return node, wasmExec
}

// buildWasmClient compiles the real js/wasm client used by the e2e tests.
func buildWasmClient(t *testing.T) string {
	t.Helper()
	wasmPath := filepath.Join(t.TempDir(), "client.wasm")
	build := exec.Command("go", "build", "-o", wasmPath, "./wasmtest")
	build.Env = append(os.Environ(), "GOOS=js", "GOARCH=wasm", "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("wasm build failed: %v\n%s", err, out)
	}
	return wasmPath
}

// runWasmClient runs the compiled client under Node with the given extra
// environment and returns the host's combined output.
func runWasmClient(t *testing.T, node, wasmPath, wasmExec, url string, extra ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(node, filepath.Join("wasmtest", "run.mjs"))
	cmd.Env = append(os.Environ(),
		"URL="+url,
		"WASM="+wasmPath,
		"WASM_EXEC="+wasmExec,
	)
	cmd.Env = append(cmd.Env, extra...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestWasmUpgradeContextE2E is the browser half of the upgrade-context proof:
// the real js/wasm client, running under Node, opens a WebSocket the host
// stamps with a session cookie (as a browser would); an http middleware
// resolves that cookie, ServerConfig.OnUpgrade attaches the session, and the
// gRPC service method answers with the identity it read out of its stream's
// context. Then the same client, without a cookie, is refused.
func TestWasmUpgradeContextE2E(t *testing.T) {
	node, wasmExec := wasmToolchain(t)
	wasmPath := buildWasmClient(t)

	url := serveWhoami(t, ServerConfig{
		Path:           "/grpc",
		OriginPatterns: []string{"*"},
		OnUpgrade:      requireSession,
	})

	t.Run("cookie reaches the service method", func(t *testing.T) {
		out, err := runWasmClient(t, node, wasmPath, wasmExec, url,
			"GRPCWS_MODE=whoami",
			"GRPCWS_COOKIE=session=tok-alice",
			"GRPCWS_USER=alice",
		)
		if err != nil {
			t.Fatalf("node host failed: %v\n%s", err, out)
		}
		if !strings.Contains(out, "WASM_OK") || !strings.Contains(out, "alice:") {
			t.Fatalf("wasm client did not see its identity:\n%s", out)
		}
		t.Logf("wasm whoami: %s", strings.TrimSpace(out))
	})

	t.Run("a different cookie yields a different identity", func(t *testing.T) {
		out, err := runWasmClient(t, node, wasmPath, wasmExec, url,
			"GRPCWS_MODE=whoami",
			"GRPCWS_COOKIE=session=tok-bob",
			"GRPCWS_USER=bob",
		)
		if err != nil {
			t.Fatalf("node host failed: %v\n%s", err, out)
		}
		if !strings.Contains(out, "WASM_OK") || !strings.Contains(out, "bob:") {
			t.Fatalf("wasm client did not see bob's identity:\n%s", out)
		}

		// Control: the same run with the wrong expected identity must fail,
		// proving the assertion above is not vacuous.
		out, err = runWasmClient(t, node, wasmPath, wasmExec, url,
			"GRPCWS_MODE=whoami",
			"GRPCWS_COOKIE=session=tok-bob",
			"GRPCWS_USER=alice",
		)
		if err == nil || !strings.Contains(out, "WASM_FAIL") {
			t.Fatalf("control run should have failed, got err=%v:\n%s", err, out)
		}
	})

	t.Run("the same client is refused without the cookie", func(t *testing.T) {
		out, err := runWasmClient(t, node, wasmPath, wasmExec, url,
			"GRPCWS_MODE=refuse",
			"GRPCWS_USER=alice",
		)
		if err != nil {
			t.Fatalf("node host failed: %v\n%s", err, out)
		}
		if !strings.Contains(out, "WASM_OK") || !strings.Contains(out, "refused") {
			t.Fatalf("wasm client was not refused:\n%s", out)
		}
		t.Logf("wasm refusal: %s", strings.TrimSpace(out))
	})

	t.Run("a forged cookie is refused with the hook's status", func(t *testing.T) {
		// The browser client cannot read the handshake status, so assert it
		// natively: the same endpoint answers 401 to a bad cookie.
		hdr := http.Header{"Cookie": []string{"session=tok-forged"}}
		if _, err := whoami(t, url, hdr, "hello"); err == nil {
			t.Fatal("forged cookie was accepted")
		}
	})
}
