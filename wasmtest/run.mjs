// Node host for the wasm end-to-end tests. Loads the Go wasm_exec.js glue,
// instantiates client.wasm, points it at the server, and prints
// WASM_OK / WASM_FAIL for the Go orchestrator to assert on.
//
// Env: URL (ws:// server), WASM (path to client.wasm),
//      WASM_EXEC (path to the toolchain's wasm_exec.js),
//      GRPCWS_MODE (echo | whoami | refuse; default echo),
//      GRPCWS_USER (identity the server is expected to report back),
//      GRPCWS_COOKIE (cookie header value to attach to the WebSocket).
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const {
  URL: wsURL, WASM, WASM_EXEC,
  GRPCWS_MODE, GRPCWS_USER, GRPCWS_COOKIE,
} = process.env;
if (!wsURL || !WASM || !WASM_EXEC) {
  console.error("WASM_FAIL missing env (URL/WASM/WASM_EXEC)");
  process.exit(2);
}

// A browser attaches the page's cookies to a same-origin WebSocket handshake
// by itself, and gives page scripts no way to set headers. Node has no cookie
// jar, so the host stands in for the browser here: the Go wasm client still
// constructs a plain `new WebSocket(url)`, exactly as it does in a browser,
// and never sees the cookie.
if (GRPCWS_COOKIE) {
  const RealWebSocket = globalThis.WebSocket;
  globalThis.WebSocket = class extends RealWebSocket {
    constructor(url, protocols) {
      super(url, { protocols, headers: { cookie: GRPCWS_COOKIE } });
    }
  };
}

await import(pathToFileURL(WASM_EXEC).href); // defines globalThis.Go
globalThis.__grpcwsURL = wsURL;
if (GRPCWS_MODE) globalThis.__grpcwsMode = GRPCWS_MODE;
if (GRPCWS_USER) globalThis.__grpcwsUser = GRPCWS_USER;

const go = new globalThis.Go();
const { instance } = await WebAssembly.instantiate(await readFile(WASM), go.importObject);
await go.run(instance); // resolves when wasm main() returns

const res = globalThis.__grpcwsResult;
if (res && res.ok) {
  console.log("WASM_OK " + res.msg);
  process.exit(0);
}
console.error("WASM_FAIL " + (res ? res.msg : "no result"));
process.exit(1);
