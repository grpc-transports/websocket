// Node host for the wasm end-to-end test. Loads the Go wasm_exec.js glue,
// instantiates client.wasm, points it at the echo server, and prints
// WASM_OK / WASM_FAIL for the Go orchestrator (TestWasmE2E) to assert on.
//
// Env: URL (ws:// echo server), WASM (path to client.wasm),
//      WASM_EXEC (path to the toolchain's wasm_exec.js).
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const { URL: wsURL, WASM, WASM_EXEC } = process.env;
if (!wsURL || !WASM || !WASM_EXEC) {
  console.error("WASM_FAIL missing env (URL/WASM/WASM_EXEC)");
  process.exit(2);
}

await import(pathToFileURL(WASM_EXEC).href); // defines globalThis.Go
globalThis.__grpcwsURL = wsURL;

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
