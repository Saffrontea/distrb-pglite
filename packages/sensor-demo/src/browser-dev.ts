import { createServer as createViteServer, type ViteDevServer } from "vite";
import { createServer } from "node:net";
import { WebSocketHub } from "@distrb-pglite/node";
import { createCasRelayHandler } from "./cas-relay.js";

const host = "0.0.0.0";
const playgroundPort = 5173;

let hub: WebSocketHub | undefined;
let vite: ViteDevServer | undefined;

async function main(): Promise<void> {
  const relayPort = await getAvailablePort(4174);
  hub = new WebSocketHub({ port: relayPort });
  vite = await createViteServer({
    root: "browser",
    define: {
      __DISTRB_RELAY_PORT__: JSON.stringify(relayPort),
    },
    server: {
      host,
      port: playgroundPort,
      strictPort: false,
    },
  });
  vite.middlewares.use("/__distrb/cas", createCasRelayHandler());
  await vite.listen();
  vite.printUrls();

  const localUrl = vite.resolvedUrls?.local[0] ?? `http://${host}:${playgroundPort}/`;
  console.log("");
  console.log("distrb-pglite browser playground");
  console.log(`  UI    ${localUrl}`);
  console.log(`  relay ws://${host}:${relayPort}`);
  console.log(`  CAS   ${localUrl.replace(/\/$/, "")}/__distrb/cas/:namespace`);
}

async function shutdown(): Promise<void> {
  await vite?.close();
  await hub?.close();
}

process.once("SIGINT", () => {
  void shutdown().finally(() => process.exit(0));
});
process.once("SIGTERM", () => {
  void shutdown().finally(() => process.exit(0));
});

await main();

async function getAvailablePort(preferredPort: number): Promise<number> {
  for (let port = preferredPort; port < preferredPort + 100; port++) {
    if (await canListen(port)) {
      return port;
    }
  }
  throw new Error(`no available relay port near ${preferredPort}`);
}

function canListen(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const server = createServer();
    server.once("error", () => resolve(false));
    server.listen(port, host, () => {
      server.close(() => resolve(true));
    });
  });
}
