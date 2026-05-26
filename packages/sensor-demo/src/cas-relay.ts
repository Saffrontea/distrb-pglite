import type { IncomingMessage, ServerResponse } from "node:http";

export type CasRelayNamespaces = Map<string, Map<string, Uint8Array>>;

export function createCasRelayHandler(
  namespaces: CasRelayNamespaces = new Map(),
): (req: IncomingMessage, res: ServerResponse) => void {
  return (req, res) => {
    void handleCasRequest(req, res, namespaces).catch((error) => {
      writeJson(res, 500, { error: String(error) });
    });
  };
}

async function handleCasRequest(
  req: IncomingMessage,
  res: ServerResponse,
  namespaces: CasRelayNamespaces,
): Promise<void> {
  const parts = (req.url ?? "").split("?")[0]!.split("/").filter(Boolean);
  const [namespace, hash] = parts;
  if (!namespace) {
    writeJson(res, 400, { error: "namespace is required" });
    return;
  }

  const store = namespaces.get(namespace) ?? new Map<string, Uint8Array>();
  namespaces.set(namespace, store);

  if (req.method === "GET" && !hash) {
    writeJson(res, 200, { hashes: [...store.keys()].sort() });
    return;
  }

  if (!hash || !/^[0-9a-f]{64}$/.test(hash)) {
    writeJson(res, 400, { error: "valid block hash is required" });
    return;
  }

  if (req.method === "GET") {
    const block = store.get(hash);
    if (!block) {
      writeJson(res, 404, { error: `block not found: ${hash}` });
      return;
    }
    res.writeHead(200, {
      "Content-Type": "application/octet-stream",
      "Cache-Control": "no-store",
    });
    res.end(block);
    return;
  }

  if (req.method === "PUT") {
    store.set(hash, await readBody(req));
    writeJson(res, 200, { ok: true });
    return;
  }

  if (req.method === "DELETE") {
    store.delete(hash);
    writeJson(res, 200, { ok: true });
    return;
  }

  writeJson(res, 405, { error: "method not allowed" });
}

function readBody(req: IncomingMessage): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    const chunks: Uint8Array[] = [];
    req.on("data", (chunk: Uint8Array) => chunks.push(chunk));
    req.on("end", () => resolve(concat(chunks)));
    req.on("error", reject);
  });
}

function concat(chunks: Uint8Array[]): Uint8Array {
  const size = chunks.reduce((total, chunk) => total + chunk.length, 0);
  const result = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result;
}

function writeJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}
