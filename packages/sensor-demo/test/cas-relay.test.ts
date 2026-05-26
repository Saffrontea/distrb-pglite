import { createServer, type Server } from "node:http";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { createCasRelayHandler } from "../src/cas-relay.js";

describe("browser CAS relay", () => {
  let server: Server | undefined;

  afterEach(async () => {
    await new Promise<void>((resolve, reject) => {
      if (!server) {
        resolve();
        return;
      }
      server.close((error) => error ? reject(error) : resolve());
      server = undefined;
    });
  });

  it("stores, lists, reads, and deletes CAS blocks by namespace", async () => {
    const baseUrl = await listen();
    const source = new TextEncoder().encode("browser relay block");
    const hash = sha256(source);

    expect(await listHashes(baseUrl, "source")).toEqual([]);

    const put = await fetch(`${baseUrl}/source/${hash}`, {
      method: "PUT",
      body: source,
    });
    expect(put.status).toBe(200);
    expect(await listHashes(baseUrl, "source")).toEqual([hash]);
    expect(await listHashes(baseUrl, "target")).toEqual([]);

    const read = await fetch(`${baseUrl}/source/${hash}`);
    expect(read.status).toBe(200);
    expect(new TextDecoder().decode(await read.arrayBuffer())).toBe("browser relay block");

    const deleted = await fetch(`${baseUrl}/source/${hash}`, { method: "DELETE" });
    expect(deleted.status).toBe(200);
    expect(await listHashes(baseUrl, "source")).toEqual([]);
  });

  it("rejects invalid block hashes and reports missing blocks", async () => {
    const baseUrl = await listen();

    const invalid = await fetch(`${baseUrl}/source/not-a-hash`, {
      method: "PUT",
      body: "bad",
    });
    expect(invalid.status).toBe(400);

    const missing = await fetch(`${baseUrl}/source/${"0".repeat(64)}`);
    expect(missing.status).toBe(404);
  });

  async function listen(): Promise<string> {
    server = createServer(createCasRelayHandler());
    await new Promise<void>((resolve, reject) => {
      server!.once("error", reject);
      server!.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("failed to bind test server");
    }
    return `http://127.0.0.1:${address.port}`;
  }
});

async function listHashes(baseUrl: string, namespace: string): Promise<string[]> {
  const response = await fetch(`${baseUrl}/${namespace}`);
  expect(response.status).toBe(200);
  const body = await response.json() as { hashes: string[] };
  return body.hashes;
}

function sha256(content: Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
}
