import { createServer } from "node:net";
import { describe, expect, it } from "vitest";
import { runNodeDemo } from "../src/index.js";
import { makeTempDir } from "./temp-dir.js";

describe("Phase 6 node demo", () => {
  it("demonstrates local write/read, subscription filtering, offline recovery, and WebSocket catch-up", async () => {
    const port = await getFreePort();
    if (port === undefined) {
      return;
    }

    const baseDir = await makeTempDir("phase6");
    const summary = await runNodeDemo({ baseDir, port });

    expect(summary.baseDir).toBe(baseDir);
    expect(summary.onlineEvent.partitionId).toBe("tenant:jp");
    expect(summary.onlineEvent.payload).toEqual({ sensor: "tokyo-1", reading: 42 });
    expect(summary.offlineEvent.payload).toEqual({
      sensor: "tokyo-1",
      reading: 43,
      mode: "offline",
    });
    expect(summary.ignoredPartitionEvents).toBe(0);
    expect(summary.nodeBEventsBeforeCatchup).toBe(1);
    expect(summary.nodeBEventsAfterCatchup).toBe(2);
    expect(summary.nodeCApplied).toBe(2);
    expect(summary.nodeCEvents).toBe(2);
  }, 20_000);
});

async function getFreePort(): Promise<number | undefined> {
  const server = createServer();
  const canListen = await new Promise<boolean>((resolve, reject) => {
    server.once("error", (error: NodeJS.ErrnoException) => {
      if (error.code === "EPERM" || error.code === "EACCES") {
        resolve(false);
        return;
      }
      reject(error);
    });
    server.listen(0, "127.0.0.1", () => resolve(true));
  });

  if (!canListen) {
    return undefined;
  }

  const address = server.address();
  await new Promise<void>((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  });

  if (!address || typeof address === "string") {
    throw new Error("failed to allocate test port");
  }

  return address.port;
}
