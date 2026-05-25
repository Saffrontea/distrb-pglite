import { MemoryBlobStore, SyncNode, type EventEnvelope, type ReplayResponse, type Transport, type TransportConnection, type TransportHandlers } from "@distrb-pglite/core";
import { PGlite } from "@electric-sql/pglite";
import * as fc from "fast-check";
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  PGliteCasFilesystem,
  PGlitePartitionStore,
} from "../src/index.js";

type Operation = "commit" | "projection-error" | "publish-error";

type CountRow = {
  count: number | string;
};

const operationsArb = fc.array(
  fc.constantFrom<Operation>("commit", "projection-error", "publish-error"),
  { minLength: 1, maxLength: 6 },
);

describe("PBT: PGlite VFS and event send transaction boundary", () => {
  it("does not let VFS writes or post-commit event send corrupt the local transaction", async () => {
    await fc.assert(
      fc.asyncProperty(operationsArb, async (operations) => {
        const blobs = createBlobStore();
        const fs = new PGliteCasFilesystem(blobs, 4096);
        let db = new PGlite({ fs: fs as never });
        const store = new PGlitePartitionStore("node-a", db);
        const transport = new ScriptedTransport();
        const node = new SyncNode("node-a", store, transport);

        await db.exec(`
          CREATE TABLE tx_notes (
            event_id text PRIMARY KEY,
            value integer NOT NULL
          )
        `);
        await store.migrate();
        await node.start();

        let committed = 0;
        let successfulPublishes = 0;

        for (const [index, operation] of operations.entries()) {
          transport.failNextPublish = operation === "publish-error";
          const beforePublishAttempts = transport.publishAttempts;

          const write = node.writeEvent(
            {
              partitionId: "tenant:jp",
              schemaVersion: 1,
              payload: { value: index },
            },
            async (event) => {
              await db.query(
                "INSERT INTO tx_notes (event_id, value) VALUES ($1, $2)",
                [event.eventId, index],
              );
              if (operation === "projection-error") {
                throw new Error("projection failed");
              }
            },
          );

          if (operation === "projection-error") {
            await expect(write).rejects.toThrow("projection failed");
            expect(transport.publishAttempts).toBe(beforePublishAttempts);
          } else if (operation === "publish-error") {
            await expect(write).rejects.toThrow("publish failed");
            committed += 1;
          } else {
            await expect(write).resolves.toMatchObject({
              sourceNodeId: "node-a",
              partitionId: "tenant:jp",
            });
            committed += 1;
            successfulPublishes += 1;
          }

          await expect(countRows(db, "tx_notes")).resolves.toBe(committed);
          await expect(store.listEventsAfterCursors("tenant:jp", new Map()))
            .resolves.toHaveLength(committed);
          expect(transport.published).toHaveLength(successfulPublishes);
        }

        await node.stop();
        await db.close();

        const manifest = await fs.snapshot();
        const restoredFs = new PGliteCasFilesystem(blobs, 4096);
        await restoredFs.restore(manifest);
        db = new PGlite({ fs: restoredFs as never });
        const restoredStore = new PGlitePartitionStore("node-a", db);

        await expect(countRows(db, "tx_notes")).resolves.toBe(committed);
        await expect(restoredStore.listEventsAfterCursors("tenant:jp", new Map()))
          .resolves.toHaveLength(committed);

        await db.close();
      }),
      { numRuns: 5 },
    );
  }, 120_000);
});

async function countRows(db: PGlite, table: string): Promise<number> {
  const result = await db.query<CountRow>(`SELECT count(*)::int AS count FROM ${table}`);
  return Number(result.rows[0]?.count ?? 0);
}

function createBlobStore(): MemoryBlobStore {
  return new MemoryBlobStore((content) =>
    createHash("sha256").update(content).digest("hex"),
  );
}

class ScriptedTransport implements Transport {
  failNextPublish = false;
  publishAttempts = 0;
  readonly published: EventEnvelope[] = [];
  private readonly handlers = new Map<string, TransportHandlers>();

  async connect(nodeId: string, handlers: TransportHandlers): Promise<TransportConnection> {
    this.handlers.set(nodeId, handlers);
    return {
      subscribe: async () => {},
      publish: async (event) => {
        this.publishAttempts += 1;
        if (this.failNextPublish) {
          this.failNextPublish = false;
          throw new Error("publish failed");
        }
        this.published.push(event);
      },
      requestReplay: async (targetNodeId, partitionId, cursors): Promise<ReplayResponse> => {
        const response = await this.handlers.get(targetNodeId)?.onReplayRequest?.({
          requesterNodeId: nodeId,
          partitionId,
          cursors,
        });
        return response ?? { partitionId, events: [] };
      },
      close: async () => {
        this.handlers.delete(nodeId);
      },
    };
  }
}
