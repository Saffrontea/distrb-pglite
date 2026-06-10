import * as fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
  InMemoryPartitionStore,
  MemoryTransport,
  SyncNode,
  type NodeId,
} from "@distrb-pglite/core";
import {
  expectStoresConverged,
  FaultInjectionTransport,
  mulberry32,
  partitionEventKey,
} from "../src/index.js";

const scenarioArb = fc.record({
  eventsPerNode: fc.array(fc.integer({ min: 1, max: 6 }), { minLength: 2, maxLength: 4 }),
  partitionId: fc.constantFrom("tenant:jp", "tenant:us", "tenant:eu"),
  seed: fc.integer({ min: 0, max: 0xffff_ffff }),
  dropEventRate: fc.constantFrom(0, 0.2, 0.5),
  duplicateEventRate: fc.constantFrom(0, 0.3),
  failReplayRate: fc.constantFrom(0, 0.3),
});

describe("PBT: SyncNode convergence under injected transport faults", () => {
  it("drop/duplicate/replay-failure の任意の組み合わせでも reconcile 後に全ノードが収束する", async () => {
    await fc.assert(
      fc.asyncProperty(
        scenarioArb,
        async ({
          eventsPerNode,
          partitionId,
          seed,
          dropEventRate,
          duplicateEventRate,
          failReplayRate,
        }) => {
          const random = mulberry32(seed);
          const transport = new FaultInjectionTransport(new MemoryTransport(), {
            dropEventRate,
            duplicateEventRate,
            failReplayRate,
            random,
          });

          const nodeIds: NodeId[] = eventsPerNode.map((_, index) => `node-${index}`);
          const stores = nodeIds.map((nodeId) => new InMemoryPartitionStore(nodeId));
          const nodes = nodeIds.map(
            (nodeId, index) => new SyncNode(nodeId, stores[index]!, transport),
          );

          for (const node of nodes) {
            await node.start();
            await node.subscribe(partitionId);
          }

          // Live writes go through the faulty transport: deliveries may be
          // dropped or duplicated on each subscriber.
          for (const [index, node] of nodes.entries()) {
            for (let seq = 0; seq < eventsPerNode[index]!; seq += 1) {
              await node.write({
                partitionId,
                schemaVersion: 1,
                payload: { n: index * 100 + seq },
              });
            }
          }

          // Recovery path: full reconcile from every peer, retrying through
          // injected replay failures. At-least-once delivery plus idempotent
          // apply must converge regardless of the fault mix.
          for (const [index, node] of nodes.entries()) {
            for (const peerId of nodeIds) {
              if (peerId === nodeIds[index]) {
                continue;
              }
              await retryThroughInjectedFaults(() => node.reconcileFrom(peerId, partitionId));
            }
          }

          const reference = await expectStoresConverged(stores, partitionId);
          const totalEvents = eventsPerNode.reduce((sum, count) => sum + count, 0);
          expect(reference.split(",").filter(Boolean)).toHaveLength(totalEvents);

          for (const node of nodes) {
            await node.stop();
          }
        },
      ),
      { numRuns: 75 },
    );
  });

  it("重複配送は idempotent apply で吸収され、event 集合は配送回数に依存しない", async () => {
    const transport = new FaultInjectionTransport(new MemoryTransport(), {
      duplicateEventRate: 1,
      random: mulberry32(7),
    });

    const writerStore = new InMemoryPartitionStore("node-writer");
    const writer = new SyncNode("node-writer", writerStore, transport);
    const readerStore = new InMemoryPartitionStore("node-reader");
    const reader = new SyncNode("node-reader", readerStore, transport);

    await writer.start();
    await reader.start();
    await writer.subscribe("tenant:jp");
    await reader.subscribe("tenant:jp");

    for (let seq = 0; seq < 5; seq += 1) {
      await writer.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { seq } });
    }

    expect(transport.stats.duplicated).toBeGreaterThan(0);
    await expect(partitionEventKey(readerStore, "tenant:jp")).resolves.toBe(
      await partitionEventKey(writerStore, "tenant:jp"),
    );

    await reader.stop();
    await writer.stop();
  });

  it("注入された replay 失敗は呼び出し側に伝播し、retry で回復できる", async () => {
    const transport = new FaultInjectionTransport(new MemoryTransport(), {
      failReplayRate: 1,
      random: mulberry32(11),
    });

    const sourceStore = new InMemoryPartitionStore("node-source");
    const source = new SyncNode("node-source", sourceStore, transport);
    const targetStore = new InMemoryPartitionStore("node-target");
    const target = new SyncNode("node-target", targetStore, transport);

    await source.start();
    await target.start();
    await source.subscribe("tenant:jp");
    await targetStore.registerPartition("tenant:jp");

    await expect(target.catchUpFrom("node-source", "tenant:jp")).rejects.toThrow(
      "injected transport fault",
    );

    await target.stop();
    await source.stop();
  });
});

async function retryThroughInjectedFaults<T>(run: () => Promise<T>, maxAttempts = 50): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      return await run();
    } catch (error) {
      lastError = error;
      if (!(error instanceof Error) || !error.message.includes("injected transport fault")) {
        throw error;
      }
    }
  }
  throw lastError;
}
