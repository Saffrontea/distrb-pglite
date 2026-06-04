/**
 * Property-based tests for convergence invariants.
 *
 * These tests verify the core correctness properties of the event replication
 * model using randomly generated inputs, complementing the fixed-scenario tests
 * in sync-node.test.ts.
 *
 * Design note on drops:
 *   catchUpFrom uses the HWM cursor, so it can only request events strictly
 *   newer than the highest known event for each source. If a later event from
 *   source S was received but an earlier event was dropped, catchUpFrom will
 *   never request the earlier event — it is "hidden" behind the HWM.
 *   This is not a bug; it is the expected behavior of at-least-once delivery.
 *   reconcileFrom (empty-cursor full replay) is the recovery path for that case.
 */

import * as fc from "fast-check";
import { describe, expect, it } from "vitest";
import { InMemoryPartitionStore } from "../src/index.js";

// ─── helpers ──────────────────────────────────────────────────────────────────

type SimpleEvent = {
  eventId: string;
  sourceNodeId: string;
  partitionId: string;
  schemaVersion: 1;
  payload: { n: number };
};

function makeEventId(sourceIndex: number, seq: number): string {
  const ts = (1_700_000_000_000 + seq).toString(16).padStart(12, "0");
  const rand = (sourceIndex * 1000 + seq).toString(16).padStart(16, "0");
  return `${ts.slice(0, 8)}-${ts.slice(8, 12)}-7${rand.slice(0, 3)}-8${rand.slice(3, 6)}-${rand.slice(6, 18).padStart(12, "0")}`;
}

async function eventKey(store: InMemoryPartitionStore, partitionId: string): Promise<string> {
  const events = await store.listEventsAfterCursors(partitionId, new Map());
  return events
    .map((e) => `${e.sourceNodeId}/${e.eventId}`)
    .sort()
    .join(",");
}

// ─── arbitraries ──────────────────────────────────────────────────────────────

/**
 * N ノード (2〜5) がそれぞれ 1〜8 件 event を生成するシナリオ。
 * nodeCount は eventsPerNode の長さから導出することで一貫性を保証する。
 */
const scenarioArb = fc.record({
  eventsPerNode: fc.array(fc.integer({ min: 1, max: 8 }), { minLength: 2, maxLength: 5 }),
  partitionId: fc.constantFrom("tenant:jp", "tenant:us", "tenant:eu"),
  seed: fc.integer({ min: 0, max: 0xffff_ffff }),
});

// ─── property: convergence (reorder + duplicates, no drops) ──────────────────

describe("PBT: convergence", () => {
  it("任意の配送順・重複があっても catchup 後に全ノードのイベント集合が一致する", async () => {
    await fc.assert(
      fc.asyncProperty(scenarioArb, async ({ eventsPerNode, partitionId, seed }) => {
        const nodeCount = eventsPerNode.length;
        const nodeIds = Array.from({ length: nodeCount }, (_, i) => `node-${i}`);

        const stores = nodeIds.map((id) => new InMemoryPartitionStore(id));
        for (const store of stores) {
          await store.registerPartition(partitionId);
        }

        // appendLocal で event を生成 (eventId は store 内部の UUIDv7 が払い出す)
        const allEvents: SimpleEvent[] = [];
        for (let i = 0; i < nodeCount; i++) {
          for (let s = 0; s < eventsPerNode[i]!; s++) {
            const stored = await stores[i]!.appendLocal({
              partitionId,
              schemaVersion: 1,
              payload: { n: i * 100 + s },
            });
            allEvents.push(stored as SimpleEvent);
          }
        }

        // ランダムな配送順・重複 (欠落なし) でイベントを各ノードに届ける
        const rng = mulberry32(seed);
        const shuffled = [...allEvents].sort(() => rng() - 0.5);

        for (const event of shuffled) {
          for (const store of stores) {
            if (store === stores[nodeIds.indexOf(event.sourceNodeId)]) continue;
            await store.applyRemote(event);
            if (rng() < 0.3) await store.applyRemote(event); // 重複配送
          }
        }

        // mutual catchup (HWM ベース)
        for (const target of stores) {
          for (const source of stores) {
            if (target === source) continue;
            const cursors = await target.getHighWatermark(partitionId);
            const missing = await source.listEventsAfterCursors(partitionId, cursors);
            for (const event of missing) {
              await target.applyRemote(event);
            }
          }
        }

        const keys = await Promise.all(stores.map((s) => eventKey(s, partitionId)));
        const reference = keys[0]!;
        for (const k of keys) {
          expect(k).toBe(reference);
        }
        expect(reference.split(",").filter(Boolean)).toHaveLength(
          eventsPerNode.reduce((a, b) => a + b, 0),
        );
      }),
      { numRuns: 200 },
    );
  });

  it("reconcileFrom は HWM より古い欠落 event も復元できる", async () => {
    // drop を含む配送 → catchUpFrom では収束しない可能性あり → reconcileFrom で完全復元
    const dropArb = scenarioArb.map((s) => ({ ...s, dropRate: 0.3 }));

    await fc.assert(
      fc.asyncProperty(dropArb, async ({ eventsPerNode, partitionId, seed, dropRate }) => {
        const nodeCount = eventsPerNode.length;
        const nodeIds = Array.from({ length: nodeCount }, (_, i) => `node-${i}`);

        const stores = nodeIds.map((id) => new InMemoryPartitionStore(id));
        for (const store of stores) {
          await store.registerPartition(partitionId);
        }

        const allEvents: SimpleEvent[] = [];
        for (let i = 0; i < nodeCount; i++) {
          for (let s = 0; s < eventsPerNode[i]!; s++) {
            const stored = await stores[i]!.appendLocal({
              partitionId,
              schemaVersion: 1,
              payload: { n: i * 100 + s },
            });
            allEvents.push(stored as SimpleEvent);
          }
        }

        const rng = mulberry32(seed);
        const shuffled = [...allEvents].sort(() => rng() - 0.5);

        for (const event of shuffled) {
          for (const store of stores) {
            if (store === stores[nodeIds.indexOf(event.sourceNodeId)]) continue;
            if (rng() < dropRate) continue; // ランダム欠落
            await store.applyRemote(event);
          }
        }

        // reconcile: 空 cursor で全イベントを取り寄せる (gaps を埋める)
        for (const target of stores) {
          for (const source of stores) {
            if (target === source) continue;
            const all = await source.listEventsAfterCursors(partitionId, new Map());
            for (const event of all) {
              await target.applyRemote(event);
            }
          }
        }

        const keys = await Promise.all(stores.map((s) => eventKey(s, partitionId)));
        const reference = keys[0]!;
        for (const k of keys) {
          expect(k).toBe(reference);
        }
        expect(reference.split(",").filter(Boolean)).toHaveLength(
          eventsPerNode.reduce((a, b) => a + b, 0),
        );
      }),
      { numRuns: 200 },
    );
  });
});

// ─── property: cursor correctness ────────────────────────────────────────────

describe("PBT: cursor correctness", () => {
  it("自分の HighWatermark を cursor に渡すと listEventsAfterCursors は常に空を返す", async () => {
    await fc.assert(
      fc.asyncProperty(scenarioArb, async ({ eventsPerNode, partitionId }) => {
        const nodeCount = eventsPerNode.length;
        const nodeIds = Array.from({ length: nodeCount }, (_, i) => `node-${i}`);

        const store = new InMemoryPartitionStore("observer");
        await store.registerPartition(partitionId);

        for (let i = 0; i < nodeCount; i++) {
          const count = eventsPerNode[i]!;
          for (let s = 0; s < count; s++) {
            await store.applyRemote({
              eventId: makeEventId(i, i * 100 + s),
              sourceNodeId: nodeIds[i]!,
              partitionId,
              schemaVersion: 1,
              payload: { n: i * 100 + s },
            });
          }
        }

        const hwm = await store.getHighWatermark(partitionId);
        const result = await store.listEventsAfterCursors(partitionId, hwm);
        expect(result).toHaveLength(0);
      }),
      { numRuns: 300 },
    );
  });
});

// ─── property: partition isolation ───────────────────────────────────────────

describe("PBT: partition isolation", () => {
  it("partition A に書かれた event は partition B のストアに混入しない", async () => {
    const partitionPairArb = fc.record({
      partitionA: fc.constantFrom("tenant:jp", "tenant:us"),
      partitionB: fc.constantFrom("tenant:eu", "tenant:kr"),
      eventCount: fc.integer({ min: 1, max: 10 }),
    });

    await fc.assert(
      fc.asyncProperty(partitionPairArb, async ({ partitionA, partitionB, eventCount }) => {
        const store = new InMemoryPartitionStore("node-x");
        await store.registerPartition(partitionA);
        await store.registerPartition(partitionB);

        for (let i = 0; i < eventCount; i++) {
          await store.applyRemote({
            eventId: makeEventId(0, i),
            sourceNodeId: "node-src",
            partitionId: partitionA,
            schemaVersion: 1,
            payload: { i },
          });
        }

        const inA = await store.listEventsAfterCursors(partitionA, new Map());
        const inB = await store.listEventsAfterCursors(partitionB, new Map());

        expect(inA).toHaveLength(eventCount);
        expect(inB).toHaveLength(0);
      }),
      { numRuns: 200 },
    );
  });
});

// ─── property: high-watermark monotonicity ───────────────────────────────────

describe("PBT: high-watermark monotonicity", () => {
  it("applyRemote 後に HWM が単調増加する (既知の source node について下がらない)", async () => {
    const eventSeqArb = fc.array(
      fc.record({
        sourceIndex: fc.integer({ min: 0, max: 2 }),
        seq: fc.integer({ min: 0, max: 999 }),
      }),
      { minLength: 2, maxLength: 20 },
    );

    await fc.assert(
      fc.asyncProperty(eventSeqArb, async (inputs) => {
        const store = new InMemoryPartitionStore("node-x");
        const partitionId = "tenant:jp";
        await store.registerPartition(partitionId);

        // seq を昇順に並べることで「単調増加な event stream」を作る
        const sorted = [...inputs].sort((a, b) => a.seq - b.seq);
        let prevHwm = await store.getHighWatermark(partitionId);

        for (const { sourceIndex, seq } of sorted) {
          await store.applyRemote({
            eventId: makeEventId(sourceIndex, seq),
            sourceNodeId: `src-${sourceIndex}`,
            partitionId,
            schemaVersion: 1,
            payload: { seq },
          });

          const hwm = await store.getHighWatermark(partitionId);

          // 既知の source の HWM は下がらない
          for (const [nodeId, prevId] of prevHwm) {
            const currentId = hwm.get(nodeId);
            if (currentId) {
              expect(currentId >= prevId).toBe(true);
            }
          }

          prevHwm = hwm;
        }
      }),
      { numRuns: 200 },
    );
  });
});

// ─── PRNG (seed-based, deterministic) ────────────────────────────────────────

function mulberry32(seed: number): () => number {
  let s = seed;
  return () => {
    s |= 0;
    s = (s + 0x6d2b79f5) | 0;
    let z = Math.imul(s ^ (s >>> 15), 1 | s);
    z = (z + Math.imul(z ^ (z >>> 7), 61 | z)) ^ z;
    return ((z ^ (z >>> 14)) >>> 0) / 0x1_0000_0000;
  };
}
