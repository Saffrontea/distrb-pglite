import { InMemoryPartitionStore } from "@distrb-pglite/core";
import * as fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
  highWatermarkMap,
  listEventsAfterPartitionSnapshot,
  replayEventsAfterPartitionSnapshot,
  storageBindingSnapshotFilter,
  type PartitionSnapshotCursor,
} from "../src/index.js";

describe("partition snapshot cursors", () => {
  it("uses snapshot high-watermarks as partition-scoped replay cursors", async () => {
    const snapshot: PartitionSnapshotCursor = {
      partitionId: "tenant:jp",
      highWatermark: [["node-a", "018bcfe5-6800-7001-8000-000000000001"]],
    };
    const store = new InMemoryPartitionStore("peer");
    await store.registerPartition("tenant:jp");
    await store.registerPartition("tenant:us");
    await store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a1" },
    });
    await store.applyRemote({
      eventId: "018bcfe5-6800-7002-8000-000000000002",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a2" },
    });
    await store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000003",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "b1" },
    });
    await store.applyRemote({
      eventId: "018bcfe5-6800-7009-8000-000000000009",
      sourceNodeId: "node-a",
      partitionId: "tenant:us",
      schemaVersion: 1,
      payload: { value: "us-a9" },
    });

    await expect(listEventsAfterPartitionSnapshot(store, snapshot)).resolves.toMatchObject([
      { sourceNodeId: "node-a", payload: { value: "a2" } },
      { sourceNodeId: "node-b", payload: { value: "b1" } },
    ]);
  });

  it("replays post-snapshot events idempotently into an empty target partition", async () => {
    const snapshot: PartitionSnapshotCursor = {
      partitionId: "tenant:jp",
      highWatermark: [["node-a", "018bcfe5-6800-7001-8000-000000000001"]],
    };
    const sourceStore = new InMemoryPartitionStore("source");
    const targetStore = new InMemoryPartitionStore("target");
    await sourceStore.registerPartition("tenant:jp");
    await sourceStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "snapshotted" },
    });
    await sourceStore.applyRemote({
      eventId: "018bcfe5-6800-7002-8000-000000000002",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "delta" },
    });

    await expect(
      replayEventsAfterPartitionSnapshot({
        sourceStore,
        targetStore,
        snapshot,
      }),
    ).resolves.toBe(1);
    await expect(
      replayEventsAfterPartitionSnapshot({
        sourceStore,
        targetStore,
        snapshot,
      }),
    ).resolves.toBe(0);
    await expect(targetStore.listEventsAfterCursors("tenant:jp", new Map())).resolves.toMatchObject(
      [{ sourceNodeId: "node-a", payload: { value: "delta" } }],
    );
  });

  it("replays exactly the post-snapshot delta for arbitrary source high-watermarks", async () => {
    const sourceArb = fc.array(
      fc.record({
        snapshotCount: fc.integer({ min: 0, max: 5 }),
        deltaCount: fc.integer({ min: 0, max: 5 }),
      }),
      { minLength: 1, maxLength: 5 },
    );

    await fc.assert(
      fc.asyncProperty(sourceArb, async (sources) => {
        const partitionId = "tenant:jp";
        const sourceStore = new InMemoryPartitionStore("source");
        const targetStore = new InMemoryPartitionStore("target");
        await sourceStore.registerPartition(partitionId);
        await sourceStore.registerPartition("tenant:us");

        const highWatermark: PartitionSnapshotCursor["highWatermark"] = [];
        const expectedDeltaKeys: string[] = [];

        for (const [sourceIndex, source] of sources.entries()) {
          const sourceNodeId = `node-${sourceIndex}`;
          const totalCount = source.snapshotCount + source.deltaCount;

          for (let seq = 0; seq < totalCount; seq++) {
            const event = {
              eventId: eventId(sourceIndex, seq),
              sourceNodeId,
              partitionId,
              schemaVersion: 1,
              payload: { sourceIndex, seq },
            };
            await sourceStore.applyRemote(event);

            if (seq >= source.snapshotCount) {
              expectedDeltaKeys.push(eventKey(event));
            }
          }

          await sourceStore.applyRemote({
            eventId: eventId(sourceIndex, 10_000 + sourceIndex),
            sourceNodeId,
            partitionId: "tenant:us",
            schemaVersion: 1,
            payload: { sourceIndex, foreign: true },
          });

          if (source.snapshotCount > 0) {
            highWatermark.push([sourceNodeId, eventId(sourceIndex, source.snapshotCount - 1)]);
          }
        }

        const snapshot: PartitionSnapshotCursor = { partitionId, highWatermark };

        await expect(
          listEventsAfterPartitionSnapshot(sourceStore, snapshot).then((events) =>
            events.map(eventKey),
          ),
        ).resolves.toEqual(expectedDeltaKeys.sort());
        await expect(
          replayEventsAfterPartitionSnapshot({ sourceStore, targetStore, snapshot }),
        ).resolves.toBe(expectedDeltaKeys.length);
        await expect(
          replayEventsAfterPartitionSnapshot({ sourceStore, targetStore, snapshot }),
        ).resolves.toBe(0);
        await expect(
          targetStore
            .listEventsAfterCursors(partitionId, new Map())
            .then((events) => events.map(eventKey)),
        ).resolves.toEqual(expectedDeltaKeys.sort());
        await expect(
          targetStore.listEventsAfterCursors("tenant:us", new Map()),
        ).resolves.toHaveLength(0);
      }),
      { numRuns: 100 },
    );
  });

  it("exposes snapshot high-watermarks as a source-node cursor map", () => {
    const snapshot: PartitionSnapshotCursor = {
      partitionId: "tenant:jp",
      highWatermark: [
        ["node-b", "018bcfe5-6800-7001-8000-000000000003"],
        ["node-a", "018bcfe5-6800-7002-8000-000000000002"],
      ],
    };

    expect(highWatermarkMap(snapshot)).toEqual(
      new Map([
        ["node-b", "018bcfe5-6800-7001-8000-000000000003"],
        ["node-a", "018bcfe5-6800-7002-8000-000000000002"],
      ]),
    );
  });

  it("rejects duplicate high-watermark cursors for the same source node", () => {
    const snapshot: PartitionSnapshotCursor = {
      partitionId: "tenant:jp",
      highWatermark: [
        ["node-a", "018bcfe5-6800-7001-8000-000000000001"],
        ["node-a", "018bcfe5-6800-7002-8000-000000000002"],
      ],
    };

    expect(() => highWatermarkMap(snapshot)).toThrow(
      "duplicate high-watermark source node: node-a",
    );
  });

  it("rejects high-watermark cursors that are not UUIDv7 event ids", () => {
    const snapshot: PartitionSnapshotCursor = {
      partitionId: "tenant:jp",
      highWatermark: [["node-a", "not-a-uuid"]],
    };

    expect(() => highWatermarkMap(snapshot)).toThrow("invalid high-watermark event id at index 0");
  });
});

describe("storage binding snapshot filters", () => {
  it("keeps data-only relation forks and required data metadata while excluding unrelated catalog files", () => {
    const filter = storageBindingSnapshotFilter({
      partitionId: "tenant:jp",
      rootRelationOid: 16_384,
      snapshotScope: "data-only",
      relations: [
        {
          databaseOid: 5,
          tablespaceOid: 1_668,
          relationOid: 16_384,
          relfilenode: 20_000,
        },
        {
          databaseOid: 5,
          tablespaceOid: 9_999,
          relationOid: 30_000,
          storagePath: "pg_tblspc/9_999/5/30_000",
        },
      ],
    });

    expect(filter?.includeFile(file("base/5/20000"))).toBe(true);
    expect(filter?.includeFile(file("base/5/20000.1"))).toBe(true);
    expect(filter?.includeFile(file("base/5/20000_fsm"))).toBe(true);
    expect(filter?.includeFile(file("pg_tblspc/9_999/5/30_000_vm.2"))).toBe(true);
    expect(filter?.includeFile(file("pg_xact/0000"))).toBe(true);
    expect(filter?.includeFile(file("global/pg_filenode.map"))).toBe(true);
    expect(filter?.includeFile(file("base/5/99999"))).toBe(false);
    expect(filter?.includeFile(file("global/pg_control"))).toBe(false);
    expect(filter?.componentsForFile?.(file("base/5/20000"))).toEqual(["data"]);
    expect(filter?.componentsForFile?.(file("global/pg_control"))).toEqual(["catalog"]);
  });
});

function file(path: string) {
  return {
    path,
    size: 1,
    sha256: "0".repeat(64),
  };
}

function eventId(sourceIndex: number, seq: number): string {
  const timestamp = (1_700_000_000_000 + seq).toString(16).padStart(12, "0");
  const random = (sourceIndex * 100_000 + seq).toString(16).padStart(16, "0");
  return `${timestamp.slice(0, 8)}-${timestamp.slice(8, 12)}-7${random.slice(0, 3)}-8${random.slice(3, 6)}-${random.slice(6, 18).padStart(12, "0")}`;
}

function eventKey(event: { sourceNodeId: string; eventId: string }): string {
  return `${event.sourceNodeId}/${event.eventId}`;
}
