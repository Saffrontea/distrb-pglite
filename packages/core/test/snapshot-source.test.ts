import { describe, expect, it, vi } from "vitest";
import { StaticSnapshotSource } from "../src/index.js";
import type { StorageCatalogManifest, StorageSnapshotCatalogEntry } from "../src/index.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeEntry(partitionId: string, snapshotId: string): StorageSnapshotCatalogEntry<unknown> {
  return {
    groupId: "sensors",
    partitionId,
    snapshotId,
    components: ["catalog", "data"],
    highWatermark: [["node-a", "018bcfe5-6800-7001-8000-000000000001"]],
    ddlGeneration: 1,
    manifestKind: "cas",
    manifest: { files: [] },
  };
}

function makeCatalogManifest(groupId: string): StorageCatalogManifest {
  return {
    version: 1,
    groupId,
    rootRelationOid: 12345,
    ddlGeneration: 1,
    bindings: [],
    snapshots: [],
  };
}

// ---------------------------------------------------------------------------
// StaticSnapshotSource
// ---------------------------------------------------------------------------

describe("StaticSnapshotSource", () => {
  describe("listSnapshotEntries", () => {
    it("returns only entries matching the requested partitionId", async () => {
      const jpEntry = makeEntry("tenant:jp", "snap-001");
      const usEntry = makeEntry("tenant:us", "snap-002");
      const source = new StaticSnapshotSource({ entries: [jpEntry, usEntry] });

      const result = await source.listSnapshotEntries("node-a", "tenant:jp");

      expect(result).toHaveLength(1);
      expect(result[0]!.partitionId).toBe("tenant:jp");
      expect(result[0]!.snapshotId).toBe("snap-001");
    });

    it("returns an empty array when no entries match the requested partitionId", async () => {
      const jpEntry = makeEntry("tenant:jp", "snap-001");
      const source = new StaticSnapshotSource({ entries: [jpEntry] });

      const result = await source.listSnapshotEntries("node-a", "tenant:us");

      expect(result).toHaveLength(0);
    });

    it("returns multiple entries when several match the requested partitionId", async () => {
      const jp1 = makeEntry("tenant:jp", "snap-001");
      const jp2 = makeEntry("tenant:jp", "snap-002");
      const usEntry = makeEntry("tenant:us", "snap-003");
      const source = new StaticSnapshotSource({ entries: [jp1, jp2, usEntry] });

      const result = await source.listSnapshotEntries("node-a", "tenant:jp");

      expect(result).toHaveLength(2);
      expect(result.map((e) => e.snapshotId)).toEqual(
        expect.arrayContaining(["snap-001", "snap-002"]),
      );
    });

    it("ignores the peerNodeId argument (entries are not filtered by peer)", async () => {
      const entry = makeEntry("tenant:jp", "snap-001");
      const source = new StaticSnapshotSource({ entries: [entry] });

      const fromNodeA = await source.listSnapshotEntries("node-a", "tenant:jp");
      const fromNodeZ = await source.listSnapshotEntries("node-z", "tenant:jp");

      expect(fromNodeA).toHaveLength(1);
      expect(fromNodeZ).toHaveLength(1);
    });
  });

  describe("fetchBlocks", () => {
    it("defaults to returning 0 when no fetchBlocks callback is provided", async () => {
      const entry = makeEntry("tenant:jp", "snap-001");
      const source = new StaticSnapshotSource({ entries: [entry] });

      const transferred = await source.fetchBlocks("node-a", entry);

      expect(transferred).toBe(0);
    });

    it("delegates to the provided fetchBlocks callback and returns its result", async () => {
      const entry = makeEntry("tenant:jp", "snap-001");
      const fetchBlocksFn = vi.fn().mockResolvedValue(7);
      const source = new StaticSnapshotSource({
        entries: [entry],
        fetchBlocks: fetchBlocksFn,
      });

      const transferred = await source.fetchBlocks("node-a", entry);

      expect(transferred).toBe(7);
      expect(fetchBlocksFn).toHaveBeenCalledWith(entry);
    });
  });

  describe("loadCatalogManifest", () => {
    it("returns the manifest when the groupId matches", async () => {
      const manifest = makeCatalogManifest("sensors");
      const source = new StaticSnapshotSource({
        entries: [],
        catalogManifest: manifest,
      });

      const result = await source.loadCatalogManifest("node-a", "sensors");

      expect(result).toBe(manifest);
    });

    it("returns undefined when the groupId does not match the catalogManifest groupId", async () => {
      const manifest = makeCatalogManifest("sensors");
      const source = new StaticSnapshotSource({
        entries: [],
        catalogManifest: manifest,
      });

      const result = await source.loadCatalogManifest("node-a", "other-group");

      expect(result).toBeUndefined();
    });

    it("returns undefined when no catalogManifest was provided", async () => {
      const source = new StaticSnapshotSource({ entries: [] });

      const result = await source.loadCatalogManifest("node-a", "sensors");

      expect(result).toBeUndefined();
    });

    it("ignores the peerNodeId argument when matching the manifest", async () => {
      const manifest = makeCatalogManifest("sensors");
      const source = new StaticSnapshotSource({
        entries: [],
        catalogManifest: manifest,
      });

      const fromNodeA = await source.loadCatalogManifest("node-a", "sensors");
      const fromNodeZ = await source.loadCatalogManifest("node-z", "sensors");

      expect(fromNodeA).toBe(manifest);
      expect(fromNodeZ).toBe(manifest);
    });
  });
});
