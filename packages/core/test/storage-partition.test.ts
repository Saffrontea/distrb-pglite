import { describe, expect, it } from "vitest";
import type { StorageBindingGroup, StoragePartitionBinding } from "../src/index.js";

describe("StoragePartitionBinding", () => {
  it("binds a partition id to Postgres storage identity and snapshot scope", () => {
    const binding: StoragePartitionBinding = {
      partitionId: "tenant:jp",
      rootRelationOid: 16_384,
      snapshotScope: "catalog-and-data",
      relations: [
        {
          databaseOid: 5,
          tablespaceOid: 1_668,
          relationOid: 16_384,
          relfilenode: 16_384,
        },
      ],
    };

    expect(binding.partitionId).toBe("tenant:jp");
    expect(binding.relations[0]?.relationOid).toBe(binding.rootRelationOid);
    expect(binding.snapshotScope).toBe("catalog-and-data");
  });

  it("allows data-only snapshots when catalog compatibility is external", () => {
    const binding: StoragePartitionBinding = {
      partitionId: "tenant:jp",
      rootRelationOid: 16_384,
      snapshotScope: "data-only",
      relations: [
        {
          databaseOid: 5,
          tablespaceOid: 1_668,
          relationOid: 16_385,
        },
      ],
    };

    expect(binding.snapshotScope).toBe("data-only");
    expect(binding.relations[0]?.relfilenode).toBeUndefined();
  });

  it("separates produced snapshot components from pull requirements", () => {
    const binding: StoragePartitionBinding = {
      partitionId: "tenant:jp",
      rootRelationOid: 16_384,
      snapshotScope: "catalog-and-data",
      snapshotComponents: ["catalog", "data"],
      pullRequires: ["data"],
      ddlGeneration: 7,
      relations: [
        {
          databaseOid: 5,
          tablespaceOid: 1_668,
          relationOid: 16_384,
          relfilenode: 16_384,
          role: "root",
        },
      ],
    };

    expect(binding.snapshotComponents).toEqual(["catalog", "data"]);
    expect(binding.pullRequires).toEqual(["data"]);
    expect(binding.ddlGeneration).toBe(7);
  });

  it("groups child table bindings for DDL, vacuum, and GC coordination", () => {
    const group: StorageBindingGroup = {
      groupId: "root:16384",
      rootRelationOid: 16_384,
      ddlGeneration: 9,
      vacuumPolicy: {
        retainSnapshotGenerations: 3,
        gcAfterGeneration: 6,
      },
      bindings: [
        {
          partitionId: "tenant:jp",
          rootRelationOid: 16_384,
          snapshotScope: "data-only",
          ddlGeneration: 9,
          relations: [
            {
              databaseOid: 5,
              tablespaceOid: 1_668,
              relationOid: 16_385,
              relfilenode: 16_385,
              role: "child",
              parentRelationOid: 16_384,
            },
          ],
        },
      ],
    };

    expect(group.bindings[0]?.relations[0]?.parentRelationOid).toBe(group.rootRelationOid);
    expect(group.vacuumPolicy?.gcAfterGeneration).toBeGreaterThan(
      group.vacuumPolicy?.retainSnapshotGenerations ?? 0,
    );
  });
});
