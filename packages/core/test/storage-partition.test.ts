import { describe, expect, it } from "vitest";
import type { StoragePartitionBinding } from "../src/index.js";

describe("StoragePartitionBinding", () => {
  it("binds a partition id to Postgres storage identity and snapshot scope", () => {
    const binding: StoragePartitionBinding = {
      partitionId: "tenant:jp",
      hypertableOid: 16_384,
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
    expect(binding.relations[0]?.relationOid).toBe(binding.hypertableOid);
    expect(binding.snapshotScope).toBe("catalog-and-data");
  });

  it("allows data-only snapshots when catalog compatibility is external", () => {
    const binding: StoragePartitionBinding = {
      partitionId: "tenant:jp",
      hypertableOid: 16_384,
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
});
