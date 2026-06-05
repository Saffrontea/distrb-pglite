import * as fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
  canSatisfyStoragePull,
  canSatisfyStorageSnapshotCatalogPull,
  componentsForPullRequirement,
  missingStorageSnapshotComponents,
  planStorageGc,
  producedStorageSnapshotComponents,
  requiredStorageSnapshotComponents,
  selectStorageSnapshotCatalogEntryForPull,
} from "../src/index.js";
import type {
  StorageBindingGroup,
  StoragePartitionBinding,
  StoragePullRequirement,
  StorageSnapshotCatalogEntry,
  StorageSnapshotComponent,
} from "../src/index.js";

describe("storage snapshot policy", () => {
  const catalogAndDataBinding: StoragePartitionBinding = {
    partitionId: "tenant:jp",
    rootRelationOid: 16_384,
    snapshotScope: "catalog-and-data",
    relations: [
      {
        databaseOid: 5,
        tablespaceOid: 1_668,
        relationOid: 16_384,
      },
    ],
  };

  it("maps pull requirements to explicit snapshot components", () => {
    expect(componentsForPullRequirement("bootstrap")).toEqual(["catalog", "data"]);
    expect(componentsForPullRequirement("catalog-refresh")).toEqual(["catalog"]);
    expect(componentsForPullRequirement("data-refresh")).toEqual(["data"]);
  });

  it("separates produced components from default pull requirements", () => {
    const dataOnlyPull: StoragePartitionBinding = {
      ...catalogAndDataBinding,
      snapshotComponents: ["catalog", "data"],
      pullRequires: ["data"],
    };

    expect(producedStorageSnapshotComponents(dataOnlyPull)).toEqual(["catalog", "data"]);
    expect(requiredStorageSnapshotComponents(dataOnlyPull)).toEqual(["data"]);
    expect(canSatisfyStoragePull(dataOnlyPull)).toBe(true);
    expect(canSatisfyStoragePull(dataOnlyPull, "bootstrap")).toBe(true);
  });

  it("reports missing components for data-only producers", () => {
    const dataOnlyProducer: StoragePartitionBinding = {
      ...catalogAndDataBinding,
      snapshotScope: "data-only",
      snapshotComponents: ["data"],
    };

    expect(canSatisfyStoragePull(dataOnlyProducer, "data-refresh")).toBe(true);
    expect(canSatisfyStoragePull(dataOnlyProducer, "bootstrap")).toBe(false);
    expect(
      missingStorageSnapshotComponents(
        producedStorageSnapshotComponents(dataOnlyProducer),
        requiredStorageSnapshotComponents(dataOnlyProducer, "bootstrap"),
      ),
    ).toEqual(["catalog"]);
  });

  it("selects the newest snapshot catalog entry that satisfies pull requirements", () => {
    const entries: StorageSnapshotCatalogEntry[] = [
      snapshotEntry("old-data", ["data"], 2),
      snapshotEntry("new-data", ["data"], 4),
      snapshotEntry("catalog", ["catalog"], 5),
      snapshotEntry("bootstrap", ["catalog", "data"], 3),
    ];

    expect(canSatisfyStorageSnapshotCatalogPull(entries[0]!, "data-refresh")).toBe(true);
    expect(canSatisfyStorageSnapshotCatalogPull(entries[0]!, "bootstrap")).toBe(false);
    expect(selectStorageSnapshotCatalogEntryForPull(entries, "data-refresh")?.snapshotId).toBe(
      "new-data",
    );
    expect(selectStorageSnapshotCatalogEntryForPull(entries, "catalog-refresh")?.snapshotId).toBe(
      "catalog",
    );
    expect(selectStorageSnapshotCatalogEntryForPull(entries, "bootstrap")?.snapshotId).toBe(
      "bootstrap",
    );
  });

  it("prefers the newest storage generation for pulls that require data", () => {
    const entries: StorageSnapshotCatalogEntry[] = [
      {
        ...snapshotEntry("z-old-storage", ["data"], 5),
        storageGeneration: 1,
      },
      {
        ...snapshotEntry("a-new-storage", ["data"], 5),
        storageGeneration: 2,
      },
    ];

    expect(selectStorageSnapshotCatalogEntryForPull(entries, "data-refresh")?.snapshotId).toBe(
      "a-new-storage",
    );
  });

  it("prefers DDL generation over storage generation when selecting pull snapshots", () => {
    const entries: StorageSnapshotCatalogEntry[] = [
      {
        ...snapshotEntry("older-ddl-fresh-storage", ["catalog"], 5),
        storageGeneration: 8,
      },
      {
        ...snapshotEntry("newer-ddl-stale-storage", ["catalog"], 6),
        storageGeneration: 6,
      },
    ];

    expect(selectStorageSnapshotCatalogEntryForPull(entries, "catalog-refresh")?.snapshotId).toBe(
      "newer-ddl-stale-storage",
    );
  });

  it("selects the max generation/snapshot id tuple among arbitrary satisfying entries", () => {
    fc.assert(
      fc.property(
        fc.array(snapshotEntryArb, { minLength: 0, maxLength: 30 }),
        fc.constantFrom<StoragePullRequirement>("bootstrap", "catalog-refresh", "data-refresh"),
        (entries, requirement) => {
          const selected = selectStorageSnapshotCatalogEntryForPull(entries, requirement);
          const expected = expectedPullSelection(entries, requirement);

          expect(selected?.snapshotId).toBe(expected?.snapshotId);
        },
      ),
      { numRuns: 300 },
    );
  });
});

describe("storage GC policy", () => {
  const group: StorageBindingGroup = {
    groupId: "root:16384",
    rootRelationOid: 16_384,
    ddlGeneration: 10,
    vacuumPolicy: {
      retainSnapshotGenerations: 3,
      gcAfterGeneration: 9,
    },
    bindings: [],
  };

  it("protects retained DDL generations even when policy allows newer GC", () => {
    const plan = planStorageGc(group);

    expect(plan.retainFromGeneration).toBe(8);
    expect(plan.protectedGenerations).toEqual([8, 9, 10]);
    expect(plan.gcAllowedThroughGeneration).toBe(7);
    expect(plan.canGcGeneration(7)).toBe(true);
    expect(plan.canGcGeneration(8)).toBe(false);
    expect(plan.storageGeneration).toBe(10);
    expect(plan.storageRetainFromGeneration).toBe(8);
    expect(plan.protectedStorageGenerations).toEqual([8, 9, 10]);
    expect(plan.storageGcAllowedThroughGeneration).toBe(7);
    expect(plan.canGcStorageGeneration(7)).toBe(true);
    expect(plan.canGcStorageGeneration(8)).toBe(false);
  });

  it("does not advance GC past active pull generations", () => {
    const plan = planStorageGc(group, {
      activePullGenerations: [6],
    });

    expect(plan.protectedGenerations).toEqual([6, 8, 9, 10]);
    expect(plan.gcAllowedThroughGeneration).toBe(5);
  });

  it("keeps blocks referenced by retained snapshot manifests", () => {
    const plan = planStorageGc(group, {
      retainedSnapshotGenerations: [4, 9],
    });

    expect(plan.protectedGenerations).toEqual([4, 8, 9, 10]);
    expect(plan.gcAllowedThroughGeneration).toBe(3);
  });

  it("uses storage generations for physical snapshot GC boundaries", () => {
    const plan = planStorageGc(
      {
        ...group,
        ddlGeneration: 3,
        storageGeneration: 10,
      },
      {
        activeStorageGenerations: [5],
        retainedSnapshotStorageGenerations: [4, 9],
      },
    );

    expect(plan.protectedGenerations).toEqual([1, 2, 3]);
    expect(plan.gcAllowedThroughGeneration).toBe(0);
    expect(plan.protectedStorageGenerations).toEqual([4, 5, 8, 9, 10]);
    expect(plan.storageGcAllowedThroughGeneration).toBe(3);
    expect(plan.canGcStorageGeneration(3)).toBe(true);
    expect(plan.canGcStorageGeneration(4)).toBe(false);
  });
});

function snapshotEntry(
  snapshotId: string,
  components: StorageSnapshotCatalogEntry["components"],
  ddlGeneration: number,
): StorageSnapshotCatalogEntry {
  return {
    groupId: "sensor_readings",
    partitionId: "tenant:jp",
    snapshotId,
    components,
    highWatermark: [],
    ddlGeneration,
    manifestKind: "cas",
    manifest: {},
  };
}

const snapshotComponentsArb = fc
  .uniqueArray(fc.constantFrom<StorageSnapshotComponent>("catalog", "data"), {
    minLength: 0,
    maxLength: 2,
  })
  .map((components) => components.toSorted());

const snapshotEntryArb: fc.Arbitrary<StorageSnapshotCatalogEntry> = fc.record({
  groupId: fc.constant("sensor_readings"),
  partitionId: fc.constant("tenant:jp"),
  snapshotId: fc
    .tuple(fc.integer({ min: 0, max: 999 }), fc.string({ minLength: 1, maxLength: 8 }))
    .map(([index, suffix]) => `snapshot-${index.toString().padStart(3, "0")}-${suffix}`),
  components: snapshotComponentsArb,
  highWatermark: fc.constant([]),
  ddlGeneration: fc.integer({ min: 0, max: 10 }),
  storageGeneration: fc.option(fc.integer({ min: 0, max: 10 }), { nil: undefined }),
  manifestKind: fc.constant("cas"),
  manifest: fc.constant({}),
});

function expectedPullSelection(
  entries: readonly StorageSnapshotCatalogEntry[],
  requirement: StoragePullRequirement,
): StorageSnapshotCatalogEntry | undefined {
  return entries
    .filter((entry) => satisfiesRequirement(entry.components, requirement))
    .reduce<StorageSnapshotCatalogEntry | undefined>(
      (best, entry) => (best && compareSnapshotEntries(best, entry) >= 0 ? best : entry),
      undefined,
    );
}

function satisfiesRequirement(
  components: readonly StorageSnapshotComponent[],
  requirement: StoragePullRequirement,
): boolean {
  const available = new Set(components);
  switch (requirement) {
    case "bootstrap":
      return available.has("catalog") && available.has("data");
    case "catalog-refresh":
      return available.has("catalog");
    case "data-refresh":
      return available.has("data");
  }
}

function compareSnapshotEntries(
  left: StorageSnapshotCatalogEntry,
  right: StorageSnapshotCatalogEntry,
): number {
  return (
    left.ddlGeneration - right.ddlGeneration ||
    (left.storageGeneration ?? left.ddlGeneration) -
      (right.storageGeneration ?? right.ddlGeneration) ||
    left.snapshotId.localeCompare(right.snapshotId)
  );
}
