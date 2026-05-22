import { describe, expect, it } from "vitest";
import {
  canSatisfyStoragePull,
  componentsForPullRequirement,
  missingStorageSnapshotComponents,
  planStorageGc,
  producedStorageSnapshotComponents,
  requiredStorageSnapshotComponents,
} from "../src/index.js";
import type { StorageBindingGroup, StoragePartitionBinding } from "../src/index.js";

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
});
