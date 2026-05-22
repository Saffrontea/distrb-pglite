import type {
  StorageBindingGroup,
  StorageGcPlan,
  StoragePartitionBinding,
  StoragePullRequirement,
  StorageSnapshotComponent,
  StorageSnapshotScope,
} from "./types.js";

const COMPONENT_ORDER: StorageSnapshotComponent[] = ["catalog", "data"];

export type StorageGcPlanOptions = {
  activePullGenerations?: readonly number[];
  retainedSnapshotGenerations?: readonly number[];
};

export function componentsForSnapshotScope(
  scope: StorageSnapshotScope,
): StorageSnapshotComponent[] {
  return scope === "catalog-and-data" ? ["catalog", "data"] : ["data"];
}

export function componentsForPullRequirement(
  requirement: StoragePullRequirement,
): StorageSnapshotComponent[] {
  switch (requirement) {
    case "bootstrap":
      return ["catalog", "data"];
    case "catalog-refresh":
      return ["catalog"];
    case "data-refresh":
      return ["data"];
  }
}

export function producedStorageSnapshotComponents(
  binding: StoragePartitionBinding,
): StorageSnapshotComponent[] {
  return normalizeStorageSnapshotComponents(
    binding.snapshotComponents ?? componentsForSnapshotScope(binding.snapshotScope),
  );
}

export function requiredStorageSnapshotComponents(
  binding: StoragePartitionBinding,
  requirement?: StoragePullRequirement,
): StorageSnapshotComponent[] {
  return normalizeStorageSnapshotComponents(
    requirement
      ? componentsForPullRequirement(requirement)
      : binding.pullRequires ?? componentsForSnapshotScope(binding.snapshotScope),
  );
}

export function missingStorageSnapshotComponents(
  available: readonly StorageSnapshotComponent[],
  required: readonly StorageSnapshotComponent[],
): StorageSnapshotComponent[] {
  const availableSet = new Set(available);
  return normalizeStorageSnapshotComponents(required).filter(
    (component) => !availableSet.has(component),
  );
}

export function canSatisfyStoragePull(
  binding: StoragePartitionBinding,
  requirement?: StoragePullRequirement,
): boolean {
  return (
    missingStorageSnapshotComponents(
      producedStorageSnapshotComponents(binding),
      requiredStorageSnapshotComponents(binding, requirement),
    ).length === 0
  );
}

export function planStorageGc(
  group: StorageBindingGroup,
  options: StorageGcPlanOptions = {},
): StorageGcPlan {
  const retainCount = Math.max(1, group.vacuumPolicy?.retainSnapshotGenerations ?? 1);
  const policyGcAfterGeneration = group.vacuumPolicy?.gcAfterGeneration ?? 0;
  const retainFromGeneration = Math.max(0, group.ddlGeneration - retainCount + 1);
  const protectedGenerations = uniqueSortedGenerations([
    ...range(retainFromGeneration, group.ddlGeneration),
    ...(options.activePullGenerations ?? []),
    ...(options.retainedSnapshotGenerations ?? []),
  ]);
  const earliestProtectedGeneration = protectedGenerations[0] ?? Number.POSITIVE_INFINITY;
  const gcAllowedThroughGeneration = Math.max(
    0,
    Math.min(policyGcAfterGeneration, earliestProtectedGeneration - 1),
  );

  return {
    ddlGeneration: group.ddlGeneration,
    retainFromGeneration,
    protectedGenerations,
    gcAllowedThroughGeneration,
    canGcGeneration: (generation: number) => generation <= gcAllowedThroughGeneration,
  };
}

function normalizeStorageSnapshotComponents(
  components: readonly StorageSnapshotComponent[],
): StorageSnapshotComponent[] {
  const componentSet = new Set(components);
  return COMPONENT_ORDER.filter((component) => componentSet.has(component));
}

function uniqueSortedGenerations(generations: readonly number[]): number[] {
  return [...new Set(generations)]
    .filter((generation) => Number.isInteger(generation) && generation >= 0)
    .sort((left, right) => left - right);
}

function range(start: number, end: number): number[] {
  if (end < start) {
    return [];
  }

  return Array.from({ length: end - start + 1 }, (_, index) => start + index);
}
