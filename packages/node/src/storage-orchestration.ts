import type {
  PartitionId,
  PartitionStore,
  StorageCatalogManifest,
  StorageSnapshotComponent,
  StoragePartitionBinding,
  StoragePullRequirement,
  StorageSnapshotCatalogEntry,
} from "@distrb-pglite/core";
import { selectStorageSnapshotCatalogEntryForPull, UuidV7Factory } from "@distrb-pglite/core";
import {
  createPartitionCasSnapshot,
  restorePartitionCasSnapshot,
  restorePartitionDataOnlyCasSnapshot,
  type PartitionCasSnapshot,
} from "./partition-snapshot.js";
import type { PGliteClient } from "./pglite-store.js";
import {
  applyStorageCatalogManifest,
  detectStorageBindingDrift,
  loadStoragePartitionBinding,
  prepareStorageSnapshotFence,
  refreshStorageBindingGeneration,
  saveStorageSnapshotCatalogEntry,
  type StorageBindingDrift,
  type StorageSnapshotFenceTarget,
} from "./storage-binding.js";
import { transferMissingBlocks, type CasChunking, type CasManifest } from "./cas.js";

export type ManagedPartitionCasSnapshot = {
  drift: StorageBindingDrift[];
  fence: StorageSnapshotFenceTarget[];
  snapshot: PartitionCasSnapshot;
  entry: StorageSnapshotCatalogEntry<CasManifest>;
};

export type PulledPartitionCasSnapshot = {
  entry: StorageSnapshotCatalogEntry<CasManifest>;
  transferredBlocks: number;
};

export async function createManagedPartitionCasSnapshot(options: {
  db: PGliteClient;
  store: PartitionStore;
  groupId: string;
  partitionId: PartitionId;
  dataDir: string;
  casDir: string;
  chunkSize?: number;
  chunking?: CasChunking;
  compress?: boolean;
  ids?: UuidV7Factory;
  saveCatalogEntry?: boolean;
  beforeStorageSnapshot?: () => Promise<void>;
}): Promise<ManagedPartitionCasSnapshot> {
  const drift = await detectStorageBindingDrift({
    db: options.db,
    groupId: options.groupId,
    partitionId: options.partitionId,
  });
  await refreshStorageBindingGeneration({
    db: options.db,
    groupId: options.groupId,
    partitionId: options.partitionId,
  });
  const fence = await prepareStorageSnapshotFence({
    db: options.db,
    groupId: options.groupId,
    partitionId: options.partitionId,
  });
  const storageBinding = await loadStoragePartitionBinding({
    db: options.db,
    groupId: options.groupId,
    partitionId: options.partitionId,
  });
  if (!storageBinding) {
    throw new Error(`storage partition binding not found: ${options.groupId}/${options.partitionId}`);
  }

  const snapshot = await createPartitionCasSnapshot({
    store: options.store,
    partitionId: options.partitionId,
    storageBinding,
    dataDir: options.dataDir,
    casDir: options.casDir,
    chunkSize: options.chunkSize,
    chunking: options.chunking,
    compress: options.compress,
    ids: options.ids,
    beforeStorageSnapshot: options.beforeStorageSnapshot,
  });
  const entry = partitionCasSnapshotCatalogEntry({
    groupId: options.groupId,
    snapshot,
  });
  if (options.saveCatalogEntry ?? true) {
    await saveStorageSnapshotCatalogEntry(options.db, entry);
  }

  return { drift, fence, snapshot, entry };
}

export async function pullPartitionCasSnapshot(options: {
  db?: PGliteClient;
  catalogManifest?: StorageCatalogManifest;
  snapshotEntries: readonly StorageSnapshotCatalogEntry<CasManifest>[];
  groupId: string;
  partitionId: PartitionId;
  requirement: StoragePullRequirement;
  sourceCasDir?: string;
  targetCasDir: string;
  targetDir: string;
  storageBinding?: StoragePartitionBinding;
}): Promise<PulledPartitionCasSnapshot> {
  const appliedCatalog = options.db && options.catalogManifest
    ? await applyStorageCatalogManifest({ db: options.db, manifest: options.catalogManifest })
    : undefined;
  const storageBinding = options.storageBinding ?? appliedCatalog?.bindings.find((binding) =>
    binding.partitionId === options.partitionId
  );
  const candidateEntries = options.snapshotEntries.filter((candidate) =>
      candidate.groupId === options.groupId && candidate.partitionId === options.partitionId
  );
  const compatibleEntries = storageBinding
    ? candidateEntries.filter((candidate) => isPullStorageCompatible(candidate, storageBinding))
    : candidateEntries;
  const entry = selectStorageSnapshotCatalogEntryForPull(
    compatibleEntries,
    options.requirement,
  );
  if (!entry) {
    const incompatibleEntry = storageBinding
      ? selectStorageSnapshotCatalogEntryForPull(candidateEntries, options.requirement)
      : undefined;
    if (incompatibleEntry) {
      validatePullStorageCompatibility(incompatibleEntry, storageBinding!);
    }
    throw new Error(`no snapshot entry satisfies ${options.requirement}: ${options.groupId}/${options.partitionId}`);
  }
  if (entry.manifestKind !== "cas") {
    throw new Error(`only cas snapshot entries can be pulled by this helper: ${entry.manifestKind}`);
  }

  const snapshot = catalogEntryToPartitionCasSnapshot(
    entry,
    storageBinding,
  );
  validatePullStorageCompatibility(entry, snapshot.storageBinding);
  const transferredBlocks = options.sourceCasDir
    ? (await transferMissingBlocks({
        manifest: entry.manifest,
        sourceCasDir: options.sourceCasDir,
        targetCasDir: options.targetCasDir,
      })).length
    : 0;
  if (snapshot.storageBinding.snapshotScope === "catalog-and-data") {
    await restorePartitionCasSnapshot({
      snapshot,
      casDir: options.targetCasDir,
      targetDir: options.targetDir,
    });
  } else {
    await restorePartitionDataOnlyCasSnapshot({
      snapshot,
      casDir: options.targetCasDir,
      targetDir: options.targetDir,
      targetStorageBinding: snapshot.storageBinding,
    });
  }

  return { entry, transferredBlocks };
}

function validatePullStorageCompatibility(
  entry: StorageSnapshotCatalogEntry<CasManifest>,
  storageBinding: StoragePartitionBinding,
): void {
  if (storageBinding.partitionId !== entry.partitionId) {
    throw new Error(
      `storage binding partition mismatch for ${entry.groupId}/${entry.partitionId}: snapshot=${entry.partitionId} target=${storageBinding.partitionId}`,
    );
  }

  const bindingGeneration = storageBinding.ddlGeneration ?? 0;
  if (bindingGeneration !== entry.ddlGeneration) {
    throw new Error(
      `storage binding DDL generation mismatch for ${entry.groupId}/${entry.partitionId}: snapshot=${entry.ddlGeneration} target=${bindingGeneration}`,
    );
  }
  const bindingStorageGeneration = storageBinding.storageGeneration ?? bindingGeneration;
  const entryStorageGeneration = entry.storageGeneration ?? entry.ddlGeneration;
  if (bindingStorageGeneration !== entryStorageGeneration) {
    throw new Error(
      `storage binding generation mismatch for ${entry.groupId}/${entry.partitionId}: snapshot=${entryStorageGeneration} target=${bindingStorageGeneration}`,
    );
  }

  if (storageBinding.snapshotScope === "data-only" && entry.components.includes("catalog")) {
    throw new Error(
      `data-only storage binding cannot apply catalog snapshot components: ${entry.groupId}/${entry.partitionId}`,
    );
  }
}

function isPullStorageCompatible(
  entry: StorageSnapshotCatalogEntry<CasManifest>,
  storageBinding: StoragePartitionBinding,
): boolean {
  if (storageBinding.partitionId !== entry.partitionId) {
    return false;
  }

  const bindingGeneration = storageBinding.ddlGeneration ?? 0;
  if (bindingGeneration !== entry.ddlGeneration) {
    return false;
  }
  const bindingStorageGeneration = storageBinding.storageGeneration ?? bindingGeneration;
  const entryStorageGeneration = entry.storageGeneration ?? entry.ddlGeneration;
  if (bindingStorageGeneration !== entryStorageGeneration) {
    return false;
  }

  return !(storageBinding.snapshotScope === "data-only" && entry.components.includes("catalog"));
}

function catalogEntryToPartitionCasSnapshot(
  entry: StorageSnapshotCatalogEntry<CasManifest>,
  storageBinding?: StoragePartitionBinding,
): PartitionCasSnapshot {
  if (!storageBinding) {
    throw new Error(`snapshot manifest is missing storage binding: ${entry.snapshotId}`);
  }

  return {
    version: 1,
    snapshotId: entry.snapshotId,
    partitionId: entry.partitionId,
    storageBinding,
    highWatermark: entry.highWatermark,
    manifest: entry.manifest,
  };
}

function partitionCasSnapshotCatalogEntry(options: {
  groupId: string;
  snapshot: PartitionCasSnapshot;
  components?: StorageSnapshotComponent[];
}): StorageSnapshotCatalogEntry<CasManifest> {
  return {
    groupId: options.groupId,
    partitionId: options.snapshot.partitionId,
    snapshotId: options.snapshot.snapshotId,
    components: options.components ?? defaultComponents(options.snapshot.storageBinding.snapshotScope),
    highWatermark: options.snapshot.highWatermark,
    ddlGeneration: options.snapshot.storageBinding.ddlGeneration ?? 0,
    storageGeneration: options.snapshot.storageBinding.storageGeneration,
    manifestKind: "cas",
    manifest: options.snapshot.manifest,
  };
}

function defaultComponents(scope: StoragePartitionBinding["snapshotScope"]): StorageSnapshotComponent[] {
  return scope === "catalog-and-data" ? ["catalog", "data"] : ["data"];
}
