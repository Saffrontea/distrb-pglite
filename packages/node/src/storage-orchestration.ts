import type {
  PartitionId,
  PartitionStore,
  StorageCatalogManifest,
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
  savePartitionCasSnapshotCatalogEntry,
  type StorageBindingDrift,
  type StorageSnapshotFenceTarget,
} from "./storage-binding.js";
import { transferMissingBlocks, type CasChunking, type CasManifest } from "./cas.js";

export type ManagedPartitionCasSnapshot = {
  drift: StorageBindingDrift[];
  fence: StorageSnapshotFenceTarget[];
  snapshot: PartitionCasSnapshot;
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
  if (options.saveCatalogEntry ?? true) {
    await savePartitionCasSnapshotCatalogEntry({
      db: options.db,
      groupId: options.groupId,
      snapshot,
    });
  }

  return { drift, fence, snapshot };
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
  const entry = selectStorageSnapshotCatalogEntryForPull(
    options.snapshotEntries.filter((candidate) =>
      candidate.groupId === options.groupId && candidate.partitionId === options.partitionId
    ),
    options.requirement,
  );
  if (!entry) {
    throw new Error(`no snapshot entry satisfies ${options.requirement}: ${options.groupId}/${options.partitionId}`);
  }
  if (entry.manifestKind !== "cas") {
    throw new Error(`only cas snapshot entries can be pulled by this helper: ${entry.manifestKind}`);
  }

  const transferredBlocks = options.sourceCasDir
    ? (await transferMissingBlocks({
        manifest: entry.manifest,
        sourceCasDir: options.sourceCasDir,
        targetCasDir: options.targetCasDir,
      })).length
    : 0;
  const snapshot = catalogEntryToPartitionCasSnapshot(
    entry,
    options.storageBinding ?? appliedCatalog?.bindings.find((binding) =>
      binding.partitionId === options.partitionId
    ),
  );
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
