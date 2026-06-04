import type {
  EventEnvelope,
  EventId,
  NodeId,
  PartitionId,
  PartitionStore,
  SnapshotId,
  SnapshotFilesystem,
  SnapshotFilesystemManifest,
  StoragePartitionBinding,
  StorageRelationLocator,
} from "@distrb-pglite/core";
import { assertValidEventEnvelope, isUuidV7, UuidV7Factory } from "@distrb-pglite/core";
import {
  restoreFromCas,
  snapshotToCas,
  type CasChunking,
  type CasManifest,
  type CasSnapshotFilter,
} from "./cas.js";
import type { FileInventoryEntry } from "./inventory.js";

export type PartitionCasSnapshot = {
  version: 1;
  snapshotId: SnapshotId;
  partitionId: PartitionId;
  storageBinding: StoragePartitionBinding;
  highWatermark: [NodeId, EventId][];
  manifest: CasManifest;
};

export type PartitionSnapshotCursor = {
  partitionId: PartitionId;
  highWatermark: [NodeId, EventId][];
};

export type PartitionFilesystemSnapshot = PartitionSnapshotCursor & {
  version: 1;
  snapshotId: SnapshotId;
  storageBinding: StoragePartitionBinding;
  manifest: SnapshotFilesystemManifest;
};

export async function createPartitionCasSnapshot(options: {
  store: PartitionStore;
  partitionId: PartitionId;
  storageBinding: StoragePartitionBinding;
  dataDir: string;
  casDir: string;
  chunkSize?: number;
  chunking?: CasChunking;
  compress?: boolean;
  ids?: UuidV7Factory;
  beforeStorageSnapshot?: () => Promise<void>;
}): Promise<PartitionCasSnapshot> {
  validateStorageBinding(options.partitionId, options.storageBinding);

  const ids = options.ids ?? new UuidV7Factory();
  const snapshotId = `partition-snapshot:${options.partitionId}:${ids.next()}`;
  const highWatermark = await options.store.getHighWatermark(options.partitionId);
  await options.beforeStorageSnapshot?.();
  const manifest = await snapshotToCas({
    dataDir: options.dataDir,
    casDir: options.casDir,
    chunkSize: options.chunkSize,
    chunking: options.chunking,
    compress: options.compress,
    snapshotId,
    filter: storageBindingSnapshotFilter(options.storageBinding),
  });

  return {
    version: 1,
    snapshotId,
    partitionId: options.partitionId,
    storageBinding: options.storageBinding,
    highWatermark: sortedHighWatermark(highWatermark),
    manifest,
  };
}

export async function createPartitionFilesystemSnapshot(options: {
  store: PartitionStore;
  partitionId: PartitionId;
  storageBinding: StoragePartitionBinding;
  fs: Pick<SnapshotFilesystem, "snapshot">;
  ids?: UuidV7Factory;
  beforeStorageSnapshot?: () => Promise<void>;
}): Promise<PartitionFilesystemSnapshot> {
  validateStorageBinding(options.partitionId, options.storageBinding);
  if (options.storageBinding.snapshotScope !== "catalog-and-data") {
    throw new Error("filesystem snapshots currently require catalog-and-data scope");
  }

  const ids = options.ids ?? new UuidV7Factory();
  const snapshotId = `partition-fs-snapshot:${options.partitionId}:${ids.next()}`;
  const highWatermark = await options.store.getHighWatermark(options.partitionId);
  await options.beforeStorageSnapshot?.();
  const manifest = await options.fs.snapshot();

  return {
    version: 1,
    snapshotId,
    partitionId: options.partitionId,
    storageBinding: options.storageBinding,
    highWatermark: sortedHighWatermark(highWatermark),
    manifest,
  };
}

export function highWatermarkMap(snapshot: PartitionSnapshotCursor): ReadonlyMap<NodeId, EventId> {
  return new Map(validateHighWatermarkEntries(snapshot.highWatermark));
}

export function validateHighWatermarkEntries(
  highWatermark: readonly unknown[],
): [NodeId, EventId][] {
  const seen = new Set<NodeId>();
  return highWatermark.map((entry, index) => {
    if (!Array.isArray(entry) || entry.length !== 2) {
      throw new Error(`invalid high-watermark entry at index ${index}`);
    }

    const [nodeId, eventId] = entry;
    if (typeof nodeId !== "string" || nodeId.length === 0) {
      throw new Error(`invalid high-watermark source node at index ${index}`);
    }
    if (typeof eventId !== "string" || !isUuidV7(eventId)) {
      throw new Error(`invalid high-watermark event id at index ${index}`);
    }
    if (seen.has(nodeId)) {
      throw new Error(`duplicate high-watermark source node: ${nodeId}`);
    }

    seen.add(nodeId);
    return [nodeId, eventId];
  });
}

export async function listEventsAfterPartitionSnapshot(
  store: PartitionStore,
  snapshot: PartitionSnapshotCursor,
): Promise<EventEnvelope[]> {
  return store.listEventsAfterCursors(snapshot.partitionId, highWatermarkMap(snapshot));
}

export async function restorePartitionFilesystemSnapshot(options: {
  snapshot: PartitionFilesystemSnapshot;
  fs: Pick<SnapshotFilesystem, "restore">;
}): Promise<void> {
  if (options.snapshot.storageBinding.snapshotScope !== "catalog-and-data") {
    throw new Error(
      "only catalog-and-data partition filesystem snapshots can restore a full filesystem",
    );
  }

  await options.fs.restore(options.snapshot.manifest);
}

export async function restorePartitionCasSnapshot(options: {
  snapshot: PartitionCasSnapshot;
  casDir: string;
  targetDir: string;
}): Promise<void> {
  if (options.snapshot.storageBinding.snapshotScope !== "catalog-and-data") {
    throw new Error("only catalog-and-data partition snapshots can restore a full data directory");
  }

  await restoreFromCas({
    manifest: options.snapshot.manifest,
    casDir: options.casDir,
    targetDir: options.targetDir,
  });
}

export async function restorePartitionDataOnlyCasSnapshot(options: {
  snapshot: PartitionCasSnapshot;
  casDir: string;
  targetDir: string;
  targetStorageBinding?: StoragePartitionBinding;
}): Promise<void> {
  if (options.snapshot.storageBinding.snapshotScope !== "data-only") {
    throw new Error(
      "only data-only partition snapshots can restore relation files into an existing data directory",
    );
  }
  if (
    options.targetStorageBinding &&
    options.targetStorageBinding.partitionId !== options.snapshot.partitionId
  ) {
    throw new Error(
      `target storage binding partition mismatch: ${options.targetStorageBinding.partitionId} !== ${options.snapshot.partitionId}`,
    );
  }
  if (options.targetStorageBinding) {
    validateDataOnlyTargetBinding(options.snapshot.storageBinding, options.targetStorageBinding);
  }

  await restoreFromCas({
    manifest: options.targetStorageBinding
      ? remapDataOnlyManifest(
          options.snapshot.manifest,
          options.snapshot.storageBinding,
          options.targetStorageBinding,
        )
      : options.snapshot.manifest,
    casDir: options.casDir,
    targetDir: options.targetDir,
  });
}

export async function replayEventsAfterPartitionSnapshot(options: {
  sourceStore: PartitionStore;
  targetStore: PartitionStore;
  snapshot: PartitionSnapshotCursor;
}): Promise<number> {
  const events = await listEventsAfterPartitionSnapshot(options.sourceStore, options.snapshot);
  validatePartitionSnapshotReplayEvents(events, options.snapshot.partitionId);

  await options.targetStore.registerPartition(options.snapshot.partitionId);
  let applied = 0;
  for (const event of events) {
    if (await options.targetStore.applyRemote(event)) {
      applied += 1;
    }
  }

  return applied;
}

function validatePartitionSnapshotReplayEvents(
  events: readonly EventEnvelope[],
  partitionId: PartitionId,
): void {
  const seen = new Map<string, EventEnvelope>();
  for (const event of events) {
    assertValidEventEnvelope(event);
    if (event.partitionId !== partitionId) {
      throw new Error(
        `partition snapshot replay event partition mismatch: snapshot=${partitionId} event=${event.partitionId}`,
      );
    }

    const key = `${event.sourceNodeId}/${event.eventId}`;
    const existing = seen.get(key);
    if (existing) {
      assertSameReplayEvent(existing, event);
      continue;
    }
    seen.set(key, event);
  }
}

function assertSameReplayEvent(left: EventEnvelope, right: EventEnvelope): void {
  if (
    left.partitionId !== right.partitionId ||
    left.schemaVersion !== right.schemaVersion ||
    canonicalJson(left.payload) !== canonicalJson(right.payload)
  ) {
    throw new Error(
      `partition snapshot replay event id conflict: ${right.sourceNodeId}/${right.eventId}`,
    );
  }
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(canonicalize(value));
}

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, child]) => [key, canonicalize(child)]),
    );
  }
  return value;
}

export function storageBindingSnapshotFilter(
  binding: StoragePartitionBinding,
): CasSnapshotFilter | undefined {
  const relationFiles = new Set(
    binding.relations.flatMap((relation) => relationFileStems(relation)),
  );

  const filter: CasSnapshotFilter = {
    includeFile(file: FileInventoryEntry): boolean {
      if (binding.snapshotScope === "catalog-and-data") {
        return true;
      }
      return isDataComponentFile(file.path, relationFiles);
    },
    componentsForFile(file: FileInventoryEntry) {
      return [isDataComponentFile(file.path, relationFiles) ? "data" : "catalog"];
    },
  };
  if (binding.snapshotScope === "catalog-and-data") {
    filter.includeDirectory = () => true;
  }
  return filter;
}

function validateStorageBinding(
  partitionId: PartitionId,
  storageBinding: StoragePartitionBinding,
): void {
  if (storageBinding.partitionId !== partitionId) {
    throw new Error(
      `storage binding partition mismatch: ${storageBinding.partitionId} !== ${partitionId}`,
    );
  }
}

function validateDataOnlyTargetBinding(
  sourceBinding: StoragePartitionBinding,
  targetBinding: StoragePartitionBinding,
): void {
  if (targetBinding.snapshotScope !== "data-only") {
    throw new Error("target storage binding must use data-only snapshot scope");
  }
  if (
    sourceBinding.ddlGeneration !== undefined &&
    targetBinding.ddlGeneration !== undefined &&
    sourceBinding.ddlGeneration !== targetBinding.ddlGeneration
  ) {
    throw new Error(
      `target storage binding DDL generation mismatch: ${targetBinding.ddlGeneration} !== ${sourceBinding.ddlGeneration}`,
    );
  }
  if (sourceBinding.relations.length !== targetBinding.relations.length) {
    throw new Error(
      `target storage binding relation count mismatch: ${targetBinding.relations.length} !== ${sourceBinding.relations.length}`,
    );
  }

  for (const [index, sourceRelation] of sourceBinding.relations.entries()) {
    const targetRelation = targetBinding.relations[index];
    if (!targetRelation) {
      throw new Error(`target storage binding is missing relation at index ${index}`);
    }
    if (
      sourceRelation.role !== targetRelation.role ||
      sourceRelation.parentRelationOid !== targetRelation.parentRelationOid
    ) {
      throw new Error(`target storage binding relation metadata mismatch at index ${index}`);
    }
  }
}

function sortedHighWatermark(highWatermark: ReadonlyMap<NodeId, EventId>): [NodeId, EventId][] {
  return [...highWatermark].sort(([left], [right]) => left.localeCompare(right));
}

function relationFileStems(relation: StorageRelationLocator): string[] {
  const relfilenode = relation.relfilenode ?? relation.relationOid;
  return [
    relation.storagePath,
    `base/${relation.databaseOid}/${relfilenode}`,
    `pg_tblspc/${relation.tablespaceOid}/${relation.databaseOid}/${relfilenode}`,
  ].filter((path): path is string => typeof path === "string" && path.length > 0);
}

function relationFileStem(path: string): string {
  return path.replace(/_(fsm|vm|init)(\.\d+)?$/, "").replace(/\.\d+$/, "");
}

function remapDataOnlyManifest(
  manifest: CasManifest,
  sourceBinding: StoragePartitionBinding,
  targetBinding: StoragePartitionBinding,
): CasManifest {
  const sourceDataCatalogFileStems = dataCatalogRelationFileStems(sourceBinding);
  return {
    ...manifest,
    directories: [],
    files: manifest.files.map((file) => ({
      ...file,
      path:
        isDataComponentMetadataFile(file.path) ||
        sourceDataCatalogFileStems.has(relationFileStem(file.path))
          ? file.path
          : remapRelationFilePath(file.path, sourceBinding, targetBinding),
    })),
  };
}

function dataCatalogRelationFileStems(binding: StoragePartitionBinding): ReadonlySet<string> {
  const dataCatalogOids = new Set(
    binding.relations
      .filter((relation) => relation.role === "data-catalog")
      .map((relation) => relation.relationOid),
  );
  return new Set(
    binding.relations
      .filter(
        (relation) =>
          relation.role === "data-catalog" ||
          (relation.parentRelationOid !== undefined &&
            dataCatalogOids.has(relation.parentRelationOid)),
      )
      .flatMap((relation) => relationFileStems(relation)),
  );
}

function remapRelationFilePath(
  path: string,
  sourceBinding: StoragePartitionBinding,
  targetBinding: StoragePartitionBinding,
): string {
  const stem = relationFileStem(path);
  for (const [index, sourceRelation] of sourceBinding.relations.entries()) {
    const sourceStem = relationFileStems(sourceRelation).find((candidate) => candidate === stem);
    if (!sourceStem) {
      continue;
    }

    const targetRelation = targetBinding.relations[index];
    if (!targetRelation) {
      throw new Error(`target storage binding is missing relation at index ${index}`);
    }

    const targetStem = relationFileStems(targetRelation)[0];
    if (!targetStem) {
      throw new Error(
        `target storage binding is missing a storage path for relation at index ${index}`,
      );
    }
    return `${targetStem}${path.slice(sourceStem.length)}`;
  }

  throw new Error(`data-only snapshot file does not match source storage binding: ${path}`);
}

function isDataComponentMetadataFile(path: string): boolean {
  return (
    path.startsWith("pg_xact/") ||
    path.startsWith("pg_subtrans/") ||
    path.startsWith("pg_multixact/") ||
    path.startsWith("pg_commit_ts/") ||
    path.startsWith("pg_serial/") ||
    path.startsWith("pg_twophase/") ||
    path === "global/pg_filenode.map" ||
    path.endsWith("/pg_filenode.map") ||
    path === "global/pg_internal.init" ||
    path.endsWith("/pg_internal.init")
  );
}

function isDataComponentFile(path: string, relationFiles: ReadonlySet<string>): boolean {
  return relationFiles.has(relationFileStem(path)) || isDataComponentMetadataFile(path);
}
