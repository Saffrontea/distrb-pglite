import type {
  NodeId,
  PartitionId,
  StorageCatalogManifest,
  StorageSnapshotCatalogEntry,
} from "./types.js";

// ADR 0009: the physical-plane peer contract, the counterpart of Transport
// on the logical plane. Exactly two responsibilities: discover which
// snapshot catalog entries a peer advertises, and make one entry's blocks
// available in the local block store. Validity fencing (ADR 0007), entry
// selection, and restore stay on the consumer side: an implementation must
// never be smarter than "list and fetch", so a buggy or hostile source can
// at worst fail hash verification, not corrupt state.
export type SnapshotSource<TManifest = unknown> = {
  listSnapshotEntries(
    peerNodeId: NodeId,
    partitionId: PartitionId,
  ): Promise<StorageSnapshotCatalogEntry<TManifest>[]>;
  // Ensures every block referenced by the entry's manifest exists locally.
  // Returns the number of blocks actually transferred (0 when all present).
  // Content addressing makes this idempotent and resumable.
  fetchBlocks(peerNodeId: NodeId, entry: StorageSnapshotCatalogEntry<TManifest>): Promise<number>;
  // Optional: the peer's storage catalog manifest for fenced catalog apply.
  loadCatalogManifest?(
    peerNodeId: NodeId,
    groupId: string,
  ): Promise<StorageCatalogManifest | undefined>;
};

// Degenerate source for tests and for the migration path where the caller
// already holds the entries and the blocks are already local.
export class StaticSnapshotSource<TManifest = unknown> implements SnapshotSource<TManifest> {
  constructor(
    private readonly options: {
      entries: readonly StorageSnapshotCatalogEntry<TManifest>[];
      catalogManifest?: StorageCatalogManifest;
      // Defaults to a no-op (blocks assumed local).
      fetchBlocks?: (entry: StorageSnapshotCatalogEntry<TManifest>) => Promise<number>;
    },
  ) {}

  async listSnapshotEntries(
    _peerNodeId: NodeId,
    partitionId: PartitionId,
  ): Promise<StorageSnapshotCatalogEntry<TManifest>[]> {
    return this.options.entries.filter((entry) => entry.partitionId === partitionId);
  }

  async fetchBlocks(
    _peerNodeId: NodeId,
    entry: StorageSnapshotCatalogEntry<TManifest>,
  ): Promise<number> {
    return this.options.fetchBlocks ? this.options.fetchBlocks(entry) : 0;
  }

  async loadCatalogManifest(
    _peerNodeId: NodeId,
    groupId: string,
  ): Promise<StorageCatalogManifest | undefined> {
    return this.options.catalogManifest?.groupId === groupId
      ? this.options.catalogManifest
      : undefined;
  }
}
