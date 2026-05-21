import type {
  PartitionId,
  StoragePartitionBinding,
  StorageSnapshotScope,
} from "@distrb-pglite/core";
import type { PGliteClient } from "./pglite-store.js";

type RelationIdentityRow = {
  database_oid: number;
  tablespace_oid: number;
  relation_oid: number;
  relfilenode: number | null;
  storage_path: string | null;
};

export async function discoverStoragePartitionBinding(options: {
  db: PGliteClient;
  partitionId: PartitionId;
  hypertable: string;
  relations?: string[];
  snapshotScope: StorageSnapshotScope;
}): Promise<StoragePartitionBinding> {
  const relationNames = options.relations?.length
    ? options.relations
    : [options.hypertable];
  const relations = [];

  for (const relationName of relationNames) {
    const result = await options.db.query<RelationIdentityRow>(
      `
        SELECT
          d.oid::int AS database_oid,
          COALESCE(NULLIF(c.reltablespace, 0), d.dattablespace)::int AS tablespace_oid,
          c.oid::int AS relation_oid,
          NULLIF(pg_relation_filenode(c.oid), 0)::int AS relfilenode,
          pg_relation_filepath(c.oid) AS storage_path
        FROM pg_class c
        JOIN pg_database d ON d.datname = current_database()
        WHERE c.oid = to_regclass($1)
      `,
      [relationName],
    );
    const row = result.rows[0];
    if (!row) {
      throw new Error(`relation not found for storage binding: ${relationName}`);
    }

    relations.push({
      databaseOid: row.database_oid,
      tablespaceOid: row.tablespace_oid,
      relationOid: row.relation_oid,
      relfilenode: row.relfilenode ?? row.relation_oid,
      storagePath: row.storage_path ?? undefined,
    });
  }

  return {
    partitionId: options.partitionId,
    hypertableOid: relations[0]?.relationOid ?? 0,
    relations,
    snapshotScope: options.snapshotScope,
  };
}
