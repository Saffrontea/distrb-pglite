import {
  planStorageGc,
  selectStorageSnapshotCatalogEntryForPull,
} from "@distrb-pglite/core";
import type {
  PartitionId,
  StorageBindingGroup,
  StorageCatalogManifest,
  StorageGcPlan,
  StoragePartitionBinding,
  StorageGcPlanOptions,
  StoragePullRequirement,
  StorageRelationLocator,
  StorageRelationRole,
  StorageSnapshotCatalogEntry,
  StorageSnapshotComponent,
  StorageSnapshotScope,
} from "@distrb-pglite/core";
import type { PartitionCasSnapshot, PartitionFilesystemSnapshot } from "./partition-snapshot.js";
import type { PGliteClient } from "./pglite-store.js";

type RelationIdentityRow = {
  database_oid: number;
  tablespace_oid: number;
  relation_oid: number;
  relfilenode: number | null;
  storage_path: string | null;
};

type BindingGroupRow = {
  group_id: string;
  root_relation_oid: number;
  ddl_generation: number;
  vacuum_policy: unknown;
};

type BindingRow = {
  partition_id: string;
  binding: unknown;
};

type BindingRelationRow = {
  group_id: string;
  partition_id: string;
  relation_ordinal: number;
  database_oid: number;
  tablespace_oid: number;
  relation_oid: number;
  relfilenode: number | null;
  storage_path: string | null;
  relation_role: string;
  parent_relation_oid: number | null;
};

type SnapshotCatalogRow = {
  group_id: string;
  partition_id: string;
  snapshot_id: string;
  components: unknown;
  high_watermark: unknown;
  ddl_generation: number;
  manifest_kind: string;
  manifest: unknown;
};

type CatalogManifestRow = {
  manifest: unknown;
};

type SnapshotFenceTargetRow = {
  partition_id: string;
  relation_ordinal: number;
  relation_oid: number;
  relation_name: string;
  relation_role: string;
};

export type StorageSnapshotFenceTarget = {
  partitionId: PartitionId;
  relationOrdinal: number;
  relationOid: number;
  relationName: string;
  relationRole: StorageRelationRole;
};

export async function migrateStorageBindingCatalog(db: PGliteClient): Promise<void> {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS distrb_storage_binding_groups (
      group_id text PRIMARY KEY,
      root_relation_oid integer NOT NULL,
      ddl_generation integer NOT NULL,
      vacuum_policy jsonb
    );

    CREATE TABLE IF NOT EXISTS distrb_storage_bindings (
      group_id text NOT NULL REFERENCES distrb_storage_binding_groups(group_id) ON DELETE CASCADE,
      partition_id text NOT NULL,
      binding jsonb NOT NULL,
      PRIMARY KEY (group_id, partition_id)
    );

    CREATE TABLE IF NOT EXISTS distrb_storage_binding_relations (
      group_id text NOT NULL,
      partition_id text NOT NULL,
      relation_ordinal integer NOT NULL,
      database_oid integer NOT NULL,
      tablespace_oid integer NOT NULL,
      relation_oid integer NOT NULL,
      relfilenode integer,
      storage_path text,
      relation_role text NOT NULL,
      parent_relation_oid integer,
      PRIMARY KEY (group_id, partition_id, relation_ordinal),
      FOREIGN KEY (group_id, partition_id)
        REFERENCES distrb_storage_bindings(group_id, partition_id)
        ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS distrb_storage_relation_lineage (
      group_id text NOT NULL,
      partition_id text NOT NULL,
      relation_oid integer NOT NULL,
      relfilenode integer,
      storage_path text,
      relation_role text NOT NULL,
      parent_relation_oid integer,
      ddl_generation integer NOT NULL,
      observed_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (group_id, partition_id, relation_oid, ddl_generation)
    );

    CREATE TABLE IF NOT EXISTS distrb_storage_snapshots (
      group_id text NOT NULL REFERENCES distrb_storage_binding_groups(group_id) ON DELETE CASCADE,
      partition_id text NOT NULL,
      snapshot_id text NOT NULL,
      components jsonb NOT NULL,
      high_watermark jsonb NOT NULL,
      ddl_generation integer NOT NULL,
      manifest_kind text NOT NULL,
      manifest jsonb NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (group_id, partition_id, snapshot_id)
    );

    CREATE OR REPLACE FUNCTION distrb_create_binding_group(
      group_name text,
      root_relation regclass,
      vacuum_policy jsonb DEFAULT NULL
    )
    RETURNS integer
    LANGUAGE plpgsql
    AS $$
    DECLARE
      root_oid integer := root_relation::oid::integer;
    BEGIN
      INSERT INTO distrb_storage_binding_groups (
        group_id,
        root_relation_oid,
        ddl_generation,
        vacuum_policy
      )
      VALUES (group_name, root_oid, 1, vacuum_policy)
      ON CONFLICT (group_id) DO UPDATE SET
        root_relation_oid = EXCLUDED.root_relation_oid,
        vacuum_policy = EXCLUDED.vacuum_policy;

      RETURN root_oid;
    END;
    $$;

    CREATE OR REPLACE FUNCTION distrb_relation_sql_name(relation_name text)
    RETURNS text
    LANGUAGE plpgsql
    AS $$
    DECLARE
      parts text[] := parse_ident(relation_name, false);
    BEGIN
      IF array_length(parts, 1) = 1 THEN
        RETURN format('%I', parts[1]);
      ELSIF array_length(parts, 1) = 2 THEN
        RETURN format('%I.%I', parts[1], parts[2]);
      END IF;

      RAISE EXCEPTION 'relation name must be unqualified or schema-qualified: %', relation_name;
    END;
    $$;

    CREATE OR REPLACE FUNCTION distrb_storage_relation_closure(
      child_relations regclass[],
      root_oid integer
    )
    RETURNS TABLE(
      relation_ordinal integer,
      database_oid integer,
      tablespace_oid integer,
      relation_oid integer,
      relfilenode integer,
      storage_path text,
      relation_role text,
      parent_relation_oid integer
    )
    LANGUAGE sql
    AS $$
      WITH input_relations AS (
        SELECT
          (u.ordinality - 1)::integer AS input_ordinal,
          u.relation_name::oid AS relation_oid
        FROM unnest(child_relations) WITH ORDINALITY AS u(relation_name, ordinality)
      ),
      child AS (
        SELECT
          input_ordinal,
          relation_oid,
          'child'::text AS relation_role,
          root_oid AS parent_relation_oid
        FROM input_relations
      ),
      index_relations AS (
        SELECT
          child.input_ordinal,
          i.indexrelid::integer AS relation_oid,
          'index'::text AS relation_role,
          child.relation_oid AS parent_relation_oid
        FROM child
        JOIN pg_index i ON i.indrelid = child.relation_oid
      ),
      toast_relations AS (
        SELECT
          child.input_ordinal,
          table_class.reltoastrelid::integer AS relation_oid,
          'toast'::text AS relation_role,
          child.relation_oid AS parent_relation_oid
        FROM child
        JOIN pg_class table_class ON table_class.oid = child.relation_oid
        WHERE table_class.reltoastrelid <> 0
      ),
      all_relations AS (
        SELECT * FROM child
        UNION ALL
        SELECT * FROM index_relations
        UNION ALL
        SELECT * FROM toast_relations
      )
      SELECT
        (row_number() OVER (
          ORDER BY
            all_relations.input_ordinal,
            CASE all_relations.relation_role
              WHEN 'child' THEN 0
              WHEN 'index' THEN 1
              WHEN 'toast' THEN 2
              ELSE 3
            END,
            all_relations.relation_oid
        ) - 1)::integer AS relation_ordinal,
        d.oid::integer AS database_oid,
        COALESCE(NULLIF(c.reltablespace, 0), d.dattablespace)::integer AS tablespace_oid,
        c.oid::integer AS relation_oid,
        COALESCE(NULLIF(pg_relation_filenode(c.oid), 0)::integer, c.oid::integer) AS relfilenode,
        pg_relation_filepath(c.oid) AS storage_path,
        all_relations.relation_role,
        all_relations.parent_relation_oid
      FROM all_relations
      JOIN pg_class c ON c.oid = all_relations.relation_oid
      JOIN pg_database d ON d.datname = current_database()
    $$;

    CREATE OR REPLACE FUNCTION distrb_bind_partition(
      group_name text,
      partition_name text,
      child_relations regclass[],
      snapshot_scope text DEFAULT 'data-only',
      snapshot_components text[] DEFAULT NULL,
      pull_requires text[] DEFAULT NULL
    )
    RETURNS integer
    LANGUAGE plpgsql
    AS $$
    DECLARE
      root_oid integer;
      next_generation integer;
      resolved_components text[];
      resolved_pull_requires text[];
      relation_row record;
      relations_json jsonb := '[]'::jsonb;
      relation_json jsonb;
    BEGIN
      IF array_length(child_relations, 1) IS NULL THEN
        RAISE EXCEPTION 'at least one relation is required to bind a storage partition';
      END IF;

      SELECT root_relation_oid, ddl_generation + 1
      INTO root_oid, next_generation
      FROM distrb_storage_binding_groups
      WHERE group_id = group_name
      FOR UPDATE;

      IF root_oid IS NULL THEN
        RAISE EXCEPTION 'storage binding group not found: %', group_name;
      END IF;

      resolved_components := COALESCE(
        snapshot_components,
        CASE WHEN snapshot_scope = 'catalog-and-data'
          THEN ARRAY['catalog', 'data']
          ELSE ARRAY['data']
        END
      );
      resolved_pull_requires := COALESCE(
        pull_requires,
        CASE WHEN snapshot_scope = 'catalog-and-data'
          THEN ARRAY['catalog', 'data']
          ELSE ARRAY['data']
        END
      );

      DELETE FROM distrb_storage_bindings
      WHERE group_id = group_name
        AND partition_id = partition_name;

      INSERT INTO distrb_storage_bindings (group_id, partition_id, binding)
      VALUES (
        group_name,
        partition_name,
        jsonb_build_object(
          'partitionId', partition_name,
          'rootRelationOid', root_oid,
          'relations', '[]'::jsonb,
          'snapshotScope', snapshot_scope,
          'snapshotComponents', to_jsonb(resolved_components),
          'pullRequires', to_jsonb(resolved_pull_requires),
          'ddlGeneration', next_generation
        )
      );

      FOR relation_row IN
        SELECT * FROM distrb_storage_relation_closure(child_relations, root_oid)
      LOOP
        relation_json := jsonb_build_object(
          'databaseOid', relation_row.database_oid,
          'tablespaceOid', relation_row.tablespace_oid,
          'relationOid', relation_row.relation_oid,
          'relfilenode', relation_row.relfilenode,
          'storagePath', relation_row.storage_path,
          'role', relation_row.relation_role,
          'parentRelationOid', relation_row.parent_relation_oid
        );
        relations_json := relations_json || jsonb_build_array(relation_json);

        INSERT INTO distrb_storage_binding_relations (
          group_id,
          partition_id,
          relation_ordinal,
          database_oid,
          tablespace_oid,
          relation_oid,
          relfilenode,
          storage_path,
          relation_role,
          parent_relation_oid
        )
        VALUES (
          group_name,
          partition_name,
          relation_row.relation_ordinal,
          relation_row.database_oid,
          relation_row.tablespace_oid,
          relation_row.relation_oid,
          relation_row.relfilenode,
          relation_row.storage_path,
          relation_row.relation_role,
          relation_row.parent_relation_oid
        );

        INSERT INTO distrb_storage_relation_lineage (
          group_id,
          partition_id,
          relation_oid,
          relfilenode,
          storage_path,
          relation_role,
          parent_relation_oid,
          ddl_generation
        )
        VALUES (
          group_name,
          partition_name,
          relation_row.relation_oid,
          relation_row.relfilenode,
          relation_row.storage_path,
          relation_row.relation_role,
          relation_row.parent_relation_oid,
          next_generation
        )
        ON CONFLICT (group_id, partition_id, relation_oid, ddl_generation) DO UPDATE SET
          relfilenode = EXCLUDED.relfilenode,
          storage_path = EXCLUDED.storage_path,
          relation_role = EXCLUDED.relation_role,
          parent_relation_oid = EXCLUDED.parent_relation_oid;
      END LOOP;

      UPDATE distrb_storage_bindings
      SET binding = jsonb_build_object(
          'partitionId', partition_name,
          'rootRelationOid', root_oid,
          'relations', relations_json,
          'snapshotScope', snapshot_scope,
          'snapshotComponents', to_jsonb(resolved_components),
          'pullRequires', to_jsonb(resolved_pull_requires),
          'ddlGeneration', next_generation
        )
      WHERE group_id = group_name
        AND partition_id = partition_name;

      UPDATE distrb_storage_binding_groups
      SET ddl_generation = next_generation
      WHERE group_id = group_name;

      RETURN next_generation;
    END;
    $$;

    CREATE OR REPLACE FUNCTION distrb_snapshot_policy(
      p_group_name text,
      p_partition_name text DEFAULT NULL,
      p_snapshot_scope text DEFAULT NULL,
      p_snapshot_components text[] DEFAULT NULL,
      p_pull_requires text[] DEFAULT NULL,
      p_vacuum_policy jsonb DEFAULT NULL
    )
    RETURNS integer
    LANGUAGE plpgsql
    AS $$
    DECLARE
      next_generation integer;
    BEGIN
      SELECT ddl_generation + 1
      INTO next_generation
      FROM distrb_storage_binding_groups
      WHERE group_id = p_group_name
      FOR UPDATE;

      IF next_generation IS NULL THEN
        RAISE EXCEPTION 'storage binding group not found: %', p_group_name;
      END IF;

      UPDATE distrb_storage_binding_groups
      SET
        ddl_generation = next_generation,
        vacuum_policy = COALESCE(p_vacuum_policy, distrb_storage_binding_groups.vacuum_policy)
      WHERE group_id = p_group_name;

      UPDATE distrb_storage_bindings
      SET binding = jsonb_set(
        jsonb_set(
          jsonb_set(
            jsonb_set(
              binding,
              '{snapshotScope}',
              to_jsonb(COALESCE(p_snapshot_scope, binding->>'snapshotScope'))
            ),
            '{snapshotComponents}',
            COALESCE(snapshot_components_json, binding->'snapshotComponents')
          ),
          '{pullRequires}',
          COALESCE(pull_requires_json, binding->'pullRequires')
        ),
        '{ddlGeneration}',
        to_jsonb(next_generation)
      )
      FROM (
        SELECT
          to_jsonb(p_snapshot_components) AS snapshot_components_json,
          to_jsonb(p_pull_requires) AS pull_requires_json
      ) policy
      WHERE group_id = p_group_name
        AND (p_partition_name IS NULL OR partition_id = p_partition_name);

      RETURN next_generation;
    END;
    $$;

    CREATE OR REPLACE FUNCTION distrb_create_root_table(
      group_name text,
      root_relation_name text,
      table_definition text,
      vacuum_policy jsonb DEFAULT NULL
    )
    RETURNS integer
    LANGUAGE plpgsql
    AS $$
    BEGIN
      EXECUTE format(
        'CREATE TABLE %s (%s)',
        distrb_relation_sql_name(root_relation_name),
        table_definition
      );

      RETURN distrb_create_binding_group(
        group_name,
        root_relation_name::regclass,
        vacuum_policy
      );
    END;
    $$;

    CREATE OR REPLACE FUNCTION distrb_create_bound_table(
      group_name text,
      partition_name text,
      child_relation_name text,
      table_definition text,
      snapshot_scope text DEFAULT 'data-only',
      snapshot_components text[] DEFAULT NULL,
      pull_requires text[] DEFAULT NULL
    )
    RETURNS integer
    LANGUAGE plpgsql
    AS $$
    BEGIN
      EXECUTE format(
        'CREATE TABLE %s (%s)',
        distrb_relation_sql_name(child_relation_name),
        table_definition
      );

      RETURN distrb_bind_partition(
        group_name,
        partition_name,
        ARRAY[child_relation_name::regclass],
        snapshot_scope,
        snapshot_components,
        pull_requires
      );
    END;
    $$;

    CREATE OR REPLACE VIEW distrb_storage_binding_metadata AS
      SELECT
        g.group_id,
        g.root_relation_oid,
        g.ddl_generation AS group_ddl_generation,
        g.vacuum_policy,
        b.partition_id,
        (b.binding->>'snapshotScope') AS snapshot_scope,
        b.binding->'snapshotComponents' AS snapshot_components,
        b.binding->'pullRequires' AS pull_requires,
        (b.binding->>'ddlGeneration')::integer AS binding_ddl_generation,
        r.relation_ordinal,
        r.database_oid,
        r.tablespace_oid,
        r.relation_oid,
        r.relfilenode,
        r.storage_path,
        r.relation_role,
        r.parent_relation_oid
      FROM distrb_storage_binding_groups g
      JOIN distrb_storage_bindings b
        ON b.group_id = g.group_id
      LEFT JOIN distrb_storage_binding_relations r
        ON r.group_id = b.group_id
       AND r.partition_id = b.partition_id;

    CREATE OR REPLACE FUNCTION distrb_retained_snapshot_generations(group_name text)
    RETURNS TABLE(ddl_generation integer)
    LANGUAGE sql
    AS $$
      SELECT DISTINCT s.ddl_generation
      FROM distrb_storage_snapshots s
      WHERE s.group_id = group_name
      ORDER BY s.ddl_generation
    $$;

    CREATE OR REPLACE FUNCTION distrb_create_catalog_manifest(group_name text)
    RETURNS jsonb
    LANGUAGE sql
    AS $$
      SELECT jsonb_build_object(
        'version', 1,
        'groupId', g.group_id,
        'rootRelationOid', g.root_relation_oid,
        'ddlGeneration', g.ddl_generation,
        'vacuumPolicy', g.vacuum_policy,
        'bindings', COALESCE((
          SELECT jsonb_agg(
            jsonb_build_object(
              'partitionId', b.partition_id,
              'rootRelationOid', g.root_relation_oid,
              'relations', COALESCE((
                SELECT jsonb_agg(
                  jsonb_build_object(
                    'databaseOid', r.database_oid,
                    'tablespaceOid', r.tablespace_oid,
                    'relationOid', r.relation_oid,
                    'relfilenode', r.relfilenode,
                    'storagePath', r.storage_path,
                    'role', r.relation_role,
                    'parentRelationOid', r.parent_relation_oid
                  )
                  ORDER BY r.relation_ordinal
                )
                FROM distrb_storage_binding_relations r
                WHERE r.group_id = b.group_id
                  AND r.partition_id = b.partition_id
              ), '[]'::jsonb),
              'snapshotScope', b.binding->>'snapshotScope',
              'snapshotComponents', b.binding->'snapshotComponents',
              'pullRequires', b.binding->'pullRequires',
              'ddlGeneration', (b.binding->>'ddlGeneration')::integer
            )
            ORDER BY b.partition_id
          )
          FROM distrb_storage_bindings b
          WHERE b.group_id = g.group_id
        ), '[]'::jsonb),
        'snapshots', COALESCE((
          SELECT jsonb_agg(
            jsonb_build_object(
              'partitionId', s.partition_id,
              'snapshotId', s.snapshot_id,
              'components', s.components,
              'ddlGeneration', s.ddl_generation,
              'manifestKind', s.manifest_kind
            )
            ORDER BY s.partition_id, s.ddl_generation, s.snapshot_id
          )
          FROM distrb_storage_snapshots s
          WHERE s.group_id = g.group_id
        ), '[]'::jsonb)
      )
      FROM distrb_storage_binding_groups g
      WHERE g.group_id = group_name
    $$;

    CREATE OR REPLACE FUNCTION distrb_storage_snapshot_fence_plan(
      group_name text,
      partition_name text DEFAULT NULL
    )
    RETURNS TABLE(
      partition_id text,
      relation_ordinal integer,
      relation_oid integer,
      relation_name text,
      relation_role text
    )
    LANGUAGE sql
    AS $$
      SELECT
        r.partition_id,
        r.relation_ordinal,
        r.relation_oid,
        r.relation_oid::oid::regclass::text AS relation_name,
        r.relation_role
      FROM distrb_storage_binding_relations r
      JOIN distrb_storage_bindings b
        ON b.group_id = r.group_id
       AND b.partition_id = r.partition_id
      WHERE r.group_id = group_name
        AND (partition_name IS NULL OR r.partition_id = partition_name)
        AND b.binding->>'snapshotScope' = 'data-only'
        AND r.relation_role IN ('root', 'child')
      ORDER BY r.partition_id, r.relation_ordinal
    $$;
  `);
}

export async function saveStorageBindingGroup(
  db: PGliteClient,
  group: StorageBindingGroup,
): Promise<void> {
  await migrateStorageBindingCatalog(db);
  await db.query(
    `
      INSERT INTO distrb_storage_binding_groups (
        group_id,
        root_relation_oid,
        ddl_generation,
        vacuum_policy
      )
      VALUES ($1, $2, $3, $4::jsonb)
      ON CONFLICT (group_id) DO UPDATE SET
        root_relation_oid = EXCLUDED.root_relation_oid,
        ddl_generation = EXCLUDED.ddl_generation,
        vacuum_policy = EXCLUDED.vacuum_policy
    `,
    [
      group.groupId,
      group.rootRelationOid,
      group.ddlGeneration,
      JSON.stringify(group.vacuumPolicy ?? null),
    ],
  );

  for (const binding of group.bindings) {
    await db.query(
      `
        INSERT INTO distrb_storage_bindings (group_id, partition_id, binding)
        VALUES ($1, $2, $3::jsonb)
        ON CONFLICT (group_id, partition_id) DO UPDATE SET
          binding = EXCLUDED.binding
      `,
      [group.groupId, binding.partitionId, JSON.stringify(binding)],
    );

    await db.query(
      `
        DELETE FROM distrb_storage_binding_relations
        WHERE group_id = $1
          AND partition_id = $2
      `,
      [group.groupId, binding.partitionId],
    );

    for (const [relationIndex, relation] of binding.relations.entries()) {
      await db.query(
        `
          INSERT INTO distrb_storage_binding_relations (
            group_id,
            partition_id,
            relation_ordinal,
            database_oid,
            tablespace_oid,
            relation_oid,
            relfilenode,
            storage_path,
            relation_role,
            parent_relation_oid
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          ON CONFLICT (group_id, partition_id, relation_ordinal) DO UPDATE SET
            database_oid = EXCLUDED.database_oid,
            tablespace_oid = EXCLUDED.tablespace_oid,
            relation_oid = EXCLUDED.relation_oid,
            relfilenode = EXCLUDED.relfilenode,
            storage_path = EXCLUDED.storage_path,
            relation_role = EXCLUDED.relation_role,
            parent_relation_oid = EXCLUDED.parent_relation_oid
        `,
        [
          group.groupId,
          binding.partitionId,
          relationIndex,
          relation.databaseOid,
          relation.tablespaceOid,
          relation.relationOid,
          relation.relfilenode ?? null,
          relation.storagePath ?? null,
          relation.role ?? "child",
          relation.parentRelationOid ?? null,
        ],
      );
    }
  }
}

export async function loadStorageBindingGroup(
  db: PGliteClient,
  groupId: string,
): Promise<StorageBindingGroup | undefined> {
  await migrateStorageBindingCatalog(db);
  const groupResult = await db.query<BindingGroupRow>(
    `
      SELECT
        group_id,
        root_relation_oid,
        ddl_generation,
        vacuum_policy
      FROM distrb_storage_binding_groups
      WHERE group_id = $1
    `,
    [groupId],
  );
  const row = groupResult.rows[0];
  if (!row) {
    return undefined;
  }

  const bindingsResult = await db.query<BindingRow>(
    `
      SELECT partition_id, binding
      FROM distrb_storage_bindings
      WHERE group_id = $1
      ORDER BY partition_id
    `,
    [groupId],
  );
  const relationRows = await listStorageBindingRelationRows(db, groupId);

  return {
    groupId: row.group_id,
    rootRelationOid: row.root_relation_oid,
    ddlGeneration: row.ddl_generation,
    vacuumPolicy: row.vacuum_policy
      ? row.vacuum_policy as StorageBindingGroup["vacuumPolicy"]
      : undefined,
    bindings: bindingsResult.rows.map((bindingRow) => {
      const binding = bindingRow.binding as StoragePartitionBinding;
      return {
        ...binding,
        relations: relationRows
          .filter((relation) => relation.partition_id === bindingRow.partition_id)
          .map(storageRelationLocatorFromRow),
      };
    }),
  };
}

export async function loadStoragePartitionBinding(options: {
  db: PGliteClient;
  groupId: string;
  partitionId: PartitionId;
}): Promise<StoragePartitionBinding | undefined> {
  const group = await loadStorageBindingGroup(options.db, options.groupId);
  return group?.bindings.find((binding) => binding.partitionId === options.partitionId);
}

export async function saveStorageSnapshotCatalogEntry<TManifest>(
  db: PGliteClient,
  entry: StorageSnapshotCatalogEntry<TManifest>,
): Promise<void> {
  await migrateStorageBindingCatalog(db);
  await db.query(
    `
      INSERT INTO distrb_storage_snapshots (
        group_id,
        partition_id,
        snapshot_id,
        components,
        high_watermark,
        ddl_generation,
        manifest_kind,
        manifest
      )
      VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, $8::jsonb)
      ON CONFLICT (group_id, partition_id, snapshot_id) DO UPDATE SET
        components = EXCLUDED.components,
        high_watermark = EXCLUDED.high_watermark,
        ddl_generation = EXCLUDED.ddl_generation,
        manifest_kind = EXCLUDED.manifest_kind,
        manifest = EXCLUDED.manifest
    `,
    [
      entry.groupId,
      entry.partitionId,
      entry.snapshotId,
      JSON.stringify(entry.components),
      JSON.stringify(entry.highWatermark),
      entry.ddlGeneration,
      entry.manifestKind,
      JSON.stringify(entry.manifest),
    ],
  );
}

export async function savePartitionCasSnapshotCatalogEntry(options: {
  db: PGliteClient;
  groupId: string;
  snapshot: PartitionCasSnapshot;
  components?: StorageSnapshotComponent[];
}): Promise<void> {
  await saveStorageSnapshotCatalogEntry(options.db, {
    groupId: options.groupId,
    partitionId: options.snapshot.partitionId,
    snapshotId: options.snapshot.snapshotId,
    components: options.components ?? defaultComponents(options.snapshot.storageBinding.snapshotScope),
    highWatermark: options.snapshot.highWatermark,
    ddlGeneration: options.snapshot.storageBinding.ddlGeneration ?? 0,
    manifestKind: "cas",
    manifest: options.snapshot.manifest,
  });
}

export async function savePartitionFilesystemSnapshotCatalogEntry(options: {
  db: PGliteClient;
  groupId: string;
  snapshot: PartitionFilesystemSnapshot;
  components?: StorageSnapshotComponent[];
}): Promise<void> {
  await saveStorageSnapshotCatalogEntry(options.db, {
    groupId: options.groupId,
    partitionId: options.snapshot.partitionId,
    snapshotId: options.snapshot.snapshotId,
    components: options.components ?? defaultComponents(options.snapshot.storageBinding.snapshotScope),
    highWatermark: options.snapshot.highWatermark,
    ddlGeneration: options.snapshot.storageBinding.ddlGeneration ?? 0,
    manifestKind: "filesystem",
    manifest: options.snapshot.manifest,
  });
}

export async function loadStorageSnapshotCatalogEntry<TManifest = unknown>(options: {
  db: PGliteClient;
  groupId: string;
  partitionId: PartitionId;
  snapshotId: string;
}): Promise<StorageSnapshotCatalogEntry<TManifest> | undefined> {
  await migrateStorageBindingCatalog(options.db);
  const result = await options.db.query<SnapshotCatalogRow>(
    `
      SELECT
        group_id,
        partition_id,
        snapshot_id,
        components,
        high_watermark,
        ddl_generation,
        manifest_kind,
        manifest
      FROM distrb_storage_snapshots
      WHERE group_id = $1
        AND partition_id = $2
        AND snapshot_id = $3
    `,
    [options.groupId, options.partitionId, options.snapshotId],
  );

  return result.rows[0] ? snapshotCatalogEntryFromRow<TManifest>(result.rows[0]) : undefined;
}

export async function listStorageSnapshotCatalogEntries<TManifest = unknown>(options: {
  db: PGliteClient;
  groupId: string;
  partitionId?: PartitionId;
}): Promise<StorageSnapshotCatalogEntry<TManifest>[]> {
  await migrateStorageBindingCatalog(options.db);
  const result = await options.db.query<SnapshotCatalogRow>(
    `
      SELECT
        group_id,
        partition_id,
        snapshot_id,
        components,
        high_watermark,
        ddl_generation,
        manifest_kind,
        manifest
      FROM distrb_storage_snapshots
      WHERE group_id = $1
        AND ($2::text IS NULL OR partition_id = $2)
      ORDER BY partition_id, ddl_generation, snapshot_id
    `,
    [options.groupId, options.partitionId ?? null],
  );

  return result.rows.map((row) => snapshotCatalogEntryFromRow<TManifest>(row));
}

export async function loadStorageSnapshotCatalogEntryForPull<TManifest = unknown>(options: {
  db: PGliteClient;
  groupId: string;
  partitionId?: PartitionId;
  requirement: StoragePullRequirement;
}): Promise<StorageSnapshotCatalogEntry<TManifest> | undefined> {
  const entries = await listStorageSnapshotCatalogEntries<TManifest>({
    db: options.db,
    groupId: options.groupId,
    partitionId: options.partitionId,
  });
  return selectStorageSnapshotCatalogEntryForPull(entries, options.requirement);
}

export async function planStorageSnapshotFence(options: {
  db: PGliteClient;
  groupId: string;
  partitionId?: PartitionId;
}): Promise<StorageSnapshotFenceTarget[]> {
  await migrateStorageBindingCatalog(options.db);
  const result = await options.db.query<SnapshotFenceTargetRow>(
    `
      SELECT
        partition_id,
        relation_ordinal,
        relation_oid,
        relation_name,
        relation_role
      FROM distrb_storage_snapshot_fence_plan($1, $2)
    `,
    [options.groupId, options.partitionId ?? null],
  );

  return result.rows.map((row) => ({
    partitionId: row.partition_id,
    relationOrdinal: row.relation_ordinal,
    relationOid: row.relation_oid,
    relationName: row.relation_name,
    relationRole: row.relation_role as StorageRelationRole,
  }));
}

export async function prepareStorageSnapshotFence(options: {
  db: PGliteClient;
  groupId: string;
  partitionId?: PartitionId;
}): Promise<StorageSnapshotFenceTarget[]> {
  const targets = await planStorageSnapshotFence(options);
  for (const target of targets) {
    await options.db.exec(`VACUUM ${target.relationName}`);
  }
  return targets;
}

export async function loadStorageCatalogManifest(options: {
  db: PGliteClient;
  groupId: string;
}): Promise<StorageCatalogManifest | undefined> {
  await migrateStorageBindingCatalog(options.db);
  const result = await options.db.query<CatalogManifestRow>(
    `
      SELECT distrb_create_catalog_manifest($1) AS manifest
    `,
    [options.groupId],
  );

  return result.rows[0]?.manifest as StorageCatalogManifest | undefined;
}

export async function planStorageGcFromCatalog(options: {
  db: PGliteClient;
  groupId: string;
  activePullGenerations?: StorageGcPlanOptions["activePullGenerations"];
}): Promise<StorageGcPlan | undefined> {
  const group = await loadStorageBindingGroup(options.db, options.groupId);
  if (!group) {
    return undefined;
  }

  const snapshotGenerations = await options.db.query<{ ddl_generation: number }>(
    `
      SELECT ddl_generation
      FROM distrb_retained_snapshot_generations($1)
    `,
    [options.groupId],
  );

  return planStorageGc(group, {
    activePullGenerations: options.activePullGenerations,
    retainedSnapshotGenerations: snapshotGenerations.rows.map((row) => row.ddl_generation),
  });
}

export async function createStorageBindingGroup(options: {
  db: PGliteClient;
  groupId: string;
  rootRelation: string;
  ddlGeneration?: number;
  vacuumPolicy?: StorageBindingGroup["vacuumPolicy"];
}): Promise<StorageBindingGroup> {
  await migrateStorageBindingCatalog(options.db);
  await options.db.query(
    `
      SELECT distrb_create_binding_group($1, $2::regclass, $3::jsonb)
    `,
    [
      options.groupId,
      options.rootRelation,
      JSON.stringify(options.vacuumPolicy ?? null),
    ],
  );

  if (options.ddlGeneration !== undefined && options.ddlGeneration !== 1) {
    await options.db.query(
      `
        UPDATE distrb_storage_binding_groups
        SET ddl_generation = $2
        WHERE group_id = $1
      `,
      [options.groupId, options.ddlGeneration],
    );
  }

  const group = await loadStorageBindingGroup(options.db, options.groupId);
  if (!group) {
    throw new Error(`storage binding group not found after create: ${options.groupId}`);
  }
  return group;
}

export async function bindStoragePartition(options: {
  db: PGliteClient;
  groupId: string;
  partitionId: PartitionId;
  relations: string[];
  snapshotScope: StorageSnapshotScope;
  snapshotComponents?: StorageSnapshotComponent[];
  pullRequires?: StorageSnapshotComponent[];
}): Promise<StorageBindingGroup> {
  await migrateStorageBindingCatalog(options.db);
  await options.db.query(
    `
      SELECT distrb_bind_partition(
        $1,
        $2,
        $3::regclass[],
        $4,
        $5::text[],
        $6::text[]
      )
    `,
    [
      options.groupId,
      options.partitionId,
      options.relations,
      options.snapshotScope,
      options.snapshotComponents ?? null,
      options.pullRequires ?? null,
    ],
  );

  const group = await loadStorageBindingGroup(options.db, options.groupId);
  if (!group) {
    throw new Error(`storage binding group not found after bind: ${options.groupId}`);
  }
  return group;
}

export async function discoverStoragePartitionBinding(options: {
  db: PGliteClient;
  partitionId: PartitionId;
  rootRelation: string;
  relations?: string[];
  snapshotScope: StorageSnapshotScope;
}): Promise<StoragePartitionBinding> {
  const relationNames = options.relations?.length
    ? options.relations
    : [options.rootRelation];
  const relations: StorageRelationLocator[] = [];

  for (const relationName of relationNames) {
    relations.push(await discoverRelationLocator(options.db, relationName));
  }

  return {
    partitionId: options.partitionId,
    rootRelationOid: relations[0]?.relationOid ?? 0,
    relations,
    snapshotScope: options.snapshotScope,
  };
}

async function discoverRelationLocator(
  db: PGliteClient,
  relationName: string,
  role?: StorageRelationRole,
  parentRelationOid?: number,
): Promise<StorageRelationLocator> {
  const result = await db.query<RelationIdentityRow>(
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

  return {
    databaseOid: row.database_oid,
    tablespaceOid: row.tablespace_oid,
    relationOid: row.relation_oid,
    relfilenode: row.relfilenode ?? row.relation_oid,
    storagePath: row.storage_path ?? undefined,
    role,
    parentRelationOid,
  };
}

function defaultComponents(scope: StorageSnapshotScope): StorageSnapshotComponent[] {
  return scope === "catalog-and-data" ? ["catalog", "data"] : ["data"];
}

async function listStorageBindingRelationRows(
  db: PGliteClient,
  groupId: string,
): Promise<BindingRelationRow[]> {
  const result = await db.query<BindingRelationRow>(
    `
      SELECT
        group_id,
        partition_id,
        relation_ordinal,
        database_oid,
        tablespace_oid,
        relation_oid,
        relfilenode,
        storage_path,
        relation_role,
        parent_relation_oid
      FROM distrb_storage_binding_relations
      WHERE group_id = $1
      ORDER BY partition_id, relation_ordinal
    `,
    [groupId],
  );

  return result.rows;
}

function storageRelationLocatorFromRow(row: BindingRelationRow): StorageRelationLocator {
  return {
    databaseOid: row.database_oid,
    tablespaceOid: row.tablespace_oid,
    relationOid: row.relation_oid,
    relfilenode: row.relfilenode ?? undefined,
    storagePath: row.storage_path ?? undefined,
    role: row.relation_role as StorageRelationRole,
    parentRelationOid: row.parent_relation_oid ?? undefined,
  };
}

function snapshotCatalogEntryFromRow<TManifest>(
  row: SnapshotCatalogRow,
): StorageSnapshotCatalogEntry<TManifest> {
  return {
    groupId: row.group_id,
    partitionId: row.partition_id,
    snapshotId: row.snapshot_id,
    components: row.components as StorageSnapshotComponent[],
    highWatermark: row.high_watermark as StorageSnapshotCatalogEntry<TManifest>["highWatermark"],
    ddlGeneration: row.ddl_generation,
    manifestKind: row.manifest_kind as StorageSnapshotCatalogEntry<TManifest>["manifestKind"],
    manifest: row.manifest as TManifest,
  };
}
