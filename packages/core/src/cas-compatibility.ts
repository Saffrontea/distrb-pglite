// ADR 0007: CAS manifests are version-fenced. Physical blocks are only shared
// between nodes whose storage format matches; mismatches are rejected, never
// migrated. `pgMajorVersion` comes from the data directory's PG_VERSION file
// and is the authoritative physical-format fence. `pgliteVersion` is enforced
// only when both sides know it, because browser runtimes cannot always
// discover their own PGlite package version.
export const CAS_MANIFEST_FORMAT = 1;

export type CasCompatibility = {
  manifestFormat: typeof CAS_MANIFEST_FORMAT;
  pgMajorVersion: string;
  pgliteVersion?: string;
};

export function validateCasCompatibility(
  value: unknown,
  name = "CAS manifest compatibility",
): CasCompatibility {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${name} must be an object`);
  }

  const record = value as Record<string, unknown>;
  if (record.manifestFormat !== CAS_MANIFEST_FORMAT) {
    throw new Error(`${name}.manifestFormat is unsupported: ${String(record.manifestFormat)}`);
  }
  if (typeof record.pgMajorVersion !== "string" || !/^\d+(\.\d+)?$/.test(record.pgMajorVersion)) {
    throw new Error(`${name}.pgMajorVersion must be a Postgres version string`);
  }
  if (
    record.pgliteVersion !== undefined &&
    (typeof record.pgliteVersion !== "string" || record.pgliteVersion.length === 0)
  ) {
    throw new Error(`${name}.pgliteVersion must be a non-empty string when present`);
  }

  return record as CasCompatibility;
}

export function isCasCompatible(
  manifestCompatibility: CasCompatibility | undefined,
  localCompatibility: CasCompatibility,
): boolean {
  if (!manifestCompatibility) {
    return false;
  }
  if (manifestCompatibility.manifestFormat !== localCompatibility.manifestFormat) {
    return false;
  }
  if (manifestCompatibility.pgMajorVersion !== localCompatibility.pgMajorVersion) {
    return false;
  }
  return (
    manifestCompatibility.pgliteVersion === undefined ||
    localCompatibility.pgliteVersion === undefined ||
    manifestCompatibility.pgliteVersion === localCompatibility.pgliteVersion
  );
}

export function assertCasCompatible(
  manifestCompatibility: CasCompatibility | undefined,
  localCompatibility: CasCompatibility,
  context: string,
): void {
  if (!manifestCompatibility) {
    throw new Error(`${context}: CAS manifest has no compatibility stamp and is rejected`);
  }
  if (!isCasCompatible(manifestCompatibility, localCompatibility)) {
    throw new Error(
      `${context}: CAS manifest compatibility mismatch: ` +
        `manifest=${describeCasCompatibility(manifestCompatibility)} ` +
        `local=${describeCasCompatibility(localCompatibility)}`,
    );
  }
}

export function describeCasCompatibility(compatibility: CasCompatibility): string {
  const pglite = compatibility.pgliteVersion ?? "unknown";
  return `format:${compatibility.manifestFormat}/pg:${compatibility.pgMajorVersion}/pglite:${pglite}`;
}
