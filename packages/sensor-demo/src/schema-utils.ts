export type ConstraintRow = {
  constraint_name: string;
  constraint_type: "p" | "u";
  columns: string[];
};

type QueryClient = {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
};

export async function tableConstraints(
  db: QueryClient,
  tableName: string,
): Promise<ConstraintRow[]> {
  const result = await db.query<ConstraintRow>(
    `
      SELECT
        c.conname AS constraint_name,
        c.contype AS constraint_type,
        array_agg(a.attname ORDER BY key.ordinality) AS columns
      FROM pg_constraint c
      JOIN unnest(c.conkey) WITH ORDINALITY AS key(attnum, ordinality) ON true
      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = key.attnum
      WHERE c.conrelid = $1::regclass
        AND c.contype IN ('p', 'u')
      GROUP BY c.conname, c.contype
    `,
    [tableName],
  );
  return result.rows;
}

export function sameColumns(actual: readonly string[], expected: readonly string[]): boolean {
  return (
    actual.length === expected.length && actual.every((column, index) => column === expected[index])
  );
}

export function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}
