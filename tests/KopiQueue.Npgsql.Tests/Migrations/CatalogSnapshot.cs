using System.Text;
using Npgsql;

namespace KopiQueue.Npgsql.Tests.Migrations;

/// <summary>
/// A text description of every object in one schema, read from <c>pg_catalog</c>: relations, columns, constraints,
/// indexes, types, function signatures, triggers and sequences. Two schemas with the same shape give the same text.
/// </summary>
/// <remarks>
/// Clean-room (C2): this records the schema's shape, which is an interface. Function bodies are deliberately not read,
/// so comparing against a River-migrated schema never copies River's SQL.
/// The schema name is replaced with <c>{schema}</c>. Columns are numbered among live columns, so a column that was
/// dropped and re-added compares equal. PostgreSQL 18's named NOT NULL constraints are left out: the column list
/// already records nullability, and older servers do not have them.
/// </remarks>
internal static class CatalogSnapshot
{
    private static readonly (string Section, string Sql)[] Queries =
    [
        ("relations", """
            SELECT c.relname || '|' || c.relkind::text || '|' || c.relpersistence::text
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relkind IN ('r', 'p', 'S', 'v', 'm', 'c', 'f')
            ORDER BY c.relname COLLATE "C"
            """),
        ("columns", """
            SELECT line FROM (
                SELECT c.relname, a.attnum,
                       c.relname || '|' || row_number() OVER (PARTITION BY c.oid ORDER BY a.attnum) || '|' || a.attname
                       || '|' || format_type(a.atttypid, a.atttypmod) || '|' || a.attnotnull::text
                       || '|' || a.attidentity::text || '|' || a.attgenerated::text
                       || '|' || coalesce(pg_get_expr(d.adbin, d.adrelid), '') AS line
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
                WHERE n.nspname = $1 AND c.relkind IN ('r', 'p', 'v', 'm', 'c', 'f') AND a.attnum > 0 AND NOT a.attisdropped
            ) x ORDER BY relname COLLATE "C", attnum
            """),
        ("constraints", """
            SELECT c.relname || '|' || k.conname || '|' || k.contype::text || '|' || pg_get_constraintdef(k.oid)
                   || '|' || k.condeferrable::text || '|' || k.condeferred::text || '|' || k.convalidated::text
            FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND k.contype <> 'n'
            ORDER BY c.relname COLLATE "C", k.conname COLLATE "C"
            """),
        ("indexes", """
            SELECT tablename || '|' || indexname || '|' || indexdef FROM pg_indexes WHERE schemaname = $1
            ORDER BY tablename COLLATE "C", indexname COLLATE "C"
            """),
        ("types", """
            SELECT t.typname || '|' || t.typtype::text || '|'
                   || coalesce((SELECT string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder) FROM pg_enum e WHERE e.enumtypid = t.oid), '')
            FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = $1 AND t.typtype IN ('e', 'd', 'r', 'm')
            ORDER BY t.typname COLLATE "C"
            """),
        ("functions", """
            SELECT p.proname || '|' || pg_get_function_arguments(p.oid) || '|' || pg_get_function_result(p.oid)
                   || '|' || l.lanname || '|' || p.provolatile::text || '|' || p.proparallel::text
                   || '|' || p.proisstrict::text || '|' || p.prosecdef::text || '|' || p.proleakproof::text
                   || '|' || p.procost::text || '|' || p.prorows::text
            FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace JOIN pg_language l ON l.oid = p.prolang
            WHERE n.nspname = $1
            ORDER BY p.proname COLLATE "C", pg_get_function_identity_arguments(p.oid) COLLATE "C"
            """),
        ("triggers", """
            SELECT c.relname || '|' || t.tgname || '|' || pg_get_triggerdef(t.oid)
            FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND NOT t.tgisinternal
            ORDER BY c.relname COLLATE "C", t.tgname COLLATE "C"
            """),
        ("sequences", """
            SELECT c.relname || '|' || s.seqtypid::regtype::text || '|' || s.seqstart || '|' || s.seqincrement
                   || '|' || s.seqmin || '|' || s.seqmax || '|' || s.seqcache || '|' || s.seqcycle::text || '|'
                   || coalesce((SELECT oc.relname || '.' || a.attname FROM pg_depend d
                                JOIN pg_class oc ON oc.oid = d.refobjid
                                JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
                                WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype IN ('a', 'i')), '')
            FROM pg_sequence s JOIN pg_class c ON c.oid = s.seqrelid JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1
            ORDER BY c.relname COLLATE "C"
            """),
    ];

    /// <summary>Describes <paramref name="schema"/>.</summary>
    public static async Task<string> TakeAsync(NpgsqlDataSource dataSource, string schema, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        // Deparsed definitions qualify every name that is not on the search path, so they all carry the schema.
        await using (var setPath = new NpgsqlCommand("SET LOCAL search_path = pg_catalog", connection, transaction))
        {
            await setPath.ExecuteNonQueryAsync(cancellationToken);
        }

        var text = new StringBuilder();
        foreach (var (section, sql) in Queries)
        {
            text.Append("== ").Append(section).Append('\n');
            await using var command = new NpgsqlCommand(sql, connection, transaction) { Parameters = { new() { Value = schema } } };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                text.Append(reader.GetString(0).Replace(schema + ".", "{schema}.", StringComparison.Ordinal)).Append('\n');
            }
        }

        return text.ToString();
    }

    /// <summary>Splits a snapshot into lines, ignoring line-ending differences, for readable assertion failures.</summary>
    public static string[] Lines(string snapshot) =>
        snapshot.ReplaceLineEndings("\n").TrimEnd('\n').Split('\n');
}
