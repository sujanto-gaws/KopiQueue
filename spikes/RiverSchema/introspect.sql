-- Spike 0.8: catalog introspection of a database migrated by River's CLI.
-- Clean-room: records the schema *shape* (an interface). Function bodies are deliberately not selected.
\pset footer off
\pset format aligned

\echo '== relations'
SELECT c.relname, c.relkind, CASE c.relpersistence WHEN 'u' THEN 'UNLOGGED' WHEN 'p' THEN 'logged' END AS persistence
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'S', 'v', 'm')
ORDER BY c.relkind, c.relname;

\echo '== columns'
SELECT c.relname AS tbl, a.attnum AS n, a.attname AS col, format_type(a.atttypid, a.atttypmod) AS type,
       a.attnotnull AS not_null, a.attidentity AS ident, a.attgenerated AS gen,
       pg_get_expr(d.adbin, d.adrelid) AS default_expr
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY c.relname, a.attnum;

\echo '== constraints'
SELECT c.relname AS tbl, k.conname, k.contype, pg_get_constraintdef(k.oid) AS def
FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
ORDER BY c.relname, k.contype, k.conname;

\echo '== indexes'
SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' ORDER BY tablename, indexname;

\echo '== enum types'
SELECT t.typname, string_agg(e.enumlabel, ', ' ORDER BY e.enumsortorder) AS labels
FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = 'public' GROUP BY t.typname;

\echo '== other types'
SELECT t.typname, t.typtype FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = 'public' AND t.typtype NOT IN ('e', 'b') AND t.typrelid = 0 OR (n.nspname = 'public' AND t.typtype = 'd');

\echo '== functions (signatures only)'
SELECT p.proname, pg_get_function_identity_arguments(p.oid) AS args, pg_get_function_result(p.oid) AS returns,
       l.lanname AS lang, p.provolatile AS volatility, p.proparallel AS parallel
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace JOIN pg_language l ON l.oid = p.prolang
WHERE n.nspname = 'public' ORDER BY p.proname;

\echo '== triggers'
SELECT c.relname AS tbl, t.tgname, pg_get_triggerdef(t.oid) AS def
FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND NOT t.tgisinternal ORDER BY c.relname, t.tgname;

\echo '== table comments'
SELECT c.relname, obj_description(c.oid, 'pg_class') AS comment
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND obj_description(c.oid, 'pg_class') IS NOT NULL;

\echo '== river_migration rows'
SELECT * FROM river_migration ORDER BY line, version;
