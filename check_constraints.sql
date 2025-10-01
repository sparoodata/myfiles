WITH relevant_schemas AS (
  SELECT n.oid AS namespace_oid, n.nspname
  FROM pg_namespace n
  WHERE nspname IN (:include_schemas)
)
SELECT
  s.nspname::text || '.' ||
  COALESCE(rel.relname, '?')::text || '.' ||
  con.contype::text || '.' ||
  con.conname::text AS check_cons
FROM pg_constraint con
JOIN relevant_schemas s ON s.namespace_oid = con.connamespace
LEFT JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
WHERE con.contype = 'c'
ORDER BY 1;

