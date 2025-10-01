#!/bin/bash
set -euo pipefail

# Usage:
#   ./db_exec_both.sh source_host target_host "'public','myschema'"   # (optional fixed schema list)
#
# What it does:
#   - Discovers common databases and (by default) common user schemas
#   - Runs every *.sql in the current directory against each common DB on SOURCE and TARGET
#   - Writes outputs to outputs/<host>/<db>/<file>.txt
#   - No comparison/reporting
#
# Safety:
#   - psql started with -X and ON_ERROR_STOP
#   - Session set to READ ONLY with conservative timeouts

SOURCE_HOST=${1:?source_host}
TARGET_HOST=${2:?target_host}
DEFAULT_INCLUDE_SCHEMAS=${3:-""}   # e.g. "'public','myschema'"

# ---- Configurable exclusions (regexes) ----
EXCLUDE_DB_REGEX="${EXCLUDE_DB_REGEX:-^(template0|template1|postgres)$}"   # include 'postgres' by removing it here
EXCLUDE_SCHEMA_REGEX="${EXCLUDE_SCHEMA_REGEX:-^(pg_catalog|information_schema|pg_toast.*)$}"

# ---- Collect SQL files ----
SQL_FILES=()
for f in *.sql; do
  [[ -f "$f" ]] && SQL_FILES+=("$f")
done
if [ ${#SQL_FILES[@]} -eq 0 ]; then
  echo "No .sql files found in the current directory"
  exit 1
fi

# ---- Helpers ----
remote_psql_list_dbs() {
  local host=$1
  ssh -o BatchMode=yes "$host" \
    "sudo -u postgres psql -At -c \"SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate ORDER BY 1;\""
}

remote_psql_list_schemas() {
  local host=$1 db=$2
  ssh -o BatchMode=yes "$host" \
    "sudo -u postgres psql -At -d \"${db}\" -c \"SELECT nspname FROM pg_namespace
      WHERE nspname NOT IN ('pg_catalog','information_schema')
        AND nspname NOT LIKE 'pg_toast%'
      ORDER BY 1;\""
}

run_remote_sql_to_file() {
  local host=$1 db=$2 file=$3 include_schemas=$4 outfile=$5

  # Pre-set a safe session (READ ONLY + short timeouts) then run file
  # Multiple -c are executed before -f, affecting the same session.
  if ! ssh -o BatchMode=yes "$host" \
    "sudo -u postgres psql -X --set=ON_ERROR_STOP=1 -d \"${db}\" \
       -v include_schemas=\"${include_schemas}\" \
       -c \"SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;\" \
       -c \"SET lock_timeout = '5s';\" \
       -c \"SET statement_timeout = '2min';\" \
       -c \"SET idle_in_transaction_session_timeout = '10s';\" \
       -f -" < \"$file\" > \"$outfile\" 2>&1; then
    echo \"ERROR: $(basename "$file") on $host/$db (see $outfile)\"
    return 1
  fi
}

mkdir -p "outputs/$SOURCE_HOST" "outputs/$TARGET_HOST"

echo "Discovering databases..."
mapfile -t SRC_DBS < <(remote_psql_list_dbs "$SOURCE_HOST" | grep -Ev "$EXCLUDE_DB_REGEX" || true)
mapfile -t TGT_DBS < <(remote_psql_list_dbs "$TARGET_HOST" | grep -Ev "$EXCLUDE_DB_REGEX" || true)

# Intersection of DBs
declare -A TGT_DB_SET=()
for d in "${TGT_DBS[@]}"; do TGT_DB_SET["$d"]=1; done

DBS_TO_USE=()
for d in "${SRC_DBS[@]}"; do
  [[ -n "${TGT_DB_SET[$d]:-}" ]] && DBS_TO_USE+=("$d")
done

if [ ${#DBS_TO_USE[@]} -eq 0 ]; then
  echo "No common databases. Exiting."
  exit 0
fi

for db in "${DBS_TO_USE[@]}"; do
  echo "DB: $db"

  mkdir -p "outputs/$SOURCE_HOST/$db" "outputs/$TARGET_HOST/$db"

  # Discover schemas on both sides
  mapfile -t SRC_SCHEMAS < <(remote_psql_list_schemas "$SOURCE_HOST" "$db" | grep -Ev "$EXCLUDE_SCHEMA_REGEX" || true)
  mapfile -t TGT_SCHEMAS < <(remote_psql_list_schemas "$TARGET_HOST" "$db" | grep -Ev "$EXCLUDE_SCHEMA_REGEX" || true)

  # Intersection of schemas unless user provided a fixed list
  if [[ -n "$DEFAULT_INCLUDE_SCHEMAS" ]]; then
    INCLUDE_SCHEMAS="$DEFAULT_INCLUDE_SCHEMAS"
  else
    declare -A TGT_SCHEMA_SET=()
    for s in "${TGT_SCHEMAS[@]}"; do TGT_SCHEMA_SET["$s"]=1; done
    SCHEMAS_TO_USE=()
    for s in "${SRC_SCHEMAS[@]}"; do
      [[ -n "${TGT_SCHEMA_SET[$s]:-}" ]] && SCHEMAS_TO_USE+=("$s")
    done
    if [ ${#SCHEMAS_TO_USE[@]} -eq 0 ]; then
      echo "  Skipping (no common user schemas)"
      continue
    fi
    INCLUDE_SCHEMAS="'$(printf "%s','" "${SCHEMAS_TO_USE[@]}" | sed "s/','\$//")'"
  fi

  echo "  Schemas: ${INCLUDE_SCHEMAS}"

  # Run each SQL on both hosts
  for file in "${SQL_FILES[@]}"; do
    base=$(basename "$file" .sql)
    src_out="outputs/$SOURCE_HOST/$db/${base}.txt"
    tgt_out="outputs/$TARGET_HOST/$db/${base}.txt"

    echo "   -> SOURCE $SOURCE_HOST : $base.sql"
    run_remote_sql_to_file "$SOURCE_HOST" "$db" "$file" "$INCLUDE_SCHEMAS" "$src_out" || true

    echo "   -> TARGET $TARGET_HOST : $base.sql"
    run_remote_sql_to_file "$TARGET_HOST" "$db" "$file" "$INCLUDE_SCHEMAS" "$tgt_out" || true
  done
done

echo "Done. Check the outputs/ folders."
