#!/bin/bash
set -euo pipefail

# Usage:
#   ./db_compare_all.sh source_host target_host "'public','myschema'"   # (optional default schema list, usually not needed)
#
# Requirements:
#   - SSH key-based access to both hosts
#   - Your user can: sudo -u postgres psql ...
#   - psql installed on both hosts
#   - Run this from a directory that contains your *.sql checks
#
# Notes:
#   - We auto-discover DBs and schemas and pass :include_schemas per DB as the
#     intersection of user schemas on both sides (so queries run comparably).
#   - If a DB exists only on one side, we note it and skip comparisons for that DB.

SOURCE_HOST=${1:?source_host}
TARGET_HOST=${2:?target_host}
DEFAULT_INCLUDE_SCHEMAS=${3:-""}   # optional; if empty we compute per-DB schema intersection

# ---- Configurable exclusions (regexes) ----
EXCLUDE_DB_REGEX="${EXCLUDE_DB_REGEX:-^(template0|template1|postgres)$}"   # skip templates; tweak if you want 'postgres' included
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
  # list dbs: not templates, allow connections
  ssh -o BatchMode=yes "$host" \
    "sudo -u postgres psql -At -c \"SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate ORDER BY 1;\""
}

remote_psql_list_schemas() {
  local host=$1
  local db=$2
  # list user schemas
  ssh -o BatchMode=yes "$host" \
    "sudo -u postgres psql -At -d \"${db}\" -c \"SELECT nspname FROM pg_namespace WHERE nspname NOT IN ('pg_catalog','information_schema') AND nspname NOT LIKE 'pg_toast%' ORDER BY 1;\""
}

run_remote_sql_to_file() {
  local host=$1
  local db=$2
  local file=$3
  local include_schemas=$4
  local outfile=$5

  # -X ignore .psqlrc, ON_ERROR_STOP to fail fast, -v var, read SQL from stdin
  if ! ssh -o BatchMode=yes "$host" "sudo -u postgres psql -X --set=ON_ERROR_STOP=1 -d \"${db}\" -v include_schemas=\"${include_schemas}\" -f -" \
     < "$file" > "$outfile" 2>&1; then
    echo "ERROR: $(basename "$file") on $host/$db (see $outfile)"
    return 1
  fi
}

mkdir -p outputs/"$SOURCE_HOST" outputs/"$TARGET_HOST"
REPORT="comparison_report_$(date +%Y%m%d_%H%M%S).txt"
: > "$REPORT"

echo "Discovering databases..."
mapfile -t SRC_DBS < <(remote_psql_list_dbs "$SOURCE_HOST" | grep -Ev "$EXCLUDE_DB_REGEX" || true)
mapfile -t TGT_DBS < <(remote_psql_list_dbs "$TARGET_HOST" | grep -Ev "$EXCLUDE_DB_REGEX" || true)

# Build intersection of DB names
declare -A TGT_DB_SET=()
for d in "${TGT_DBS[@]}"; do TGT_DB_SET["$d"]=1; done

DBS_TO_COMPARE=()
for d in "${SRC_DBS[@]}"; do
  if [[ -n "${TGT_DB_SET[$d]:-}" ]]; then
    DBS_TO_COMPARE+=("$d")
  else
    echo "Note: DB '$d' exists on $SOURCE_HOST but not on $TARGET_HOST" | tee -a "$REPORT"
  fi
done
for d in "${TGT_DBS[@]}"; do
  if ! printf '%s\0' "${DBS_TO_COMPARE[@]}" | grep -Fxzq "$d"; then
    echo "Note: DB '$d' exists on $TARGET_HOST but not on $SOURCE_HOST" | tee -a "$REPORT"
  fi
done

if [ ${#DBS_TO_COMPARE[@]} -eq 0 ]; then
  echo "No common databases to compare. Exiting."
  exit 0
fi

echo | tee -a "$REPORT"
echo "=== Databases to compare (${#DBS_TO_COMPARE[@]}) ===" | tee -a "$REPORT"
printf ' - %s\n' "${DBS_TO_COMPARE[@]}" | tee -a "$REPORT"
echo | tee -a "$REPORT"

# Iterate DBs
for db in "${DBS_TO_COMPARE[@]}"; do
  echo "Processing DB: $db"
  mkdir -p "outputs/$SOURCE_HOST/$db" "outputs/$TARGET_HOST/$db"

  # Discover schemas on both sides
  mapfile -t SRC_SCHEMAS < <(remote_psql_list_schemas "$SOURCE_HOST" "$db" | grep -Ev "$EXCLUDE_SCHEMA_REGEX" || true)
  mapfile -t TGT_SCHEMAS < <(remote_psql_list_schemas "$TARGET_HOST" "$db" | grep -Ev "$EXCLUDE_SCHEMA_REGEX" || true)

  # Build intersection of schemas (so we compare apples to apples)
  declare -A TGT_SCHEMA_SET=()
  for s in "${TGT_SCHEMAS[@]}"; do TGT_SCHEMA_SET["$s"]=1; done

  SCHEMAS_TO_USE=()
  for s in "${SRC_SCHEMAS[@]}"; do
    [[ -n "${TGT_SCHEMA_SET[$s]:-}" ]] && SCHEMAS_TO_USE+=("$s")
  done

  if [ ${#SCHEMAS_TO_USE[@]} -eq 0 ]; then
    echo "  Skipping $db (no common user schemas)" | tee -a "$REPORT"
    continue
  fi

  # Compose include_schemas value like:  'public','myschema'
  if [[ -n "$DEFAULT_INCLUDE_SCHEMAS" ]]; then
    INCLUDE_SCHEMAS=$DEFAULT_INCLUDE_SCHEMAS
  else
    INCLUDE_SCHEMAS="'$(printf "%s','" "${SCHEMAS_TO_USE[@]}" | sed "s/','\$//")'"
  fi

  echo "  Common schemas: ${INCLUDE_SCHEMAS}"

  # Run each SQL on both sides for this DB
  for file in "${SQL_FILES[@]}"; do
    base=$(basename "$file" .sql)
    src_out="outputs/$SOURCE_HOST/$db/${base}.txt"
    tgt_out="outputs/$TARGET_HOST/$db/${base}.txt"

    echo "   -> $db : $(basename "$file") on $SOURCE_HOST"
    run_remote_sql_to_file "$SOURCE_HOST" "$db" "$file" "$INCLUDE_SCHEMAS" "$src_out" || true

    echo "   -> $db : $(basename "$file") on $TARGET_HOST"
    run_remote_sql_to_file "$TARGET_HOST" "$db" "$file" "$INCLUDE_SCHEMAS" "$tgt_out" || true
  done
done

# Diff section
echo | tee -a "$REPORT"
echo "=== Migration Comparison Report ===" | tee -a "$REPORT"
overall_mismatch=0
for db in "${DBS_TO_COMPARE[@]}"; do
  echo | tee -a "$REPORT"
  echo "DB: $db" | tee -a "$REPORT"
  for file in "${SQL_FILES[@]}"; do
    base=$(basename "$file" .sql)
    s="outputs/$SOURCE_HOST/$db/${base}.txt"
    t="outputs/$TARGET_HOST/$db/${base}.txt"
    if [[ ! -f "$s" || ! -f "$t" ]]; then
      echo "  $base: SKIP (missing output on one side)" | tee -a "$REPORT"
      continue
    fi
    if diff_output=$(diff -u "$s" "$t"); then
      echo "  $base: MATCH" | tee -a "$REPORT"
    else
      echo "  $base: DIFFERENCE" | tee -a "$REPORT"
      echo "$diff_output" | tee -a "$REPORT"
      echo "  ----------------------------------------" | tee -a "$REPORT"
      overall_mismatch=1
    fi
  done
done

echo | tee -a "$REPORT"
if [ $overall_mismatch -eq 0 ]; then
  echo "All compared outputs MATCH." | tee -a "$REPORT"
else
  echo "Differences found. See $REPORT and outputs/ folders." | tee -a "$REPORT"
fi

echo
echo "Done. Summary in: $REPORT"
