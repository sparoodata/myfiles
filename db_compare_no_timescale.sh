#!/bin/bash
set -euo pipefail

# Usage:
#   ./db_compare_all.sh source_host target_host
#   ./db_compare_all.sh source_host target_host "'public','myschema'"
#
# Behavior:
#   - Compares only the intersection of databases and user schemas by default
#   - Runs each SQL in read-only mode with stable, quiet output
#   - Reports only items present on SOURCE but missing on TARGET
#     Export SHOW_EXTRA_ON_TARGET=1 to also see items present only on TARGET
#   - NEW: Supports schema-specific table exclusion (post-filter)
#
# Requirements:
#   - SSH key-based access to both hosts
#   - Your user can: sudo -u postgres psql ...
#   - psql installed on both hosts
#   - Run from a directory that contains your *.sql checks

SOURCE_HOST=${1:?source_host}
TARGET_HOST=${2:?target_host}
DEFAULT_INCLUDE_SCHEMAS=${3:-""}   # optional; if empty we compute per-DB schema intersection

# ---- Configurable exclusions (regexes) ----
EXCLUDE_DB_REGEX="${EXCLUDE_DB_REGEX:-^(template0|template1|postgres)$}"   # tweak if you want 'postgres' included
EXCLUDE_SCHEMA_REGEX="${EXCLUDE_SCHEMA_REGEX:-^(pg_catalog|information_schema|pg_toast.*)$}"

# Optional: also show items that exist only on TARGET
SHOW_EXTRA_ON_TARGET="${SHOW_EXTRA_ON_TARGET:-0}"

# Optional: global table exclusion across ALL schemas (applied post-run)
# e.g., EXCLUDE_TABLE_REGEX='^(audit_log|tmp_.*|backup_\d+)$'
EXCLUDE_TABLE_REGEX="${EXCLUDE_TABLE_REGEX:-}"

# NEW: schema-specific table exclusions (applied post-run)
#   - Keys are schema names
#   - Values are regexes for table names in that schema (NO schema prefix here)
#   - Example overrides:
#       export TABLE_EXCLUDE_public='^(audit_log|event_.*|tmp_.*)$'
#       export TABLE_EXCLUDE_reporting='^(matview_.*|etl_.*)$'
#
# You can define them as env vars before running the script.
# Below are safe defaults (none).
declare -A TABLE_EXCLUDE_BY_SCHEMA
# Auto-import from environment variables starting with TABLE_EXCLUDE_<schema>=...
# Convert to associative array at runtime:
import_schema_excludes_from_env() {
  # shellcheck disable=SC2048
  for v in ${!TABLE_EXCLUDE_*}; do
    # v like TABLE_EXCLUDE_public
    local schema="${v#TABLE_EXCLUDE_}"
    # Turn possible uppercase to lowercase for consistency
    schema="$(echo "$schema" | tr '[:upper:]' '[:lower:]')"
    # Read value
    # shellcheck disable=SC2001
    local val
    val="$(eval "printf '%s' \"\$$v\"")"
    TABLE_EXCLUDE_BY_SCHEMA["$schema"]="$val"
  done
}

import_schema_excludes_from_env

# Locale-neutral sorting and comparisons
export LC_ALL=C

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
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new)

remote_psql_list_dbs() {
  local host=$1
  ssh "${SSH_OPTS[@]}" "$host" \
    "sudo -u postgres psql -At -X -c \"SELECT datname
                                       FROM pg_database
                                       WHERE datallowconn AND NOT datistemplate
                                       ORDER BY 1;\""
}

remote_psql_list_schemas() {
  local host=$1
  local db=$2
  ssh "${SSH_OPTS[@]}" "$host" \
    "sudo -u postgres psql -At -X -d \"${db}\" -c \"SELECT nspname
                                                    FROM pg_namespace
                                                    WHERE nspname NOT IN ('pg_catalog','information_schema')
                                                      AND nspname NOT LIKE 'pg_toast%%'
                                                    ORDER BY 1;\""
}

# Run SQL safely, capture stdout to .txt and stderr to .err (do not mix)
run_remote_sql_to_file() {
  local host=$1
  local db=$2
  local file=$3
  local include_schemas=$4
  local outfile=$5
  local errfile="${outfile%.txt}.err"

  {
    echo "SET lock_timeout = '5s';"
    echo "SET statement_timeout = '30min';"
    echo "SET default_transaction_read_only = on;"
    echo "BEGIN READ ONLY;"
    cat "$file"
    echo "COMMIT;"
  } | ssh "${SSH_OPTS[@]}" "$host" \
        "sudo -u postgres psql -X -qAt -P pager=off --set=ON_ERROR_STOP=1 \
         -v include_schemas=\"${include_schemas}\" -d \"${db}\" -f -" \
        >"$outfile" 2>"$errfile"

  if [[ -s "$errfile" ]]; then
    echo "ERROR: $(basename "$file") on $host/$db (see $errfile)"
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

# Build a combined schema.table exclusion regex for a given set of schemas
# Input: list of schema names (space-separated)
# Output: prints a single ERE suitable for grep -E (empty if none)
build_schema_table_exclude_regex() {
  local -a schemas=("$@")
  local parts=()

  # Global table name regex across ALL schemas (if provided)
  # This becomes ^(schema1|schema2|...)\.(<EXCLUDE_TABLE_REGEX>) as a part
  if [[ -n "$EXCLUDE_TABLE_REGEX" && ${#schemas[@]} -gt 0 ]]; then
    local joined_schemas
    joined_schemas="$(printf "%s|" "${schemas[@]}" | sed 's/|$//')"
    parts+=("^(${joined_schemas})\\.(${EXCLUDE_TABLE_REGEX#^})") # avoid double ^ if provided
  fi

  # Schema-specific patterns
  for s in "${schemas[@]}"; do
    local pat="${TABLE_EXCLUDE_BY_SCHEMA[$s]:-}"
    if [[ -n "$pat" ]]; then
      # Build ^schema\.(pat)
      parts+=("^${s}\\.(${pat#^})")
    fi
  done

  if [[ ${#parts[@]} -eq 0 ]]; then
    printf ""
  else
    # Join with |, do not wrap in extra ()
    local joined
    joined="$(printf "%s|" "${parts[@]}" | sed 's/|$//')"
    printf "%s" "$joined"
  fi
}

# Iterate DBs
for db in "${DBS_TO_COMPARE[@]}"; do
  echo "Processing DB: $db"
  mkdir -p "outputs/$SOURCE_HOST/$db" "outputs/$TARGET_HOST/$db"

  # Discover schemas on both sides
  mapfile -t SRC_SCHEMAS < <(remote_psql_list_schemas "$SOURCE_HOST" "$db" | grep -Ev "$EXCLUDE_SCHEMA_REGEX" || true)
  mapfile -t TGT_SCHEMAS < <(remote_psql_list_schemas "$TARGET_HOST" "$db" | grep -Ev "$EXCLUDE_SCHEMA_REGEX" || true)

  # Build intersection of schemas
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

  # Compose include_schemas like:  'public','myschema'
  if [[ -n "$DEFAULT_INCLUDE_SCHEMAS" ]]; then
    INCLUDE_SCHEMAS=$DEFAULT_INCLUDE_SCHEMAS
  else
    INCLUDE_SCHEMAS="'$(printf "%s','" "${SCHEMAS_TO_USE[@]}" | sed "s/','\$//")'"
  fi

  # Build the combined schema.table exclude regex for this DB's schemas
  # This lets you exclude specific tables only within the common schemas
  EXCLUDE_SCHEMA_TABLE_REGEX="$(build_schema_table_exclude_regex "${SCHEMAS_TO_USE[@]}")"

  echo "  Common schemas: ${INCLUDE_SCHEMAS}"
  if [[ -n "$EXCLUDE_SCHEMA_TABLE_REGEX" ]]; then
    echo "  Applying schema.table exclude filter: $EXCLUDE_SCHEMA_TABLE_REGEX"
  else
    echo "  No schema.table exclusions for these schemas."
  fi

  # Run each SQL on both sides for this DB
  for file in "${SQL_FILES[@]}"; do
    base=$(basename "$file" .sql)
    src_out="outputs/$SOURCE_HOST/$db/${base}.txt"
    tgt_out="outputs/$TARGET_HOST/$db/${base}.txt"

    echo "   -> $db : $(basename "$file") on $SOURCE_HOST"
    run_remote_sql_to_file "$SOURCE_HOST" "$db" "$file" "$INCLUDE_SCHEMAS" "$src_out" || true

    echo "   -> $db : $(basename "$file") on $TARGET_HOST"
    run_remote_sql_to_file "$TARGET_HOST" "$db" "$file" "$INCLUDE_SCHEMAS" "$tgt_out" || true

    # Post-filter outputs to drop excluded schema.table rows (non-destructive; create .filtered files)
    # Only apply if we actually have a schema.table pattern
    if [[ -n "$EXCLUDE_SCHEMA_TABLE_REGEX" ]]; then
      # Filter only lines that look like schema.table. Other lines pass through unchanged.
      # First remove matches, then keep the rest.
      grep -Eiv -- "$EXCLUDE_SCHEMA_TABLE_REGEX" "$src_out" > "${src_out}.filtered" || true
      grep -Eiv -- "$EXCLUDE_SCHEMA_TABLE_REGEX" "$tgt_out" > "${tgt_out}.filtered" || true
    else
      cp "$src_out" "${src_out}.filtered"
      cp "$tgt_out" "${tgt_out}.filtered"
    fi
  done
done

# Diff section: report only target deltas
echo | tee -a "$REPORT"
echo "=== Migration Comparison Report (Target deltas only) ===" | tee -a "$REPORT"
overall_mismatch=0

for db in "${DBS_TO_COMPARE[@]}"; do
  echo | tee -a "$REPORT"
  echo "DB: $db" | tee -a "$REPORT"

  for file in "${SQL_FILES[@]}"; do
    base=$(basename "$file" .sql)
    s="outputs/$SOURCE_HOST/$db/${base}.txt.filtered"
    t="outputs/$TARGET_HOST/$db/${base}.txt.filtered"

    if [[ ! -f "$s" || ! -f "$t" ]]; then
      echo "  $base: SKIP (missing output on one side)" | tee -a "$REPORT"
      continue
    fi

    # Normalize and compare as sets
    missing_on_target=$(comm -23 <(sort -u "$s") <(sort -u "$t") || true)
    extra_on_target=""
    if [[ "$SHOW_EXTRA_ON_TARGET" == "1" ]]; then
      extra_on_target=$(comm -13 <(sort -u "$s") <(sort -u "$t") || true)
    fi

    if [[ -z "$missing_on_target" && -z "$extra_on_target" ]]; then
      echo "  $base: OK (no target changes needed)" | tee -a "$REPORT"
    else
      overall_mismatch=1
      echo "  $base: TARGET needs attention" | tee -a "$REPORT"

      if [[ -n "$missing_on_target" ]]; then
        echo "    Missing on TARGET (present in SOURCE, absent in TARGET):" | tee -a "$REPORT"
        while IFS= read -r line; do
          [[ -n "$line" ]] && echo "      $line" | tee -a "$REPORT"
        done <<< "$missing_on_target"
      fi

      if [[ -n "$extra_on_target" ]]; then
        echo "    Extra on TARGET (present in TARGET, absent in SOURCE):" | tee -a "$REPORT"
        while IFS= read -r line; do
          [[ -n "$line" ]] && echo "      $line" | tee -a "$REPORT"
        done <<< "$extra_on_target"
      fi

      echo "  ----------------------------------------" | tee -a "$REPORT"
    fi
  done
done

echo | tee -a "$REPORT"
if [ $overall_mismatch -eq 0 ]; then
  echo "All checks match; no target changes needed." | tee -a "$REPORT"
else
  echo "Target differences found. See $REPORT and outputs/ folders." | tee -a "$REPORT"
fi

echo
echo "Done. Summary in: $REPORT"

# ---- Quick reference for exclusions ----
# Global table ignore across all schemas (name only):
#   export EXCLUDE_TABLE_REGEX='^(audit_log|tmp_.*)$'
#
# Schema-specific table ignores (per schema):
#   export TABLE_EXCLUDE_public='^(audit_log|event_.*)$'
#   export TABLE_EXCLUDE_reporting='^(matview_.*|etl_.*)$'
#
# Example run:
#   EXCLUDE_TABLE_REGEX='^tmp_.*$' \
#   TABLE_EXCLUDE_public='^(audit_log|test_.*)$' \
#   ./db_compare_all.sh source_host target_host
