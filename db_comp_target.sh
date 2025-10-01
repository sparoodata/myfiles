#!/bin/bash
set -euo pipefail

# Usage:
#   ./db_compare_all.sh source_host target_host "'public','myschema'"
#
# New behavior:
#   - Still writes the full unified diff report like before
#   - Additionally writes a "Target Gaps Only" section in the report
#     and per-file gap outputs under gaps/<db>/<check>.missing_in_target.txt,
#     showing ONLY lines that are present in SOURCE and absent in TARGET.
#
# Notes:
#   - This assumes your *.sql checks produce stable, canonical, sorted text rows.
#     Add ORDER BY in the queries to avoid false positives due to row order.

SOURCE_HOST=${1:?source_host}
TARGET_HOST=${2:?target_host}
DEFAULT_INCLUDE_SCHEMAS=${3:-""}

EXCLUDE_DB_REGEX="${EXCLUDE_DB_REGEX:-^(template0|template1|postgres)$}"
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
  local host=$1
  local db=$2
  ssh -o BatchMode=yes "$host" \
    "sudo -u postgres psql -At -d \"${db}\" -c \"SELECT nspname FROM pg_namespace WHERE nspname NOT IN ('pg_catalog','information_schema') AND nspname NOT LIKE 'pg_toast%' ORDER BY 1;\""
}

run_remote_sql_to_file() {
  local host=$1 db=$2 file=$3 include_schemas=$4 outfile=$5
  if ! ssh -o BatchMode=yes "$host" "sudo -u postgres psql -X --set=ON_ERROR_STOP=1 -d \"${db}\" -v include_schemas=\"${include_schemas}\" -f -" \
     < "$file" > "$outfile" 2>&1; then
    echo "ERROR: $(basename "$file") on $host/$db (see $outfile)"
    return 1
  fi
}

mkdir -p outputs/"$SOURCE_HOST" outputs/"$TARGET_HOST"
mkdir -p gaps
REPORT="comparison_report_$(date +%Y%m%d_%H%M%S).txt"
: > "$REPORT"

echo "Discovering databases..."
mapfile -t SRC_DBS < <(remote_psql_list_dbs "$SOURCE_HOST" | grep -Ev "$EXCLUDE_DB_REGEX" || true)
mapfile -t TGT_DBS < <(remote_psql_list_dbs "$TARGET_HOST" | grep -Ev "$EXCLUDE_DB_REGEX" || true)

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

for db in "${DBS_TO_COMPARE[@]}"; do
  echo "Processing DB: $db"
  mkdir -p "outputs/$SOURCE_HOST/$db" "outputs/$TARGET_HOST/$db" "gaps/$db"

  mapfile -t SRC_SCHEMAS < <(remote_psql_list_schemas "$SOURCE_HOST" "$db" | grep -Ev "$EXCLUDE_SCHEMA_REGEX" || true)
  mapfile -t TGT_SCHEMAS < <(remote_psql_list_schemas "$TARGET_HOST" "$db" | grep -Ev "$EXCLUDE_SCHEMA_REGEX" || true)

  declare -A TGT_SCHEMA_SET=()
  for s in "${TGT_SCHEMAS[@]}"; do TGT_SCHEMA_SET["$s"]=1; done

  SCHEMAS_TO_USE=()
  for s in "${SRC_SCHEMAS[@]}"; do [[ -n "${TGT_SCHEMA_SET[$s]:-}" ]] && SCHEMAS_TO_USE+=("$s"); done
  if [ ${#SCHEMAS_TO_USE[@]} -eq 0 ]; then
    echo "  Skipping $db (no common user schemas)" | tee -a "$REPORT"
    continue
  fi

  if [[ -n "$DEFAULT_INCLUDE_SCHEMAS" ]]; then
    INCLUDE_SCHEMAS=$DEFAULT_INCLUDE_SCHEMAS
  else
    INCLUDE_SCHEMAS="'$(printf "%s','" "${SCHEMAS_TO_USE[@]}" | sed "s/','\$//")'"
  fi
  echo "  Common schemas: ${INCLUDE_SCHEMAS}"

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

# Full diff section (kept for context)
echo | tee -a "$REPORT"
echo "=== Migration Comparison Report (Full Diff) ===" | tee -a "$REPORT"
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

# Target gaps only section
echo | tee -a "$REPORT"
echo "=== Target Gaps Only (lines present in SOURCE and missing in TARGET) ===" | tee -a "$REPORT"
any_gaps=0
for db in "${DBS_TO_COMPARE[@]}"; do
  echo | tee -a "$REPORT"
  echo "DB: $db" | tee -a "$REPORT"
  gaps_found_in_db=0
  for file in "${SQL_FILES[@]}"; do
    base=$(basename "$file" .sql)
    s="outputs/$SOURCE_HOST/$db/${base}.txt"
    t="outputs/$TARGET_HOST/$db/${base}.txt"
    gap_file="gaps/$db/${base}.missing_in_target.txt"

    if [[ ! -f "$s" || ! -f "$t" ]]; then
      echo "  $base: SKIP (missing output on one side)" | tee -a "$REPORT"
      continue
    fi

    # Extract only '-' lines from unified diff (source-only lines), ignore headers/range lines
    # Then strip the leading '-' so you see the exact missing content.
    diff -u "$s" "$t" | awk '
      /^--- / {next} /^+++ / {next} /^@@/ {next}
      /^-/ && !/^--/ { sub(/^-/, "", $0); print }
    ' > "$gap_file" || true

    if [[ -s "$gap_file" ]]; then
      echo "  $base: MISSING IN TARGET (${gap_file})" | tee -a "$REPORT"
      echo "    --- begin ---" | tee -a "$REPORT"
      sed 's/^/    /' "$gap_file" | tee -a "$REPORT" >/dev/null
      echo "    --- end -----" | tee -a "$REPORT"
      gaps_found_in_db=1
      any_gaps=1
    else
      echo "  $base: No gaps" | tee -a "$REPORT"
      rm -f "$gap_file"
    fi
  done
  if [[ $gaps_found_in_db -eq 0 ]]; then
    echo "  No gaps in this DB." | tee -a "$REPORT"
  fi
done

echo | tee -a "$REPORT"
if [[ $any_gaps -eq 0 ]]; then
  echo "Target is fully aligned with source for all checks." | tee -a "$REPORT"
else
  echo "Gaps found. See 'Target Gaps Only' section above and the 'gaps/' folder." | tee -a "$REPORT"
fi

echo
echo "Done. Summary in: $REPORT"
echo "Gap files under: gaps/<db>/<check>.missing_in_target.txt"
