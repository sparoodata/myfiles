#!/bin/bash
set -euo pipefail

# Usage:
#   ./db_compare.sh source_host target_host source_db target_db "'public','myschema'"
#
# Requires:
#   - SSH key-based access to both hosts
#   - Your user can run: sudo -u postgres psql ...
#   - psql available on both hosts

SOURCE_HOST=${1:?source_host}
TARGET_HOST=${2:?target_host}
SOURCE_DB=${3:?source_db}
TARGET_DB=${4:?target_db}
INCLUDE_SCHEMAS=${5:-"'public'"}   # e.g. "'public','myschema'"

# Gather .sql files in current dir
SQL_FILES=()
for f in *.sql; do
  [[ -f "$f" ]] && SQL_FILES+=("$f")
done
if [ ${#SQL_FILES[@]} -eq 0 ]; then
  echo "No .sql files found in the current directory"
  exit 1
fi

mkdir -p source_outputs target_outputs

run_remote_sql () {
  local host=$1
  local db=$2
  local file=$3
  local out=$4

  # -X to ignore .psqlrc, -v to pass schemas, -f - to read from STDIN
  # We stream the SQL file content into the remote psql via SSH.
  if ! ssh -o BatchMode=yes "$host" "sudo -u postgres psql -X -d '$db' -v include_schemas=\"$INCLUDE_SCHEMAS\" -f -" < "$file" > "$out" 2>&1; then
    echo "Error executing $(basename "$file") on $host/$db"
    echo "See: $out"
    exit 1
  fi
}

echo "=== Running queries on source: $SOURCE_HOST/$SOURCE_DB ==="
for file in "${SQL_FILES[@]}"; do
  base=$(basename "$file" .sql)
  echo " -> $file"
  run_remote_sql "$SOURCE_HOST" "$SOURCE_DB" "$file" "source_outputs/$base.txt"
done

echo "=== Running queries on target: $TARGET_HOST/$TARGET_DB ==="
for file in "${SQL_FILES[@]}"; do
  base=$(basename "$file" .sql)
  echo " -> $file"
  run_remote_sql "$TARGET_HOST" "$TARGET_DB" "$file" "target_outputs/$base.txt"
done

echo
echo "=== Migration Comparison Report ==="
for file in "${SQL_FILES[@]}"; do
  base=$(basename "$file" .sql)
  s="source_outputs/$base.txt"
  t="target_outputs/$base.txt"
  if diff_output=$(diff -u "$s" "$t"); then
    echo "$base: MATCH"
  else
    echo "$base: DIFFERENCE"
    echo "$diff_output"
    echo "----------------------------------------"
  fi
done
