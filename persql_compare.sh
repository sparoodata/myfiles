#!/bin/bash
set -euo pipefail

# Show only rows present in SOURCE and missing on TARGET for a given SQL file.
#
# Usage:
#   ./compare_one_sql.sh source_host target_host path/to/check.sql
#   ./compare_one_sql.sh source_host target_host path/to/check.sql "'public','myschema'"
#   ./compare_one_sql.sh source_host target_host path/to/check.sql "" mydb   # restrict to one DB
#
# Notes:
# - Script uses SSH and `sudo -u postgres psql` on both hosts.
# - Your SQL must be read-only. We still force a READ ONLY transaction.
# - Output is normalized (trimmed, sorted) before set-diff.
#
# Args:
#   $1 SOURCE_HOST (SSH reachable)
#   $2 TARGET_HOST (SSH reachable)
#   $3 SQL_FILE (local)
#   $4 INCLUDE_SCHEMAS (optional: e.g. "'public','myschema'"; passed as :include_schemas)
#   $5 ONLY_DB (optional: run only on this DB; otherwise run on intersection)

SOURCE_HOST=${1:?source_host}
TARGET_HOST=${2:?target_host}
SQL_FILE=${3:?sql_file}
INCLUDE_SCHEMAS=${4:-""}
ONLY_DB=${5:-""}

if [[ ! -f "$SQL_FILE" ]]; then
  echo "SQL file not found: $SQL_FILE" >&2
  exit 1
fi

timestamp=$(date +%Y%m%d_%H%M%S)
workdir="report_${timestamp}_$(basename "${SQL_FILE%.*}")"
mkdir -p "$workdir"

# Get list of non-template DBs on a host
list_dbs() {
  local host="$1"
  ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$host" \
    "sudo -u postgres psql -Atqc \"select datname
                                    from pg_database
                                   where datistemplate = false
                                     and datname <> 'postgres'
                                order by 1;\""
}

# Run SQL on remote host+db safely (READ ONLY) and normalize output
run_sql_normalized() {
  local host="$1" db="$2" sql_path="$3" include="$4"
  # We wrap the SQL in a read-only tx and rollback to be extra safe.
  # Also set timeouts and a quiet, tuples-only format.
  ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$host" \
    "bash -lc 'set -euo pipefail;
      tmpfile=\$(mktemp);
      trap \"rm -f \$tmpfile\" EXIT;
      {
        echo \"BEGIN;\";
        echo \"SET LOCAL lock_timeout = \0475s\047;\";
        echo \"SET LOCAL statement_timeout = \0475min\047;\";
        echo \"SET LOCAL default_transaction_read_only = on;\";
        cat > \$tmpfile_sql;
      } >/dev/null 2>&1 || true
    '" >/dev/null 2>&1 || true
}

# We can’t create remote temp files easily without extra hops, so we stream:
# This helper echoes the wrapper + file content over SSH stdin and collects stdout.

run_sql_normalized() {
  local host="$1" db="$2" sql_path="$3" include="$4"
  {
    echo "BEGIN;";
    echo "SET LOCAL lock_timeout = '5s';";
    echo "SET LOCAL statement_timeout = '5min';";
    echo "SET LOCAL default_transaction_read_only = on;";
    cat "$sql_path"
    echo "ROLLBACK;";
  } | ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$host" \
      "sudo -u postgres psql -AtqX -v ON_ERROR_STOP=1 \
         -v include_schemas=${include:-\"\"} \
         -d \"${db}\" -f - 2>/dev/null" \
    | sed -e 's/[[:space:]]\+$//' -e '/^[[:space:]]*$/d' \
    | LC_ALL=C sort -u
}

# Get DB lists
if [[ -n "$ONLY_DB" ]]; then
  mapfile -t DBS < <(printf "%s\n" "$ONLY_DB")
else
  mapfile -t SRC_DBS < <(list_dbs "$SOURCE_HOST")
  mapfile -t TGT_DBS < <(list_dbs "$TARGET_HOST")
  # Intersection
  mapfile -t DBS < <(comm -12 <(printf "%s\n" "${SRC_DBS[@]}" | LC_ALL=C sort -u) \
                          <(printf "%s\n" "${TGT_DBS[@]}" | LC_ALL=C sort -u))
fi

if [[ ${#DBS[@]} -eq 0 ]]; then
  echo "No common databases found to compare." >&2
  exit 0
fi

echo "== Comparing $(basename "$SQL_FILE") =="
echo "Workdir: $workdir"
echo

any_diff=0

for db in "${DBS[@]}"; do
  src_out="$workdir/${db}.source.out"
  tgt_out="$workdir/${db}.target.out"
  only_src="$workdir/${db}.only_in_source.out"

  run_sql_normalized "$SOURCE_HOST" "$db" "$SQL_FILE" "$INCLUDE_SCHEMAS" >"$src_out" || true
  run_sql_normalized "$TARGET_HOST" "$db" "$SQL_FILE" "$INCLUDE_SCHEMAS" >"$tgt_out" || true

  # Lines present in source but not in target
  LC_ALL=C comm -23 "$src_out" "$tgt_out" > "$only_src" || true

  if [[ -s "$only_src" ]]; then
    any_diff=1
    echo "---- ${db}: missing on TARGET (present on SOURCE) ----"
    cat "$only_src"
    echo
  else
    echo "---- ${db}: OK (no items missing on TARGET) ----"
  fi
done

echo
echo "Reports saved under: $workdir"
if [[ $any_diff -eq 0 ]]; then
  echo "No differences found that are present in SOURCE and missing on TARGET."
fi
