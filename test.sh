#!/bin/bash
set -euo pipefail

# Speed-tuned db compare
# Usage:
#   ./db_compare_all_fast.sh source_host target_host
#   NPROCS=8 ./db_compare_all_fast.sh source_host target_host "'public','myschema'"
#
# Notes:
#   - Parallelized and SSH-multiplexed
#   - Still read-only; no DDL/DML
#   - Report lists only items present on SOURCE but missing on TARGET
#     Export SHOW_EXTRA_ON_TARGET=1 to also list items present only on TARGET

SOURCE_HOST=${1:?source_host}
TARGET_HOST=${2:?target_host}
DEFAULT_INCLUDE_SCHEMAS=${3:-""}

EXCLUDE_DB_REGEX="${EXCLUDE_DB_REGEX:-^(template0|template1|postgres)$}"
EXCLUDE_SCHEMA_REGEX="${EXCLUDE_SCHEMA_REGEX:-^(pg_catalog|information_schema|pg_toast.*)$}"
SHOW_EXTRA_ON_TARGET="${SHOW_EXTRA_ON_TARGET:-0}"

# Parallelism (jobs)
NPROCS=${NPROCS:-$(command -v nproc >/dev/null 2>&1 && nproc || echo 4)}

export LC_ALL=C

# ---- Collect SQL files ----
SQL_FILES=()
for f in *.sql; do [[ -f "$f" ]] && SQL_FILES+=("$f"); done
if [ ${#SQL_FILES[@]} -eq 0 ]; then
  echo "No .sql files found in the current directory"
  exit 1
fi

# ---- SSH ControlMaster setup (multiplexing) ----
SOCKDIR="${TMPDIR:-/tmp}/ssh-cm-$$"
mkdir -p "$SOCKDIR"
SSH_BASE=(-o BatchMode=yes -o ControlMaster=auto -o ControlPersist=5m -o ControlPath="${SOCKDIR}/cm-%r@%h:%p")

# Pre-open control masters so subsequent calls are instant
ssh "${SSH_BASE[@]}" -N -f "$SOURCE_HOST" || true
ssh "${SSH_BASE[@]}" -N -f "$TARGET_HOST" || true

cleanup() {
  # Close control masters
  ssh "${SSH_BASE[@]}" -O exit "$SOURCE_HOST" >/dev/null 2>&1 || true
  ssh "${SSH_BASE[@]}" -O exit "$TARGET_HOST" >/dev/null 2>&1 || true
  # Close semaphore FD if open
  exec 3>&- 2>/dev/null || true
  rm -rf "$SOCKDIR" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ---- Helpers ----
remote_psql_list_dbs() {
  local host=$1
  ssh "${SSH_BASE[@]}" "$host" \
    "sudo -u postgres psql -At -X -c \"SELECT datname
                                       FROM pg_database
                                       WHERE datallowconn AND NOT datistemplate
                                       ORDER BY 1;\""
}

remote_psql_list_schemas() {
  local host=$1 db=$2
  ssh "${SSH_BASE[@]}" "$host" \
    "sudo -u postgres psql -At -X -d \"${db}\" -c \"SELECT nspname
                                                    FROM pg_namespace
                                                    WHERE nspname NOT IN ('pg_catalog','information_schema')
                                                      AND nspname NOT LIKE 'pg_toast%%'
                                                    ORDER BY 1;\""
}

# Generate normalized (sorted-unique) output once to speed up diffs later.
run_remote_sql_norm() {
  local host=$1 db=$2 file=$3 include_schemas=$4 outfile=$5
  {
    echo "SET lock_timeout = '5s';"
    echo "SET statement_timeout = '30min';"
    echo "SET default_transaction_read_only = on;"
    echo "BEGIN READ ONLY;"
    cat "$file"
    echo "COMMIT;"
  } | ssh "${SSH_BASE[@]}" "$host" \
        "sudo -u postgres psql -X -qAt -P pager=off --set=ON_ERROR_STOP=1 \
         -v include_schemas=\"${include_schemas}\" -d \"${db}\" -f -" \
        2>&1 | sort -u > "$outfile" || {
          echo "ERROR: $(basename "$file") on $host/$db (see $outfile)"
          return 1
        }
}

# ---- Simple semaphore for parallel jobs ----
sem_init() {
  SEM_FIFO="${TMPDIR:-/tmp}/sem.$$"
  mkfifo "$SEM_FIFO"
  exec 3<>"$SEM_FIFO"
  rm -f "$SEM_FIFO"
  for ((i=0;i<NPROCS;i++)); do echo >&3; done
}
sem_acquire() { read -u 3; }
sem_release() { echo >&3; }

mkdir -p outputs/"$SOURCE_HOST" outputs/"$TARGET_HOST"
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

# Prepare parallel tasks
sem_init
pids=()

for db in "${DBS_TO_COMPARE[@]}"; do
  echo "Processing DB: $db"
  mkdir -p "outputs/$SOURCE_HOST/$db" "outputs/$TARGET_HOST/$db"

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

    # Source job
    sem_acquire
    {
      echo "   -> $db : $(basename "$file") on $SOURCE_HOST"
      run_remote_sql_norm "$SOURCE_HOST" "$db" "$file" "$INCLUDE_SCHEMAS" "$src_out" || true
      sem_release
    } &
    pids+=($!)

    # Target job
    sem_acquire
    {
      echo "   -> $db : $(basename "$file") on $TARGET_HOST"
      run_remote_sql_norm "$TARGET_HOST" "$db" "$file" "$INCLUDE_SCHEMAS" "$tgt_out" || true
      sem_release
    } &
    pids+=($!)
  done
done

# Wait for all jobs
for pid in "${pids[@]}"; do wait "$pid"; done

# Diff section (no resorting—already normalized)
echo | tee -a "$REPORT"
echo "=== Migration Comparison Report (Target deltas only) ===" | tee -a "$REPORT"
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

    missing_on_target=$(comm -23 "$s" "$t" || true)
    extra_on_target=""
    if [[ "$SHOW_EXTRA_ON_TARGET" == "1" ]]; then
      extra_on_target=$(comm -13 "$s" "$t" || true)
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

echo "Done. Summary in: $REPORT"
