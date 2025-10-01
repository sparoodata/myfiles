#!/bin/bash
set -euo pipefail

# Usage:
#   ./compare_outputs.sh "outputs/sadepollutp_gcp@10.6.66.14" \
#                        "outputs/sadepollutp_gcp@gcpapp29lhc" \
#                        "reports_source_minus_target"
#
# It writes only "present in SOURCE, missing in TARGET" per file.

SOURCE_ROOT=${1:?Provide SOURCE root dir}
TARGET_ROOT=${2:?Provide TARGET root dir}
REPORT_ROOT=${3:-reports_source_minus_target}

mkdir -p "$REPORT_ROOT"

normalize() {
  # Normalize for stable set comparison:
  # - drop SQL comments that start with --
  # - trim leading/trailing whitespace
  # - squeeze internal whitespace to single space
  # - drop empty lines
  # - sort unique (order-insensitive compare)
  sed -E 's/--.*$//' \
  | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//' \
  | sed -E 's/[[:space:]]+/ /g' \
  | awk 'NF' \
  | LC_ALL=C sort -u
}

summary_file="$REPORT_ROOT/_summary.txt"
: > "$summary_file"

total_files=0
files_with_diffs=0
missing_on_target=0

# Iterate db folders in SOURCE
find "$SOURCE_ROOT" -mindepth 2 -maxdepth 2 -type f -name '*.txt' | while read -r src_file; do
  total_files=$((total_files+1))

  # Build relative path like "<db>/indexes.txt"
  rel_path="${src_file#"$SOURCE_ROOT/"}"
  db_dir="$(dirname "$rel_path")"
  base_file="$(basename "$rel_path")"

  tgt_file="$TARGET_ROOT/$rel_path"
  report_dir="$REPORT_ROOT/$db_dir"
  report_file="$report_dir/$base_file"

  mkdir -p "$report_dir"

  if [[ ! -f "$tgt_file" ]]; then
    # Target missing this file entirely -> everything in source is "missing on target"
    normalize < "$src_file" > "$report_file"
    if [[ -s "$report_file" ]]; then
      echo "[MISSING FILE ON TARGET] $rel_path (copied normalized SOURCE content)" >> "$summary_file"
      missing_on_target=$((missing_on_target+1))
      files_with_diffs=$((files_with_diffs+1))
    else
      rm -f "$report_file"
    fi
    continue
  fi

  # Do set difference: SOURCE minus TARGET
  tmp_src="$(mktemp)"; tmp_tgt="$(mktemp)"
  normalize < "$src_file" > "$tmp_src"
  normalize < "$tgt_file" > "$tmp_tgt"

  # comm requires sorted input; we already sorted in normalize
  comm -23 "$tmp_src" "$tmp_tgt" > "$report_file" || true

  if [[ -s "$report_file" ]]; then
    echo "[DIFF] $rel_path -> lines present in SOURCE, missing in TARGET" >> "$summary_file"
    files_with_diffs=$((files_with_diffs+1))
  else
    rm -f "$report_file"
  fi

  rm -f "$tmp_src" "$tmp_tgt"
done

{
  echo
  echo "===== Summary ====="
  echo "Source root : $SOURCE_ROOT"
  echo "Target root : $TARGET_ROOT"
  echo "Report root : $REPORT_ROOT"
  echo "Total files scanned           : $total_files"
  echo "Files with differences        : $files_with_diffs"
  echo "Files missing entirely target : $missing_on_target"
} >> "$summary_file"

echo "Done. See: $REPORT_ROOT"
echo "Quick peek: tail -n +1 $summary_file"
