#!/usr/bin/env bash
# Validates migration file integrity:
#   1. Every .up.sql has a matching .down.sql and vice versa
#   2. Filenames follow NNNNNN_description.{up,down}.sql convention
#   3. Migration numbering is sequential with no gaps or duplicates
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIGRATE_DIR="${ROOT}/migrations"

if [ ! -d "$MIGRATE_DIR" ]; then
  echo "ERROR: migrations directory not found at $MIGRATE_DIR"
  exit 1
fi

errors=()

# -------------------------------------------------------
# 1. Check that every .up.sql has a matching .down.sql
# -------------------------------------------------------
for up_file in "$MIGRATE_DIR"/*.up.sql; do
  [ -f "$up_file" ] || continue
  base="$(basename "$up_file" .up.sql)"
  down_file="$MIGRATE_DIR/${base}.down.sql"
  if [ ! -f "$down_file" ]; then
    errors+=("Missing .down.sql for: ${base}.up.sql")
  fi
done

for down_file in "$MIGRATE_DIR"/*.down.sql; do
  [ -f "$down_file" ] || continue
  base="$(basename "$down_file" .down.sql)"
  up_file="$MIGRATE_DIR/${base}.up.sql"
  if [ ! -f "$up_file" ]; then
    errors+=("Missing .up.sql for: ${base}.down.sql")
  fi
done

# -------------------------------------------------------
# 2. Validate filename format (NNNNNN_description.{up,down}.sql)
# -------------------------------------------------------
for f in "$MIGRATE_DIR"/*.up.sql "$MIGRATE_DIR"/*.down.sql; do
  [ -f "$f" ] || continue
  basename="$(basename "$f")"
  if [[ ! $basename =~ ^[0-9]{6}_.*\.(up|down)\.sql$ ]]; then
    errors+=("Migration file '${basename}' does not follow the naming convention (NNNNNN_description.{up,down}.sql)")
  fi
done

# If any filenames are malformed, stop early — sequential numbering checks
# rely on the 6-digit prefix being valid and would produce confusing errors.
if [ ${#errors[@]} -ne 0 ]; then
  printf "Migration validation failed with %d error(s):\n" "${#errors[@]}"
  for err in "${errors[@]}"; do
    printf "  - %s\n" "$err"
  done
  exit 1
fi

# -------------------------------------------------------
# 3. Check sequential numbering (no gaps, no duplicates)
# -------------------------------------------------------
# Extract unique migration numbers from .up.sql files, sorted numerically.
numbers=()
while IFS= read -r line; do
  numbers+=("$line")
done < <(
  find "$MIGRATE_DIR" -maxdepth 1 -name "*.up.sql" -exec basename {} \; \
    | sed 's/_.*//' \
    | sort -n \
    | uniq
)

if [ ${#numbers[@]} -eq 0 ]; then
  echo "WARNING: No migration files found in $MIGRATE_DIR"
  exit 0
fi

# Check for duplicates (same number appearing more than once in .up.sql files)
all_numbers=()
while IFS= read -r line; do
  all_numbers+=("$line")
done < <(
  find "$MIGRATE_DIR" -maxdepth 1 -name "*.up.sql" -exec basename {} \; \
    | sed 's/_.*//' \
    | sort -n
)

prev=""
for num in "${all_numbers[@]}"; do
  if [ "$num" = "$prev" ]; then
    errors+=("Duplicate migration number: $num")
  fi
  prev="$num"
done

# Check sequential (starting from 1, incrementing by 1)
expected=1
for num in "${numbers[@]}"; do
  # Strip leading zeros for numeric comparison
  actual=$((10#$num))
  if [ "$actual" -ne "$expected" ]; then
    errors+=("Gap in migration numbering: expected $(printf '%06d' $expected), found $num")
    # Adjust expected to continue checking from actual position
    expected=$((actual + 1))
  else
    expected=$((expected + 1))
  fi
done

# -------------------------------------------------------
# Report results
# -------------------------------------------------------
if [ ${#errors[@]} -ne 0 ]; then
  printf "Migration validation failed with %d error(s):\n" "${#errors[@]}"
  for err in "${errors[@]}"; do
    printf "  - %s\n" "$err"
  done
  exit 1
fi

echo "All migration files are valid (${#numbers[@]} migrations, paired and sequential)."
exit 0
