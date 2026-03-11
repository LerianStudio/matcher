#!/usr/bin/env bash
# Verifies test build tags use one of the approved values.
# - Tests under ./tests MUST declare an approved tag.
# - Co-located tests elsewhere are validated when they opt into build tags.
# Current suites use unit (static guards), integration, chaos, and e2e tags.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

missing=()

mapfile -t test_files < <(
  find . -name "*_test.go" \
    -not -path "./vendor/*" \
    -not -path "./.worktrees/*" \
    -not -path "./.git/*" 2>/dev/null || true
)

valid_tag_pattern='^//go:build (unit|integration|chaos|e2e)($|[[:space:]].*)'

for file in "${test_files[@]}"; do
  [ -z "$file" ] && continue
  
  first_line=$(head -1 "$file")
  
  if [[ "$file" == ./tests/* ]]; then
    if ! echo "$first_line" | grep -qE "$valid_tag_pattern"; then
      missing+=("$file")
    fi
    continue
  fi

  if echo "$first_line" | grep -qE "^//go:build" && \
    ! echo "$first_line" | grep -qE "$valid_tag_pattern"; then
    missing+=("$file")
  fi
done

if [ ${#missing[@]} -ne 0 ]; then
  printf "❌ Missing build tags in %d test files:\n" "${#missing[@]}"
  printf "   %s\n" "${missing[@]}"
  echo ""
  echo "   Tests in ./tests must have one of:"
  echo "     //go:build unit | //go:build integration | //go:build chaos | //go:build e2e"
  echo "   Co-located tests elsewhere may omit build tags, but if present they must use the same approved set."
  echo ""
  exit 1
fi

echo "✅ All required test build tags are valid."
exit 0
