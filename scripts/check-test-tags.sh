#!/usr/bin/env bash
# Verifies integration and e2e test files have required build tags
# Unit tests (in internal/) do not require build tags
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

missing=()

# Only check tests/ directory for integration/e2e tags
mapfile -t test_files < <(
  find ./tests -name "*_test.go" \
    -not -path "./vendor/*" \
    -not -path "./.git/*" 2>/dev/null || true
)

for file in "${test_files[@]}"; do
  [ -z "$file" ] && continue
  
  first_line=$(head -1 "$file")
  
  # Check if file has build tag on first line (integration or e2e)
  if ! echo "$first_line" | grep -qE "^//go:build (integration|e2e)"; then
    missing+=("$file")
  fi
done

if [ ${#missing[@]} -ne 0 ]; then
  printf "❌ Missing build tags in %d test files:\n" "${#missing[@]}"
  printf "   %s\n" "${missing[@]}"
  echo ""
  echo "   Tests in tests/ directory must have: //go:build integration | //go:build e2e"
  echo ""
  exit 1
fi

echo "✅ All integration/e2e test files have valid build tags."
exit 0
