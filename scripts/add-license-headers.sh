#!/usr/bin/env bash
#
# add-license-headers.sh — prepend the Elastic License 2.0 header to any .go
# file under internal/, cmd/, pkg/, or tools/ that does not already have a
# "Copyright" marker in the first 10 lines.
#
# Usage:
#   scripts/add-license-headers.sh            # write mode: prepend headers
#   scripts/add-license-headers.sh --check    # check mode: report missing, exit 1
#
# The header follows the canonical Matcher pattern (reference:
# internal/bootstrap/init.go:1-3):
#
#     // Copyright 2025 Lerian Studio. All rights reserved.
#     // Use of this source code is governed by an Elastic License 2.0
#     // that can be found in the LICENSE.md file.
#
# For files that already begin with a `//go:build` directive, the header is
# placed BEFORE the build tag with a blank line separating them, matching the
# existing convention in the codebase (e.g., init_exception_test.go).
#
# Idempotent: re-running the script on an already-headered tree is a no-op.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="write"
if [[ "${1:-}" == "--check" ]]; then
  MODE="check"
fi

HEADER="// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file."

# Exclude patterns
# - vendor/: third-party code
# - mocks/, mock/: generated/manual mocks (often auto-regenerated)
# - generated/, gen/: auto-generated code
# - *.pb.go, *.gen.go, *_generated.go: generated code
# - tools/vendor/: vendored build tools
# - docs/: not under scanned roots
exclude_path() {
  case "$1" in
    */vendor/*|*/mocks/*|*/mock/*|*/generated/*|*/gen/*) return 0 ;;
    *.pb.go|*.gen.go|*_generated.go) return 0 ;;
  esac
  return 1
}

missing_files=()

# Discover candidate Go files under the four scanned roots.
while IFS= read -r file; do
  if exclude_path "$file"; then
    continue
  fi

  # Skip if the first 10 lines already contain "Copyright" (idempotency).
  if head -n 10 "$file" 2>/dev/null | grep -q "Copyright"; then
    continue
  fi

  missing_files+=("$file")
done < <(find internal cmd pkg tools -type f -name '*.go')

if [[ ${#missing_files[@]} -eq 0 ]]; then
  echo "[ok] all applicable Go files have a license header"
  exit 0
fi

if [[ "$MODE" == "check" ]]; then
  echo "[fail] ${#missing_files[@]} file(s) missing license header:"
  printf '  %s\n' "${missing_files[@]}"
  exit 1
fi

# Write mode — prepend the header, preserving any //go:build directive.
for file in "${missing_files[@]}"; do
  tmp="$(mktemp)"
  first_line="$(head -n 1 "$file" 2>/dev/null || true)"

  if [[ "$first_line" == //go:build* ]]; then
    # Header goes BEFORE the existing build tag; separated by a blank line.
    # Pattern matches internal/bootstrap/init_exception_test.go convention.
    {
      printf '%s\n' "$HEADER"
      printf '\n'
      cat "$file"
    } > "$tmp"
  else
    # No build tag — header goes at the very top, followed by a blank line.
    # Pattern matches internal/bootstrap/init.go convention.
    {
      printf '%s\n' "$HEADER"
      printf '\n'
      cat "$file"
    } > "$tmp"
  fi

  mv "$tmp" "$file"
done

echo "[ok] prepended license header to ${#missing_files[@]} file(s)"
