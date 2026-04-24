#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mapfile -t files < <(
  rg --files \
    -g "*.go" \
    -g "!*_test.go" \
    -g "!**/vendor/**" \
    -g "!**/testdata/**" \
    -g "!**/mocks/**" \
    -g "!**/mock/**" \
    -g "!**/generated/**" \
    -g "!**/gen/**" \
    -g "!**/*_gen.go" \
    -g "!**/*_generated.go" \
    -g "!**/*.pb.go" \
    -g "!**/doc.go" \
    -g "!**/pagination.go"
)

missing=()
for file in "${files[@]}"; do
  if rg -q "Code generated|DO NOT EDIT" "$file"; then
    continue
  fi

  # interface-only:skip-check-tests marker — production file whose behaviour
  # is covered by tests elsewhere (e.g. bootstrap init module signature locks
  # whose behaviour lives in init_test.go). The marker must appear as a comment
  # at the top of the file.
  if rg -q "interface-only:skip-check-tests" "$file"; then
    continue
  fi

  dir="$(dirname "$file")"
  base="$(basename "$file" .go)"
  test_file="$dir/${base}_test.go"
  if [ ! -f "$test_file" ]; then
    missing+=("$test_file (for $file)")
  fi
done

if [ ${#missing[@]} -ne 0 ]; then
  printf "Missing _test.go files for %d source files:\n" "${#missing[@]}"
  printf " - %s\n" "${missing[@]}"
  exit 1
fi

echo "All .go files have corresponding _test.go files."
