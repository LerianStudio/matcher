#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/check-tests.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

touch "$tmp_dir/exact_test.go"
touch "$tmp_dir/config_dsn_test.go"
touch "$tmp_dir/handlers_test.go"
touch "$tmp_dir/exception.postgresql_test.go"
touch "$tmp_dir/commands_create_test.go"
touch "$tmp_dir/foo_test.go"
touch "$tmp_dir/dispute_commands_test.go"
touch "$tmp_dir/start_ingestion_setup_test.go"

has_matching_test_file "$tmp_dir" "exact"
has_matching_test_file "$tmp_dir" "config"
has_matching_test_file "$tmp_dir" "config_defaults"
has_matching_test_file "$tmp_dir" "handlers_resolution"
has_matching_test_file "$tmp_dir" "exception_queries.postgresql"
has_matching_test_file "$tmp_dir" "commands"
has_matching_test_file "$tmp_dir" "dispute_commands"
has_matching_test_file "$tmp_dir" "upload_commands"

if has_matching_test_file "$tmp_dir" "foo_bar_baz"; then
  printf 'expected foo_bar_baz.go not to match foo_test.go\n' >&2
  exit 1
fi

if has_matching_test_file "$tmp_dir" "payment_commands"; then
  printf 'expected payment_commands.go not to match unrelated dispute_commands_test.go\n' >&2
  exit 1
fi

printf 'check-tests matcher self-test passed.\n'
