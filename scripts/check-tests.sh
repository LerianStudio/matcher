#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

has_matching_test_file() {
  local dir="$1"
  local base="$2"

  # Exact same-stem matches and explicit same-stem families, e.g.
  # config.go -> config_dsn_test.go. Do not trim arbitrary underscore
  # segments: foo_bar_baz.go must not pass because foo_test.go exists.
  compgen -G "$dir/${base}_test.go" >/dev/null && return 0
  compgen -G "$dir/${base}"'_*_test.go' >/dev/null && return 0

  local grouped_prefixes=(
    audit_log
    archival
    callback
    config
    confirmable_publisher
    context
    dashboard
    dispute
    exception
    export
    export_job
    extraction
    fee_schedule.postgresql
    field_map
    handlers
    health_check
    http_connector
    init
    match_rule
    middleware
    queries
    report
    rule_config_decode
    systemplane_keys
    transaction
  )

  local grouped_prefix
  for grouped_prefix in "${grouped_prefixes[@]}"; do
    if [[ "$base" == "$grouped_prefix" || "$base" == "${grouped_prefix}_"* ]]; then
      compgen -G "$dir/${grouped_prefix}*_test.go" >/dev/null && return 0
    fi
  done

  # Explicit grouped-test conventions used by matcher packages. Note: the
  # config|handlers|health_check|init prefixes are already covered by the
  # grouped_prefixes loop above; only suffix-rewriting cases (queries/commands)
  # need bespoke logic here.
  case "$base" in
    *_queries.*)
      local query_prefix="${base%%_queries.*}"
      local query_suffix="${base#*_queries.}"
      compgen -G "$dir/${query_prefix}.${query_suffix}*_test.go" >/dev/null && return 0
      ;;
    *_commands.*)
      local command_prefix="${base%%_commands.*}"
      local command_suffix="${base#*_commands.}"
      compgen -G "$dir/${command_prefix}.${command_suffix}*_test.go" >/dev/null && return 0
      ;;
  esac

  # Explicit package-local groups where behavior is intentionally tested under
  # domain operation names rather than file stems. Keep this narrow: it prevents
  # unrelated command tests from masking a newly added source file.
  case "$base" in
    context_commands)
      compgen -G "$dir/commands_test.go" >/dev/null && return 0
      ;;
    dedupe_commands)
      compgen -G "$dir/helpers_dedupe_test.go" >/dev/null && return 0
      compgen -G "$dir/usecase_construction_test.go" >/dev/null && return 0
      ;;
    lifecycle_commands)
      compgen -G "$dir/ignore_transaction_test.go" >/dev/null && return 0
      compgen -G "$dir/cleanup_test.go" >/dev/null && return 0
      ;;
    upload_commands)
      compgen -G "$dir/start_ingestion*_test.go" >/dev/null && return 0
      ;;
  esac

  # Entry-point files often group behaviour under themed command/query test
  # files instead of a single exact-basename companion. Do not apply this to
  # entity-specific files like payment_commands.go; those must be covered by
  # payment_commands*_test.go to avoid unrelated command tests masking gaps.
  case "$base" in
    commands)
      compgen -G "$dir/*commands*_test.go" >/dev/null && return 0
      ;;
    queries)
      compgen -G "$dir/*queries*_test.go" >/dev/null && return 0
      ;;
  esac

  return 1
}

main() {
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
    if ! has_matching_test_file "$dir" "$base"; then
      missing+=("$dir/${base}_test.go (for $file)")
    fi
  done

  if [ ${#missing[@]} -ne 0 ]; then
    printf "Missing _test.go files for %d source files:\n" "${#missing[@]}"
    printf " - %s\n" "${missing[@]}"
    exit 1
  fi

  echo "All .go files have corresponding _test.go files."
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
