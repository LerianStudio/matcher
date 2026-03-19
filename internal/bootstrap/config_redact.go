// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"math"
	"net/url"
	"os"
	"reflect"
	"strings"
)

// sensitiveConfigKeys lists the exact config keys whose values must always be
// redacted in API responses, config diffs, and audit logs.
//
// This intentionally uses exact keys instead of substring matching. Some keys
// contain terms like "uri" or "key" without being secrets themselves
// (`rabbitmq.uri`, `server.tls_key_file`). URI-shaped values still receive
// credential redaction via redactCredentialURI.
var sensitiveConfigKeys = map[string]bool{ //nolint:unused // Used by isSensitiveKey; reachable from config_schema.go (test-only) and diffConfigs (test-only).
	"postgres.primary_password":            true,
	"postgres.replica_password":            true,
	"redis.password":                       true,
	"redis.ca_cert":                        true,
	"rabbitmq.password":                    true,
	"auth.token_secret":                    true,
	"idempotency.hmac_secret":              true,
	"object_storage.access_key_id":         true,
	"object_storage.secret_access_key":     true,
	"tenancy.multi_tenant_service_api_key": true,
}

// diffConfigs computes field-level changes between two configs.
// Uses reflection on exported struct fields of Config, recursing into
// sub-structs to produce dotted keys (e.g., "rate_limit.max") consistent
// with the API update path. Fields tagged `mapstructure:"-"` are skipped.
//
// Secret fields (passwords, tokens, secrets) are redacted in the diff output
// to prevent credential leakage in API responses and audit logs.
func diffConfigs(oldCfg, newCfg *Config) []ConfigChange { //nolint:unused // Called from config_manager_test.go and config_redact_test.go.
	if oldCfg == nil || newCfg == nil {
		return []ConfigChange{}
	}

	var changes []ConfigChange

	diffConfigsRecursive(reflect.ValueOf(*oldCfg), reflect.ValueOf(*newCfg), "", &changes)

	return changes
}

// diffConfigsRecursive walks struct fields, comparing leaf values and recursing
// into nested structs. Keys are built from mapstructure tags joined by dots.
//
// maxDiffDepth guards against hypothetical pointer cycles; Config has none today
// but the guard is cheap insurance against future struct evolution.
func diffConfigsRecursive(oldVal, newVal reflect.Value, prefix string, changes *[]ConfigChange) { //nolint:unused // Called from diffConfigs (test-only).
	const maxDiffDepth = 10

	if depthFromPrefix(prefix) > maxDiffDepth {
		return
	}

	oldType := oldVal.Type()

	for i := range oldType.NumField() {
		field := oldType.Field(i)

		if !field.IsExported() {
			continue
		}

		tag := field.Tag.Get("mapstructure")
		if tag == "-" || tag == "" {
			continue
		}

		key := tag
		if prefix != "" {
			key = prefix + "." + tag
		}

		oldField := oldVal.Field(i)
		newField := newVal.Field(i)

		// Recurse into nested structs for field-level granularity.
		if field.Type.Kind() == reflect.Struct {
			diffConfigsRecursive(oldField, newField, key, changes)
			continue
		}

		// Leaf field: compare and record if different.
		if !reflect.DeepEqual(oldField.Interface(), newField.Interface()) {
			*changes = append(*changes, ConfigChange{
				Key:      key,
				OldValue: redactIfSensitive(key, oldField.Interface()),
				NewValue: redactIfSensitive(key, newField.Interface()),
			})
		}
	}
}

// depthFromPrefix counts the nesting depth by counting dots in the key prefix.
// An empty prefix is depth 0, "rate_limit" is depth 1, "a.b.c" is depth 3.
func depthFromPrefix(prefix string) int { //nolint:unused // Called from diffConfigsRecursive (test-only).
	if prefix == "" {
		return 0
	}

	return strings.Count(prefix, ".") + 1
}

// restoreZeroedFields restores fields in dst that loadConfigFromEnv() zeroed out.
//
// loadConfigFromEnv() uses SetConfigFromEnvVars which sets EVERY field from its
// env tag, even when the env var is absent (resulting in the zero value). This
// obliterates values from defaultConfig(). This function walks the nested structs
// and restores any field that was non-zero in snapshot but became zero in dst.
//
// When an env var is explicitly present (even as an empty string), restore is
// skipped for that field so operators can intentionally override default values
// to zero values ("", 0, false).
func restoreZeroedFields(dst, snapshot *Config) {
	if dst == nil || snapshot == nil {
		return
	}

	restoreZeroedFieldsRecursive(reflect.ValueOf(dst).Elem(), reflect.ValueOf(snapshot).Elem(), 0)
}

func restoreZeroedFieldsRecursive(dst, snapshot reflect.Value, depth int) {
	// maxRestoreDepth caps recursion as cheap insurance against pointer cycles.
	// Config currently has no pointer cycles, but this guard prevents stack
	// overflow if the struct evolves to include self-referential types.
	const maxRestoreDepth = 10

	if depth > maxRestoreDepth {
		return
	}

	dstType := dst.Type()

	for i := range dstType.NumField() {
		field := dstType.Field(i)

		if !field.IsExported() {
			continue
		}

		// Skip non-config fields (Logger, ShutdownGracePeriod).
		if field.Tag.Get("mapstructure") == "-" {
			continue
		}

		dstField := dst.Field(i)
		snapField := snapshot.Field(i)

		// Recurse into embedded structs (AppConfig, ServerConfig, etc).
		if field.Type.Kind() == reflect.Struct {
			restoreZeroedFieldsRecursive(dstField, snapField, depth+1)

			continue
		}

		// Interaction with envDefault tags: lib-commons' SetConfigFromEnvVars may
		// set fields to zero when the corresponding env var is absent. The snapshot
		// comparison here restores default values that were incorrectly zeroed.
		// This relies on envDefault values in struct tags being aligned with
		// defaultConfig() values — misalignment would cause surprising behavior.
		//
		// Restore only when env var was not explicitly set for this field.
		if dstField.IsZero() && !snapField.IsZero() && !hasExplicitEnvOverride(field) {
			dstField.Set(snapField)
		}
	}
}

func hasExplicitEnvOverride(field reflect.StructField) bool {
	const envTagSplitParts = 2

	envTag := strings.TrimSpace(field.Tag.Get("env"))
	if envTag == "" {
		return false
	}

	envName := strings.TrimSpace(strings.SplitN(envTag, ",", envTagSplitParts)[0])
	if envName == "" {
		return false
	}

	_, exists := os.LookupEnv(envName)

	return exists
}

// redactIfSensitive returns "***REDACTED***" if the key matches a sensitive
// field pattern, otherwise returns the value unchanged.
func redactIfSensitive(key string, value any) any { //nolint:unused // Called from diffConfigsRecursive (test-only) and directly from tests.
	if isSensitiveKey(key) {
		return "***REDACTED***"
	}

	return redactCredentialURI(value)
}

// safeUint64ToInt converts a uint64 to int, capping at math.MaxInt to prevent
// integer overflow on 32-bit architectures. Used by config_manager.go to safely
// pass atomic version counters to structured logger fields that expect int.
// Config version counters will never reach this limit in practice, but gosec
// requires the bounds check.
func safeUint64ToInt(version uint64) int { //nolint:unused // Called from config_manager_test.go.
	if version > uint64(math.MaxInt) {
		return math.MaxInt
	}

	return int(version)
}

// isSensitiveKey returns true if the given mapstructure key contains a
// sensitive fragment (password, secret, token).
func isSensitiveKey(key string) bool { //nolint:unused // Called from config_schema.go (test-only) and redactIfSensitive.
	return sensitiveConfigKeys[strings.ToLower(key)]
}

func redactCredentialURI(value any) any { //nolint:unused // Called from redactIfSensitive and directly from tests.
	str, ok := value.(string)
	if !ok || str == "" {
		return value
	}

	parsed, err := url.Parse(str)
	if err != nil || parsed.User == nil {
		return value
	}

	if _, hasPassword := parsed.User.Password(); hasPassword {
		parsed.User = url.UserPassword("***REDACTED***", "***REDACTED***")
	} else {
		parsed.User = url.User("***REDACTED***")
	}

	return parsed.String()
}
