package bootstrap

import (
	"math"
	"net/url"
	"os"
	"reflect"
	"strings"
)

// sensitiveKeyFragments lists substrings of mapstructure tags that identify
// fields containing secrets. Used by diffConfigs to redact secret values
// from config change diffs, preventing credential leakage in API responses
// and audit logs.
var sensitiveKeyFragments = []string{"password", "secret", "token", "key", "cert"}

// diffConfigs computes the list of top-level field changes between two configs.
// Uses reflection on the exported struct fields of Config. Fields tagged with
// `mapstructure:"-"` (like Logger) are skipped.
//
// Secret fields (passwords, tokens, secrets) are redacted in the diff output
// to prevent credential leakage in API responses and audit logs.
func diffConfigs(oldCfg, newCfg *Config) []ConfigChange {
	if oldCfg == nil || newCfg == nil {
		return []ConfigChange{}
	}

	var changes []ConfigChange

	oldVal := reflect.ValueOf(*oldCfg)
	newVal := reflect.ValueOf(*newCfg)
	oldType := oldVal.Type()

	for i := range oldType.NumField() {
		field := oldType.Field(i)

		// Skip unexported fields and non-config fields.
		if !field.IsExported() {
			continue
		}

		tag := field.Tag.Get("mapstructure")
		if tag == "-" || tag == "" {
			continue
		}

		oldField := oldVal.Field(i)
		newField := newVal.Field(i)

		// Compare using reflect.DeepEqual for nested structs.
		if !reflect.DeepEqual(oldField.Interface(), newField.Interface()) {
			changes = append(changes, ConfigChange{
				Key:      tag,
				OldValue: redactStructSecrets(oldField),
				NewValue: redactStructSecrets(newField),
			})
		}
	}

	return changes
}

// restoreZeroedFields restores fields in dst that loadConfigFromEnv() zeroed out.
//
// loadConfigFromEnv() uses SetConfigFromEnvVars which sets EVERY field from its
// env tag, even when the env var is absent (resulting in the zero value). This
// obliterates values from YAML/defaults. This function walks the nested structs
// and restores any field that was non-zero in snapshot but became zero in dst.
//
// When an env var is explicitly present (even as an empty string), restore is
// skipped for that field so operators can intentionally override YAML/default
// values to zero values ("", 0, false).
func restoreZeroedFields(dst, snapshot *Config) {
	if dst == nil || snapshot == nil {
		return
	}

	restoreZeroedFieldsRecursive(reflect.ValueOf(dst).Elem(), reflect.ValueOf(snapshot).Elem())
}

func restoreZeroedFieldsRecursive(dst, snapshot reflect.Value) {
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
			restoreZeroedFieldsRecursive(dstField, snapField)

			continue
		}

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

// redactStructSecrets returns a copy of a config sub-struct with sensitive
// fields (password, secret, token) replaced by "***REDACTED***". For non-struct
// values, returns the value as-is since leaf values in the diff are individual
// keys that get redacted via redactIfSensitive.
func redactStructSecrets(val reflect.Value) any {
	if val.Kind() != reflect.Struct {
		return val.Interface()
	}

	structType := val.Type()
	redacted := make(map[string]any, structType.NumField())

	for i := range structType.NumField() {
		field := structType.Field(i)
		if !field.IsExported() {
			continue
		}

		tag := field.Tag.Get("mapstructure")
		if tag == "" || tag == "-" {
			continue
		}

		fieldVal := val.Field(i)

		// Recurse into nested structs.
		if field.Type.Kind() == reflect.Struct {
			redacted[tag] = redactStructSecrets(fieldVal)
			continue
		}

		if isSensitiveKey(tag) {
			redacted[tag] = "***REDACTED***"
		} else {
			redacted[tag] = redactCredentialURI(fieldVal.Interface())
		}
	}

	return redacted
}

// redactIfSensitive returns "***REDACTED***" if the key matches a sensitive
// field pattern, otherwise returns the value unchanged.
func redactIfSensitive(key string, value any) any {
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
func safeUint64ToInt(version uint64) int {
	if version > uint64(math.MaxInt) {
		return math.MaxInt
	}

	return int(version)
}

// isSensitiveKey returns true if the given mapstructure key contains a
// sensitive fragment (password, secret, token).
func isSensitiveKey(key string) bool {
	lower := strings.ToLower(key)
	for _, frag := range sensitiveKeyFragments {
		if strings.Contains(lower, frag) {
			return true
		}
	}

	return false
}

func redactCredentialURI(value any) any {
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
