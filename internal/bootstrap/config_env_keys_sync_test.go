// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestConfigEnvVarKeys_SyncWithStructTags uses reflection to walk the Config
// struct's `env:` tags and verifies that every env key appears in configEnvVarKeys.
// This prevents drift when new config fields are added but configEnvVarKeys isn't
// updated. Addresses the TODO in config_test_helpers_test.go.
func TestConfigEnvVarKeys_SyncWithStructTags(t *testing.T) {
	t.Parallel()

	// Build a set of all env keys from configEnvVarKeys for O(1) lookup.
	registeredKeys := make(map[string]bool, len(configEnvVarKeys))
	for _, key := range configEnvVarKeys {
		registeredKeys[key] = true
	}

	// Walk the Config struct and extract all env: tag values.
	structKeys := collectEnvTagKeys(reflect.TypeOf(Config{}))

	// Verify every struct tag env key appears in configEnvVarKeys.
	var missing []string

	for _, key := range structKeys {
		if !registeredKeys[key] {
			missing = append(missing, key)
		}
	}

	assert.Empty(t, missing,
		"env: struct tag keys missing from configEnvVarKeys — add them to config_test_helpers_test.go: %v",
		missing)

	// Also verify configEnvVarKeys doesn't contain stale keys not in struct tags.
	structKeySet := make(map[string]bool, len(structKeys))
	for _, key := range structKeys {
		structKeySet[key] = true
	}

	var stale []string

	for _, key := range configEnvVarKeys {
		if !structKeySet[key] {
			stale = append(stale, key)
		}
	}

	assert.Empty(t, stale,
		"configEnvVarKeys contains stale keys not found in Config struct env: tags — remove them: %v",
		stale)
}

func TestMatcherOverrideEnvVarKeys_SyncWithConfigSchema(t *testing.T) {
	t.Parallel()

	expectedKeys := make(map[string]bool)
	for _, def := range matcherKeyDefs() {
		expectedKeys["MATCHER_"+strings.ToUpper(strings.ReplaceAll(def.key, ".", "_"))] = true
	}

	actualKeys := make(map[string]bool, len(matcherOverrideEnvVarKeys))
	for _, key := range matcherOverrideEnvVarKeys {
		actualKeys[key] = true
	}

	var missing []string
	for key := range expectedKeys {
		if !actualKeys[key] {
			missing = append(missing, key)
		}
	}

	assert.Empty(t, missing,
		"MATCHER override keys missing from matcherOverrideEnvVarKeys — add them to config_override_env_keys_test.go: %v",
		missing)

	var stale []string
	for _, key := range matcherOverrideEnvVarKeys {
		if !expectedKeys[key] {
			stale = append(stale, key)
		}
	}

	assert.Empty(t, stale,
		"matcherOverrideEnvVarKeys contains stale keys not found in Config schema — remove them: %v",
		stale)
}

// collectEnvTagKeys recursively walks a struct type and collects all env: tag values.
func collectEnvTagKeys(t reflect.Type) []string {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil
	}

	var keys []string

	for i := range t.NumField() {
		field := t.Field(i)

		// Recurse into struct fields (skip Logger, ShutdownGracePeriod which
		// don't have env tags).
		if field.Type.Kind() == reflect.Struct &&
			field.Tag.Get("env") == "" &&
			field.Tag.Get("mapstructure") != "-" {
			keys = append(keys, collectEnvTagKeys(field.Type)...)

			continue
		}

		envTag := field.Tag.Get("env")
		if envTag == "" {
			continue
		}

		// Extract the env var name (first comma-separated token).
		envName := strings.SplitN(envTag, ",", 2)[0]
		if envName != "" {
			keys = append(keys, envName)
		}
	}

	return keys
}
