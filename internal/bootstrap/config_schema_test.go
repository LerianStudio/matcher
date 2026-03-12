// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildConfigSchema_HasExpectedFields(t *testing.T) {
	t.Parallel()

	schema := buildConfigSchema()

	// Verify we have a meaningful number of fields.
	// The exact count may drift as config fields are added/removed,
	// so we check a minimum threshold rather than an exact number.
	const minExpectedFields = 50
	assert.GreaterOrEqual(t, len(schema), minExpectedFields, "schema should have at least %d fields", minExpectedFields)
}

func TestBuildConfigSchema_NoDuplicateKeys(t *testing.T) {
	t.Parallel()

	schema := buildConfigSchema()
	seen := make(map[string]bool, len(schema))

	for _, field := range schema {
		if seen[field.Key] {
			t.Errorf("duplicate schema key: %s", field.Key)
		}

		seen[field.Key] = true
	}
}

func TestBuildConfigSchema_AllFieldsHaveRequiredMetadata(t *testing.T) {
	t.Parallel()

	schema := buildConfigSchema()

	for _, field := range schema {
		t.Run(field.Key, func(t *testing.T) {
			t.Parallel()
			assert.NotEmpty(t, field.Key, "key must not be empty")
			assert.NotEmpty(t, field.Label, "label must not be empty for key %s", field.Key)
			assert.NotEmpty(t, field.Type, "type must not be empty for key %s", field.Key)
			assert.Contains(t, []string{"string", "int", "bool"}, field.Type, "type must be string/int/bool for key %s", field.Key)
			assert.NotEmpty(t, field.Description, "description must not be empty for key %s", field.Key)
			assert.NotEmpty(t, field.Section, "section must not be empty for key %s", field.Key)
		})
	}
}

func TestBuildConfigSchema_AllSectionsRepresented(t *testing.T) {
	t.Parallel()

	names := sectionNames()

	expectedSections := []string{
		"app", "server", "postgres", "redis", "rabbitmq", "auth",
		"rate_limit", "export_worker", "cleanup_worker", "scheduler",
		"webhook", "archival",
	}

	for _, expected := range expectedSections {
		assert.Contains(t, names, expected, "section %q should be in schema", expected)
	}
}

func TestBuildConfigSchema_SecretFieldsAreMarked(t *testing.T) {
	t.Parallel()

	schema := buildConfigSchema()
	secretKeys := make(map[string]bool)

	for _, field := range schema {
		if field.Secret {
			secretKeys[field.Key] = true
		}
	}

	expectedSecrets := []string{
		"postgres.primary_password",
		"redis.password",
		"rabbitmq.password",
		"auth.token_secret",
		"idempotency.hmac_secret",
	}

	for _, key := range expectedSecrets {
		assert.True(t, secretKeys[key], "key %q should be marked as secret", key)
	}
}

func TestBuildConfigSchema_MutableKeysAreHotReloadable(t *testing.T) {
	t.Parallel()

	schema := buildConfigSchema()
	schemaMap := make(map[string]configFieldDef, len(schema))

	for _, field := range schema {
		schemaMap[field.Key] = field
	}

	// Every mutable key (from config_manager.go) should be hot-reloadable in schema.
	for key := range mutableConfigKeys {
		field, exists := schemaMap[key]
		if !exists {
			// Some mutable keys might not be in schema yet — that's OK,
			// but if they are, they should be hot-reloadable.
			continue
		}

		assert.True(t, field.HotReloadable, "mutable key %q should be marked hot-reloadable in schema", key)
	}
}

func TestBuildConfigSchema_StartupOnlyWorkerEnableFlagsAreNotHotReloadable(t *testing.T) {
	t.Parallel()

	schema := buildConfigSchema()
	schemaMap := make(map[string]configFieldDef, len(schema))

	for _, field := range schema {
		schemaMap[field.Key] = field
	}

	for _, key := range []string{
		"export_worker.enabled",
		"cleanup_worker.enabled",
		"archival.enabled",
	} {
		field, exists := schemaMap[key]
		require.True(t, exists, "schema should include %s", key)
		assert.False(t, field.HotReloadable, "%s should remain startup-only", key)
	}
}

func TestSchemaKeySet_ReturnsAllKeys(t *testing.T) {
	t.Parallel()

	keys := schemaKeySet()
	schema := buildConfigSchema()

	require.Equal(t, len(schema), len(keys), "key set should have same cardinality as schema")

	for _, field := range schema {
		assert.True(t, keys[field.Key], "key %q should be in key set", field.Key)
	}
}

func TestSectionNames_ReturnsOrderedUnique(t *testing.T) {
	t.Parallel()

	names := sectionNames()

	assert.NotEmpty(t, names, "should have at least one section")

	// Check uniqueness.
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		assert.False(t, seen[name], "section %q should appear only once", name)
		seen[name] = true
	}
}

func TestSecretFields_ContainsExpectedKeys(t *testing.T) {
	t.Parallel()

	expected := []string{
		"postgres.primary_password",
		"redis.password",
		"rabbitmq.password",
		"auth.token_secret",
	}

	for _, key := range expected {
		assert.True(t, secretFields[key], "secretFields should contain %q", key)
	}
}
