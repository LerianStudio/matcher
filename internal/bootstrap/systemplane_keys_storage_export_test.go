//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// --- matcherKeyDefsStorageExport ---

func TestMatcherKeyDefsStorageExport_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsStorageExport()

	require.NotEmpty(t, defs, "matcherKeyDefsStorageExport must return at least one key definition")
}

func TestMatcherKeyDefsStorageExport_CombinesSubGroups(t *testing.T) {
	t.Parallel()

	dedup := matcherKeyDefsDeduplication()
	objStorage := matcherKeyDefsObjectStorage()
	exportWorker := matcherKeyDefsExportWorker()

	combined := matcherKeyDefsStorageExport()

	assert.Len(t, combined, len(dedup)+len(objStorage)+len(exportWorker),
		"matcherKeyDefsStorageExport must combine deduplication + object storage + export worker")
}

// --- matcherKeyDefsDeduplication ---

func TestMatcherKeyDefsDeduplication_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsDeduplication()

	require.Len(t, defs, 1)

	def := defs[0]
	assert.Equal(t, "deduplication.ttl_sec", def.Key)
	assert.Equal(t, domain.KindConfig, def.Kind)
	assert.Equal(t, "deduplication", def.Group)
	assert.Equal(t, domain.ValueTypeInt, def.ValueType)
	assert.Equal(t, domain.ApplyLiveRead, def.ApplyBehavior)
	assert.True(t, def.MutableAtRuntime)
	assert.NotNil(t, def.Validator, "deduplication.ttl_sec must have a validator")
	assert.NotEmpty(t, def.Description)
	require.Len(t, def.AllowedScopes, 1)
	assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
}

// --- matcherKeyDefsObjectStorage ---

func TestMatcherKeyDefsObjectStorage_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsObjectStorage()

	require.NotEmpty(t, defs)
}

func TestMatcherKeyDefsObjectStorage_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsObjectStorage()

	expectedKeys := []string{
		"object_storage.endpoint",
		"object_storage.region",
		"object_storage.bucket",
		"object_storage.access_key_id",
		"object_storage.secret_access_key",
		"object_storage.use_path_style",
	}

	require.Len(t, defs, len(expectedKeys))

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "object_storage", def.Group)
			assert.Equal(t, "s3", def.Component)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
			require.Len(t, def.AllowedScopes, 1)
			assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
		})
	}
}

func TestMatcherKeyDefsObjectStorage_SecretFields(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsObjectStorage()

	secretKeys := map[string]bool{
		"object_storage.access_key_id":     true,
		"object_storage.secret_access_key": true,
	}

	for _, def := range defs {
		if secretKeys[def.Key] {
			t.Run(def.Key+"_is_secret", func(t *testing.T) {
				t.Parallel()

				assert.True(t, def.Secret, "%s must be marked as secret", def.Key)
				assert.Equal(t, domain.RedactFull, def.RedactPolicy, "%s must use full redaction", def.Key)
			})
		}
	}
}

func TestMatcherKeyDefsObjectStorage_EndpointHasHTTPSValidator(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsObjectStorage()
	require.NotEmpty(t, defs)

	endpointDef := defs[0]
	assert.Equal(t, "object_storage.endpoint", endpointDef.Key)
	assert.NotNil(t, endpointDef.Validator, "object_storage.endpoint must have a validator")
}

// --- matcherKeyDefsExportWorker ---

func TestMatcherKeyDefsExportWorker_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsExportWorker()

	require.NotEmpty(t, defs)
}

func TestMatcherKeyDefsExportWorker_KeyProperties(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsExportWorker()

	expectedKeys := []string{
		"export_worker.enabled",
		"export_worker.poll_interval_sec",
		"export_worker.page_size",
		"export_worker.presign_expiry_sec",
	}

	require.Len(t, defs, len(expectedKeys))

	for i, expKey := range expectedKeys {
		t.Run(expKey, func(t *testing.T) {
			t.Parallel()

			def := defs[i]
			assert.Equal(t, expKey, def.Key)
			assert.Equal(t, domain.KindConfig, def.Kind)
			assert.Equal(t, "export_worker", def.Group)
			assert.True(t, def.MutableAtRuntime)
			assert.NotEmpty(t, def.Description)
			require.Len(t, def.AllowedScopes, 1)
			assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0])
		})
	}
}

func TestMatcherKeyDefsExportWorker_EnabledIsBool(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsExportWorker()
	require.NotEmpty(t, defs)

	assert.Equal(t, "export_worker.enabled", defs[0].Key)
	assert.Equal(t, domain.ValueTypeBool, defs[0].ValueType)
}

func TestMatcherKeyDefsExportWorker_IntFieldsHaveValidators(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefsExportWorker()

	for _, def := range defs {
		if def.ValueType == domain.ValueTypeInt {
			t.Run(def.Key+"_has_validator", func(t *testing.T) {
				t.Parallel()

				assert.NotNil(t, def.Validator, "%s must have a validator", def.Key)
			})
		}
	}
}
