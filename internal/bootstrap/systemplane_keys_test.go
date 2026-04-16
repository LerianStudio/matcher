//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/registry"
)

// expectedTotalKeys is the total number of registered keys.
// RegisterMatcherKeys. This constant MUST be updated when keys are added or
// removed from matcherKeyDefs.
// T-003 added 3 fetcher keys: max_extraction_bytes (BootstrapOnly),
// bridge_interval_sec (WorkerReconcile), bridge_batch_size (WorkerReconcile).
// T-004 added 1 fetcher key: bridge_stale_threshold_sec (LiveRead).
const expectedTotalKeys = 139

const expectedConfigKeyCount = 125

const expectedSettingKeyCount = 14

// expectedBootstrapOnlyCount is the count of keys with ApplyBootstrapOnly.
const expectedBootstrapOnlyCount = 25

// expectedLiveReadCount is the count of keys with ApplyLiveRead.
const expectedLiveReadCount = 22

// expectedWorkerReconcileCount is the count of keys with ApplyWorkerReconcile.
const expectedWorkerReconcileCount = 15

// expectedBundleRebuildCount is the count of keys with ApplyBundleRebuild.
const expectedBundleRebuildCount = 70

// expectedBundleRebuildAndReconcileCount is the count of keys with ApplyBundleRebuildAndReconcile.
const expectedBundleRebuildAndReconcileCount = 7

// expectedSecretKeyCount is the number of keys marked Secret=true.
const expectedSecretKeyCount = 12

func TestRegisterMatcherKeys_Success(t *testing.T) {
	t.Parallel()

	reg := registry.New()

	err := RegisterMatcherKeys(reg)

	require.NoError(t, err)
}

func TestRegisterMatcherKeys_KeyCount(t *testing.T) {
	t.Parallel()

	reg := registry.New()

	err := RegisterMatcherKeys(reg)
	require.NoError(t, err)

	allKeys := append(reg.List(domain.KindConfig), reg.List(domain.KindSetting)...)

	assert.Len(t, allKeys, expectedTotalKeys,
		"total registered key count mismatch; update expectedTotalKeys if keys were added/removed")
}

func TestRegisterMatcherKeys_ConfigAndSettingCounts(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	assert.Len(t, reg.List(domain.KindConfig), expectedConfigKeyCount)
	assert.Len(t, reg.List(domain.KindSetting), expectedSettingKeyCount)
}

func TestRegisterMatcherKeys_BootstrapOnlyCount(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	count := countByApplyBehavior(reg, domain.ApplyBootstrapOnly)

	assert.Equal(t, expectedBootstrapOnlyCount, count,
		"bootstrap-only key count mismatch")
}

func TestRegisterMatcherKeys_LiveReadCount(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	count := countByApplyBehavior(reg, domain.ApplyLiveRead)

	assert.Equal(t, expectedLiveReadCount, count,
		"live-read key count mismatch")
}

func TestRegisterMatcherKeys_WorkerReconcileCount(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	count := countByApplyBehavior(reg, domain.ApplyWorkerReconcile)

	assert.Equal(t, expectedWorkerReconcileCount, count,
		"worker-reconcile key count mismatch")
}

func TestRegisterMatcherKeys_BundleRebuildCount(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	count := countByApplyBehavior(reg, domain.ApplyBundleRebuild)

	assert.Equal(t, expectedBundleRebuildCount, count,
		"bundle-rebuild key count mismatch")
}

func TestRegisterMatcherKeys_BundleRebuildAndReconcileCount(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	count := countByApplyBehavior(reg, domain.ApplyBundleRebuildAndReconcile)

	assert.Equal(t, expectedBundleRebuildAndReconcileCount, count,
		"bundle-rebuild+worker-reconcile key count mismatch")
}

func TestRegisterMatcherKeys_ConfigKeysRemainGlobal(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	for _, def := range reg.List(domain.KindConfig) {
		require.Len(t, def.AllowedScopes, 1,
			"config key %q must have exactly one allowed scope", def.Key)
		assert.Equal(t, domain.ScopeGlobal, def.AllowedScopes[0],
			"config key %q must have ScopeGlobal", def.Key)
	}
}

func TestRegisterMatcherKeys_SettingsUseExpectedScopes(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	for _, def := range reg.List(domain.KindSetting) {
		switch def.Key {
		case "rate_limit.enabled", "export_worker.presign_expiry_sec", "archival.presign_expiry_sec":
			assert.Equal(t, []domain.Scope{domain.ScopeGlobal}, def.AllowedScopes,
				"setting key %q must remain global-only", def.Key)
		default:
			assert.Equal(t, []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant}, def.AllowedScopes,
				"setting key %q must support global and tenant scopes", def.Key)
		}
	}
}

func TestRegisterMatcherKeys_SelectedApplyBehaviors(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	tests := []struct {
		key      string
		kind     domain.Kind
		behavior domain.ApplyBehavior
		mutable  bool
	}{
		{key: "app.log_level", kind: domain.KindConfig, behavior: domain.ApplyLiveRead, mutable: true},
		{key: "cors.allowed_origins", kind: domain.KindConfig, behavior: domain.ApplyLiveRead, mutable: true},
		{key: "postgres.query_timeout_sec", kind: domain.KindConfig, behavior: domain.ApplyLiveRead, mutable: true},
		{key: "idempotency.retry_window_sec", kind: domain.KindSetting, behavior: domain.ApplyLiveRead, mutable: true},
		{key: "idempotency.success_ttl_hours", kind: domain.KindSetting, behavior: domain.ApplyLiveRead, mutable: true},
		{key: "webhook.timeout_sec", kind: domain.KindSetting, behavior: domain.ApplyLiveRead, mutable: true},
		{key: "tenancy.default_tenant_id", kind: domain.KindConfig, behavior: domain.ApplyBootstrapOnly, mutable: false},
		{key: "tenancy.default_tenant_slug", kind: domain.KindConfig, behavior: domain.ApplyBootstrapOnly, mutable: false},
		{key: "server.body_limit_bytes", kind: domain.KindConfig, behavior: domain.ApplyBundleRebuild, mutable: true},
		{key: "postgres.migrations_path", kind: domain.KindConfig, behavior: domain.ApplyBootstrapOnly, mutable: false},
		{key: "idempotency.hmac_secret", kind: domain.KindConfig, behavior: domain.ApplyBootstrapOnly, mutable: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.key, func(t *testing.T) {
			t.Parallel()

			def, ok := reg.Get(tt.key)
			require.True(t, ok)
			assert.Equal(t, tt.kind, def.Kind)
			assert.Equal(t, tt.behavior, def.ApplyBehavior)
			assert.Equal(t, tt.mutable, def.MutableAtRuntime)
		})
	}
}

func TestRegisterMatcherKeys_ApplyBehaviorCountsAddUp(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	total := countByApplyBehavior(reg, domain.ApplyBootstrapOnly) +
		countByApplyBehavior(reg, domain.ApplyLiveRead) +
		countByApplyBehavior(reg, domain.ApplyWorkerReconcile) +
		countByApplyBehavior(reg, domain.ApplyBundleRebuild) +
		countByApplyBehavior(reg, domain.ApplyBundleRebuildAndReconcile)

	assert.Equal(t, expectedTotalKeys, total,
		"sum of per-behavior counts must equal total key count")
}

func TestRegisterMatcherKeys_NoDuplicates(t *testing.T) {
	t.Parallel()

	reg := registry.New()

	err := RegisterMatcherKeys(reg)
	require.NoError(t, err)

	// Registering a second time must fail because all keys already exist.
	err = RegisterMatcherKeys(reg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
}

func TestRegisterMatcherKeys_ValidatorPositiveInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   any
		wantErr bool
	}{
		{name: "positive", value: 42, wantErr: false},
		{name: "zero", value: 0, wantErr: true},
		{name: "negative", value: -1, wantErr: true},
		{name: "large_positive", value: 100000, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validatePositiveInt(tt.value)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, domain.ErrValueInvalid)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterMatcherKeys_ValidatorNonNegativeInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   any
		wantErr bool
	}{
		{name: "positive", value: 42, wantErr: false},
		{name: "zero", value: 0, wantErr: false},
		{name: "negative", value: -1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateNonNegativeInt(tt.value)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, domain.ErrValueInvalid)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterMatcherKeys_ValidatorLogLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   any
		wantErr bool
	}{
		{name: "debug", value: "debug", wantErr: false},
		{name: "info", value: "info", wantErr: false},
		{name: "warn", value: "warn", wantErr: false},
		{name: "error", value: "error", wantErr: false},
		{name: "DEBUG_uppercase", value: "DEBUG", wantErr: false},
		{name: "invalid_trace", value: "trace", wantErr: true},
		{name: "invalid_empty", value: "", wantErr: true},
		{name: "invalid_bogus", value: "bogus", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateLogLevel(tt.value)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, domain.ErrValueInvalid)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterMatcherKeys_ValidatorSSLMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   any
		wantErr bool
	}{
		{name: "disable", value: "disable", wantErr: false},
		{name: "require", value: "require", wantErr: false},
		{name: "verify_ca", value: "verify-ca", wantErr: false},
		{name: "verify_full", value: "verify-full", wantErr: false},
		{name: "invalid_empty", value: "", wantErr: true},
		{name: "invalid_prefer", value: "prefer", wantErr: true},
		{name: "invalid_bogus", value: "bogus", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateSSLMode(tt.value)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, domain.ErrValueInvalid)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterMatcherKeys_ValidatorOptionalSSLMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   any
		wantErr bool
	}{
		{name: "empty_allowed", value: "", wantErr: false},
		{name: "disable", value: "disable", wantErr: false},
		{name: "require", value: "require", wantErr: false},
		{name: "invalid_bogus", value: "bogus", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateOptionalSSLMode(tt.value)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, domain.ErrValueInvalid)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterMatcherKeys_ValidatorNonEmptyString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   any
		wantErr bool
	}{
		{name: "valid", value: "localhost", wantErr: false},
		{name: "empty", value: "", wantErr: true},
		{name: "whitespace_only", value: "   ", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateNonEmptyString(tt.value)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, domain.ErrValueInvalid)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterMatcherKeys_SecretKeysHaveRedactFull(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	allKeys := reg.List(domain.KindConfig)

	for _, def := range allKeys {
		if def.Secret {
			assert.Equal(t, domain.RedactFull, def.RedactPolicy,
				"secret key %q must have RedactFull policy", def.Key)
		}
	}
}

func TestRegisterMatcherKeys_NonSecretKeysHaveRedactNone(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	allKeys := reg.List(domain.KindConfig)

	for _, def := range allKeys {
		if !def.Secret {
			assert.Equal(t, domain.RedactNone, def.RedactPolicy,
				"non-secret key %q must have RedactNone policy", def.Key)
		}
	}
}

func TestRegisterMatcherKeys_AllKeysHaveDescription(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	allKeys := reg.List(domain.KindConfig)

	for _, def := range allKeys {
		assert.NotEmpty(t, def.Description,
			"key %q must have a non-empty description", def.Key)
	}
}

func TestRegisterMatcherKeys_AllKeysHaveGroup(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	allKeys := reg.List(domain.KindConfig)

	for _, def := range allKeys {
		assert.NotEmpty(t, def.Group,
			"key %q must have a non-empty group", def.Key)
	}
}

func TestRegisterMatcherKeys_BootstrapOnlyKeysAreImmutable(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefs()

	for _, def := range defs {
		if def.ApplyBehavior == domain.ApplyBootstrapOnly {
			assert.False(t, def.MutableAtRuntime,
				"bootstrap-only key %q must have MutableAtRuntime=false", def.Key)
		}
	}
}

func TestRegisterMatcherKeys_RuntimeKeysAreMutable(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefs()

	for _, def := range defs {
		if def.ApplyBehavior != domain.ApplyBootstrapOnly {
			assert.True(t, def.MutableAtRuntime,
				"runtime key %q (behavior=%s) must have MutableAtRuntime=true", def.Key, def.ApplyBehavior)
		}
	}
}

func TestRegisterMatcherKeys_NoDuplicateKeys(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefs()
	seen := make(map[string]bool, len(defs))

	for _, def := range defs {
		assert.False(t, seen[def.Key],
			"duplicate key definition: %q", def.Key)

		seen[def.Key] = true
	}
}

func TestRegisterMatcherKeys_SecretKeyCount(t *testing.T) {
	t.Parallel()

	defs := matcherKeyDefs()

	secretCount := 0

	for _, def := range defs {
		if def.Secret {
			secretCount++
		}
	}

	assert.Equal(t, expectedSecretKeyCount, secretCount, "secret key count mismatch")
}

func TestRegisterMatcherKeys_ValidatorIntFromFloat64(t *testing.T) {
	t.Parallel()

	// JSON unmarshalling delivers numbers as float64. Validators must handle this.
	require.NoError(t, validatePositiveInt(float64(42)))
	require.Error(t, validatePositiveInt(float64(-1)))
	require.Error(t, validatePositiveInt(float64(0)))

	// Non-integer float64 should fail.
	require.Error(t, validatePositiveInt(3.14))
}

func TestRegisterMatcherKeys_ValidatorRejectsWrongType(t *testing.T) {
	t.Parallel()

	err := validatePositiveInt("not-a-number")
	require.Error(t, err)
	require.ErrorIs(t, err, domain.ErrValueInvalid)

	err = validateLogLevel(42)
	require.Error(t, err)
	require.ErrorIs(t, err, domain.ErrValueInvalid)

	err = validateSSLMode(true)
	require.Error(t, err)
	require.ErrorIs(t, err, domain.ErrValueInvalid)

	err = validateNonEmptyString(42)
	require.Error(t, err)
	require.ErrorIs(t, err, domain.ErrValueInvalid)
}

func TestRegisterMatcherKeys_RegistryValidation(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	// Verify that the registry's Validate method delegates to our custom validators.
	// app.log_level has a log level validator.
	require.NoError(t, reg.Validate("app.log_level", "debug"))
	require.Error(t, reg.Validate("app.log_level", "trace"))

	// postgres.primary_ssl_mode has an SSL mode validator.
	require.NoError(t, reg.Validate("postgres.primary_ssl_mode", "require"))
	require.Error(t, reg.Validate("postgres.primary_ssl_mode", "prefer"))

	// rate_limit.max has a positive int validator.
	require.NoError(t, reg.Validate("rate_limit.max", 100))
	require.Error(t, reg.Validate("rate_limit.max", 0))
	require.NoError(t, reg.Validate("webhook.timeout_sec", 300))
	require.Error(t, reg.Validate("webhook.timeout_sec", 301))
	require.NoError(t, reg.Validate("export_worker.presign_expiry_sec", 604800))
	require.Error(t, reg.Validate("export_worker.presign_expiry_sec", 604801))
	require.NoError(t, reg.Validate("archival.presign_expiry_sec", 604800))
	require.Error(t, reg.Validate("archival.presign_expiry_sec", 604801))

	// redis.db has a non-negative int validator.
	require.NoError(t, reg.Validate("redis.db", 0))
	require.Error(t, reg.Validate("redis.db", -1))
}

// countByApplyBehavior counts the number of registered keys with the given apply behavior.
func countByApplyBehavior(reg registry.Registry, behavior domain.ApplyBehavior) int {
	allKeys := append(reg.List(domain.KindConfig), reg.List(domain.KindSetting)...)

	count := 0

	for _, def := range allKeys {
		if def.ApplyBehavior == behavior {
			count++
		}
	}

	return count
}
