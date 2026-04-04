//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	spdomain "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	spports "github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
	spservice "github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
)

type runtimeSettingsManagerStub struct {
	getSettings func(context.Context, spservice.Subject) (spservice.ResolvedSet, error)
	lastSubject spservice.Subject
}

func (stub *runtimeSettingsManagerStub) GetConfigs(context.Context) (spservice.ResolvedSet, error) {
	return spservice.ResolvedSet{}, nil
}

func (stub *runtimeSettingsManagerStub) GetSettings(ctx context.Context, subject spservice.Subject) (spservice.ResolvedSet, error) {
	stub.lastSubject = subject
	if stub.getSettings == nil {
		return spservice.ResolvedSet{}, nil
	}

	return stub.getSettings(ctx, subject)
}

func (stub *runtimeSettingsManagerStub) PatchConfigs(context.Context, spservice.PatchRequest) (spservice.WriteResult, error) {
	return spservice.WriteResult{}, nil
}

func (stub *runtimeSettingsManagerStub) PatchSettings(context.Context, spservice.Subject, spservice.PatchRequest) (spservice.WriteResult, error) {
	return spservice.WriteResult{}, nil
}

func (stub *runtimeSettingsManagerStub) GetConfigSchema(context.Context) ([]spservice.SchemaEntry, error) {
	return nil, nil
}

func (stub *runtimeSettingsManagerStub) GetSettingSchema(context.Context) ([]spservice.SchemaEntry, error) {
	return nil, nil
}

func (stub *runtimeSettingsManagerStub) GetConfigHistory(context.Context, spports.HistoryFilter) ([]spports.HistoryEntry, error) {
	return nil, nil
}

func (stub *runtimeSettingsManagerStub) GetSettingHistory(context.Context, spports.HistoryFilter) ([]spports.HistoryEntry, error) {
	return nil, nil
}

func (stub *runtimeSettingsManagerStub) ApplyChangeSignal(context.Context, spports.ChangeSignal) error {
	return nil
}

func (stub *runtimeSettingsManagerStub) Resync(context.Context) error {
	return nil
}

func TestRuntimeSettingsResolver_GlobalEnvOverrideMasksGlobalSetting(t *testing.T) {
	manager := &runtimeSettingsManagerStub{
		getSettings: func(_ context.Context, _ spservice.Subject) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"rate_limit.max": {
						Key:    "rate_limit.max",
						Value:  100,
						Source: "global-override",
					},
				},
			}, nil
		},
	}

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return manager })
	require.NotNil(t, resolver)

	t.Setenv("RATE_LIMIT_MAX", "900")

	resolved := resolver.rateLimit(context.Background(), RateLimitConfig{Enabled: true, Max: 900, ExpirySec: 60})
	assert.Equal(t, 900, resolved.Max)
}

func TestRuntimeSettingsResolver_TenantOverrideBeatsGlobalEnvOverride(t *testing.T) {
	manager := &runtimeSettingsManagerStub{
		getSettings: func(_ context.Context, subject spservice.Subject) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"rate_limit.max": {
						Key:    "rate_limit.max",
						Value:  100,
						Source: "tenant-override",
					},
				},
				Revision: 1,
			}, nil
		},
	}

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return manager })
	require.NotNil(t, resolver)

	t.Setenv("RATE_LIMIT_MAX", "900")

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-123")
	resolved := resolver.rateLimit(ctx, RateLimitConfig{Enabled: true, Max: 900, ExpirySec: 60})
	assert.Equal(t, 100, resolved.Max)
}

func TestRuntimeSettingsResolver_RateLimitAppliesAllTierFields(t *testing.T) {
	manager := &runtimeSettingsManagerStub{
		getSettings: func(_ context.Context, subject spservice.Subject) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"rate_limit.enabled":             {Key: "rate_limit.enabled", Value: true, Source: "tenant-override"},
					"rate_limit.max":                 {Key: "rate_limit.max", Value: 111, Source: "tenant-override"},
					"rate_limit.expiry_sec":          {Key: "rate_limit.expiry_sec", Value: 61, Source: "tenant-override"},
					"rate_limit.export_max":          {Key: "rate_limit.export_max", Value: 222, Source: "tenant-override"},
					"rate_limit.export_expiry_sec":   {Key: "rate_limit.export_expiry_sec", Value: 62, Source: "tenant-override"},
					"rate_limit.dispatch_max":        {Key: "rate_limit.dispatch_max", Value: 333, Source: "tenant-override"},
					"rate_limit.dispatch_expiry_sec": {Key: "rate_limit.dispatch_expiry_sec", Value: 63, Source: "tenant-override"},
				},
				Revision: 1,
			}, nil
		},
	}

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return manager })
	require.NotNil(t, resolver)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-123")
	resolved := resolver.rateLimit(ctx, RateLimitConfig{Enabled: true, Max: 10, ExpirySec: 60, ExportMax: 20, ExportExpirySec: 60, DispatchMax: 30, DispatchExpirySec: 60})

	assert.True(t, resolved.Enabled)
	assert.Equal(t, 111, resolved.Max)
	assert.Equal(t, 61, resolved.ExpirySec)
	assert.Equal(t, 222, resolved.ExportMax)
	assert.Equal(t, 62, resolved.ExportExpirySec)
	assert.Equal(t, 333, resolved.DispatchMax)
	assert.Equal(t, 63, resolved.DispatchExpirySec)
	assert.Equal(t, spservice.Subject{Scope: spdomain.ScopeTenant, SubjectID: "tenant-123"}, manager.lastSubject)
}

func TestRuntimeSettingsResolver_WebhookTimeoutCapsAtMaximum(t *testing.T) {
	manager := &runtimeSettingsManagerStub{
		getSettings: func(_ context.Context, _ spservice.Subject) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"webhook.timeout_sec": {Key: "webhook.timeout_sec", Value: maxWebhookTimeoutSec + 30, Source: "global-override"},
				},
			}, nil
		},
	}

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return manager })
	require.NotNil(t, resolver)

	assert.Equal(t, 300*time.Second, resolver.webhookTimeout(context.Background(), 30*time.Second))
}

func TestRuntimeSettingsResolver_PresignExpiryCapsAtMaximum(t *testing.T) {
	manager := &runtimeSettingsManagerStub{
		getSettings: func(_ context.Context, _ spservice.Subject) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"export_worker.presign_expiry_sec": {Key: "export_worker.presign_expiry_sec", Value: maxPresignExpirySec + 1, Source: "global-override"},
					"archival.presign_expiry_sec":      {Key: "archival.presign_expiry_sec", Value: maxPresignExpirySec + 1, Source: "global-override"},
				},
			}, nil
		},
	}

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return manager })
	require.NotNil(t, resolver)

	assert.Equal(t, 604800*time.Second, resolver.exportPresignExpiry(context.Background(), time.Hour))
	assert.Equal(t, 604800*time.Second, resolver.archivalPresignExpiry(context.Background(), time.Hour))
}

func TestRuntimeSettingsResolver_CachesTenantValuesBriefly(t *testing.T) {
	callCount := 0
	manager := &runtimeSettingsManagerStub{
		getSettings: func(_ context.Context, subject spservice.Subject) (spservice.ResolvedSet, error) {
			callCount++
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"callback_rate_limit.per_minute": {
						Key:    "callback_rate_limit.per_minute",
						Value:  42,
						Source: "tenant-override",
					},
				},
				Revision: 1,
			}, nil
		},
	}

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return manager })
	require.NotNil(t, resolver)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-123")
	assert.Equal(t, 42, resolver.callbackRateLimitPerMinute(ctx, 60))
	assert.Equal(t, 42, resolver.callbackRateLimitPerMinute(ctx, 60))
	assert.Equal(t, 1, callCount, "tenant settings reads should be collapsed by the resolver cache")
	assert.Equal(t, spservice.Subject{Scope: spdomain.ScopeTenant, SubjectID: "tenant-123"}, manager.lastSubject)
}

func TestRuntimeSettingsResolver_InvalidateAllClearsTenantCache(t *testing.T) {
	t.Parallel()

	callCount := 0
	currentValue := 42
	manager := &runtimeSettingsManagerStub{
		getSettings: func(_ context.Context, subject spservice.Subject) (spservice.ResolvedSet, error) {
			callCount++
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"callback_rate_limit.per_minute": {
						Key:    "callback_rate_limit.per_minute",
						Value:  currentValue,
						Source: "tenant-override",
					},
				},
				Revision: 1,
			}, nil
		},
	}

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return manager })
	require.NotNil(t, resolver)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-123")
	assert.Equal(t, 42, resolver.callbackRateLimitPerMinute(ctx, 60))
	assert.Equal(t, 1, callCount)

	currentValue = 99
	resolver.invalidateAll()

	assert.Equal(t, 99, resolver.callbackRateLimitPerMinute(ctx, 60))
	assert.Equal(t, 2, callCount, "invalidateAll should force a fresh settings read on the next request")
}

func TestRuntimeSettingsResolver_ExpiredTenantCacheForcesRefresh(t *testing.T) {
	t.Parallel()

	callCount := 0
	manager := &runtimeSettingsManagerStub{
		getSettings: func(_ context.Context, subject spservice.Subject) (spservice.ResolvedSet, error) {
			callCount++
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"callback_rate_limit.per_minute": {
						Key:    "callback_rate_limit.per_minute",
						Value:  42,
						Source: "tenant-override",
					},
				},
				Revision: 1,
			}, nil
		},
	}

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return manager })
	require.NotNil(t, resolver)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-123")
	assert.Equal(t, 42, resolver.callbackRateLimitPerMinute(ctx, 60))
	assert.Equal(t, 1, callCount)

	resolver.mu.Lock()
	entry := resolver.tenantCache["tenant-123"]
	entry.expiresAt = time.Now().UTC().Add(-time.Second)
	resolver.tenantCache["tenant-123"] = entry
	resolver.mu.Unlock()

	assert.Equal(t, 42, resolver.callbackRateLimitPerMinute(ctx, 60))
	assert.Equal(t, 2, callCount, "expired tenant cache entries should force a fresh lookup")
}

func TestRuntimeSettingsResolver_StoreTenantValuesPrunesExpiredEntries(t *testing.T) {
	t.Parallel()

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return nil })
	require.NotNil(t, resolver)

	resolver.tenantCache = map[string]cachedRuntimeSettings{
		"tenant-expired": {
			values: map[string]spdomain.EffectiveValue{
				"callback_rate_limit.per_minute": {
					Key:    "callback_rate_limit.per_minute",
					Value:  10,
					Source: "tenant-override",
				},
			},
			expiresAt: time.Now().UTC().Add(-time.Second),
		},
	}

	resolver.storeTenantValues("tenant-active", map[string]spdomain.EffectiveValue{
		"callback_rate_limit.per_minute": {
			Key:    "callback_rate_limit.per_minute",
			Value:  42,
			Source: "tenant-override",
		},
	})

	assert.Len(t, resolver.tenantCache, 1)
	assert.Contains(t, resolver.tenantCache, "tenant-active")
	assert.NotContains(t, resolver.tenantCache, "tenant-expired")
	assert.True(t, resolver.tenantCache["tenant-active"].expiresAt.After(time.Now().UTC()))
}

func TestRuntimeSettingsResolver_SubjectUsesExplicitTenantContext(t *testing.T) {
	t.Parallel()

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return nil })
	require.NotNil(t, resolver)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)

	assert.Equal(t, spservice.Subject{Scope: spdomain.ScopeTenant, SubjectID: auth.DefaultTenantID}, resolver.subject(ctx))
}

func TestRuntimeSettingsResolver_SubjectFallsBackToGlobalWithoutExplicitTenantContext(t *testing.T) {
	t.Parallel()

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return nil })
	require.NotNil(t, resolver)

	assert.Equal(t, spservice.Subject{Scope: spdomain.ScopeGlobal}, resolver.subject(context.Background()))
}

func TestRuntimeSettingsResolver_ValuesIgnoresTypedNilManager(t *testing.T) {
	t.Parallel()

	var manager *runtimeSettingsManagerStub
	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return manager })
	require.NotNil(t, resolver)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-123")
	values, ok := resolver.values(ctx)

	assert.False(t, ok)
	assert.Nil(t, values)
}

func TestRuntimeSettingsResolver_LogsLookupFailures(t *testing.T) {
	t.Parallel()

	logger := &testLogger{}
	manager := &runtimeSettingsManagerStub{
		getSettings: func(_ context.Context, _ spservice.Subject) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{}, errors.New("store unavailable")
		},
	}

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return manager }, logger)
	require.NotNil(t, resolver)

	resolved := resolver.callbackRateLimitPerMinute(context.Background(), 60)

	assert.Equal(t, 60, resolved)
	assert.Contains(t, logger.messages, "runtime settings lookup failed; falling back to config")
}
