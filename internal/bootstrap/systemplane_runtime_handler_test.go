//go:build unit

package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	spdomain "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	spports "github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
	spregistry "github.com/LerianStudio/lib-commons/v4/commons/systemplane/registry"
	spservice "github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"

	"github.com/LerianStudio/matcher/internal/auth"
)

type runtimeHandlerStoreStub struct {
	putCalls []mockPutCall
	putErr   error
	getResp  spports.ReadResult
}

func (stub *runtimeHandlerStoreStub) Get(_ context.Context, _ spdomain.Target) (spports.ReadResult, error) {
	return stub.getResp, nil
}

func (stub *runtimeHandlerStoreStub) Put(
	_ context.Context,
	target spdomain.Target,
	ops []spports.WriteOp,
	expected spdomain.Revision,
	actor spdomain.Actor,
	source string,
) (spdomain.Revision, error) {
	stub.putCalls = append(stub.putCalls, mockPutCall{
		Target:   target,
		Ops:      ops,
		Expected: expected,
		Actor:    actor,
		Source:   source,
	})
	if stub.putErr != nil {
		return spdomain.RevisionZero, stub.putErr
	}

	return expected.Next(), nil
}

type runtimeHandlerSupervisorStub struct {
	snapshot spdomain.Snapshot
}

func (stub *runtimeHandlerSupervisorStub) Current() spdomain.RuntimeBundle { return nil }
func (stub *runtimeHandlerSupervisorStub) Snapshot() spdomain.Snapshot     { return stub.snapshot }
func (stub *runtimeHandlerSupervisorStub) PublishSnapshot(context.Context, spdomain.Snapshot, string) error {
	return nil
}

func (stub *runtimeHandlerSupervisorStub) ReconcileCurrent(context.Context, spdomain.Snapshot, string) error {
	return nil
}

func (stub *runtimeHandlerSupervisorStub) Reload(context.Context, string, ...string) error {
	return nil
}
func (stub *runtimeHandlerSupervisorStub) Stop(context.Context) error { return nil }

type runtimeHandlerManagerStub struct {
	getConfigs        func(context.Context) (spservice.ResolvedSet, error)
	getSettings       func(context.Context, spservice.Subject) (spservice.ResolvedSet, error)
	patchSettings     func(context.Context, spservice.Subject, spservice.PatchRequest) (spservice.WriteResult, error)
	applyChangeSignal func(context.Context, spports.ChangeSignal) error
	runtimeRegistry   spregistry.Registry
	runtimeStore      spports.Store
	runtimeSupervisor spservice.Supervisor
}

func (stub *runtimeHandlerManagerStub) GetConfigs(ctx context.Context) (spservice.ResolvedSet, error) {
	if stub.getConfigs != nil {
		return stub.getConfigs(ctx)
	}
	return spservice.ResolvedSet{}, nil
}

func (stub *runtimeHandlerManagerStub) GetSettings(ctx context.Context, subject spservice.Subject) (spservice.ResolvedSet, error) {
	if stub.getSettings != nil {
		return stub.getSettings(ctx, subject)
	}
	return spservice.ResolvedSet{}, nil
}

func (stub *runtimeHandlerManagerStub) PatchConfigs(context.Context, spservice.PatchRequest) (spservice.WriteResult, error) {
	return spservice.WriteResult{}, nil
}

func (stub *runtimeHandlerManagerStub) PatchSettings(ctx context.Context, subject spservice.Subject, req spservice.PatchRequest) (spservice.WriteResult, error) {
	if stub.patchSettings != nil {
		return stub.patchSettings(ctx, subject, req)
	}
	return spservice.WriteResult{}, nil
}

func (stub *runtimeHandlerManagerStub) GetConfigSchema(context.Context) ([]spservice.SchemaEntry, error) {
	return nil, nil
}

func (stub *runtimeHandlerManagerStub) GetSettingSchema(context.Context) ([]spservice.SchemaEntry, error) {
	return nil, nil
}

func (stub *runtimeHandlerManagerStub) GetConfigHistory(context.Context, spports.HistoryFilter) ([]spports.HistoryEntry, error) {
	return nil, nil
}

func (stub *runtimeHandlerManagerStub) GetSettingHistory(context.Context, spports.HistoryFilter) ([]spports.HistoryEntry, error) {
	return nil, nil
}

func (stub *runtimeHandlerManagerStub) ApplyChangeSignal(ctx context.Context, signal spports.ChangeSignal) error {
	if stub.applyChangeSignal != nil {
		return stub.applyChangeSignal(ctx, signal)
	}
	return nil
}

func (stub *runtimeHandlerManagerStub) Resync(context.Context) error { return nil }

func (stub *runtimeHandlerManagerStub) registry() spregistry.Registry { return stub.runtimeRegistry }
func (stub *runtimeHandlerManagerStub) store() spports.Store          { return stub.runtimeStore }
func (stub *runtimeHandlerManagerStub) supervisor() spservice.Supervisor {
	return stub.runtimeSupervisor
}

func TestMatcherSystemplaneHandler_GetConfigs_ShowsEnvMasking(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, reg.Register(spdomain.KeyDef{
		Key:              "rate_limit.max",
		Kind:             spdomain.KindConfig,
		AllowedScopes:    []spdomain.Scope{spdomain.ScopeGlobal},
		DefaultValue:     100,
		ValueType:        spdomain.ValueTypeInt,
		ApplyBehavior:    spdomain.ApplyLiveRead,
		MutableAtRuntime: true,
	}))

	manager := &runtimeHandlerManagerStub{
		getConfigs: func(context.Context) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"rate_limit.max": {
						Key:     "rate_limit.max",
						Value:   100,
						Default: 100,
						Source:  "global-override",
					},
				},
				Revision: 7,
			}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config {
		cfg := defaultConfig()
		cfg.RateLimit.Max = 999
		return cfg
	}, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/v1/system/configs", func(c *fiber.Ctx) error {
		return handler.getConfigs(c)
	})

	t.Setenv("RATE_LIMIT_MAX", "999")

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/v1/system/configs", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body matcherSystemplaneConfigsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

	entry := body.Values["rate_limit.max"]
	assert.Equal(t, float64(999), toFloat64ForTest(entry.Value))
	assert.Equal(t, "env-override", entry.Source)
	assert.True(t, entry.MaskedByEnv)
	assert.Equal(t, "RATE_LIMIT_MAX", entry.EnvVar)
	assert.Equal(t, float64(100), toFloat64ForTest(entry.PersistedValue))
	assert.Equal(t, "global-override", entry.PersistedSource)
}

func TestMatcherSystemplaneHandler_PatchConfigs_AllowsRestartRequiredKeys(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, reg.Register(spdomain.KeyDef{
		Key:              "server.address",
		Kind:             spdomain.KindConfig,
		AllowedScopes:    []spdomain.Scope{spdomain.ScopeGlobal},
		DefaultValue:     ":4018",
		ValueType:        spdomain.ValueTypeString,
		ApplyBehavior:    spdomain.ApplyBootstrapOnly,
		MutableAtRuntime: false,
	}))

	store := &runtimeHandlerStoreStub{}
	manager := &runtimeHandlerManagerStub{
		getConfigs: func(context.Context) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{Values: map[string]spdomain.EffectiveValue{}, Revision: 3}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      store,
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Patch("/v1/system/configs", func(c *fiber.Ctx) error {
		return handler.patchConfigs(c)
	})

	body := []byte(`{"values":{"server.address":":4999"}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/configs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "3")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var patchResp matcherSystemplanePatchResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&patchResp))
	assert.True(t, patchResp.PendingRestart)
	assert.False(t, patchResp.AppliedRuntime)
	assert.Equal(t, []string{"server.address"}, patchResp.PendingRestartKeys)
	require.Len(t, store.putCalls, 1)
	assert.Equal(t, spdomain.KindConfig, store.putCalls[0].Target.Kind)
	assert.Equal(t, ":4999", store.putCalls[0].Ops[0].Value)
}

func TestMatcherSystemplaneHandler_PatchConfigs_RejectsSettingKeysAfterHardCutover(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	store := &runtimeHandlerStoreStub{}
	manager := &runtimeHandlerManagerStub{
		getConfigs: func(context.Context) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{Values: map[string]spdomain.EffectiveValue{}, Revision: 3}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      store,
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Patch("/v1/system/configs", func(c *fiber.Ctx) error {
		return handler.patchConfigs(c)
	})

	body := []byte(`{"values":{"rate_limit.max":5}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/configs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "3")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Empty(t, store.putCalls)
}

func TestMatcherSystemplaneHandler_PatchConfigs_RuntimeKeyAppliesSignalAndReportsMaskedEnvKeys(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	store := &runtimeHandlerStoreStub{}
	var capturedSignal spports.ChangeSignal
	manager := &runtimeHandlerManagerStub{
		getConfigs: func(context.Context) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{Values: map[string]spdomain.EffectiveValue{}, Revision: 3}, nil
		},
		applyChangeSignal: func(_ context.Context, signal spports.ChangeSignal) error {
			capturedSignal = signal

			return nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      store,
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Patch("/v1/system/configs", func(c *fiber.Ctx) error {
		return handler.patchConfigs(c)
	})

	t.Setenv("REDIS_HOST", "redis-from-env")

	body := []byte(`{"values":{"redis.host":"redis-runtime.internal"}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/configs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "3")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var patchResp matcherSystemplanePatchResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&patchResp))
	assert.Equal(t, uint64(4), patchResp.Revision)
	assert.True(t, patchResp.AppliedRuntime)
	assert.False(t, patchResp.PendingRestart)
	assert.Equal(t, []string{"redis.host"}, patchResp.AppliedKeys)
	assert.Equal(t, []string{"redis.host"}, patchResp.MaskedByEnvKeys)

	require.Len(t, store.putCalls, 1)
	assert.Equal(t, spdomain.KindConfig, store.putCalls[0].Target.Kind)
	assert.Equal(t, "redis.host", store.putCalls[0].Ops[0].Key)
	assert.Equal(t, "redis-runtime.internal", store.putCalls[0].Ops[0].Value)

	assert.Equal(t, spdomain.KindConfig, capturedSignal.Target.Kind)
	assert.Equal(t, spdomain.ScopeGlobal, capturedSignal.Target.Scope)
	assert.Equal(t, spdomain.Revision(4), capturedSignal.Revision)
	assert.Equal(t, spdomain.ApplyBundleRebuild, capturedSignal.ApplyBehavior)
}

func TestMatcherSystemplaneHandler_PatchConfigs_ApplyChangeSignalFailureReturnsInternalError(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	store := &runtimeHandlerStoreStub{}
	manager := &runtimeHandlerManagerStub{
		getConfigs: func(context.Context) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"redis.host": {Key: "redis.host", Value: "redis-before.internal", Source: "global-override"},
				},
				Revision: 3,
			}, nil
		},
		applyChangeSignal: func(context.Context, spports.ChangeSignal) error {
			return errors.New("signal failed")
		},
		runtimeRegistry:   reg,
		runtimeStore:      store,
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Patch("/v1/system/configs", func(c *fiber.Ctx) error {
		return handler.patchConfigs(c)
	})

	body := []byte(`{"values":{"redis.host":"redis-runtime.internal"}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/configs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "3")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var responseBody map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&responseBody))
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "system_internal_error", responseBody["code"])
	require.Len(t, store.putCalls, 2)
	assert.Equal(t, "redis-runtime.internal", store.putCalls[0].Ops[0].Value)
	assert.Equal(t, spdomain.Revision(4), store.putCalls[1].Expected)
	assert.Equal(t, "api-rollback", store.putCalls[1].Source)
	assert.Equal(t, "redis.host", store.putCalls[1].Ops[0].Key)
	assert.Equal(t, "redis-before.internal", store.putCalls[1].Ops[0].Value)
}

func TestRollbackConfigOps_ResetsDefaultBackedKeys(t *testing.T) {
	t.Parallel()

	rollbackOps := rollbackConfigOps(
		map[string]spdomain.EffectiveValue{
			"redis.host": {Key: "redis.host", Value: "localhost", Source: "default"},
		},
		[]spports.WriteOp{{Key: "redis.host", Value: "redis-runtime.internal"}},
	)

	require.Len(t, rollbackOps, 1)
	assert.Equal(t, "redis.host", rollbackOps[0].Key)
	assert.True(t, rollbackOps[0].Reset)
	assert.Nil(t, rollbackOps[0].Value)
}

func TestMatcherSystemplaneHandler_GetSettings_TenantScopeReturnsResolvedValues(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	manager := &runtimeHandlerManagerStub{
		getSettings: func(_ context.Context, subject spservice.Subject) (spservice.ResolvedSet, error) {
			require.Equal(t, spservice.Subject{Scope: spdomain.ScopeTenant, SubjectID: "tenant-123"}, subject)
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"rate_limit.max": {
						Key:     "rate_limit.max",
						Value:   7,
						Default: defaultRateLimitMax,
						Source:  "tenant-override",
					},
				},
				Revision: 4,
			}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-123")
		c.SetUserContext(ctx)
		return c.Next()
	})
	app.Get("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.getSettings(c)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/v1/system/settings", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body matcherSystemplaneSettingsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Contains(t, body.Values, "rate_limit.max")
	assert.Equal(t, float64(7), toFloat64ForTest(body.Values["rate_limit.max"].Value))
	assert.Equal(t, "tenant-override", body.Values["rate_limit.max"].Source)
	assert.Equal(t, "tenant", body.Scope)
	assert.Equal(t, uint64(4), body.Revision)
}

func TestMatcherSystemplaneHandler_GetSettings_NoExplicitTenantFallsBackToGlobalScope(t *testing.T) {
	t.Parallel()

	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	manager := &runtimeHandlerManagerStub{
		getSettings: func(_ context.Context, subject spservice.Subject) (spservice.ResolvedSet, error) {
			require.Equal(t, spservice.Subject{Scope: spdomain.ScopeGlobal}, subject)
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"rate_limit.max": {
						Key:     "rate_limit.max",
						Value:   9,
						Default: defaultRateLimitMax,
						Source:  "global-override",
					},
				},
				Revision: 2,
			}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.getSettings(c)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/v1/system/settings", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body matcherSystemplaneSettingsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "global", body.Scope)
	assert.Equal(t, float64(9), toFloat64ForTest(body.Values["rate_limit.max"].Value))
}

func TestMatcherSystemplaneHandler_GetSettings_TenantScopeSuppressesGlobalPersistedMetadata(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	manager := &runtimeHandlerManagerStub{
		getSettings: func(_ context.Context, subject spservice.Subject) (spservice.ResolvedSet, error) {
			require.Equal(t, spservice.Subject{Scope: spdomain.ScopeTenant, SubjectID: "tenant-123"}, subject)
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"rate_limit.max": {
						Key:     "rate_limit.max",
						Value:   7,
						Default: defaultRateLimitMax,
						Source:  "global-override",
					},
				},
				Revision: 4,
			}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config {
		cfg := defaultConfig()
		cfg.RateLimit.Max = 999
		return cfg
	}, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-123")
		c.SetUserContext(ctx)
		return c.Next()
	})
	app.Get("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.getSettings(c)
	})

	t.Setenv("RATE_LIMIT_MAX", "999")

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/v1/system/settings", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body matcherSystemplaneSettingsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

	entry := body.Values["rate_limit.max"]
	assert.Equal(t, float64(999), toFloat64ForTest(entry.Value))
	assert.Equal(t, "env-override-global", entry.Source)
	assert.True(t, entry.MaskedByEnv)
	assert.Empty(t, entry.EnvVar)
	assert.Nil(t, entry.PersistedValue)
	assert.Empty(t, entry.PersistedSource)
}

func TestMatcherSystemplaneHandler_PatchSettings_InvalidatesTenantResolverCache(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	currentValue := 42
	getSettingsCalls := 0
	manager := &runtimeHandlerManagerStub{
		getSettings: func(_ context.Context, subject spservice.Subject) (spservice.ResolvedSet, error) {
			getSettingsCalls++
			require.Equal(t, spservice.Subject{Scope: spdomain.ScopeTenant, SubjectID: "tenant-123"}, subject)
			return spservice.ResolvedSet{
				Values: map[string]spdomain.EffectiveValue{
					"callback_rate_limit.per_minute": {
						Key:     "callback_rate_limit.per_minute",
						Value:   currentValue,
						Default: defaultCallbackPerMinute,
						Source:  "tenant-override",
					},
				},
				Revision: 4,
			}, nil
		},
		patchSettings: func(context.Context, spservice.Subject, spservice.PatchRequest) (spservice.WriteResult, error) {
			return spservice.WriteResult{Revision: 5}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	resolver := newRuntimeSettingsResolver(func() spservice.Manager { return manager })
	require.NotNil(t, resolver)

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, resolver)
	require.NotNil(t, handler)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-123")
	assert.Equal(t, 42, resolver.callbackRateLimitPerMinute(ctx, 60))
	assert.Equal(t, 1, getSettingsCalls)

	currentValue = 99

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.SetUserContext(context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-123"))
		return c.Next()
	})
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	body := []byte(`{"values":{"callback_rate_limit.per_minute":99}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "4")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, 99, resolver.callbackRateLimitPerMinute(ctx, 60))
	assert.Equal(t, 2, getSettingsCalls, "cache should be invalidated after a successful tenant settings patch")
}

func TestMatcherSystemplaneHandler_PatchSettings_TenantSuccessReturnsETagAndAppliedKeys(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	var capturedSubject spservice.Subject
	var capturedReq spservice.PatchRequest
	manager := &runtimeHandlerManagerStub{
		patchSettings: func(_ context.Context, subject spservice.Subject, req spservice.PatchRequest) (spservice.WriteResult, error) {
			capturedSubject = subject
			capturedReq = req
			return spservice.WriteResult{Revision: 8}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-123")
		ctx = context.WithValue(ctx, auth.UserIDKey, "settings-admin")
		c.SetUserContext(ctx)
		return c.Next()
	})
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	body := []byte(`{"values":{"rate_limit.enabled":false,"rate_limit.max":5}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "7")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, `"8"`, resp.Header.Get("ETag"))

	var patchResp matcherSystemplanePatchResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&patchResp))
	assert.Equal(t, uint64(8), patchResp.Revision)
	assert.True(t, patchResp.AppliedRuntime)
	assert.False(t, patchResp.PendingRestart)
	assert.Equal(t, []string{"rate_limit.enabled", "rate_limit.max"}, patchResp.AppliedKeys)
	assert.Empty(t, patchResp.MaskedByEnvKeys)

	assert.Equal(t, spservice.Subject{Scope: spdomain.ScopeTenant, SubjectID: "tenant-123"}, capturedSubject)
	assert.Equal(t, spdomain.Revision(7), capturedReq.ExpectedRevision)
	assert.Equal(t, spdomain.Actor{ID: "settings-admin"}, capturedReq.Actor)
	assert.Equal(t, "api", capturedReq.Source)
	require.Len(t, capturedReq.Ops, 2)
}

func TestMatcherSystemplaneHandler_PatchSettings_NoExplicitTenantFallsBackToGlobalScope(t *testing.T) {
	t.Parallel()

	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	var capturedSubject spservice.Subject
	manager := &runtimeHandlerManagerStub{
		patchSettings: func(_ context.Context, subject spservice.Subject, req spservice.PatchRequest) (spservice.WriteResult, error) {
			capturedSubject = subject
			return spservice.WriteResult{Revision: 3}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.UserIDKey, "settings-admin")
		c.SetUserContext(ctx)
		return c.Next()
	})
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	body := []byte(`{"values":{"rate_limit.max":5}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "2")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, spservice.Subject{Scope: spdomain.ScopeGlobal}, capturedSubject)
}

func TestMatcherSystemplaneHandler_PatchSettings_GlobalSuccessReportsMaskedEnvKeys(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	var capturedSubject spservice.Subject
	manager := &runtimeHandlerManagerStub{
		patchSettings: func(_ context.Context, subject spservice.Subject, req spservice.PatchRequest) (spservice.WriteResult, error) {
			capturedSubject = subject
			return spservice.WriteResult{Revision: 5}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.UserIDKey, "settings-admin")
		c.SetUserContext(ctx)
		return c.Next()
	})
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	t.Setenv("RATE_LIMIT_MAX", "900")

	body := []byte(`{"values":{"rate_limit.max":5}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings?scope=global", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "4")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, `"5"`, resp.Header.Get("ETag"))

	var patchResp matcherSystemplanePatchResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&patchResp))
	assert.False(t, patchResp.AppliedRuntime)
	assert.Empty(t, patchResp.AppliedKeys)
	assert.Equal(t, []string{"rate_limit.max"}, patchResp.MaskedByEnvKeys)
	assert.Equal(t, spservice.Subject{Scope: spdomain.ScopeGlobal}, capturedSubject)
}

func TestMatcherSystemplaneHandler_PatchSettings_GlobalMixedMaskingReportsOnlyLiveAppliedKeys(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	manager := &runtimeHandlerManagerStub{
		patchSettings: func(_ context.Context, subject spservice.Subject, req spservice.PatchRequest) (spservice.WriteResult, error) {
			return spservice.WriteResult{Revision: 6}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.UserIDKey, "settings-admin")
		c.SetUserContext(ctx)
		return c.Next()
	})
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	t.Setenv("RATE_LIMIT_MAX", "900")

	body := []byte(`{"values":{"rate_limit.max":5,"rate_limit.enabled":false}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings?scope=global", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "5")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var patchResp matcherSystemplanePatchResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&patchResp))
	assert.True(t, patchResp.AppliedRuntime)
	assert.Equal(t, []string{"rate_limit.enabled"}, patchResp.AppliedKeys)
	assert.Equal(t, []string{"rate_limit.max"}, patchResp.MaskedByEnvKeys)
}

func TestMatcherSystemplaneHandler_PatchSettings_InvalidScopeReturnsBadRequest(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	patchSettingsCalled := false
	manager := &runtimeHandlerManagerStub{
		patchSettings: func(context.Context, spservice.Subject, spservice.PatchRequest) (spservice.WriteResult, error) {
			patchSettingsCalled = true
			return spservice.WriteResult{Revision: 2}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	body := []byte(`{"values":{"rate_limit.max":5}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings?scope=invalid", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.False(t, patchSettingsCalled)
}

func TestMatcherSystemplaneHandler_PatchSettings_InvalidJSONReturnsBadRequest(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	handler := newMatcherSystemplaneHandler(&runtimeHandlerManagerStub{
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}, &runtimeHandlerManagerStub{
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings", bytes.NewReader([]byte(`{"values":`)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "system_invalid_request", body["code"])
}

func TestMatcherSystemplaneHandler_PatchSettings_EmptyValuesReturnsBadRequest(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	handler := newMatcherSystemplaneHandler(&runtimeHandlerManagerStub{
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}, &runtimeHandlerManagerStub{
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings", bytes.NewReader([]byte(`{"values":{}}`)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "system_invalid_request", body["code"])
}

func TestMatcherSystemplaneHandler_PatchSettings_InvalidIfMatchReturnsBadRequest(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	handler := newMatcherSystemplaneHandler(&runtimeHandlerManagerStub{
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}, &runtimeHandlerManagerStub{
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings", bytes.NewReader([]byte(`{"values":{"rate_limit.max":5}}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "invalid-revision")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "system_invalid_revision", body["code"])
}

func TestMatcherSystemplaneHandler_PatchSettings_RevisionMismatchMapsToConflict(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	manager := &runtimeHandlerManagerStub{
		patchSettings: func(context.Context, spservice.Subject, spservice.PatchRequest) (spservice.WriteResult, error) {
			return spservice.WriteResult{}, spdomain.ErrRevisionMismatch
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings?scope=global", bytes.NewReader([]byte(`{"values":{"rate_limit.max":5}}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "1")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	assert.Equal(t, "system_revision_mismatch", body["code"])
}

func TestMatcherSystemplaneHandler_GetSettings_PermissionDeniedMapsToForbidden(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	manager := &runtimeHandlerManagerStub{
		getSettings: func(context.Context, spservice.Subject) (spservice.ResolvedSet, error) {
			return spservice.ResolvedSet{}, spdomain.ErrPermissionDenied
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.getSettings(c)
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/v1/system/settings?scope=global", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, "system_permission_denied", body["code"])
	assert.Equal(t, "permission denied", body["message"])
}

func TestMatcherSystemplaneHandler_PatchSettings_TenantScopeRejectsUnboundedRateLimitValue(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))

	patchSettingsCalled := false
	manager := &runtimeHandlerManagerStub{
		patchSettings: func(context.Context, spservice.Subject, spservice.PatchRequest) (spservice.WriteResult, error) {
			patchSettingsCalled = true
			return spservice.WriteResult{Revision: 2}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: defaultSnapshotFromKeyDefs(matcherKeyDefs())},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config { return defaultConfig() }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-123")
		c.SetUserContext(ctx)
		return c.Next()
	})
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	body := []byte(`{"values":{"rate_limit.max":1000001}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "1")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.False(t, patchSettingsCalled)
}

func TestMatcherSystemplaneHandler_PatchSettings_RejectsInvalidGlobalRuntimeCandidate(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))
	snapshot := defaultSnapshotFromKeyDefs(matcherKeyDefs())
	snapshot.Configs["app.env_name"] = spdomain.EffectiveValue{
		Key:     "app.env_name",
		Value:   "production",
		Default: defaultEnvName,
		Source:  "global-override",
	}

	patchSettingsCalled := false
	manager := &runtimeHandlerManagerStub{
		patchSettings: func(context.Context, spservice.Subject, spservice.PatchRequest) (spservice.WriteResult, error) {
			patchSettingsCalled = true
			return spservice.WriteResult{Revision: 4}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: snapshot},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config {
		cfg := defaultConfig()
		cfg.App.EnvName = "production"

		return cfg
	}, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	body := []byte(`{"values":{"rate_limit.enabled":false}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings?scope=global", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "3")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var responseBody map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&responseBody))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "system_invalid_runtime_candidate", responseBody["code"])
	assert.False(t, patchSettingsCalled, "invalid global settings must be rejected before persistence")
}

func TestMatcherSystemplaneHandler_PatchSettings_GlobalPatchPreservesBootstrapOnlyAuthFields(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))
	snapshot := defaultSnapshotFromKeyDefs(matcherKeyDefs())
	snapshot.Configs["cors.allowed_origins"] = spdomain.EffectiveValue{
		Key:     "cors.allowed_origins",
		Value:   "https://example.com",
		Default: defaultCORSAllowedOrigins,
		Source:  "global-override",
	}
	snapshot.Configs["object_storage.endpoint"] = spdomain.EffectiveValue{
		Key:     "object_storage.endpoint",
		Value:   "https://storage.example.com",
		Default: defaultObjStorageEndpoint,
		Source:  "global-override",
	}
	snapshot.Configs["postgres.primary_password"] = spdomain.EffectiveValue{
		Key:     "postgres.primary_password",
		Value:   "pr0d-s3cure-p@ss!",
		Default: defaultPGPassword,
		Source:  "global-override",
	}
	snapshot.Configs["redis.password"] = spdomain.EffectiveValue{
		Key:     "redis.password",
		Value:   "redis-secret",
		Default: "",
		Source:  "global-override",
	}
	snapshot.Configs["rabbitmq.user"] = spdomain.EffectiveValue{
		Key:     "rabbitmq.user",
		Value:   "matcher",
		Default: defaultRabbitUser,
		Source:  "global-override",
	}
	snapshot.Configs["rabbitmq.password"] = spdomain.EffectiveValue{
		Key:     "rabbitmq.password",
		Value:   "secure-rabbitmq",
		Default: defaultRabbitPassword,
		Source:  "global-override",
	}

	patchSettingsCalled := false
	manager := &runtimeHandlerManagerStub{
		patchSettings: func(context.Context, spservice.Subject, spservice.PatchRequest) (spservice.WriteResult, error) {
			patchSettingsCalled = true
			return spservice.WriteResult{Revision: 4}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: snapshot},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config {
		cfg := validProductionConfigForPatchTest()
		cfg.Auth.Enabled = true
		cfg.Auth.Host = "https://auth.internal"
		cfg.Auth.TokenSecret = "super-secret"

		return cfg
	}, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	body := []byte(`{"values":{"rate_limit.max":250}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings?scope=global", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "3")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, patchSettingsCalled, "valid global settings should not fail because bootstrap-only auth fields are omitted from the snapshot")
}

func TestMatcherSystemplaneHandler_PatchSettings_RejectsLatentInvalidMaskedState(t *testing.T) {
	reg := spregistry.New()
	require.NoError(t, RegisterMatcherKeys(reg))
	snapshot := defaultSnapshotFromKeyDefs(matcherKeyDefs())
	snapshot.Configs["cors.allowed_origins"] = spdomain.EffectiveValue{
		Key:     "cors.allowed_origins",
		Value:   "https://example.com",
		Default: defaultCORSAllowedOrigins,
		Source:  "global-override",
	}
	snapshot.Configs["object_storage.endpoint"] = spdomain.EffectiveValue{
		Key:     "object_storage.endpoint",
		Value:   "https://storage.example.com",
		Default: defaultObjStorageEndpoint,
		Source:  "global-override",
	}

	patchSettingsCalled := false
	manager := &runtimeHandlerManagerStub{
		patchSettings: func(context.Context, spservice.Subject, spservice.PatchRequest) (spservice.WriteResult, error) {
			patchSettingsCalled = true
			return spservice.WriteResult{Revision: 4}, nil
		},
		runtimeRegistry:   reg,
		runtimeStore:      &runtimeHandlerStoreStub{},
		runtimeSupervisor: &runtimeHandlerSupervisorStub{snapshot: snapshot},
	}

	handler := newMatcherSystemplaneHandler(manager, manager, func() *Config {
		cfg := validProductionConfigForPatchTest()
		cfg.RateLimit.Enabled = true

		return cfg
	}, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Patch("/v1/system/settings", func(c *fiber.Ctx) error {
		return handler.patchSettings(c)
	})

	t.Setenv("RATE_LIMIT_ENABLED", "true")

	body := []byte(`{"values":{"rate_limit.enabled":false}}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings?scope=global", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "3")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var responseBody map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&responseBody))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "system_invalid_runtime_candidate", responseBody["code"])
	assert.False(t, patchSettingsCalled, "masked persisted invalid settings must be rejected before persistence")
}

func toFloat64ForTest(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case int:
		return float64(typed)
	default:
		return 0
	}
}

func validProductionConfigForPatchTest() *Config {
	cfg := defaultConfig()
	cfg.App.EnvName = "production"
	cfg.Postgres.PrimaryPassword = "pr0d-s3cure-p@ss!"
	cfg.Redis.Password = "redis-secret"
	cfg.RabbitMQ.User = "matcher"
	cfg.RabbitMQ.Password = "secure-rabbitmq"
	cfg.Server.CORSAllowedOrigins = "https://example.com"

	return cfg
}

var (
	_ systemplaneRuntimeManager = (*runtimeHandlerManagerStub)(nil)
	_ spservice.Manager         = (*runtimeHandlerManagerStub)(nil)
	_ spservice.Manager         = (*runtimeSettingsManagerStub)(nil)
)

func init() {
	_ = auth.DefaultTenantID
}
