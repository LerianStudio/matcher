//go:build unit

// Copyright 2025 Lerian Studio.

package fiberhttp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/service"
)

func TestGetSettings_TenantScope(t *testing.T) {
	t.Parallel()

	app, _, mgr, id, _ := newTestApp(t)

	id.tenantIDFn = func(_ context.Context) (string, error) {
		return "tenant-abc", nil
	}

	var capturedSubject service.Subject

	mgr.getSettingsFn = func(_ context.Context, subject service.Subject) (service.ResolvedSet, error) {
		capturedSubject = subject

		return service.ResolvedSet{
			Values: map[string]domain.EffectiveValue{
				"ui.theme": {
					Key:    "ui.theme",
					Value:  "dark",
					Source: "tenant-override",
				},
			},
			Revision: domain.Revision(2),
		}, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/system/settings/", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body := readBody(t, resp)

	var result SettingsResponse
	require.NoError(t, json.Unmarshal([]byte(body), &result))

	assert.Equal(t, "tenant", result.Scope)
	assert.Equal(t, uint64(2), result.Revision)
	assert.Equal(t, domain.ScopeTenant, capturedSubject.Scope)
	assert.Equal(t, "tenant-abc", capturedSubject.SubjectID)
}

func TestGetSettings_GlobalScope(t *testing.T) {
	t.Parallel()

	app, _, mgr, _, _ := newTestApp(t)

	var capturedSubject service.Subject

	mgr.getSettingsFn = func(_ context.Context, subject service.Subject) (service.ResolvedSet, error) {
		capturedSubject = subject

		return service.ResolvedSet{
			Values:   map[string]domain.EffectiveValue{},
			Revision: domain.Revision(1),
		}, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/system/settings/?scope=global", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body := readBody(t, resp)

	var result SettingsResponse
	require.NoError(t, json.Unmarshal([]byte(body), &result))

	assert.Equal(t, "global", result.Scope)
	assert.Equal(t, domain.ScopeGlobal, capturedSubject.Scope)
	assert.Empty(t, capturedSubject.SubjectID)
}

func TestGetSettings_InvalidScope(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/system/settings/?scope=invalid", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_scope_invalid", errResp.Code)
}

func TestPatchSettings_Success(t *testing.T) {
	t.Parallel()

	app, _, mgr, id, _ := newTestApp(t)

	id.tenantIDFn = func(_ context.Context) (string, error) {
		return "tenant-xyz", nil
	}

	var capturedSubject service.Subject

	var capturedReq service.PatchRequest

	mgr.patchSettingsFn = func(_ context.Context, subject service.Subject, req service.PatchRequest) (service.WriteResult, error) {
		capturedSubject = subject
		capturedReq = req

		return service.WriteResult{Revision: domain.Revision(8)}, nil
	}

	body := `{"values": {"ui.theme": "light"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", `"3"`)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	respBody := readBody(t, resp)

	var result PatchResponse
	require.NoError(t, json.Unmarshal([]byte(respBody), &result))

	assert.Equal(t, uint64(8), result.Revision)
	assert.Equal(t, domain.ScopeTenant, capturedSubject.Scope)
	assert.Equal(t, "tenant-xyz", capturedSubject.SubjectID)
	assert.Equal(t, domain.Revision(3), capturedReq.ExpectedRevision)
	assert.Len(t, capturedReq.Ops, 1)
}

func TestGetSettingSchema_Success(t *testing.T) {
	t.Parallel()

	app, _, mgr, _, _ := newTestApp(t)

	mgr.getSettingSchemaFn = func(_ context.Context) ([]service.SchemaEntry, error) {
		return []service.SchemaEntry{
			{
				Key:              "ui.theme",
				Kind:             domain.KindSetting,
				AllowedScopes:    []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant},
				ValueType:        domain.ValueTypeString,
				DefaultValue:     "system",
				MutableAtRuntime: true,
				ApplyBehavior:    domain.ApplyLiveRead,
				Description:      "UI color theme",
				Group:            "ui",
			},
		}, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/system/settings/schema", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body := readBody(t, resp)

	var result SchemaResponse
	require.NoError(t, json.Unmarshal([]byte(body), &result))

	require.Len(t, result.Keys, 1)
	assert.Equal(t, "ui.theme", result.Keys[0].Key)
	assert.Equal(t, "setting", result.Keys[0].Kind)
	assert.Equal(t, []string{"global", "tenant"}, result.Keys[0].AllowedScopes)
}

func TestGetSettingHistory_Success(t *testing.T) {
	t.Parallel()

	app, _, mgr, _, _ := newTestApp(t)

	mgr.getSettingHistoryFn = func(_ context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
		assert.Equal(t, domain.KindSetting, filter.Kind)

		return []ports.HistoryEntry{
			{
				Revision:  domain.Revision(4),
				Key:       "ui.theme",
				OldValue:  "system",
				NewValue:  "dark",
				ActorID:   "user@example.com",
				ChangedAt: testTime(),
			},
		}, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/system/settings/history", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body := readBody(t, resp)

	var result HistoryResponse
	require.NoError(t, json.Unmarshal([]byte(body), &result))

	require.Len(t, result.Entries, 1)
	assert.Equal(t, uint64(4), result.Entries[0].Revision)
	assert.Equal(t, "dark", result.Entries[0].NewValue)
}

func TestGetSettingHistory_InvalidScope(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/system/settings/history?scope=invalid", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))
	assert.Equal(t, "system_scope_invalid", errResp.Code)
}

func TestPatchSettings_InvalidBody(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings/", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_invalid_request", errResp.Code)
}

func TestPatchSettings_EmptyValues(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings/", strings.NewReader(`{"values": {}}`))
	req.Header.Set("Content-Type", "application/json")
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_invalid_request", errResp.Code)
	assert.Contains(t, errResp.Message, "must not be empty")
}

func TestPatchSettings_InvalidRevision(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	body := `{"values": {"ui.theme": "dark"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/settings/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "abc")
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	respBody := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(respBody), &errResp))

	assert.Equal(t, "system_invalid_revision", errResp.Code)
}

func TestGetSettings_TenantResolutionFailure(t *testing.T) {
	t.Parallel()

	app, _, _, id, _ := newTestApp(t)

	id.tenantIDFn = func(_ context.Context) (string, error) {
		return "", fmt.Errorf("token expired: %w", domain.ErrPermissionDenied)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/system/settings/", nil)
	resp := doRequest(t, app, req)

	// The tenant resolution error wraps ErrPermissionDenied, which maps to 403
	// through writeError's errors.Is chain.
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_permission_denied", errResp.Code)
}
