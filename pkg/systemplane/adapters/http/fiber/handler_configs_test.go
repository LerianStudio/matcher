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

func TestGetConfigs_Success(t *testing.T) {
	t.Parallel()

	app, _, mgr, _, _ := newTestApp(t)

	mgr.getConfigsFn = func(_ context.Context) (service.ResolvedSet, error) {
		return service.ResolvedSet{
			Values: map[string]domain.EffectiveValue{
				"matching.batch_size": {
					Key:     "matching.batch_size",
					Value:   100,
					Default: 50,
					Source:  "override",
				},
			},
			Revision: domain.Revision(5),
		}, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/system/configs/", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body := readBody(t, resp)

	var result ConfigsResponse
	require.NoError(t, json.Unmarshal([]byte(body), &result))

	assert.Equal(t, uint64(5), result.Revision)
	assert.Len(t, result.Values, 1)

	val := result.Values["matching.batch_size"]
	assert.Equal(t, "matching.batch_size", val.Key)
	assert.Equal(t, "override", val.Source)
	assert.False(t, val.Redacted)
}

func TestGetConfigs_Error(t *testing.T) {
	t.Parallel()

	app, _, mgr, _, _ := newTestApp(t)

	mgr.getConfigsFn = func(_ context.Context) (service.ResolvedSet, error) {
		return service.ResolvedSet{}, domain.ErrSnapshotBuildFailed
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/system/configs/", nil)
	resp := doRequest(t, app, req)

	// ErrSnapshotBuildFailed is not in our error mapping, so it falls through to 500.
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_internal_error", errResp.Code)
}

func TestPatchConfigs_Success(t *testing.T) {
	t.Parallel()

	app, _, mgr, _, _ := newTestApp(t)

	var capturedReq service.PatchRequest

	mgr.patchConfigsFn = func(_ context.Context, req service.PatchRequest) (service.WriteResult, error) {
		capturedReq = req

		return service.WriteResult{Revision: domain.Revision(6)}, nil
	}

	body := `{"values": {"matching.batch_size": 200}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/configs/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "5")
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	respBody := readBody(t, resp)

	var result PatchResponse
	require.NoError(t, json.Unmarshal([]byte(respBody), &result))

	assert.Equal(t, uint64(6), result.Revision)
	assert.Equal(t, domain.Revision(5), capturedReq.ExpectedRevision)
	assert.Equal(t, "api", capturedReq.Source)
	assert.Equal(t, "test-actor", capturedReq.Actor.ID)
	assert.Len(t, capturedReq.Ops, 1)
}

func TestPatchConfigs_InvalidBody(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/configs/", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_invalid_request", errResp.Code)
}

func TestPatchConfigs_EmptyValues(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	req := httptest.NewRequest(http.MethodPatch, "/v1/system/configs/", strings.NewReader(`{"values": {}}`))
	req.Header.Set("Content-Type", "application/json")
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_invalid_request", errResp.Code)
	assert.Contains(t, errResp.Message, "must not be empty")
}

func TestPatchConfigs_RevisionMismatch(t *testing.T) {
	t.Parallel()

	app, _, mgr, _, _ := newTestApp(t)

	mgr.patchConfigsFn = func(_ context.Context, _ service.PatchRequest) (service.WriteResult, error) {
		return service.WriteResult{}, domain.ErrRevisionMismatch
	}

	body := `{"values": {"key": "value"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/configs/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "1")
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	respBody := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(respBody), &errResp))

	assert.Equal(t, "system_revision_mismatch", errResp.Code)
}

func TestPatchConfigs_InvalidRevisionHeader(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	body := `{"values": {"key": "value"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/configs/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "not-a-number")
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	respBody := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(respBody), &errResp))

	assert.Equal(t, "system_invalid_revision", errResp.Code)
}

func TestPatchConfigs_NullValueReset(t *testing.T) {
	t.Parallel()

	app, _, mgr, _, _ := newTestApp(t)

	var capturedReq service.PatchRequest

	mgr.patchConfigsFn = func(_ context.Context, req service.PatchRequest) (service.WriteResult, error) {
		capturedReq = req

		return service.WriteResult{Revision: domain.Revision(7)}, nil
	}

	body := `{"values": {"matching.batch_size": null}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/configs/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Len(t, capturedReq.Ops, 1)
	assert.True(t, capturedReq.Ops[0].Reset)
	assert.Nil(t, capturedReq.Ops[0].Value)
}

func TestGetConfigSchema_Success(t *testing.T) {
	t.Parallel()

	app, _, mgr, _, _ := newTestApp(t)

	mgr.getConfigSchemaFn = func(_ context.Context) ([]service.SchemaEntry, error) {
		return []service.SchemaEntry{
			{
				Key:              "matching.batch_size",
				Kind:             domain.KindConfig,
				AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
				ValueType:        domain.ValueTypeInt,
				DefaultValue:     50,
				MutableAtRuntime: true,
				ApplyBehavior:    domain.ApplyLiveRead,
				Secret:           false,
				Description:      "Batch size for matching",
				Group:            "matching",
			},
		}, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/system/configs/schema", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body := readBody(t, resp)

	var result SchemaResponse
	require.NoError(t, json.Unmarshal([]byte(body), &result))

	require.Len(t, result.Keys, 1)
	assert.Equal(t, "matching.batch_size", result.Keys[0].Key)
	assert.Equal(t, "config", result.Keys[0].Kind)
	assert.Equal(t, "int", result.Keys[0].ValueType)
	assert.True(t, result.Keys[0].MutableAtRuntime)
	assert.Equal(t, "matching", result.Keys[0].Group)
}

func TestGetConfigHistory_Success(t *testing.T) {
	t.Parallel()

	app, _, mgr, _, _ := newTestApp(t)

	mgr.getConfigHistoryFn = func(_ context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
		assert.Equal(t, domain.KindConfig, filter.Kind)
		assert.Equal(t, 20, filter.Limit)
		assert.Equal(t, "matching.batch_size", filter.Key)

		return []ports.HistoryEntry{
			{
				Revision:  domain.Revision(3),
				Key:       "matching.batch_size",
				OldValue:  50,
				NewValue:  100,
				ActorID:   "admin@example.com",
				ChangedAt: testTime(),
			},
		}, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/system/configs/history?limit=20&key=matching.batch_size", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body := readBody(t, resp)

	var result HistoryResponse
	require.NoError(t, json.Unmarshal([]byte(body), &result))

	require.Len(t, result.Entries, 1)
	assert.Equal(t, uint64(3), result.Entries[0].Revision)
	assert.Equal(t, "admin@example.com", result.Entries[0].ActorID)
	assert.Equal(t, "2026-03-17T12:00:00Z", result.Entries[0].ChangedAt)
}

func TestPatchConfigs_ActorResolutionFailure(t *testing.T) {
	t.Parallel()

	app, _, _, id, _ := newTestApp(t)

	id.actorFn = func(_ context.Context) (domain.Actor, error) {
		return domain.Actor{}, fmt.Errorf("actor lookup failed: %w", domain.ErrPermissionDenied)
	}

	body := `{"values": {"matching.batch_size": 200}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/system/configs/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", "5")
	resp := doRequest(t, app, req)

	// The actor resolution error wraps ErrPermissionDenied, which maps to 403.
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	respBody := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(respBody), &errResp))

	assert.Equal(t, "system_permission_denied", errResp.Code)
}

func TestReload_Success(t *testing.T) {
	t.Parallel()

	app, _, mgr, _, _ := newTestApp(t)

	resyncCalled := false

	mgr.resyncFn = func(_ context.Context) error {
		resyncCalled = true

		return nil
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/system/configs/reload", nil)
	resp := doRequest(t, app, req)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, resyncCalled)

	body := readBody(t, resp)

	var result ReloadResponse
	require.NoError(t, json.Unmarshal([]byte(body), &result))

	assert.Equal(t, "ok", result.Status)
}

func TestReload_Failure(t *testing.T) {
	t.Parallel()

	app, _, mgr, _, _ := newTestApp(t)

	mgr.resyncFn = func(_ context.Context) error {
		return domain.ErrSupervisorStopped
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/system/configs/reload", nil)
	resp := doRequest(t, app, req)

	// The error wraps ErrReloadFailed + ErrSupervisorStopped.
	// ErrReloadFailed maps to 500.
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	body := readBody(t, resp)

	var errResp ErrorResponse
	require.NoError(t, json.Unmarshal([]byte(body), &errResp))

	assert.Equal(t, "system_reload_failed", errResp.Code)
}
