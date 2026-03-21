//go:build unit

// Copyright 2025 Lerian Studio.

package fiberhttp

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/service"
)

func TestParseRevision_EmptyHeader(t *testing.T) {
	t.Parallel()

	rev, err := parseRevision("")
	assert.NoError(t, err)
	assert.Equal(t, domain.RevisionZero, rev)
}

func TestParseRevision_ValidNumber(t *testing.T) {
	t.Parallel()

	rev, err := parseRevision("42")
	assert.NoError(t, err)
	assert.Equal(t, domain.Revision(42), rev)
}

func TestParseRevision_QuotedNumber(t *testing.T) {
	t.Parallel()

	rev, err := parseRevision(`"123"`)
	assert.NoError(t, err)
	assert.Equal(t, domain.Revision(123), rev)
}

func TestParseRevision_Invalid(t *testing.T) {
	t.Parallel()

	_, err := parseRevision("not-a-number")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse revision")
}

func TestParseRevision_Zero(t *testing.T) {
	t.Parallel()

	rev, err := parseRevision("0")
	assert.NoError(t, err)
	assert.Equal(t, domain.RevisionZero, rev)
}

func TestToWriteOps_NullValue(t *testing.T) {
	t.Parallel()

	ops := toWriteOps(map[string]any{
		"key1": nil,
		"key2": "value2",
	})

	require.Len(t, ops, 2)

	opsMap := make(map[string]ports.WriteOp)
	for _, op := range ops {
		opsMap[op.Key] = op
	}

	// nil value should set Reset=true
	assert.True(t, opsMap["key1"].Reset)
	assert.Nil(t, opsMap["key1"].Value)

	// Non-nil value should not set Reset
	assert.False(t, opsMap["key2"].Reset)
	assert.Equal(t, "value2", opsMap["key2"].Value)
}

func TestToWriteOps_EmptyMap(t *testing.T) {
	t.Parallel()

	ops := toWriteOps(map[string]any{})
	assert.Empty(t, ops)
}

func TestToEffectiveValueDTO_Normal(t *testing.T) {
	t.Parallel()

	ev := domain.EffectiveValue{
		Key:      "test.key",
		Value:    "hello",
		Default:  "world",
		Source:   "override",
		Redacted: false,
	}

	dto := toEffectiveValueDTO(ev)

	assert.Equal(t, "test.key", dto.Key)
	assert.Equal(t, "hello", dto.Value)
	assert.Equal(t, "world", dto.Default)
	assert.Equal(t, "override", dto.Source)
	assert.False(t, dto.Redacted)
}

func TestToEffectiveValueDTO_Redacted(t *testing.T) {
	t.Parallel()

	ev := domain.EffectiveValue{
		Key:      "db.password",
		Value:    "secret123",
		Default:  "default-secret",
		Source:   "env",
		Redacted: true,
	}

	dto := toEffectiveValueDTO(ev)

	assert.Equal(t, "db.password", dto.Key)
	assert.Equal(t, redactedPlaceholder, dto.Value)
	assert.Nil(t, dto.Default)
	assert.True(t, dto.Redacted)
}

func TestToHistoryResponse(t *testing.T) {
	t.Parallel()

	entries := []ports.HistoryEntry{
		{
			Revision:  domain.Revision(1),
			Key:       "key1",
			OldValue:  "old",
			NewValue:  "new",
			ActorID:   "actor1",
			ChangedAt: testTime(),
		},
	}

	resp := toHistoryResponse(entries)

	require.Len(t, resp.Entries, 1)
	assert.Equal(t, uint64(1), resp.Entries[0].Revision)
	assert.Equal(t, "key1", resp.Entries[0].Key)
	assert.Equal(t, "old", resp.Entries[0].OldValue)
	assert.Equal(t, "new", resp.Entries[0].NewValue)
	assert.Equal(t, "actor1", resp.Entries[0].ActorID)
	assert.Equal(t, "2026-03-17T12:00:00Z", resp.Entries[0].ChangedAt)
}

func TestToHistoryResponse_Empty(t *testing.T) {
	t.Parallel()

	resp := toHistoryResponse(nil)
	assert.Len(t, resp.Entries, 0)
}

func TestToSchemaResponse(t *testing.T) {
	t.Parallel()

	entries := []service.SchemaEntry{
		{
			Key:              "test.key",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal, domain.ScopeTenant},
			ValueType:        domain.ValueTypeString,
			DefaultValue:     "default",
			MutableAtRuntime: true,
			ApplyBehavior:    domain.ApplyLiveRead,
			Secret:           true,
			Description:      "A test key",
			Group:            "test",
		},
	}

	resp := toSchemaResponse(entries)

	require.Len(t, resp.Keys, 1)
	assert.Equal(t, "test.key", resp.Keys[0].Key)
	assert.Equal(t, "config", resp.Keys[0].Kind)
	assert.Equal(t, []string{"global", "tenant"}, resp.Keys[0].AllowedScopes)
	assert.Equal(t, "string", resp.Keys[0].ValueType)
	assert.True(t, resp.Keys[0].MutableAtRuntime)
	assert.Equal(t, "live-read", resp.Keys[0].ApplyBehavior)
	assert.True(t, resp.Keys[0].Secret)
	assert.Equal(t, "A test key", resp.Keys[0].Description)
	assert.Equal(t, "test", resp.Keys[0].Group)
}

func TestParseHistoryFilter_Defaults(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	app.Get("/test-filter", func(c *fiber.Ctx) error {
		filter, err := parseHistoryFilter(c, domain.KindConfig)
		require.NoError(t, err)
		assert.Equal(t, domain.KindConfig, filter.Kind)
		assert.Equal(t, defaultHistoryLimit, filter.Limit)
		assert.Equal(t, 0, filter.Offset)
		assert.Empty(t, filter.Key)
		assert.Empty(t, filter.SubjectID)

		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test-filter", nil)
	doRequest(t, app, req)
}

func TestParseHistoryFilter_CustomValues(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	app.Get("/test-filter", func(c *fiber.Ctx) error {
		filter, err := parseHistoryFilter(c, domain.KindSetting)
		require.NoError(t, err)
		assert.Equal(t, domain.KindSetting, filter.Kind)
		assert.Equal(t, 25, filter.Limit)
		assert.Equal(t, 10, filter.Offset)
		assert.Equal(t, "ui.theme", filter.Key)
		assert.Equal(t, domain.Scope("tenant"), filter.Scope)
		// SubjectID is intentionally NOT populated from query params —
		// the handler overrides it from auth context for tenant isolation.
		assert.Empty(t, filter.SubjectID)

		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test-filter?limit=25&offset=10&key=ui.theme&scope=tenant&subjectId=tenant-123", nil)
	doRequest(t, app, req)
}

func TestParseHistoryFilter_ClampsBadLimit(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	app.Get("/test-filter", func(c *fiber.Ctx) error {
		filter, err := parseHistoryFilter(c, domain.KindConfig)
		require.NoError(t, err)
		assert.Equal(t, defaultHistoryLimit, filter.Limit)

		return c.SendStatus(http.StatusOK)
	})

	// Limit > maxHistoryLimit should be clamped to default
	req := httptest.NewRequest(http.MethodGet, "/test-filter?limit=999", nil)
	doRequest(t, app, req)
}

func TestParseHistoryFilter_ClampsNegativeOffset(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	app.Get("/test-filter", func(c *fiber.Ctx) error {
		filter, err := parseHistoryFilter(c, domain.KindConfig)
		require.NoError(t, err)
		assert.Equal(t, 0, filter.Offset)

		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test-filter?offset=-5", nil)
	doRequest(t, app, req)
}

func TestParseHistoryFilter_InvalidScope(t *testing.T) {
	t.Parallel()

	app, _, _, _, _ := newTestApp(t)

	app.Get("/test-filter", func(c *fiber.Ctx) error {
		_, err := parseHistoryFilter(c, domain.KindConfig)
		require.ErrorIs(t, err, domain.ErrScopeInvalid)

		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test-filter?scope=invalid", nil)
	doRequest(t, app, req)
}
