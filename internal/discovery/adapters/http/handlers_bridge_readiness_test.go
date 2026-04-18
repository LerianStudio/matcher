// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	discoveryQuery "github.com/LerianStudio/matcher/internal/discovery/services/query"
)

// readinessHandlerStub injects controllable behaviour into the existing
// mockExtractionRepo without altering its baseline contract.
type readinessHandlerStub struct {
	mockExtractionRepo

	countFn func(ctx context.Context, threshold time.Duration) (repositories.BridgeReadinessCounts, error)
	listFn  func(ctx context.Context, state string, threshold time.Duration, ca time.Time, ia uuid.UUID, limit int) ([]*entities.ExtractionRequest, error)
}

func (r *readinessHandlerStub) CountBridgeReadiness(ctx context.Context, threshold time.Duration) (repositories.BridgeReadinessCounts, error) {
	if r.countFn != nil {
		return r.countFn(ctx, threshold)
	}

	return repositories.BridgeReadinessCounts{}, nil
}

func (r *readinessHandlerStub) ListBridgeCandidates(
	ctx context.Context,
	state string,
	threshold time.Duration,
	ca time.Time,
	ia uuid.UUID,
	limit int,
) ([]*entities.ExtractionRequest, error) {
	if r.listFn != nil {
		return r.listFn(ctx, state, threshold, ca, ia, limit)
	}

	return nil, nil
}

func newReadinessTestApp(t *testing.T, repo *readinessHandlerStub, threshold time.Duration) *fiber.App {
	t.Helper()

	// Build a query use case that points the bridge readiness methods at our
	// stub. The other dependencies are unused by the readiness handlers but
	// the constructor still validates them, so we share the standard fixture.
	fixture := newHandlerFixture(t)

	queryUC, err := discoveryQuery.NewUseCase(
		fixture.fetcherMock,
		fixture.connRepo,
		fixture.schemaRepo,
		repo,
		nil,
	)
	require.NoError(t, err)

	cmdUC, err := discoveryCommand.NewUseCase(
		fixture.fetcherMock,
		fixture.connRepo,
		fixture.schemaRepo,
		repo,
		nil,
	)
	require.NoError(t, err)

	handler, err := NewHandler(cmdUC, queryUC, false)
	require.NoError(t, err)

	handler.WithStalenessProvider(func() time.Duration { return threshold })

	app := fiber.New()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	tracer := tp.Tracer("test")

	app.Use(func(c *fiber.Ctx) error {
		ctx := c.UserContext()
		ctx = libCommons.ContextWithTracer(ctx, tracer)
		c.SetUserContext(ctx)
		return c.Next()
	})

	app.Get("/v1/discovery/extractions/bridge/summary", handler.GetBridgeReadinessSummary)
	app.Get("/v1/discovery/extractions/bridge/candidates", handler.ListBridgeCandidates)

	return app
}

func TestGetBridgeReadinessSummary_ReturnsAllFiveBuckets(t *testing.T) {
	t.Parallel()

	repo := &readinessHandlerStub{
		countFn: func(_ context.Context, threshold time.Duration) (repositories.BridgeReadinessCounts, error) {
			assert.Equal(t, 30*time.Minute, threshold)
			return repositories.BridgeReadinessCounts{
				Ready: 5, Pending: 3, Stale: 2, Failed: 1, InFlightCount: 4,
			}, nil
		},
	}

	app := newReadinessTestApp(t, repo, 30*time.Minute)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/bridge/summary", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var got dto.BridgeReadinessSummaryResponse
	require.NoError(t, json.Unmarshal(body, &got))

	assert.Equal(t, int64(5), got.ReadyCount)
	assert.Equal(t, int64(3), got.PendingCount)
	assert.Equal(t, int64(2), got.StaleCount)
	assert.Equal(t, int64(1), got.FailedCount)
	assert.Equal(t, int64(4), got.InFlightCount)
	assert.Equal(t, int64(15), got.TotalCount)
	assert.Equal(t, int64(1800), got.StaleThresholdSec)
	assert.WithinDuration(t, time.Now().UTC(), got.GeneratedAt, 5*time.Second)
}

func TestGetBridgeReadinessSummary_RepositoryError_Returns500(t *testing.T) {
	t.Parallel()

	repo := &readinessHandlerStub{
		countFn: func(_ context.Context, _ time.Duration) (repositories.BridgeReadinessCounts, error) {
			return repositories.BridgeReadinessCounts{}, errors.New("db down")
		},
	}

	app := newReadinessTestApp(t, repo, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/bridge/summary", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestListBridgeCandidates_HappyPath_PendingState(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	row := &entities.ExtractionRequest{
		ID:           uuid.New(),
		ConnectionID: uuid.New(),
		Status:       vo.ExtractionStatusComplete,
		FetcherJobID: "fetcher-job-abc",
		CreatedAt:    now.Add(-2 * time.Minute),
		UpdatedAt:    now.Add(-1 * time.Minute),
	}

	repo := &readinessHandlerStub{
		listFn: func(_ context.Context, state string, _ time.Duration, _ time.Time, _ uuid.UUID, limit int) ([]*entities.ExtractionRequest, error) {
			assert.Equal(t, "pending", state)
			assert.Equal(t, 50, limit)
			return []*entities.ExtractionRequest{row}, nil
		},
	}

	app := newReadinessTestApp(t, repo, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/bridge/candidates?state=pending", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var got dto.ListBridgeCandidatesResponse
	require.NoError(t, json.Unmarshal(body, &got))

	require.Len(t, got.Items, 1)
	assert.Equal(t, "pending", got.State)
	assert.Equal(t, 50, got.Limit)
	assert.Equal(t, "pending", got.Items[0].ReadinessState)
	assert.Equal(t, row.ID, got.Items[0].ExtractionID)
	assert.Equal(t, "COMPLETE", got.Items[0].Status)
	assert.Equal(t, "fetcher-job-abc", got.Items[0].FetcherJobID)
	assert.GreaterOrEqual(t, got.Items[0].AgeSeconds, int64(110))
	// Empty page → no next cursor
	assert.Empty(t, got.NextCursor)
}

func TestListBridgeCandidates_FullPage_EmitsNextCursor(t *testing.T) {
	t.Parallel()

	pageSize := 2
	rows := make([]*entities.ExtractionRequest, 0, pageSize)

	for i := 0; i < pageSize; i++ {
		rows = append(rows, &entities.ExtractionRequest{
			ID:           uuid.New(),
			ConnectionID: uuid.New(),
			Status:       vo.ExtractionStatusComplete,
			CreatedAt:    time.Now().UTC().Add(time.Duration(i) * time.Second),
			UpdatedAt:    time.Now().UTC(),
		})
	}

	repo := &readinessHandlerStub{
		listFn: func(_ context.Context, _ string, _ time.Duration, _ time.Time, _ uuid.UUID, _ int) ([]*entities.ExtractionRequest, error) {
			return rows, nil
		},
	}

	app := newReadinessTestApp(t, repo, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/bridge/candidates?state=ready&limit=2", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var got dto.ListBridgeCandidatesResponse
	require.NoError(t, json.Unmarshal(body, &got))

	require.Len(t, got.Items, pageSize)
	require.NotEmpty(t, got.NextCursor, "full page should emit cursor")

	// Round-trip the cursor: decode it (URL-safe base64) and confirm it
	// points at the last row.
	decoded, err := base64.URLEncoding.DecodeString(got.NextCursor)
	require.NoError(t, err)

	var cursor bridgeReadinessCursor
	require.NoError(t, json.Unmarshal(decoded, &cursor))

	assert.Equal(t, rows[pageSize-1].ID.String(), cursor.ID)
	// CreatedAt anchors the keyset paging — a cursor that drops the
	// timestamp would paginate by ID alone and miss rows with equal
	// CreatedAt across pages.
	assert.WithinDuration(t, rows[pageSize-1].CreatedAt, cursor.CreatedAt, time.Microsecond,
		"cursor.CreatedAt must match the last row's CreatedAt for keyset paging")
}

func TestListBridgeCandidates_PassesCursorThrough(t *testing.T) {
	t.Parallel()

	cursorTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	cursorID := uuid.New()

	repo := &readinessHandlerStub{
		listFn: func(_ context.Context, _ string, _ time.Duration, ca time.Time, ia uuid.UUID, _ int) ([]*entities.ExtractionRequest, error) {
			assert.WithinDuration(t, cursorTime, ca, time.Second)
			assert.Equal(t, cursorID, ia)
			return nil, nil
		},
	}

	app := newReadinessTestApp(t, repo, time.Hour)

	cursorJSON, err := json.Marshal(bridgeReadinessCursor{
		CreatedAt: cursorTime,
		ID:        cursorID.String(),
	})
	require.NoError(t, err)

	cursor := base64.URLEncoding.EncodeToString(cursorJSON)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/bridge/candidates?state=stale&cursor="+cursor, nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestListBridgeCandidates_MissingState_Returns400(t *testing.T) {
	t.Parallel()

	app := newReadinessTestApp(t, &readinessHandlerStub{}, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/bridge/candidates", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestListBridgeCandidates_InvalidState_Returns400(t *testing.T) {
	t.Parallel()

	app := newReadinessTestApp(t, &readinessHandlerStub{}, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/bridge/candidates?state=bogus", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestListBridgeCandidates_InvalidCursor_Returns400(t *testing.T) {
	t.Parallel()

	app := newReadinessTestApp(t, &readinessHandlerStub{}, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/bridge/candidates?state=ready&cursor=not-base64!!!", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestListBridgeCandidates_LimitTooLarge_Returns400(t *testing.T) {
	t.Parallel()

	app := newReadinessTestApp(t, &readinessHandlerStub{}, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/bridge/candidates?state=ready&limit=99999", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestListBridgeCandidates_NegativeLimit_Returns400(t *testing.T) {
	t.Parallel()

	app := newReadinessTestApp(t, &readinessHandlerStub{}, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/bridge/candidates?state=ready&limit=-1", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestListBridgeCandidates_NonIntegerLimit_Returns400(t *testing.T) {
	t.Parallel()

	app := newReadinessTestApp(t, &readinessHandlerStub{}, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/bridge/candidates?state=ready&limit=abc", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestListBridgeCandidates_RepositoryError_Returns500(t *testing.T) {
	t.Parallel()

	repo := &readinessHandlerStub{
		listFn: func(_ context.Context, _ string, _ time.Duration, _ time.Time, _ uuid.UUID, _ int) ([]*entities.ExtractionRequest, error) {
			return nil, errors.New("db down")
		},
	}

	app := newReadinessTestApp(t, repo, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/v1/discovery/extractions/bridge/candidates?state=ready", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestParseBridgeReadinessLimit_Defaults(t *testing.T) {
	t.Parallel()

	got, err := parseBridgeReadinessLimit("")
	require.NoError(t, err)
	assert.Equal(t, defaultBridgeReadinessLimit, got)
}

func TestParseBridgeReadinessLimit_Bounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid 1", "1", false},
		{"valid max", "200", false},
		{"valid mid", "75", false},
		{"zero invalid", "0", true},
		{"negative invalid", "-1", true},
		{"above max invalid", "201", true},
		{"non-integer invalid", "abc", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := parseBridgeReadinessLimit(tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestParseBridgeReadinessCursor_EmptyReturnsZeros(t *testing.T) {
	t.Parallel()

	ts, id, err := parseBridgeReadinessCursor("")
	require.NoError(t, err)
	assert.True(t, ts.IsZero())
	assert.Equal(t, uuid.Nil, id)
}

func TestParseBridgeReadinessCursor_InvalidBase64(t *testing.T) {
	t.Parallel()

	_, _, err := parseBridgeReadinessCursor("not-base64!!!")
	require.Error(t, err)
}

func TestParseBridgeReadinessCursor_InvalidJSON(t *testing.T) {
	t.Parallel()

	bad := base64.URLEncoding.EncodeToString([]byte("{not json"))
	_, _, err := parseBridgeReadinessCursor(bad)
	require.Error(t, err)
}

func TestParseBridgeReadinessCursor_MissingCreatedAt(t *testing.T) {
	t.Parallel()

	payload, _ := json.Marshal(bridgeReadinessCursor{ID: uuid.New().String()})
	encoded := base64.URLEncoding.EncodeToString(payload)
	_, _, err := parseBridgeReadinessCursor(encoded)
	require.Error(t, err)
}

func TestParseBridgeReadinessCursor_InvalidUUID(t *testing.T) {
	t.Parallel()

	payload, _ := json.Marshal(bridgeReadinessCursor{
		CreatedAt: time.Now().UTC(),
		ID:        "not-a-uuid",
	})
	encoded := base64.URLEncoding.EncodeToString(payload)
	_, _, err := parseBridgeReadinessCursor(encoded)
	require.Error(t, err)
}

// TestCursor_RoundTripsViaURLQueryParameter pins Fix 3 (URL-safe base64).
// Standard base64 emits '+', '/', '=' which the HTTP url-decoder mangles
// when the cursor is round-tripped through ?cursor=...; URL-safe base64
// uses '-', '_' which survive intact. The test forces a payload long enough
// to require padding and characters that would land in the unsafe set if
// std encoding were used, then proves the decoded cursor matches input.
func TestCursor_RoundTripsViaURLQueryParameter(t *testing.T) {
	t.Parallel()

	// Pick a payload whose base64 encoding will include characters that
	// std-encoding would emit as '+' or '/'. The padding ensures we hit '='.
	// Run many trials so we statistically exercise the unsafe characters.
	for trial := 0; trial < 64; trial++ {
		original := bridgeReadinessCursor{
			CreatedAt: time.Date(2026, time.April, 16, 12, trial%60, trial%60, 123456789, time.UTC),
			ID:        uuid.New().String(),
		}

		payload, err := json.Marshal(original)
		require.NoError(t, err)

		encoded := base64.URLEncoding.EncodeToString(payload)

		// Simulate query-string round-trip: net/url unescape the cursor as
		// the Fiber layer would after `?cursor=...` reaches the handler.
		urlDecoded, urlErr := url.QueryUnescape(encoded)
		require.NoError(t, urlErr)
		// URL-safe encoding produces the same bytes pre/post unescape.
		assert.Equal(t, encoded, urlDecoded,
			"URL-safe base64 must survive query-string transport unchanged")

		gotTime, gotID, parseErr := parseBridgeReadinessCursor(urlDecoded)
		require.NoError(t, parseErr)
		assert.Equal(t, original.CreatedAt.UTC(), gotTime)
		assert.Equal(t, original.ID, gotID.String())
	}
}

func TestComputeNextCursor_PartialPage_NoCursor(t *testing.T) {
	t.Parallel()

	got, err := computeNextCursor(nil, 10)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestResolveStaleThreshold_NilHandler_Default(t *testing.T) {
	t.Parallel()

	var h *Handler
	assert.Equal(t, defaultBridgeStaleThreshold, h.resolveStaleThreshold(context.Background()))
}

func TestResolveStaleThreshold_NilProvider_Default(t *testing.T) {
	t.Parallel()

	h := &Handler{}
	assert.Equal(t, defaultBridgeStaleThreshold, h.resolveStaleThreshold(context.Background()))
}

func TestResolveStaleThreshold_ZeroProvider_Default(t *testing.T) {
	t.Parallel()

	h := &Handler{staleness: func() time.Duration { return 0 }}
	assert.Equal(t, defaultBridgeStaleThreshold, h.resolveStaleThreshold(context.Background()))
}

func TestResolveStaleThreshold_PositiveProvider_Used(t *testing.T) {
	t.Parallel()

	h := &Handler{staleness: func() time.Duration { return 30 * time.Second }}
	assert.Equal(t, 30*time.Second, h.resolveStaleThreshold(context.Background()))
}

func TestWithStalenessProvider_NilHandler_Noop(t *testing.T) {
	t.Parallel()

	var h *Handler
	h.WithStalenessProvider(func() time.Duration { return time.Hour })
	// no panic
}
