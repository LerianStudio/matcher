// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/http/dto"
	discoveryQuery "github.com/LerianStudio/matcher/internal/discovery/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

// Anchor the sharedhttp import so swaggo can resolve the
// sharedhttp.ErrorResponse references in the @Failure comments below.
var _ = sharedhttp.ErrorResponse{}

// Bridge readiness handler tunables. These ceilings keep a single dashboard
// page from forcing the database to scan more rows than a human can sensibly
// review; operators wanting a fuller picture should iterate cursor pages.
//
// defaultBridgeStaleThreshold is the single source of truth for the fallback
// staleness window when the systemplane provider is missing or returns a
// non-positive value. Centralising it here keeps config.go's
// FetcherBridgeStaleThreshold() and the handler's resolveStaleThreshold()
// from drifting apart.
const (
	defaultBridgeReadinessLimit = 50
	maxBridgeReadinessLimit     = 200
	defaultBridgeStaleThreshold = time.Hour

	// invalidLimitMessage is the stable, user-facing message returned for
	// every limit-related 400. The internal sentinel chain stays intact so
	// errors.Is callers can branch on the specific cause; the HTTP body just
	// commits to one phrasing so dashboard clients can match it reliably.
	invalidLimitMessage = "limit must be between 1 and 200"
)

// Sentinel errors for the bridge readiness HTTP layer. Static so err113
// stays happy and so callers can errors.Is on them when they bubble up.
var (
	errBridgeReadinessLimitNotInteger   = errors.New("limit must be a positive integer")
	errBridgeReadinessLimitNotPositive  = errors.New("limit must be > 0")
	errBridgeReadinessLimitTooLarge     = errors.New("limit exceeds maximum")
	errBridgeReadinessCursorNoCreatedAt = errors.New("cursor missing created_at")
)

// bridgeReadinessCursor is the opaque token shape echoed between the
// dashboard and the API. It is intentionally local to the handler — the
// repository's keyset surface is (createdAt, id) but downstream consumers
// don't need to know that. base64-encoded JSON keeps the contract evolvable.
type bridgeReadinessCursor struct {
	CreatedAt time.Time `json:"c"`
	ID        string    `json:"i"`
}

// stalenessProvider returns the operator-tunable stale threshold. The
// handler delegates to a closure so the systemplane can hot-reload the
// value without restarting the HTTP layer.
type stalenessProvider func() time.Duration

// GetBridgeReadinessSummary handles GET /v1/discovery/extractions/bridge/summary.
//
// @ID getDiscoveryBridgeReadinessSummary
// @Summary Get Fetcher bridge readiness summary
// @Description Returns aggregate counts of Fetcher extractions partitioned by bridge readiness state (pending, ready, stale, failed, in_flight) for the requesting tenant. Powers the operational dashboard backlog widget. AC-F1.
// @Tags Discovery
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Success 200 {object} dto.BridgeReadinessSummaryResponse "Aggregate readiness counts"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/discovery/extractions/bridge/summary [get]
func (handler *Handler) GetBridgeReadinessSummary(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.get_bridge_readiness_summary")
	defer span.End()

	threshold := handler.resolveStaleThreshold(ctx)

	summary, err := handler.query.CountBridgeReadinessByTenant(ctx, threshold)
	if err != nil {
		logSpanError(ctx, span, logger, "count bridge readiness", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to count bridge readiness")
	}

	if summary == nil {
		return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.BridgeReadinessSummaryResponse{})
	}

	response := dto.NewBridgeReadinessSummaryResponse(
		summary.Counts.Ready,
		summary.Counts.Pending,
		summary.Counts.Stale,
		summary.Counts.Failed,
		summary.Counts.InFlightCount,
		int64(summary.StaleThreshold.Seconds()),
		summary.GeneratedAt,
		summary.WorkerLastTickAt,
		summary.WorkerStalenessSeconds,
		summary.WorkerHealthy,
	)

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, response)
}

// ListBridgeCandidates handles GET /v1/discovery/extractions/bridge/candidates.
//
// @ID listDiscoveryBridgeCandidates
// @Summary List Fetcher bridge readiness candidates
// @Description Returns extractions in the requested readiness state with cursor pagination. Drilldown surface for the operational dashboard summary. AC-F2.
// @Tags Discovery
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param state query string true "Readiness state filter (pending, ready, stale, failed, in_flight)" Enums(pending, ready, stale, failed, in_flight)
// @Param cursor query string false "Opaque pagination cursor returned by the previous response"
// @Param limit query int false "Page size (default 50, max 200)"
// @Success 200 {object} dto.ListBridgeCandidatesResponse "Page of bridge candidates"
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/discovery/extractions/bridge/candidates [get]
func (handler *Handler) ListBridgeCandidates(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "discovery.http.list_bridge_candidates")
	defer span.End()

	state := fiberCtx.Query("state")
	if state == "" {
		return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "state query parameter is required")
	}

	limit, err := parseBridgeReadinessLimit(fiberCtx.Query("limit"))
	if err != nil {
		logSpanError(ctx, span, logger, "invalid limit", err)

		// Sentinel chain stays intact for callers; the user-facing string is
		// stable so dashboard clients can match it without parsing internals.
		return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", invalidLimitMessage)
	}

	cursorCreatedAt, cursorID, err := parseBridgeReadinessCursor(fiberCtx.Query("cursor"))
	if err != nil {
		logSpanError(ctx, span, logger, "invalid cursor", err)

		return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid cursor")
	}

	threshold := handler.resolveStaleThreshold(ctx)

	candidates, err := handler.query.ListBridgeCandidates(
		ctx,
		state,
		threshold,
		cursorCreatedAt,
		cursorID,
		limit,
	)
	if err != nil {
		switch {
		case errors.Is(err, discoveryQuery.ErrInvalidReadinessState):
			logSpanError(ctx, span, logger, "invalid state", err)
			return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid state value")
		case errors.Is(err, discoveryQuery.ErrReadinessLimitInvalid):
			logSpanError(ctx, span, logger, "invalid limit", err)
			return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", invalidLimitMessage)
		case errors.Is(err, discoveryQuery.ErrReadinessThresholdInvalid):
			logSpanError(ctx, span, logger, "invalid threshold", err)
			return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid threshold value")
		default:
			logSpanError(ctx, span, logger, "list bridge candidates", err)
			return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to list bridge candidates")
		}
	}

	items := make([]dto.BridgeCandidateResponse, 0, len(candidates))
	// Use-case ListBridgeCandidates already drops nil rows before returning,
	// so every candidate here has a non-nil Extraction. No defensive guard.
	for _, candidate := range candidates {
		var ingestionJobID *uuid.UUID

		if candidate.Extraction.IngestionJobID != uuid.Nil {
			jobID := candidate.Extraction.IngestionJobID
			ingestionJobID = &jobID
		}

		items = append(items, dto.NewBridgeCandidateResponse(
			candidate.Extraction.ID,
			candidate.Extraction.ConnectionID,
			candidate.Extraction.Status.String(),
			candidate.ReadinessState.String(),
			ingestionJobID,
			candidate.Extraction.FetcherJobID,
			candidate.Extraction.CreatedAt,
			candidate.Extraction.UpdatedAt,
			candidate.AgeSeconds,
			candidate.Extraction.BridgeLastError.String(),
		))
	}

	nextCursor, err := computeNextCursor(candidates, limit)
	if err != nil {
		logSpanError(ctx, span, logger, "encode next cursor", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "failed to encode pagination cursor")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ListBridgeCandidatesResponse{
		Items:      items,
		NextCursor: nextCursor,
		State:      state,
		Limit:      limit,
	})
}

// resolveStaleThreshold reads the operator-tunable threshold from the
// systemplane provider, falling back to defaultBridgeStaleThreshold when the
// handler was constructed without one (which only happens in tests) or when
// the provider hands back a non-positive value (misconfiguration).
//
// A non-positive provider value is still returned by the systemplane (callers
// mistuned it to 0 or negative). The clamp path is emitted at warn level so
// operators notice the misconfiguration in logs instead of silently getting
// the default.
func (handler *Handler) resolveStaleThreshold(ctx context.Context) time.Duration {
	if handler == nil || handler.staleness == nil {
		return defaultBridgeStaleThreshold
	}

	threshold := handler.staleness()
	if threshold <= 0 {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		if logger != nil {
			logger.With(
				libLog.String("configured", threshold.String()),
				libLog.String("clamped_to", defaultBridgeStaleThreshold.String()),
			).Log(ctx, libLog.LevelWarn, "resolveStaleThreshold: non-positive provider value clamped to default (below 1s sub-unit)")
		}

		return defaultBridgeStaleThreshold
	}

	return threshold
}

// parseBridgeReadinessLimit normalises the limit query parameter. Empty
// strings fall back to the default; values outside [1, max] are rejected
// so misuse is loud rather than silently clamped.
func parseBridgeReadinessLimit(raw string) (int, error) {
	if raw == "" {
		return defaultBridgeReadinessLimit, nil
	}

	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errBridgeReadinessLimitNotInteger
	}

	if parsed <= 0 {
		return 0, errBridgeReadinessLimitNotPositive
	}

	if parsed > maxBridgeReadinessLimit {
		return 0, fmt.Errorf("%w: %d", errBridgeReadinessLimitTooLarge, maxBridgeReadinessLimit)
	}

	return parsed, nil
}

// parseBridgeReadinessCursor decodes the opaque cursor token. An empty
// cursor decodes to the zero anchor (start of the result set).
//
// URL-safe base64 (RFC 4648 §5) is used because the cursor is transported in
// query strings — standard base64's '+' and '/' get url-decoded back to
// space and '/' which corrupts the payload before it reaches this decoder.
func parseBridgeReadinessCursor(raw string) (time.Time, uuid.UUID, error) {
	if raw == "" {
		return time.Time{}, uuid.Nil, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(raw)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("decode cursor: %w", err)
	}

	var cursor bridgeReadinessCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("unmarshal cursor: %w", err)
	}

	// Cheap zero-value check first: a cursor with no CreatedAt cannot
	// produce a meaningful keyset predicate regardless of the ID field, so
	// reject before paying the uuid.Parse cost (and avoid emitting a parse-
	// error log line when the real problem is an empty anchor).
	if cursor.CreatedAt.IsZero() {
		return time.Time{}, uuid.Nil, errBridgeReadinessCursorNoCreatedAt
	}

	id, err := uuid.Parse(cursor.ID)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor id: %w", err)
	}

	return cursor.CreatedAt.UTC(), id, nil
}

// computeNextCursor builds the cursor that resumes paging after the last
// returned item. Returns the empty string when the page is partial (i.e. no
// more rows exist) so the dashboard can hide the "next page" affordance.
//
// URL-safe base64 (RFC 4648 §5) is used so the cursor survives query-string
// transport without url-decoding corruption (see parseBridgeReadinessCursor).
func computeNextCursor(candidates []discoveryQuery.BridgeCandidate, limit int) (string, error) {
	if len(candidates) < limit {
		return "", nil
	}

	// Use-case ListBridgeCandidates guarantees non-nil Extraction on every
	// returned element, and the handler only reaches this path via that
	// use case, so skip the nil guard.
	last := candidates[len(candidates)-1]

	cursor := bridgeReadinessCursor{
		CreatedAt: last.Extraction.CreatedAt.UTC(),
		ID:        last.Extraction.ID.String(),
	}

	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("marshal cursor: %w", err)
	}

	return base64.URLEncoding.EncodeToString(payload), nil
}

// WithStalenessProvider attaches the operator-tunable stale-threshold
// provider to the handler. Exposed so bootstrap can wire the
// systemplane-backed closure after handler construction without expanding
// NewHandler's signature.
//
// Concurrency: set-once at bootstrap; readers are safe due to publish-before-
// serve ordering (the HTTP server is not accepting requests until after the
// provider is wired). The closure assignment is not guarded by a mutex
// because there is no scenario where two goroutines race to call this
// method.
func (handler *Handler) WithStalenessProvider(provider stalenessProvider) {
	if handler == nil {
		return
	}

	handler.staleness = provider
}
