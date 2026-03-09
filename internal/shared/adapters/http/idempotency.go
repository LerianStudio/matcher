// Package http provides shared HTTP utilities and DTOs.
package http

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	pkghttp "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
)

// HTTP header constants for idempotency key extraction.
const (
	HeaderXIdempotencyKey      = "X-Idempotency-Key"
	HeaderIdempotencyKey       = "Idempotency-Key"
	HeaderXIdempotencyReplayed = "X-Idempotency-Replayed"
	httpErrorStatusThreshold   = 400
)

// Context key for tracking idempotency middleware execution.
const idempotencyProcessedKey = "idempotency_processed"

// Idempotency key validation constraints.
const (
	idempotencyKeyMaxLength = 128
)

// idempotencyKeyPattern allows alphanumeric characters, colons, underscores, and hyphens.
// This matches the validation in internal/exception/domain/value_objects/idempotency_key.go.
var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9:_-]+$`)

// Idempotency key validation errors.
var (
	ErrEmptyIdempotencyKey   = errors.New("idempotency key is required")
	ErrIdempotencyKeyTooLong = errors.New(
		"idempotency key exceeds maximum length of 128 characters",
	)
	ErrInvalidIdempotencyKey = errors.New(
		"idempotency key contains invalid characters; allowed: alphanumeric, colons, underscores, hyphens",
	)
	ErrMissingTenantID = errors.New(
		"tenant ID is required for idempotency; ensure auth middleware runs before idempotency middleware",
	)
)

// IdempotencyStatus represents the state of an idempotency key.
type IdempotencyStatus string

// Idempotency status constants.
const (
	IdempotencyStatusUnknown  IdempotencyStatus = "unknown"
	IdempotencyStatusPending  IdempotencyStatus = "pending"
	IdempotencyStatusComplete IdempotencyStatus = "complete"
	IdempotencyStatusFailed   IdempotencyStatus = "failed"
)

// IdempotencyResult holds the cached response for a completed idempotent request.
type IdempotencyResult struct {
	Status     IdempotencyStatus
	Response   []byte
	HTTPStatus int
}

// IdempotencyKey is the string identifier for an idempotent request.
type IdempotencyKey string

// IdempotencyRepository defines the interface for idempotency storage operations.
// This interface is implemented by the Redis adapter in the exception bounded context.
//
// TTL note: TryAcquire sets the initial pending marker with the same TTL as completed
// entries (typically 7 days via DefaultSuccessTTL). If a request crashes between
// TryAcquire and MarkComplete/MarkFailed, the pending marker blocks retries for
// that full duration. MarkFailed resets the TTL to a shorter window (typically 5
// minutes via DefaultFailedRetryWindow). Implementations should consider using a
// shorter initial TTL for the pending state (e.g., 5 minutes) and only extending
// to the full TTL on MarkComplete.
type IdempotencyRepository interface {
	TryAcquire(ctx context.Context, key IdempotencyKey) (acquired bool, err error)
	TryReacquireFromFailed(ctx context.Context, key IdempotencyKey) (acquired bool, err error)
	MarkComplete(ctx context.Context, key IdempotencyKey, response []byte, httpStatus int) error
	MarkFailed(ctx context.Context, key IdempotencyKey) error
	GetCachedResult(ctx context.Context, key IdempotencyKey) (*IdempotencyResult, error)
}

// IdempotencyMiddlewareConfig configures the idempotency middleware behavior.
type IdempotencyMiddlewareConfig struct {
	Repository IdempotencyRepository
	KeyPrefix  string
	SkipPaths  []string
}

// validateIdempotencyKeyFormat validates the user-provided idempotency key format.
// Returns nil if the key is valid, or a specific error describing the validation failure.
// This validation is applied ONLY to user-provided keys, not to auto-generated hash keys.
func validateIdempotencyKeyFormat(userKey string) error {
	if userKey == "" {
		return ErrEmptyIdempotencyKey
	}

	if len(userKey) > idempotencyKeyMaxLength {
		return ErrIdempotencyKeyTooLong
	}

	if !idempotencyKeyPattern.MatchString(userKey) {
		return ErrInvalidIdempotencyKey
	}

	return nil
}

// shouldSkipIdempotency checks if the request should bypass idempotency processing.
func shouldSkipIdempotency(fiberCtx *fiber.Ctx, cfg IdempotencyMiddlewareConfig) bool {
	if cfg.Repository == nil {
		return true
	}

	if fiberCtx.Locals(idempotencyProcessedKey) != nil {
		return true
	}

	method := fiberCtx.Method()
	if method != fiber.MethodPost && method != fiber.MethodPut && method != fiber.MethodPatch {
		return true
	}

	for _, path := range cfg.SkipPaths {
		if strings.HasPrefix(fiberCtx.Path(), path) {
			return true
		}
	}

	return false
}

// NewIdempotencyMiddleware creates a Fiber middleware that enforces idempotency
// for POST/PUT/PATCH requests. It implements the optimistic locking strategy:
// 1. Extract X-Idempotency-Key or Idempotency-Key header (fallback: SHA-256 of body)
// 2. Try to acquire lock via SETNX
// 3. If lock acquired: process request, cache response, return
// 4. If lock exists: check status - return 409 if pending, replay cached response if complete.
func NewIdempotencyMiddleware(cfg IdempotencyMiddlewareConfig) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		if shouldSkipIdempotency(fiberCtx, cfg) {
			return fiberCtx.Next()
		}

		fiberCtx.Locals(idempotencyProcessedKey, true)

		ctx := fiberCtx.UserContext()

		logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
		ctx, span := tracer.Start(ctx, "middleware.idempotency")

		defer span.End()

		return executeIdempotencyLogic(ctx, fiberCtx, cfg, logger, span)
	}
}

// handleKeyValidationError maps idempotency key validation errors to appropriate HTTP responses.
// Missing tenant ID is an internal configuration error (middleware ordering), not a client
// input error, so it returns 500 instead of 400.
func handleKeyValidationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	logger idempotencyLogger,
	span trace.Span,
	validationErr error,
) error {
	if errors.Is(validationErr, ErrMissingTenantID) {
		libOpentelemetry.HandleSpanError(span, "missing tenant ID for idempotency", validationErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("idempotency middleware: %v", validationErr))

		return pkghttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "idempotency_configuration_error", "an unexpected error occurred")
	}

	libOpentelemetry.HandleSpanError(span, "invalid idempotency key format", validationErr)

	logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("idempotency middleware: invalid key format: %v", validationErr))

	return pkghttp.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_idempotency_key", validationErr.Error())
}

// executeIdempotencyLogic handles the core idempotency logic after initial checks pass.
func executeIdempotencyLogic(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	cfg IdempotencyMiddlewareConfig,
	logger idempotencyLogger,
	span trace.Span,
) error {
	key, validationErr := extractIdempotencyKey(ctx, fiberCtx, cfg.KeyPrefix)
	if validationErr != nil {
		return handleKeyValidationError(ctx, fiberCtx, logger, span, validationErr)
	}

	if key == "" {
		return fiberCtx.Next()
	}

	idempotencyKey := IdempotencyKey(key)

	acquired, err := cfg.Repository.TryAcquire(ctx, idempotencyKey)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to acquire idempotency lock", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("idempotency middleware: failed to acquire lock: %v", err))

		return pkghttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "idempotency_error", "an unexpected error occurred")
	}

	if acquired {
		return processNewRequest(ctx, fiberCtx, cfg.Repository, idempotencyKey, logger, span)
	}

	return handleDuplicateRequest(ctx, fiberCtx, cfg.Repository, idempotencyKey, logger, span)
}

// extractIdempotencyKey extracts and constructs a tenant-scoped idempotency key.
// The key format is: prefix:tenantID:method:path:userKey
// This ensures tenant isolation - different tenants cannot share idempotency keys,
// and the same key on different endpoints/methods is treated as distinct.
//
// Returns:
//   - key: the constructed idempotency key (empty if no key should be used)
//   - validationErr: non-nil if the user-provided key fails validation (return 400)
func extractIdempotencyKey(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	prefix string,
) (string, error) {
	// Extract user-provided key from headers
	userKey := fiberCtx.Get(HeaderXIdempotencyKey)
	if userKey == "" {
		userKey = fiberCtx.Get(HeaderIdempotencyKey)
	}

	// Track whether key was user-provided (requires validation) or auto-generated
	userProvidedKey := userKey != ""

	if userKey == "" {
		// Body hash fallback: when no Idempotency-Key header is provided, a SHA-256
		// hash of the request body is used as the key. This means identical payloads
		// sent to the same tenant+method+path will be deduplicated within the TTL window.
		// If this behavior is undesirable, clients should always provide an explicit
		// Idempotency-Key header.
		body := fiberCtx.Body()
		if len(body) == 0 {
			return "", nil
		}

		// Compute SHA-256 of the body to produce a deterministic key. Note that two
		// different requests carrying byte-identical payloads will map to the same
		// idempotency key and therefore the second request will receive a cached
		// (replayed) response instead of being processed independently.
		hash := sha256.Sum256(body)
		userKey = "hash:" + hex.EncodeToString(hash[:])
	}

	// Validate user-provided keys BEFORE any further processing
	// Auto-generated hash keys are safe and don't need validation
	if userProvidedKey {
		if err := validateIdempotencyKeyFormat(userKey); err != nil {
			return "", err
		}
	}

	// Extract tenant ID from context (set by auth middleware).
	// A missing tenant ID indicates a middleware ordering bug (idempotency
	// middleware running before auth middleware) and must be treated as an
	// internal error to prevent unscoped keys that could leak across tenants.
	tenantID := auth.GetTenantID(ctx)
	if strings.TrimSpace(tenantID) == "" {
		return "", ErrMissingTenantID
	}

	// Include method and path for complete request scoping
	method := fiberCtx.Method()
	path := fiberCtx.Path()

	// Build the scoped key: prefix:tenantID:method:path:userKey
	// This prevents cross-tenant data leakage and ensures the same
	// idempotency key used on different endpoints is treated as distinct
	if prefix != "" {
		return fmt.Sprintf("%s:%s:%s:%s:%s", prefix, tenantID, method, path, userKey), nil
	}

	return fmt.Sprintf("%s:%s:%s:%s", tenantID, method, path, userKey), nil
}

type idempotencyLogger = libLog.Logger

func idempotencyKeyFingerprint(key IdempotencyKey) string {
	hash := sha256.Sum256([]byte(key))

	return hex.EncodeToString(hash[:])[:12]
}

// markRequestFailed records a failed request in the idempotency repository.
func markRequestFailed(
	ctx context.Context,
	repo IdempotencyRepository,
	key IdempotencyKey,
	logger idempotencyLogger,
) {
	if markErr := repo.MarkFailed(ctx, key); markErr != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("idempotency: failed to mark failed: %v", markErr))
	}
}

// markRequestComplete records a successful request in the idempotency repository.
func markRequestComplete(
	ctx context.Context,
	repo IdempotencyRepository,
	key IdempotencyKey,
	responseBody []byte,
	statusCode int,
	logger idempotencyLogger,
) {
	if markErr := repo.MarkComplete(ctx, key, responseBody, statusCode); markErr != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("idempotency: failed to mark complete: %v", markErr))
	}
}

func processNewRequest(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	repo IdempotencyRepository,
	key IdempotencyKey,
	logger idempotencyLogger,
	span trace.Span,
) error {
	err := fiberCtx.Next()

	statusCode := fiberCtx.Response().StatusCode()
	responseBody := fiberCtx.Response().Body()

	if err != nil {
		libOpentelemetry.HandleSpanError(span, "request processing failed", err)
		markRequestFailed(ctx, repo, key, logger)

		return err
	}

	if statusCode >= httpErrorStatusThreshold {
		markRequestFailed(ctx, repo, key, logger)

		return nil
	}

	markRequestComplete(ctx, repo, key, responseBody, statusCode, logger)

	return nil
}

func handleDuplicateRequest(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	repo IdempotencyRepository,
	key IdempotencyKey,
	logger idempotencyLogger,
	span trace.Span,
) error {
	result, err := repo.GetCachedResult(ctx, key)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get cached result", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("idempotency: failed to get cached result: %v", err))

		return pkghttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "idempotency_error", "an unexpected error occurred")
	}

	if result == nil {
		logger.Log(ctx, libLog.LevelError, "idempotency: cached result is nil")

		return pkghttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "idempotency_error", "an unexpected error occurred")
	}

	switch result.Status {
	case IdempotencyStatusPending:
		logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("idempotency: request in progress (key_hash=%s)", idempotencyKeyFingerprint(key)))

		return pkghttp.RespondError(
			fiberCtx,
			fiber.StatusConflict,
			"request_in_progress",
			"A request with this idempotency key is currently being processed",
		)

	case IdempotencyStatusComplete:
		logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("idempotency: replaying cached response (key_hash=%s)", idempotencyKeyFingerprint(key)))

		fiberCtx.Set(HeaderXIdempotencyReplayed, "true")

		statusCode := result.HTTPStatus
		if statusCode == 0 {
			statusCode = fiber.StatusOK
		}

		fiberCtx.Response().Header.SetContentType(fiber.MIMEApplicationJSON)

		return fiberCtx.Status(statusCode).Send(result.Response)

	case IdempotencyStatusFailed:
		reacquired, reacquireErr := repo.TryReacquireFromFailed(ctx, key)
		if reacquireErr != nil {
			libOpentelemetry.HandleSpanError(span, "failed to reacquire failed idempotency key", reacquireErr)

			logger.Log(ctx, libLog.LevelError, fmt.Sprintf("idempotency: failed to reacquire failed key: %v", reacquireErr))

			return pkghttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "idempotency_error", "an unexpected error occurred")
		}

		if !reacquired {
			logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("idempotency: failed-key retry already in progress (key_hash=%s)", idempotencyKeyFingerprint(key)))

			return pkghttp.RespondError(
				fiberCtx,
				fiber.StatusConflict,
				"request_in_progress",
				"A request with this idempotency key is currently being processed",
			)
		}

		logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("idempotency: previous request failed, allowing retry (key_hash=%s)", idempotencyKeyFingerprint(key)))

		return processNewRequest(ctx, fiberCtx, repo, key, logger, span)

	default:
		return fiberCtx.Next()
	}
}
