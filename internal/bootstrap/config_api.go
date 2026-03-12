// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	// Direct OTel imports required for span type signatures (trace.Span) and
	// nil-tracer fallback (otel.Tracer). lib-commons provides tracer extraction
	// via NewTrackingFromContext but does not re-export the trace.Span interface.
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	sharedhttp "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for config API handler.
var (
	ErrConfigManagerRequired = errors.New("config manager is required for config API")
	ErrEmptyChanges          = errors.New("changes map must not be empty")
)

const configHistoryLimit = 50

// ConfigAPIHandler handles HTTP requests for runtime configuration management.
// It exposes read/write endpoints for the system config under /v1/system/config.
type ConfigAPIHandler struct {
	configManager   *ConfigManager
	auditPublisher  *ConfigAuditPublisher
	auditRepository sharedPorts.AuditLogRepository
	logger          libLog.Logger
	authRequired    bool
	production      bool
}

// NewConfigAPIHandler creates a new ConfigAPIHandler.
func NewConfigAPIHandler(configManager *ConfigManager, logger libLog.Logger, production bool) (*ConfigAPIHandler, error) {
	if configManager == nil {
		return nil, ErrConfigManagerRequired
	}

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	// Auth route exposure is decided at startup. Changing auth.enabled requires a
	// restart because routes are mounted once during bootstrap.
	var authRequired bool

	if cfg := configManager.Get(); cfg != nil {
		authRequired = cfg.Auth.Enabled
	}

	return &ConfigAPIHandler{
		configManager: configManager,
		logger:        logger,
		authRequired:  authRequired,
		production:    production,
	}, nil
}

// requireConfigAuth verifies that the request has valid auth context.
//
// Auth route exposure is a startup-time concern. If auth was disabled when the
// handler was created, the config API remains unavailable until restart. If it
// was enabled, the request must still carry an authenticated principal.
// Returns true when it already wrote an error response and request handling
// must stop.
func (handler *ConfigAPIHandler) requireConfigAuth(fiberCtx *fiber.Ctx) bool {
	if !handler.authRequired {
		_ = sharedhttp.RespondError(fiberCtx, fiber.StatusForbidden, "forbidden", "config API requires authentication")
		return true
	}

	if strings.TrimSpace(auth.GetUserID(fiberCtx.UserContext())) == "" {
		_ = sharedhttp.RespondError(fiberCtx, fiber.StatusUnauthorized, "unauthorized", "missing authenticated principal")
		return true
	}

	return false
}

// SetAuditPublisher attaches an audit publisher to the handler.
// This is set after construction because the outbox repository may not be
// available at handler creation time (depends on module init ordering).
func (handler *ConfigAPIHandler) SetAuditPublisher(publisher *ConfigAuditPublisher) {
	if handler != nil {
		handler.auditPublisher = publisher
	}
}

// SetAuditRepository attaches an audit repository for config history reads.
func (handler *ConfigAPIHandler) SetAuditRepository(repository sharedPorts.AuditLogRepository) {
	if handler != nil {
		if isNilInterface(repository) {
			handler.auditRepository = nil
			return
		}

		handler.auditRepository = repository
	}
}

// SECURITY: systemTenantContext injects a stable system tenant ID into the
// request context, bypassing the normal JWT-based tenant extraction path.
//
// This is intentional for system-level config operations (audit/history reads)
// where no tenant should be derived from request input. The tenant comes from
// auth's stable default-tenant source so config history does not fragment when
// the runtime-config default tenant changes.
func (handler *ConfigAPIHandler) systemTenantContext(ctx context.Context) context.Context {
	if handler == nil {
		return ctx
	}

	tenantID := strings.TrimSpace(auth.GetDefaultTenantID())
	if tenantID == "" {
		return ctx
	}

	return context.WithValue(ctx, auth.TenantIDKey, tenantID)
}

// startConfigSpan starts an OpenTelemetry span for a config API operation.
// Follows the same nil-tracer fallback pattern used by all handler packages:
// when the context lacks a tracer (e.g., telemetry disabled), falls back to
// the global tracer so callers always receive a valid span.
func startConfigSpan(c *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	ctx := c.UserContext()
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	if tracer == nil {
		tracer = otel.Tracer("commons.default")
	}

	ctx, span := tracer.Start(ctx, name)

	return ctx, span, logger
}

// logConfigSpanError records an error on the span and logs it.
func logConfigSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error, production bool) {
	libOpentelemetry.HandleSpanError(span, message, err)
	libLog.SafeError(logger, ctx, message, err, production)
}

// GetConfig returns the current effective configuration with secrets redacted.
// @Summary      Get current configuration
// @Description  Returns the current effective configuration values with secrets redacted.
// @Description  Includes metadata: version, last reload timestamp, and env var overrides.
// @Description  Route is only registered when AUTH_ENABLED=true at startup.
// @ID           getSystemConfig
// @Tags         System
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id header string false "Request ID for tracing"
// @Success      200 {object} GetConfigResponse
// @Failure      401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure      403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure      500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router       /v1/system/config [get]
func (handler *ConfigAPIHandler) GetConfig(fiberCtx *fiber.Ctx) error {
	if handler == nil || handler.configManager == nil {
		return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "handler_unavailable", "configuration handler is not initialized")
	}

	if handler.requireConfigAuth(fiberCtx) {
		return nil
	}

	ctx, span, logger := startConfigSpan(fiberCtx, "handler.system.get_config")
	defer span.End()

	response := GetConfigResponse{
		Config:       buildRedactedConfig(handler.configManager),
		Version:      handler.configManager.Version(),
		LastReloadAt: handler.configManager.LastReloadAt(),
		EnvOverrides: buildEnvOverridesList(),
	}

	if writeErr := sharedhttp.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		logConfigSpanError(ctx, span, logger, "failed to write get config response", writeErr, handler.production)

		return fmt.Errorf("write get config response: %w", writeErr)
	}

	return nil
}

// GetSchema returns field metadata for all managed configuration fields.
// @Summary      Get configuration schema
// @Description  Returns field metadata for all YAML-managed configuration fields,
// @Description  grouped by section for UI rendering. Includes key, type, default,
// @Description  current value, hot-reloadability, env override status, and constraints.
// @Description  Route is only registered when AUTH_ENABLED=true at startup.
// @ID           getSystemConfigSchema
// @Tags         System
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id header string false "Request ID for tracing"
// @Success      200 {object} ConfigSchemaResponse
// @Failure      401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure      403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure      500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router       /v1/system/config/schema [get]
func (handler *ConfigAPIHandler) GetSchema(fiberCtx *fiber.Ctx) error {
	if handler == nil || handler.configManager == nil {
		return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "handler_unavailable", "configuration handler is not initialized")
	}

	if handler.requireConfigAuth(fiberCtx) {
		return nil
	}

	ctx, span, logger := startConfigSpan(fiberCtx, "handler.system.get_config_schema")
	defer span.End()

	response := buildSchemaResponse(handler.configManager)

	if writeErr := sharedhttp.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		logConfigSpanError(ctx, span, logger, "failed to write get schema response", writeErr, handler.production)

		return fmt.Errorf("write get schema response: %w", writeErr)
	}

	return nil
}

// UpdateConfig applies runtime configuration changes.
// @Summary      Update configuration
// @Description  Apply runtime configuration changes. Changes are validated, written to
// @Description  YAML, and hot-reloaded. Immutable keys (infrastructure-bound) are rejected.
// @Description  Route is only registered when AUTH_ENABLED=true at startup.
// @Description  Requires an initialized audit/history backend; failures can return runtime_apply_failed or audit_failed.
// @ID           updateSystemConfig
// @Tags         System
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id header string false "Request ID for tracing"
// @Param        request body UpdateConfigRequest true "Configuration changes"
// @Success      200 {object} UpdateConfigResponse
// @Failure      400 {object} sharedhttp.ErrorResponse "Invalid request"
// @Failure      401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure      403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure      422 {object} sharedhttp.ErrorResponse "Validation failed"
// @Failure      500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router       /v1/system/config [patch]
func (handler *ConfigAPIHandler) UpdateConfig(fiberCtx *fiber.Ctx) error {
	if handler == nil || handler.configManager == nil {
		return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "handler_unavailable", "configuration handler is not initialized")
	}

	if handler.auditRepository == nil {
		return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "audit_unavailable", "configuration audit backend is not initialized")
	}

	if handler.requireConfigAuth(fiberCtx) {
		return nil
	}

	ctx, span, logger := startConfigSpan(fiberCtx, "handler.system.update_config")
	defer span.End()

	var req UpdateConfigRequest
	if err := fiberCtx.BodyParser(&req); err != nil {
		logConfigSpanError(ctx, span, logger, "invalid request body", err, handler.production)

		return sharedhttp.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid JSON body")
	}

	if len(req.Changes) == 0 {
		logConfigSpanError(ctx, span, logger, "empty changes", ErrEmptyChanges, handler.production)

		return sharedhttp.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "changes map must not be empty")
	}

	result, err := handler.configManager.Update(req.Changes)
	if err != nil {
		logConfigSpanError(ctx, span, logger, "config update failed", err, handler.production)

		return respondConfigUpdateError(fiberCtx, err)
	}

	if auditErr := handler.publishConfigUpdateAudit(ctx, result); auditErr != nil {
		logConfigSpanError(ctx, span, logger, "failed to publish config update audit event", auditErr, handler.production)

		return sharedhttp.RespondError(
			fiberCtx,
			fiber.StatusInternalServerError,
			"audit_failed",
			"configuration update was applied but audit persistence failed",
		)
	}

	response := UpdateConfigResponse{
		Applied:  result.Applied,
		Rejected: result.Rejected,
		Version:  handler.configManager.Version(),
	}

	if writeErr := sharedhttp.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		logConfigSpanError(ctx, span, logger, "failed to write update config response", writeErr, handler.production)

		return fmt.Errorf("write update config response: %w", writeErr)
	}

	return nil
}

func respondConfigUpdateError(fiberCtx *fiber.Ctx, err error) error {
	if errors.Is(err, ErrConfigSubscriberFailure) {
		return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "runtime_apply_failed", "configuration update could not be fully applied at runtime")
	}

	if errors.Is(err, ErrConfigValidationFailure) {
		return sharedhttp.RespondError(fiberCtx, fiber.StatusUnprocessableEntity, "validation_failed", err.Error())
	}

	return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "update_failed", "configuration update failed")
}

func (handler *ConfigAPIHandler) publishConfigUpdateAudit(
	ctx context.Context,
	result *UpdateResult,
) error {
	if handler == nil || result == nil || len(result.Applied) == 0 {
		return nil
	}

	if handler.auditRepository == nil {
		return ErrNilAuditRepoForConfigAudit
	}

	actor := auth.GetUserID(ctx)
	if actor == "" {
		actor = "unknown"
	}

	changes := appliedToConfigChanges(result.Applied)
	auditCtx := handler.systemTenantContext(ctx)

	return publishConfigChangeWithFallback(auditCtx, handler.auditPublisher, handler.auditRepository, actor, "updated", changes)
}

// ReloadConfig forces a configuration reload from disk.
// @Summary      Force reload configuration from disk
// @Description  Re-reads the YAML configuration file, applies environment variable overlays,
// @Description  validates the result, and atomically swaps the active config. Returns a diff
// @Description  of detected changes.
// @Description  Route is only registered when AUTH_ENABLED=true at startup.
// @Description  Requires an initialized audit/history backend; failures can return runtime_apply_failed or audit_failed.
// @ID           reloadSystemConfig
// @Tags         System
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id header string false "Request ID for tracing"
// @Success      200 {object} ReloadConfigResponse
// @Failure      401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure      403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure      500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router       /v1/system/config/reload [post]
func (handler *ConfigAPIHandler) ReloadConfig(fiberCtx *fiber.Ctx) error {
	if handler == nil || handler.configManager == nil {
		return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "handler_unavailable", "configuration handler is not initialized")
	}

	if handler.auditRepository == nil {
		return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "audit_unavailable", "configuration audit backend is not initialized")
	}

	if handler.requireConfigAuth(fiberCtx) {
		return nil
	}

	ctx, span, logger := startConfigSpan(fiberCtx, "handler.system.reload_config")
	defer span.End()

	result, err := handler.configManager.ReloadFromAPI()
	if err != nil {
		logConfigSpanError(ctx, span, logger, "config reload failed", err, handler.production)

		if errors.Is(err, ErrConfigSubscriberFailure) {
			return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "runtime_apply_failed", "configuration reload could not be fully applied at runtime")
		}

		return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "reload_failed", "configuration reload failed")
	}

	// Publish audit event for manual reload (best-effort).
	if result.ChangesDetected > 0 {
		actor := auth.GetUserID(fiberCtx.UserContext())
		if actor == "" {
			actor = "manual_reload"
		}

		auditCtx := handler.systemTenantContext(ctx)

		if auditErr := publishConfigChangeWithFallback(auditCtx, handler.auditPublisher, handler.auditRepository, actor, "reloaded", result.Changes); auditErr != nil {
			logConfigSpanError(ctx, span, logger, "failed to publish config reload audit event", auditErr, handler.production)

			return sharedhttp.RespondError(
				fiberCtx,
				fiber.StatusInternalServerError,
				"audit_failed",
				"configuration reload was applied but audit persistence failed",
			)
		}
	}

	response := ReloadConfigResponse{
		Version:         result.Version,
		ReloadedAt:      result.ReloadedAt,
		ChangesDetected: result.ChangesDetected,
		Changes:         result.Changes,
	}

	if writeErr := sharedhttp.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		logConfigSpanError(ctx, span, logger, "failed to write reload config response", writeErr, handler.production)

		return fmt.Errorf("write reload config response: %w", writeErr)
	}

	return nil
}

// GetConfigHistory returns recent configuration change history.
// @Summary      Get configuration change history
// @Description  Returns recent configuration changes with timestamps, actors, and diffs.
// @Description  Route is only registered when AUTH_ENABLED=true at startup.
// @Description  Requires an initialized audit/history backend.
// @ID           getSystemConfigHistory
// @Tags         System
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id header string false "Request ID for tracing"
// @Success      200 {object} ConfigHistoryResponse
// @Failure      401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure      403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure      500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router       /v1/system/config/history [get]
func (handler *ConfigAPIHandler) GetConfigHistory(fiberCtx *fiber.Ctx) error {
	if handler == nil || handler.configManager == nil {
		return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "handler_unavailable", "configuration handler is not initialized")
	}

	if handler.auditRepository == nil {
		return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "history_unavailable", "configuration history backend is not initialized")
	}

	if handler.requireConfigAuth(fiberCtx) {
		return nil
	}

	ctx, span, logger := startConfigSpan(fiberCtx, "handler.system.get_config_history")
	defer span.End()

	historyCtx := handler.systemTenantContext(ctx)

	logs, _, err := handler.auditRepository.ListByEntity(historyCtx, systemConfigEntityType, systemConfigEntityID, nil, configHistoryLimit)
	if err != nil {
		logConfigSpanError(ctx, span, logger, "failed to load config history", err, handler.production)

		return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "history_load_failed", "failed to load configuration history")
	}

	items := make([]ConfigHistoryEntry, 0, len(logs))
	for _, auditLog := range logs {
		if auditLog == nil {
			continue
		}

		items = append(items, ConfigHistoryEntry{
			Timestamp:  auditLog.CreatedAt,
			Actor:      auditActor(auditLog.ActorID),
			ChangeType: auditLog.Action,
			Changes:    extractAuditConfigChanges(auditLog.Changes),
		})
	}

	response := ConfigHistoryResponse{
		Items: items,
	}

	if writeErr := sharedhttp.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		logConfigSpanError(ctx, span, logger, "failed to write config history response", writeErr, handler.production)

		return fmt.Errorf("write config history response: %w", writeErr)
	}

	return nil
}

func auditActor(actor *string) string {
	if actor == nil || *actor == "" {
		return "system"
	}

	return *actor
}

func extractAuditConfigChanges(payload []byte) []ConfigChange {
	if len(payload) == 0 {
		return nil
	}

	var raw struct {
		ConfigChanges []struct {
			Key      string `json:"key"`
			OldValue any    `json:"old_value"`
			NewValue any    `json:"new_value"`
		} `json:"config_changes"`
	}

	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil
	}

	changes := make([]ConfigChange, 0, len(raw.ConfigChanges))
	for _, item := range raw.ConfigChanges {
		oldValue := redactIfSensitive(item.Key, item.OldValue)
		newValue := redactIfSensitive(item.Key, item.NewValue)

		changes = append(changes, ConfigChange{
			Key:      item.Key,
			OldValue: oldValue,
			NewValue: newValue,
		})
	}

	return changes
}

// appliedToConfigChanges converts applied change results back to ConfigChange
// for the audit publisher. This avoids leaking the internal ConfigChangeResult
// type into the audit interface.
func appliedToConfigChanges(applied []ConfigChangeResult) []ConfigChange {
	changes := make([]ConfigChange, 0, len(applied))
	for _, a := range applied {
		changes = append(changes, ConfigChange{
			Key:      a.Key,
			OldValue: a.OldValue,
			NewValue: a.NewValue,
		})
	}

	return changes
}
