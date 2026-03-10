package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	sharedhttp "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
)

// Sentinel errors for config API handler.
var (
	ErrConfigManagerRequired = errors.New("config manager is required for config API")
	ErrEmptyChanges          = errors.New("changes map must not be empty")
)

// ConfigAPIHandler handles HTTP requests for runtime configuration management.
// It exposes read/write endpoints for the system config under /v1/system/config.
type ConfigAPIHandler struct {
	configManager  *ConfigManager
	auditPublisher *ConfigAuditPublisher
	logger         libLog.Logger
}

// NewConfigAPIHandler creates a new ConfigAPIHandler.
func NewConfigAPIHandler(configManager *ConfigManager, logger libLog.Logger) (*ConfigAPIHandler, error) {
	if configManager == nil {
		return nil, ErrConfigManagerRequired
	}

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	return &ConfigAPIHandler{
		configManager: configManager,
		logger:        logger,
	}, nil
}

// SetAuditPublisher attaches an audit publisher to the handler.
// This is set after construction because the outbox repository may not be
// available at handler creation time (depends on module init ordering).
func (handler *ConfigAPIHandler) SetAuditPublisher(publisher *ConfigAuditPublisher) {
	if handler != nil {
		handler.auditPublisher = publisher
	}
}

// startConfigSpan starts an OpenTelemetry span for a config API operation.
func startConfigSpan(c *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	ctx := c.UserContext()
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if tracer == nil {
		tracer = otel.Tracer("bootstrap.config_api")
	}

	ctx, span := tracer.Start(ctx, name)

	return ctx, span, logger
}

// logConfigSpanError records an error on the span and logs it.
func logConfigSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	libOpentelemetry.HandleSpanError(span, message, err)
	libLog.SafeError(logger, ctx, message, err, false)
}

// GetConfig returns the current effective configuration with secrets redacted.
// @Summary      Get current configuration
// @Description  Returns the current effective configuration values with secrets redacted.
// @Description  Includes metadata: version, last reload timestamp, and env var overrides.
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
	ctx, span, logger := startConfigSpan(fiberCtx, "handler.system.get_config")
	defer span.End()

	response := GetConfigResponse{
		Config:       buildRedactedConfig(handler.configManager),
		Version:      handler.configManager.Version(),
		LastReloadAt: handler.configManager.LastReloadAt(),
		EnvOverrides: buildEnvOverridesList(),
	}

	if writeErr := sharedhttp.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		logConfigSpanError(ctx, span, logger, "failed to write get config response", writeErr)

		return fmt.Errorf("write get config response: %w", writeErr)
	}

	return nil
}

// GetSchema returns field metadata for all managed configuration fields.
// @Summary      Get configuration schema
// @Description  Returns field metadata for all YAML-managed configuration fields,
// @Description  grouped by section for UI rendering. Includes key, type, default,
// @Description  current value, hot-reloadability, env override status, and constraints.
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
	ctx, span, logger := startConfigSpan(fiberCtx, "handler.system.get_config_schema")
	defer span.End()

	response := buildSchemaResponse(handler.configManager)

	if writeErr := sharedhttp.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		logConfigSpanError(ctx, span, logger, "failed to write get schema response", writeErr)

		return fmt.Errorf("write get schema response: %w", writeErr)
	}

	return nil
}

// UpdateConfig applies runtime configuration changes.
// @Summary      Update configuration
// @Description  Apply runtime configuration changes. Changes are validated, written to
// @Description  YAML, and hot-reloaded. Immutable keys (infrastructure-bound) are rejected.
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
	ctx, span, logger := startConfigSpan(fiberCtx, "handler.system.update_config")
	defer span.End()

	var req UpdateConfigRequest
	if err := fiberCtx.BodyParser(&req); err != nil {
		logConfigSpanError(ctx, span, logger, "invalid request body", err)

		return sharedhttp.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid JSON body")
	}

	if len(req.Changes) == 0 {
		logConfigSpanError(ctx, span, logger, "empty changes", ErrEmptyChanges)

		return sharedhttp.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "changes map must not be empty")
	}

	result, err := handler.configManager.Update(req.Changes)
	if err != nil {
		logConfigSpanError(ctx, span, logger, "config update failed", err)

		return sharedhttp.RespondError(fiberCtx, fiber.StatusUnprocessableEntity, "validation_failed", "configuration validation failed")
	}

	// Publish audit event for the config update (best-effort — don't fail the request).
	if handler.auditPublisher != nil && len(result.Applied) > 0 {
		actor := auth.GetUserID(fiberCtx.UserContext())
		if actor == "" {
			actor = "unknown"
		}

		changes := appliedToConfigChanges(result.Applied)

		if auditErr := handler.auditPublisher.PublishConfigChange(ctx, actor, "updated", changes); auditErr != nil {
			// Log but don't fail the request — the config update itself succeeded.
			logConfigSpanError(ctx, span, logger, "failed to publish config update audit event", auditErr)
		}
	}

	response := UpdateConfigResponse{
		Applied:  result.Applied,
		Rejected: result.Rejected,
		Version:  handler.configManager.Version(),
	}

	if writeErr := sharedhttp.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		logConfigSpanError(ctx, span, logger, "failed to write update config response", writeErr)

		return fmt.Errorf("write update config response: %w", writeErr)
	}

	return nil
}

// ReloadConfig forces a configuration reload from disk.
// @Summary      Force reload configuration from disk
// @Description  Re-reads the YAML configuration file, applies environment variable overlays,
// @Description  validates the result, and atomically swaps the active config. Returns a diff
// @Description  of detected changes.
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
	ctx, span, logger := startConfigSpan(fiberCtx, "handler.system.reload_config")
	defer span.End()

	result, err := handler.configManager.Reload()
	if err != nil {
		logConfigSpanError(ctx, span, logger, "config reload failed", err)

		return sharedhttp.RespondError(fiberCtx, fiber.StatusInternalServerError, "reload_failed", "configuration reload failed")
	}

	// Publish audit event for manual reload (best-effort).
	if handler.auditPublisher != nil && result.ChangesDetected > 0 {
		actor := auth.GetUserID(fiberCtx.UserContext())
		if actor == "" {
			actor = "manual_reload"
		}

		if auditErr := handler.auditPublisher.PublishConfigChange(ctx, actor, "reloaded", result.Changes); auditErr != nil {
			logConfigSpanError(ctx, span, logger, "failed to publish config reload audit event", auditErr)
		}
	}

	response := ReloadConfigResponse{
		Version:         result.Version,
		ReloadedAt:      result.ReloadedAt,
		ChangesDetected: result.ChangesDetected,
		Changes:         result.Changes,
	}

	if writeErr := sharedhttp.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		logConfigSpanError(ctx, span, logger, "failed to write reload config response", writeErr)

		return fmt.Errorf("write reload config response: %w", writeErr)
	}

	return nil
}

// GetConfigHistory returns recent configuration change history.
// @Summary      Get configuration change history
// @Description  Returns recent configuration changes with timestamps, actors, and diffs.
// @Description  Currently returns an empty list — full audit integration is planned for T10.
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
	ctx, span, logger := startConfigSpan(fiberCtx, "handler.system.get_config_history")
	defer span.End()

	// TODO(T10): Query audit_log table for entity_type="system_config" to return
	// real history entries. For now, return an empty list as a placeholder.
	response := ConfigHistoryResponse{
		Items: []ConfigHistoryEntry{},
	}

	if writeErr := sharedhttp.Respond(fiberCtx, fiber.StatusOK, response); writeErr != nil {
		logConfigSpanError(ctx, span, logger, "failed to write config history response", writeErr)

		return fmt.Errorf("write config history response: %w", writeErr)
	}

	return nil
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
