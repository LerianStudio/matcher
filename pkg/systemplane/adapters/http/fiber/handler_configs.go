// Copyright 2025 Lerian Studio.

package fiberhttp

import (
	"fmt"
	"net/http"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/service"
)

// GetConfigs returns all resolved config values.
//
// @ID getSystemConfigs
// @Summary      Get effective configs
// @Description  Returns all resolved configuration values with their source and redaction status.
// @Description  Bootstrap-only keys (require restart) are included with their current effective value.
// @Tags         System Configs
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id  header  string  false  "Request ID for tracing"
// @Success      200  {object}  ConfigsResponse    "Resolved configuration values"
// @Failure      401  {object}  ErrorResponse      "Unauthorized"
// @Failure      403  {object}  ErrorResponse      "Forbidden"
// @Failure      500  {object}  ErrorResponse      "Internal server error"
// @Router       /v1/system/configs [get]
func (handler *Handler) GetConfigs(fiberCtx *fiber.Ctx) error {
	resolved, err := handler.manager.GetConfigs(fiberCtx.UserContext())
	if err != nil {
		return writeError(fiberCtx, err)
	}

	return fiberCtx.JSON(toConfigsResponse(resolved))
}

// PatchConfigs applies a batch of config mutations with optimistic concurrency.
// The expected revision is conveyed via the If-Match header. A null JSON value
// for any key resets it to its registered default.
//
// @ID patchSystemConfigs
// @Summary      Patch config values
// @Description  Applies a batch of configuration mutations with optimistic concurrency control.
// @Description  Pass the expected revision in the If-Match header (ETag format). A null JSON
// @Description  value for any key resets it to its registered default.
// @Tags         System Configs
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id  header  string              false  "Request ID for tracing"
// @Param        If-Match      header  string              false  "Expected revision for optimistic concurrency (e.g. \"3\")"
// @Param        request       body    PatchConfigsRequest  true   "Configuration values to update"
// @Success      200  {object}  PatchResponse   "New revision after successful write"
// @Failure      400  {object}  ErrorResponse   "Invalid request body or empty values"
// @Failure      401  {object}  ErrorResponse   "Unauthorized"
// @Failure      403  {object}  ErrorResponse   "Forbidden"
// @Failure      409  {object}  ErrorResponse   "Revision conflict"
// @Failure      500  {object}  ErrorResponse   "Internal server error"
// @Router       /v1/system/configs [patch]
func (handler *Handler) PatchConfigs(fiberCtx *fiber.Ctx) error {
	var req PatchConfigsRequest
	if err := fiberCtx.BodyParser(&req); err != nil {
		return fiberCtx.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Code:    "system_invalid_request",
			Message: "invalid request body",
		})
	}

	if len(req.Values) == 0 {
		return fiberCtx.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Code:    "system_invalid_request",
			Message: "values must not be empty",
		})
	}

	revision, err := parseRevision(fiberCtx.Get("If-Match"))
	if err != nil {
		return fiberCtx.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Code:    "system_invalid_revision",
			Message: "If-Match header must contain a valid revision number",
		})
	}

	actor, err := handler.identity.Actor(fiberCtx.UserContext())
	if err != nil {
		return writeError(fiberCtx, err)
	}

	ops := toWriteOps(req.Values)

	result, err := handler.manager.PatchConfigs(fiberCtx.UserContext(), service.PatchRequest{
		Ops:              ops,
		ExpectedRevision: revision,
		Actor:            actor,
		Source:           "api",
	})
	if err != nil {
		return writeError(fiberCtx, err)
	}

	fiberCtx.Set("ETag", fmt.Sprintf(`"%d"`, result.Revision.Uint64()))

	return fiberCtx.JSON(PatchResponse{Revision: result.Revision.Uint64()})
}

// GetConfigSchema returns the registry metadata for all config keys.
//
// @ID getSystemConfigSchema
// @Summary      Get config key schema
// @Description  Returns the registry metadata for all configuration keys, including
// @Description  value type, default value, allowed scopes, mutability, and apply behavior.
// @Tags         System Configs
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id  header  string  false  "Request ID for tracing"
// @Success      200  {object}  SchemaResponse  "Schema metadata for all config keys"
// @Failure      401  {object}  ErrorResponse   "Unauthorized"
// @Failure      403  {object}  ErrorResponse   "Forbidden"
// @Failure      500  {object}  ErrorResponse   "Internal server error"
// @Router       /v1/system/configs/schema [get]
func (handler *Handler) GetConfigSchema(fiberCtx *fiber.Ctx) error {
	entries, err := handler.manager.GetConfigSchema(fiberCtx.UserContext())
	if err != nil {
		return writeError(fiberCtx, err)
	}

	return fiberCtx.JSON(toSchemaResponse(entries))
}

// GetConfigHistory returns the change audit trail for config entries.
//
// @ID getSystemConfigHistory
// @Summary      Get config change history
// @Description  Returns the change audit trail for configuration entries, ordered by revision.
// @Description  Supports pagination via limit/offset and optional key filtering.
// @Tags         System Configs
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id  header  string  false  "Request ID for tracing"
// @Param        key           query   string  false  "Filter history by config key"
// @Param        limit         query   int     false  "Maximum number of entries to return"  default(50) minimum(1) maximum(100)
// @Param        offset        query   int     false  "Number of entries to skip"            default(0) minimum(0)
// @Success      200  {object}  HistoryResponse  "Configuration change history entries"
// @Failure      400  {object}  ErrorResponse    "Invalid query parameters"
// @Failure      401  {object}  ErrorResponse    "Unauthorized"
// @Failure      403  {object}  ErrorResponse    "Forbidden"
// @Failure      500  {object}  ErrorResponse    "Internal server error"
// @Router       /v1/system/configs/history [get]
func (handler *Handler) GetConfigHistory(fiberCtx *fiber.Ctx) error {
	filter, err := parseHistoryFilter(fiberCtx, domain.KindConfig)
	if err != nil {
		return writeError(fiberCtx, err)
	}

	entries, err := handler.manager.GetConfigHistory(fiberCtx.UserContext(), filter)
	if err != nil {
		return writeError(fiberCtx, err)
	}

	return fiberCtx.JSON(toHistoryResponse(entries))
}

// Reload triggers a full configuration reload via the supervisor. The
// response is a simple status acknowledgment; any underlying failure is
// wrapped with ErrReloadFailed for consistent error mapping.
//
// @ID reloadSystemConfigs
// @Summary      Reload configuration
// @Description  Triggers a full configuration resynchronization from the backing store.
// @Description  Use after manual database changes or to force a cache refresh.
// @Tags         System Configs
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id  header  string  false  "Request ID for tracing"
// @Success      200  {object}  ReloadResponse  "Reload acknowledged"
// @Failure      401  {object}  ErrorResponse   "Unauthorized"
// @Failure      403  {object}  ErrorResponse   "Forbidden"
// @Failure      500  {object}  ErrorResponse   "Reload failed"
// @Router       /v1/system/configs/reload [post]
func (handler *Handler) Reload(fiberCtx *fiber.Ctx) error {
	if err := handler.manager.Resync(fiberCtx.UserContext()); err != nil {
		return writeError(fiberCtx, fmt.Errorf("%w: %w", domain.ErrReloadFailed, err))
	}

	return fiberCtx.JSON(ReloadResponse{Status: "ok"})
}
