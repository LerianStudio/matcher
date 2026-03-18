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
func (handler *Handler) GetConfigSchema(fiberCtx *fiber.Ctx) error {
	entries, err := handler.manager.GetConfigSchema(fiberCtx.UserContext())
	if err != nil {
		return writeError(fiberCtx, err)
	}

	return fiberCtx.JSON(toSchemaResponse(entries))
}

// GetConfigHistory returns the change audit trail for config entries.
func (handler *Handler) GetConfigHistory(fiberCtx *fiber.Ctx) error {
	filter := parseHistoryFilter(fiberCtx, domain.KindConfig)

	entries, err := handler.manager.GetConfigHistory(fiberCtx.UserContext(), filter)
	if err != nil {
		return writeError(fiberCtx, err)
	}

	return fiberCtx.JSON(toHistoryResponse(entries))
}

// Reload triggers a full configuration reload via the supervisor. The
// response is a simple status acknowledgment; any underlying failure is
// wrapped with ErrReloadFailed for consistent error mapping.
func (handler *Handler) Reload(fiberCtx *fiber.Ctx) error {
	if err := handler.manager.Resync(fiberCtx.UserContext()); err != nil {
		return writeError(fiberCtx, fmt.Errorf("%w: %w", domain.ErrReloadFailed, err))
	}

	return fiberCtx.JSON(ReloadResponse{Status: "ok"})
}
