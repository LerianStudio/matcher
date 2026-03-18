// Copyright 2025 Lerian Studio.

package fiberhttp

import (
	"fmt"
	"net/http"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/service"
)

// GetSettings returns resolved settings for the current scope. The scope
// defaults to "tenant" and can be overridden with ?scope=global.
func (handler *Handler) GetSettings(fiberCtx *fiber.Ctx) error {
	subject, err := handler.resolveSubject(fiberCtx)
	if err != nil {
		return writeError(fiberCtx, err)
	}

	resolved, err := handler.manager.GetSettings(fiberCtx.UserContext(), subject)
	if err != nil {
		return writeError(fiberCtx, err)
	}

	return fiberCtx.JSON(toSettingsResponse(resolved, subject))
}

// PatchSettings applies a batch of settings mutations for the resolved scope.
func (handler *Handler) PatchSettings(fiberCtx *fiber.Ctx) error {
	subject, err := handler.resolveSubject(fiberCtx)
	if err != nil {
		return writeError(fiberCtx, err)
	}

	var req PatchSettingsRequest
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

	result, err := handler.manager.PatchSettings(fiberCtx.UserContext(), subject, service.PatchRequest{
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

// GetSettingSchema returns the registry metadata for all setting keys.
func (handler *Handler) GetSettingSchema(fiberCtx *fiber.Ctx) error {
	entries, err := handler.manager.GetSettingSchema(fiberCtx.UserContext())
	if err != nil {
		return writeError(fiberCtx, err)
	}

	return fiberCtx.JSON(toSchemaResponse(entries))
}

// GetSettingHistory returns the change audit trail for settings entries.
// The subject is resolved from auth context — clients cannot specify
// subjectId via query params (tenant isolation enforcement).
func (handler *Handler) GetSettingHistory(fiberCtx *fiber.Ctx) error {
	filter := parseHistoryFilter(fiberCtx, domain.KindSetting)

	// Override subject from auth context — never trust client-supplied subjectId.
	subject, err := handler.resolveSubject(fiberCtx)
	if err != nil {
		return writeError(fiberCtx, err)
	}

	filter.Scope = subject.Scope
	filter.SubjectID = subject.SubjectID

	entries, err := handler.manager.GetSettingHistory(fiberCtx.UserContext(), filter)
	if err != nil {
		return writeError(fiberCtx, err)
	}

	return fiberCtx.JSON(toHistoryResponse(entries))
}

// resolveSubject determines the settings subject from the "scope" query
// parameter and the identity resolver. Defaults to tenant scope when the
// parameter is absent.
func (handler *Handler) resolveSubject(fiberCtx *fiber.Ctx) (service.Subject, error) {
	scope := fiberCtx.Query("scope", "tenant")

	switch scope {
	case "global":
		return service.Subject{Scope: domain.ScopeGlobal}, nil
	case "tenant":
		tenantID, err := handler.identity.TenantID(fiberCtx.UserContext())
		if err != nil {
			return service.Subject{}, fmt.Errorf("resolve tenant: %w", err)
		}

		return service.Subject{
			Scope:     domain.ScopeTenant,
			SubjectID: tenantID,
		}, nil
	default:
		return service.Subject{}, domain.ErrScopeInvalid
	}
}
