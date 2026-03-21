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
//
// @ID getSystemSettings
// @Summary      Get effective settings
// @Description  Returns resolved settings for the current scope. The scope defaults to "tenant"
// @Description  and can be overridden with ?scope=global. Tenant-scoped settings inherit from
// @Description  global defaults when no tenant override exists.
// @Tags         System Settings
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id  header  string  false  "Request ID for tracing"
// @Param        scope         query   string  false  "Resolution scope"  Enums(tenant, global) default(tenant)
// @Success      200  {object}  SettingsResponse  "Resolved setting values with scope"
// @Failure      400  {object}  ErrorResponse     "Invalid scope parameter"
// @Failure      401  {object}  ErrorResponse     "Unauthorized"
// @Failure      403  {object}  ErrorResponse     "Forbidden"
// @Failure      500  {object}  ErrorResponse     "Internal server error"
// @Router       /v1/system/settings [get]
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
//
// @ID patchSystemSettings
// @Summary      Patch setting values
// @Description  Applies a batch of settings mutations for the resolved scope with optimistic
// @Description  concurrency control. Pass the expected revision in the If-Match header.
// @Description  A null JSON value for any key resets it to its registered default.
// @Tags         System Settings
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id  header  string               false  "Request ID for tracing"
// @Param        If-Match      header  string               false  "Expected revision for optimistic concurrency (e.g. \"3\")"
// @Param        scope         query   string               false  "Resolution scope"  Enums(tenant, global) default(tenant)
// @Param        request       body    PatchSettingsRequest  true   "Setting values to update"
// @Success      200  {object}  PatchResponse   "New revision after successful write"
// @Failure      400  {object}  ErrorResponse   "Invalid request body or empty values"
// @Failure      401  {object}  ErrorResponse   "Unauthorized"
// @Failure      403  {object}  ErrorResponse   "Forbidden"
// @Failure      409  {object}  ErrorResponse   "Revision conflict"
// @Failure      500  {object}  ErrorResponse   "Internal server error"
// @Router       /v1/system/settings [patch]
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
//
// @ID getSystemSettingSchema
// @Summary      Get setting key schema
// @Description  Returns the registry metadata for all setting keys, including
// @Description  value type, default value, allowed scopes, mutability, and apply behavior.
// @Tags         System Settings
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id  header  string  false  "Request ID for tracing"
// @Success      200  {object}  SchemaResponse  "Schema metadata for all setting keys"
// @Failure      401  {object}  ErrorResponse   "Unauthorized"
// @Failure      403  {object}  ErrorResponse   "Forbidden"
// @Failure      500  {object}  ErrorResponse   "Internal server error"
// @Router       /v1/system/settings/schema [get]
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
//
// @ID getSystemSettingHistory
// @Summary      Get setting change history
// @Description  Returns the change audit trail for settings entries, ordered by revision.
// @Description  The subject is resolved from the auth context (tenant isolation enforcement).
// @Description  Supports pagination via limit/offset and optional key filtering.
// @Tags         System Settings
// @Produce      json
// @Security     BearerAuth
// @Param        X-Request-Id  header  string  false  "Request ID for tracing"
// @Param        key           query   string  false  "Filter history by setting key"
// @Param        scope         query   string  false  "Resolution scope"  Enums(tenant, global) default(tenant)
// @Param        limit         query   int     false  "Maximum number of entries to return"  default(50) minimum(1) maximum(100)
// @Param        offset        query   int     false  "Number of entries to skip"            default(0) minimum(0)
// @Success      200  {object}  HistoryResponse  "Setting change history entries"
// @Failure      400  {object}  ErrorResponse    "Invalid query parameters or scope"
// @Failure      401  {object}  ErrorResponse    "Unauthorized"
// @Failure      403  {object}  ErrorResponse    "Forbidden"
// @Failure      500  {object}  ErrorResponse    "Internal server error"
// @Router       /v1/system/settings/history [get]
func (handler *Handler) GetSettingHistory(fiberCtx *fiber.Ctx) error {
	filter, err := parseHistoryFilter(fiberCtx, domain.KindSetting)
	if err != nil {
		return writeError(fiberCtx, err)
	}

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
