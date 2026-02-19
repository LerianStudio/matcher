package http

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/internal/auth"
)

// Sentinel errors for route registration.
var (
	ErrProtectedRouteHelperRequired = errors.New("protected route helper is required")
	ErrHandlerRequired              = errors.New("handler is required")
)

// RegisterRoutes registers all configuration routes with the provided router.
//
//nolint:funlen // route registration is a single declarative block
func RegisterRoutes(protected func(resource, action string) fiber.Router, handler *Handler) error {
	if protected == nil {
		return ErrProtectedRouteHelperRequired
	}

	if handler == nil {
		return ErrHandlerRequired
	}

	protected(
		auth.ResourceConfiguration,
		auth.ActionContextCreate,
	).Post("/v1/config/contexts", handler.CreateContext)
	protected(
		auth.ResourceConfiguration,
		auth.ActionContextRead,
	).Get("/v1/config/contexts", handler.ListContexts)
	protected(
		auth.ResourceConfiguration,
		auth.ActionContextRead,
	).Get("/v1/config/contexts/:contextId", handler.GetContext)
	protected(
		auth.ResourceConfiguration,
		auth.ActionContextUpdate,
	).Patch("/v1/config/contexts/:contextId", handler.UpdateContext)
	protected(
		auth.ResourceConfiguration,
		auth.ActionContextDelete,
	).Delete("/v1/config/contexts/:contextId", handler.DeleteContext)

	protected(
		auth.ResourceConfiguration,
		auth.ActionContextCreate,
	).Post("/v1/config/contexts/:contextId/clone", handler.CloneContext)

	protected(
		auth.ResourceConfiguration,
		auth.ActionSourceCreate,
	).Post("/v1/config/contexts/:contextId/sources", handler.CreateSource)
	protected(
		auth.ResourceConfiguration,
		auth.ActionSourceRead,
	).Get("/v1/config/contexts/:contextId/sources", handler.ListSources)
	protected(
		auth.ResourceConfiguration,
		auth.ActionSourceRead,
	).Get("/v1/config/contexts/:contextId/sources/:sourceId", handler.GetSource)
	protected(
		auth.ResourceConfiguration,
		auth.ActionSourceUpdate,
	).Patch("/v1/config/contexts/:contextId/sources/:sourceId", handler.UpdateSource)
	protected(
		auth.ResourceConfiguration,
		auth.ActionSourceDelete,
	).Delete("/v1/config/contexts/:contextId/sources/:sourceId", handler.DeleteSource)

	protected(
		auth.ResourceConfiguration,
		auth.ActionFieldMapCreate,
	).Post("/v1/config/contexts/:contextId/sources/:sourceId/field-maps", handler.CreateFieldMap)
	protected(
		auth.ResourceConfiguration,
		auth.ActionFieldMapRead,
	).Get("/v1/config/contexts/:contextId/sources/:sourceId/field-maps", handler.GetFieldMapBySource)
	protected(
		auth.ResourceConfiguration,
		auth.ActionFieldMapUpdate,
	).Patch("/v1/config/field-maps/:fieldMapId", handler.UpdateFieldMap)
	protected(
		auth.ResourceConfiguration,
		auth.ActionFieldMapDelete,
	).Delete("/v1/config/field-maps/:fieldMapId", handler.DeleteFieldMap)

	protected(
		auth.ResourceConfiguration,
		auth.ActionRuleCreate,
	).Post("/v1/config/contexts/:contextId/rules", handler.CreateMatchRule)
	protected(
		auth.ResourceConfiguration,
		auth.ActionRuleRead,
	).Get("/v1/config/contexts/:contextId/rules", handler.ListMatchRules)
	protected(
		auth.ResourceConfiguration,
		auth.ActionRuleRead,
	).Get("/v1/config/contexts/:contextId/rules/:ruleId", handler.GetMatchRule)
	protected(
		auth.ResourceConfiguration,
		auth.ActionRuleUpdate,
	).Patch("/v1/config/contexts/:contextId/rules/:ruleId", handler.UpdateMatchRule)
	protected(
		auth.ResourceConfiguration,
		auth.ActionRuleDelete,
	).Delete("/v1/config/contexts/:contextId/rules/:ruleId", handler.DeleteMatchRule)
	protected(
		auth.ResourceConfiguration,
		auth.ActionRuleUpdate,
	).Post("/v1/config/contexts/:contextId/rules/reorder", handler.ReorderMatchRules)

	// Fee schedule routes
	protected(
		auth.ResourceConfiguration,
		auth.ActionFeeScheduleCreate,
	).Post("/v1/config/fee-schedules", handler.CreateFeeSchedule)
	protected(
		auth.ResourceConfiguration,
		auth.ActionFeeScheduleRead,
	).Get("/v1/config/fee-schedules", handler.ListFeeSchedules)
	protected(
		auth.ResourceConfiguration,
		auth.ActionFeeScheduleRead,
	).Get("/v1/config/fee-schedules/:scheduleId", handler.GetFeeSchedule)
	protected(
		auth.ResourceConfiguration,
		auth.ActionFeeScheduleUpdate,
	).Patch("/v1/config/fee-schedules/:scheduleId", handler.UpdateFeeSchedule)
	protected(
		auth.ResourceConfiguration,
		auth.ActionFeeScheduleDelete,
	).Delete("/v1/config/fee-schedules/:scheduleId", handler.DeleteFeeSchedule)
	protected(
		auth.ResourceConfiguration,
		auth.ActionFeeScheduleRead,
	).Post("/v1/config/fee-schedules/:scheduleId/simulate", handler.SimulateFeeSchedule)

	// Schedule routes
	protected(
		auth.ResourceConfiguration,
		auth.ActionScheduleCreate,
	).Post("/v1/config/contexts/:contextId/schedules", handler.CreateSchedule)
	protected(
		auth.ResourceConfiguration,
		auth.ActionScheduleRead,
	).Get("/v1/config/contexts/:contextId/schedules", handler.ListSchedules)
	protected(
		auth.ResourceConfiguration,
		auth.ActionScheduleRead,
	).Get("/v1/config/contexts/:contextId/schedules/:scheduleId", handler.GetSchedule)
	protected(
		auth.ResourceConfiguration,
		auth.ActionScheduleUpdate,
	).Patch("/v1/config/contexts/:contextId/schedules/:scheduleId", handler.UpdateSchedule)
	protected(
		auth.ResourceConfiguration,
		auth.ActionScheduleDelete,
	).Delete("/v1/config/contexts/:contextId/schedules/:scheduleId", handler.DeleteSchedule)

	return nil
}
