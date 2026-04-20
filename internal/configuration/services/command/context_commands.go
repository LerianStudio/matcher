package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/ports"
)

// TODO(telemetry): configuration/adapters/http/handlers.go — logSpanError uses HandleSpanError for
// business outcomes (badRequest, notFound, writeNotFound). Add logSpanBusinessEvent using
// HandleSpanBusinessErrorEvent and create badRequestBiz/notFoundBiz variants for 400/404 responses.
// See reporting/adapters/http/handlers_export_job.go for the reference implementation.

const postgresUniqueViolationCode = "23505"

var (
	// ErrInlineCreateRequiresInfrastructure is returned when inline source/rule creation is attempted without an infrastructure provider.
	ErrInlineCreateRequiresInfrastructure = errors.New("infrastructure provider is required for inline source/rule creation")
	// ErrCreateContextTxSupportRequired indicates the repository does not support transactional create operations.
	ErrCreateContextTxSupportRequired = errors.New("transactional create is not supported by the configured repository")
	// ErrCreateContextReturnedNil indicates the context repository returned a nil entity without an error.
	ErrCreateContextReturnedNil = errors.New("context repository returned nil context")
	// ErrCreateSourceReturnedNil indicates the source repository returned a nil entity without an error.
	ErrCreateSourceReturnedNil = errors.New("source repository returned nil source")
)

// publishAudit publishes an audit event if the publisher is configured.
//
// Design Decision: Audit publish is intentionally non-blocking and best-effort.
// Per governance requirements (SOX compliance), audit failures must NOT block
// business operations. This follows the principle that observability/compliance
// infrastructure should be resilient to failures without impacting core workflows.
//
// Failures are observable via:
// - Structured logging with correlation IDs (from tracking context)
// - OpenTelemetry span events (for tracing integration)
//
// If durable audit guarantees are required in the future, consider implementing
// the outbox pattern: persist audit events in the same DB transaction, then
// publish asynchronously via a background worker.
func (uc *UseCase) publishAudit(
	ctx context.Context,
	entityType string,
	entityID uuid.UUID,
	action string,
	changes map[string]any,
) {
	if uc.auditPublisher == nil {
		return
	}

	logger, tracer, _, headerID := libCommons.NewTrackingFromContext(ctx)

	_, span := tracer.Start(ctx, "audit.publish")
	defer span.End()

	event := ports.AuditEvent{
		EntityType: entityType,
		EntityID:   entityID,
		Action:     action,
		OccurredAt: time.Now().UTC(),
		Changes:    changes,
	}

	if err := uc.auditPublisher.Publish(ctx, event); err != nil {
		libOpentelemetry.HandleSpanError(span, "audit_publish_failed", err)

		logger.With(
			libLog.Any("audit.entity_type", entityType),
			libLog.Any("audit.entity_id", entityID.String()),
			libLog.Any("audit.action", action),
			libLog.Any("correlation.header_id", headerID),
			libLog.Err(err),
		).Log(ctx, libLog.LevelError, "failed to publish audit event")
	}
}

// CreateContext creates a new reconciliation context.
func (uc *UseCase) CreateContext(
	ctx context.Context,
	tenantID uuid.UUID,
	input entities.CreateReconciliationContextInput,
) (*entities.ReconciliationContext, error) {
	if uc == nil || uc.contextRepo == nil {
		return nil, ErrNilContextRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.create_reconciliation_context")
	defer span.End()

	// Enforce unique context name within tenant scope.
	// This is a best-effort pre-check to return early conflicts.
	// The DB unique index remains the source of truth under concurrency.
	existing, err := uc.contextRepo.FindByName(ctx, input.Name)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("checking context name uniqueness: %w", err)
	}

	if existing != nil {
		return nil, ErrContextNameAlreadyExists
	}

	entity, err := entities.NewReconciliationContext(ctx, tenantID, input)
	if err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid reconciliation context input", err)
		return nil, err
	}

	inlineCreateRequested := len(input.Sources) > 0 || len(input.Rules) > 0
	if inlineCreateRequested && uc.infraProvider == nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "inline create requires infrastructure provider", ErrInlineCreateRequiresInfrastructure)

		logger.With(
			libLog.String("context.name", input.Name),
			libLog.Any("sources.count", len(input.Sources)),
			libLog.Any("rules.count", len(input.Rules)),
		).Log(ctx, libLog.LevelError, "inline create requested without infrastructure provider")

		return nil, ErrInlineCreateRequiresInfrastructure
	}

	created, err := uc.createContextWithChildren(ctx, entity, input)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create reconciliation context", err)

		logger.With(
			libLog.String("context.name", input.Name),
			libLog.Err(err),
		).Log(ctx, libLog.LevelError, "failed to create reconciliation context")

		return nil, err
	}

	uc.publishAudit(ctx, "context", created.ID, "create", map[string]any{
		"name":          created.Name,
		"type":          created.Type,
		"interval":      created.Interval,
		"sources_count": len(input.Sources),
		"rules_count":   len(input.Rules),
	})

	return created, nil
}

func (uc *UseCase) createContextWithChildren(
	ctx context.Context,
	entity *entities.ReconciliationContext,
	input entities.CreateReconciliationContextInput,
) (*entities.ReconciliationContext, error) {
	if len(input.Sources) == 0 && len(input.Rules) == 0 {
		return uc.createContextOnly(ctx, entity)
	}

	return uc.createContextTransactional(ctx, entity, input)
}

func (uc *UseCase) createContextOnly(ctx context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
	created, err := uc.contextRepo.Create(ctx, entity)
	if err != nil {
		return nil, normalizeContextCreateError(err)
	}

	if created == nil {
		return nil, ErrCreateContextReturnedNil
	}

	return created, nil
}

// inlineTxCreators holds the transactional creator interfaces resolved from
// the UseCase repositories. This struct is populated by resolveTxCreators
// and consumed by createContextTransactional.
type inlineTxCreators struct {
	context  contextTxCreator
	source   sourceTxCreator
	fieldMap fieldMapTxCreator
	rule     matchRuleTxCreator
}

func (uc *UseCase) resolveTxCreators(input entities.CreateReconciliationContextInput) (*inlineTxCreators, error) {
	txCtx, ok := uc.contextRepo.(contextTxCreator)
	if !ok {
		return nil, fmt.Errorf("context transactional create support: %w", ErrCreateContextTxSupportRequired)
	}

	creators := &inlineTxCreators{context: txCtx}

	if len(input.Sources) > 0 {
		creators.source, ok = uc.sourceRepo.(sourceTxCreator)
		if !ok {
			return nil, fmt.Errorf("source transactional create support: %w", ErrCreateContextTxSupportRequired)
		}
	}

	if hasInlineMappings(input.Sources) {
		creators.fieldMap, ok = uc.fieldMapRepo.(fieldMapTxCreator)
		if !ok {
			return nil, fmt.Errorf("field map transactional create support: %w", ErrCreateContextTxSupportRequired)
		}
	}

	if len(input.Rules) > 0 {
		creators.rule, ok = uc.matchRuleRepo.(matchRuleTxCreator)
		if !ok {
			return nil, fmt.Errorf("match rule transactional create support: %w", ErrCreateContextTxSupportRequired)
		}
	}

	return creators, nil
}

func (uc *UseCase) createContextTransactional(
	ctx context.Context,
	entity *entities.ReconciliationContext,
	input entities.CreateReconciliationContextInput,
) (*entities.ReconciliationContext, error) {
	if err := validateInlineRulePriorities(input.Rules); err != nil {
		return nil, err
	}

	creators, err := uc.resolveTxCreators(input)
	if err != nil {
		return nil, err
	}

	tx, cancel, txErr := beginTenantTx(ctx, uc.infraProvider)
	if txErr != nil {
		return nil, fmt.Errorf("begin create transaction: %w", txErr)
	}

	defer cancel()
	defer func() { _ = tx.Rollback() }()

	created, err := creators.context.CreateWithTx(ctx, tx, entity)
	if err != nil {
		return nil, normalizeContextCreateError(err)
	}

	if created == nil {
		return nil, ErrCreateContextReturnedNil
	}

	if err := createInlineSources(ctx, tx, created.ID, input.Sources, creators.source, creators.fieldMap); err != nil {
		return nil, err
	}

	if err := createInlineRules(ctx, tx, created.ID, input.Rules, creators.rule); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit create transaction: %w", err)
	}

	return created, nil
}

func createInlineSources(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	sources []entities.CreateContextSourceInput,
	txSourceCreator sourceTxCreator,
	txFieldMapCreator fieldMapTxCreator,
) error {
	for _, srcInput := range sources {
		srcEntity, srcErr := entities.NewReconciliationSource(ctx, contextID, entities.CreateReconciliationSourceInput{
			Name:   srcInput.Name,
			Type:   srcInput.Type,
			Side:   srcInput.Side,
			Config: srcInput.Config,
		})
		if srcErr != nil {
			return fmt.Errorf("invalid source input: %w", srcErr)
		}

		createdSrc, srcErr := txSourceCreator.CreateWithTx(ctx, tx, srcEntity)
		if srcErr != nil {
			return fmt.Errorf("creating source: %w", srcErr)
		}

		if createdSrc == nil {
			return ErrCreateSourceReturnedNil
		}

		if len(srcInput.Mapping) == 0 {
			continue
		}

		fmEntity, fmErr := entities.NewFieldMap(
			ctx,
			contextID,
			createdSrc.ID,
			entities.CreateFieldMapInput{Mapping: srcInput.Mapping},
		)
		if fmErr != nil {
			return fmt.Errorf("invalid field map input: %w", fmErr)
		}

		if _, fmErr = txFieldMapCreator.CreateWithTx(ctx, tx, fmEntity); fmErr != nil {
			return fmt.Errorf("creating field map: %w", fmErr)
		}
	}

	return nil
}

func createInlineRules(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	rules []entities.CreateMatchRuleInput,
	txRuleCreator matchRuleTxCreator,
) error {
	for _, ruleInput := range rules {
		ruleEntity, ruleErr := entities.NewMatchRule(ctx, contextID, ruleInput)
		if ruleErr != nil {
			return fmt.Errorf("invalid rule input: %w", ruleErr)
		}

		if _, ruleErr = txRuleCreator.CreateWithTx(ctx, tx, ruleEntity); ruleErr != nil {
			return fmt.Errorf("creating rule: %w", ruleErr)
		}
	}

	return nil
}

func hasInlineMappings(sources []entities.CreateContextSourceInput) bool {
	for _, src := range sources {
		if len(src.Mapping) > 0 {
			return true
		}
	}

	return false
}

func validateInlineRulePriorities(rules []entities.CreateMatchRuleInput) error {
	if len(rules) == 0 {
		return nil
	}

	seen := make(map[int]struct{}, len(rules))

	for _, rule := range rules {
		if _, exists := seen[rule.Priority]; exists {
			return entities.ErrRulePriorityConflict
		}

		seen[rule.Priority] = struct{}{}
	}

	return nil
}

func normalizeContextCreateError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == postgresUniqueViolationCode {
		switch pgErr.ConstraintName {
		case "uq_rule_context_priority":
			return entities.ErrRulePriorityConflict
		case "uq_context_tenant_name", "idx_reconciliation_contexts_name_unique":
			return ErrContextNameAlreadyExists
		default:
			return fmt.Errorf("unique constraint violation (%s): %w", pgErr.ConstraintName, err)
		}
	}

	return fmt.Errorf("creating reconciliation context: %w", err)
}

// UpdateContext modifies an existing reconciliation context.
func (uc *UseCase) UpdateContext(
	ctx context.Context,
	contextID uuid.UUID,
	input entities.UpdateReconciliationContextInput,
) (*entities.ReconciliationContext, error) {
	if uc == nil || uc.contextRepo == nil {
		return nil, ErrNilContextRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.update_reconciliation_context")
	defer span.End()

	entity, err := uc.contextRepo.FindByID(ctx, contextID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load reconciliation context", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to load reconciliation context")

		return nil, fmt.Errorf("finding reconciliation context: %w", err)
	}

	// Enforce unique context name when name is being changed.
	if err := uc.checkContextNameUniqueness(ctx, input.Name, entity.Name, contextID); err != nil {
		return nil, err
	}

	if err := entity.Update(ctx, input); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid reconciliation context update", err)
		return nil, err
	}

	updated, err := uc.contextRepo.Update(ctx, entity)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update reconciliation context", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to update reconciliation context")

		return nil, fmt.Errorf("updating reconciliation context: %w", err)
	}

	uc.publishAudit(ctx, "context", updated.ID, "update", map[string]any{
		"name":     updated.Name,
		"type":     updated.Type,
		"interval": updated.Interval,
	})

	return updated, nil
}

// checkContextNameUniqueness verifies that the new name (if changed) does not
// conflict with an existing context. Returns nil when no rename is requested
// or the name is available. FindByName returns (nil, nil) when no context
// exists with the given name. A non-nil error (other than sql.ErrNoRows)
// indicates a transient failure that must not be silently swallowed.
func (uc *UseCase) checkContextNameUniqueness(
	ctx context.Context,
	newName *string,
	currentName string,
	contextID uuid.UUID,
) error {
	if newName == nil || *newName == currentName {
		return nil
	}

	existing, err := uc.contextRepo.FindByName(ctx, *newName)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("checking context name uniqueness: %w", err)
	}

	if existing != nil && existing.ID != contextID {
		return ErrContextNameAlreadyExists
	}

	return nil
}

// DeleteContext removes a reconciliation context.
//
// Before deleting, this method checks for child entities (sources, match rules,
// and schedules) that reference this context. If any exist, the deletion is
// rejected with ErrContextHasChildEntities to prevent orphan data and
// referential integrity violations.
func (uc *UseCase) DeleteContext(ctx context.Context, contextID uuid.UUID) error {
	if uc == nil || uc.contextRepo == nil {
		return ErrNilContextRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.delete_reconciliation_context")
	defer span.End()

	if _, err := uc.contextRepo.FindByID(ctx, contextID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load reconciliation context", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to load reconciliation context")

		return fmt.Errorf("finding reconciliation context: %w", err)
	}

	if err := uc.checkContextChildren(ctx, contextID); err != nil {
		libOpentelemetry.HandleSpanError(span, "context has child entities", err)

		logger.With(libLog.String("context.id", contextID.String())).Log(ctx, libLog.LevelError, "cannot delete context: has child entities")

		return err
	}

	if err := uc.contextRepo.Delete(ctx, contextID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to delete reconciliation context", err)

		logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to delete reconciliation context")

		return fmt.Errorf("deleting reconciliation context: %w", err)
	}

	uc.publishAudit(ctx, "context", contextID, "delete", nil)

	return nil
}

// checkContextChildren verifies that no child entities (sources, match rules,
// fee rules, or schedules) reference the given context. Returns ErrContextHasChildEntities
// if any children exist. This is a guard against accidental data loss.
func (uc *UseCase) checkContextChildren(ctx context.Context, contextID uuid.UUID) error {
	// Check for associated sources.
	sources, _, err := uc.sourceRepo.FindByContextID(ctx, contextID, "", 1)
	if err != nil {
		return fmt.Errorf("checking context sources: %w", err)
	}

	if len(sources) > 0 {
		return ErrContextHasChildEntities
	}

	// Check for associated match rules.
	rules, _, err := uc.matchRuleRepo.FindByContextID(ctx, contextID, "", 1)
	if err != nil {
		return fmt.Errorf("checking context match rules: %w", err)
	}

	if len(rules) > 0 {
		return ErrContextHasChildEntities
	}

	// Check for associated fee rules (optional dependency).
	if uc.feeRuleRepo != nil {
		feeRules, err := uc.feeRuleRepo.FindByContextID(ctx, contextID)
		if err != nil {
			return fmt.Errorf("checking context fee rules: %w", err)
		}

		if len(feeRules) > 0 {
			return ErrContextHasChildEntities
		}
	}

	// Check for associated schedules (optional dependency).
	if uc.scheduleRepo != nil {
		schedules, err := uc.scheduleRepo.FindByContextID(ctx, contextID)
		if err != nil {
			return fmt.Errorf("checking context schedules: %w", err)
		}

		if len(schedules) > 0 {
			return ErrContextHasChildEntities
		}
	}

	return nil
}
