package command

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const maxClonePaginationLimit = 200

// ErrCloneNameRequired indicates the new context name was not provided for the clone operation.
var ErrCloneNameRequired = errors.New("new context name is required for clone")

// ErrCloneProviderRequired indicates the repository does not support transactional create for clone.
var ErrCloneProviderRequired = errors.New("repository does not support transactional create for clone")

type (
	contextTxCreator interface {
		CreateWithTx(ctx context.Context, tx *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error)
	}
	sourceTxCreator interface {
		CreateWithTx(ctx context.Context, tx *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error)
	}
	fieldMapTxCreator interface {
		CreateWithTx(ctx context.Context, tx *sql.Tx, entity *entities.FieldMap) (*entities.FieldMap, error)
	}
	matchRuleTxCreator interface {
		CreateWithTx(ctx context.Context, tx *sql.Tx, entity *entities.MatchRule) (*entities.MatchRule, error)
	}
	feeRuleTxCreator interface {
		CreateWithTx(ctx context.Context, tx *sql.Tx, rule *fee.FeeRule) error
	}
)

// WithInfrastructureProvider returns a UseCaseOption that sets the infrastructure provider for transactional clone operations.
func WithInfrastructureProvider(provider sharedPorts.InfrastructureProvider) UseCaseOption {
	return func(uc *UseCase) {
		if provider != nil {
			uc.infraProvider = provider
		}
	}
}

// CloneContextInput holds the parameters required to clone a reconciliation context.
type CloneContextInput struct {
	SourceContextID   uuid.UUID
	NewName           string
	IncludeSources    bool
	IncludeRules      bool
	AutoMatchOnUpload *bool
}

// CloneContext creates a deep copy of a reconciliation context with its associated resources.
func (uc *UseCase) CloneContext(ctx context.Context, input CloneContextInput) (*entities.CloneResult, error) {
	if err := uc.validateCloneDependencies(input); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.clone_context")
	defer span.End()

	sourceContext, err := uc.contextRepo.FindByID(ctx, input.SourceContextID)
	if err != nil {
		wrappedErr := fmt.Errorf("loading source context: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to load source context", wrappedErr)

		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to load source context")

		return nil, wrappedErr
	}

	autoMatchOnUpload := sourceContext.AutoMatchOnUpload

	if input.AutoMatchOnUpload != nil {
		autoMatchOnUpload = *input.AutoMatchOnUpload
	}

	if uc.infraProvider != nil {
		result, txErr := uc.cloneContextTransactional(ctx, input, sourceContext, autoMatchOnUpload)
		if txErr != nil {
			libOpentelemetry.HandleSpanError(span, "failed to clone context (transactional)", txErr)

			logger.With(libLog.Any("error", txErr.Error())).Log(ctx, libLog.LevelError, "failed to clone context (transactional)")

			return nil, txErr
		}

		uc.publishCloneAudit(ctx, input, result)

		return result, nil
	}

	logger.Log(ctx, libLog.LevelWarn, "clone operation running without transactional guarantee; set InfrastructureProvider via WithInfrastructureProvider to enable atomic clones")

	result, err := uc.cloneContextNonTransactional(ctx, input, sourceContext, autoMatchOnUpload)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to clone context", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to clone context")

		return nil, err
	}

	uc.publishCloneAudit(ctx, input, result)

	return result, nil
}

func (uc *UseCase) cloneContextTransactional(ctx context.Context, input CloneContextInput, sourceContext *entities.ReconciliationContext, autoMatchOnUpload bool) (*entities.CloneResult, error) {
	tx, cancel, err := beginTenantTx(ctx, uc.infraProvider)
	if err != nil {
		return nil, fmt.Errorf("begin clone transaction: %w", err)
	}

	defer cancel()
	defer func() { _ = tx.Rollback() }()

	created, err := uc.createClonedContextWithTx(ctx, tx, input, sourceContext, autoMatchOnUpload)
	if err != nil {
		return nil, err
	}

	result, err := entities.NewCloneResult(ctx, created)
	if err != nil {
		return nil, fmt.Errorf("initialize clone result: %w", err)
	}

	if input.IncludeSources {
		if cloneErr := uc.cloneSourcesIntoResultWithTx(ctx, tx, input, created.ID, result); cloneErr != nil {
			return nil, fmt.Errorf("cloning sources: %w", cloneErr)
		}
	}

	if input.IncludeRules {
		rulesCloned, cloneErr := uc.cloneMatchRulesWithTx(ctx, tx, input.SourceContextID, created.ID)
		if cloneErr != nil {
			return nil, fmt.Errorf("cloning rules: %w", cloneErr)
		}

		result.RulesCloned = rulesCloned

		feeRulesCloned, cloneErr := uc.cloneFeeRulesWithTx(ctx, tx, input.SourceContextID, created.ID)
		if cloneErr != nil {
			return nil, fmt.Errorf("cloning fee rules: %w", cloneErr)
		}

		result.FeeRulesCloned = feeRulesCloned
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit clone transaction: %w", err)
	}

	return result, nil
}

func (uc *UseCase) cloneContextNonTransactional(ctx context.Context, input CloneContextInput, sourceContext *entities.ReconciliationContext, autoMatchOnUpload bool) (*entities.CloneResult, error) {
	created, err := uc.createClonedContext(ctx, input, sourceContext, autoMatchOnUpload)
	if err != nil {
		return nil, err
	}

	result, err := entities.NewCloneResult(ctx, created)
	if err != nil {
		return nil, fmt.Errorf("initialize clone result: %w", err)
	}

	if input.IncludeSources {
		if cloneErr := uc.cloneSourcesIntoResult(ctx, input, created.ID, result); cloneErr != nil {
			return nil, fmt.Errorf("cloning sources: %w", cloneErr)
		}
	}

	if input.IncludeRules {
		rulesCloned, cloneErr := uc.cloneMatchRules(ctx, input.SourceContextID, created.ID)
		if cloneErr != nil {
			return nil, fmt.Errorf("cloning rules: %w", cloneErr)
		}

		result.RulesCloned = rulesCloned

		feeRulesCloned, cloneErr := uc.cloneFeeRules(ctx, input.SourceContextID, created.ID)
		if cloneErr != nil {
			return nil, fmt.Errorf("cloning fee rules: %w", cloneErr)
		}

		result.FeeRulesCloned = feeRulesCloned
	}

	return result, nil
}

func (uc *UseCase) publishCloneAudit(ctx context.Context, input CloneContextInput, result *entities.CloneResult) {
	uc.publishAudit(ctx, "context", result.Context.ID, "clone", map[string]any{
		"source_context_id": input.SourceContextID.String(), "name": result.Context.Name,
		"sources_cloned": result.SourcesCloned, "rules_cloned": result.RulesCloned,
		"fee_rules_cloned": result.FeeRulesCloned, "field_maps_cloned": result.FieldMapsCloned,
	})
}

func (uc *UseCase) validateCloneDependencies(input CloneContextInput) error {
	if uc == nil || uc.contextRepo == nil {
		return ErrNilContextRepository
	}

	if input.IncludeSources && uc.sourceRepo == nil {
		return ErrNilSourceRepository
	}

	if input.IncludeSources && uc.fieldMapRepo == nil {
		return ErrNilFieldMapRepository
	}

	if input.IncludeRules && uc.matchRuleRepo == nil {
		return ErrNilMatchRuleRepository
	}

	if input.IncludeRules && uc.feeRuleRepo == nil {
		return ErrNilFeeRuleRepository
	}

	if input.NewName == "" {
		return ErrCloneNameRequired
	}

	return nil
}

func (uc *UseCase) buildClonedContextEntity(input CloneContextInput, sourceContext *entities.ReconciliationContext, autoMatchOnUpload bool) *entities.ReconciliationContext {
	now := time.Now().UTC()

	return &entities.ReconciliationContext{
		ID: uuid.New(), TenantID: sourceContext.TenantID, Name: input.NewName,
		Type: sourceContext.Type, Interval: sourceContext.Interval, Status: value_objects.ContextStatusActive,
		RateID: sourceContext.RateID, FeeToleranceAbs: sourceContext.FeeToleranceAbs,
		FeeTolerancePct: sourceContext.FeeTolerancePct, FeeNormalization: sourceContext.FeeNormalization,
		AutoMatchOnUpload: autoMatchOnUpload, CreatedAt: now, UpdatedAt: now,
	}
}

func (uc *UseCase) createClonedContextWithTx(ctx context.Context, tx *sql.Tx, input CloneContextInput, sourceContext *entities.ReconciliationContext, autoMatchOnUpload bool) (*entities.ReconciliationContext, error) {
	newContext := uc.buildClonedContextEntity(input, sourceContext, autoMatchOnUpload)

	txCreator, ok := uc.contextRepo.(contextTxCreator)
	if !ok {
		return nil, fmt.Errorf("context repository does not support CreateWithTx: %w", ErrCloneProviderRequired)
	}

	created, err := txCreator.CreateWithTx(ctx, tx, newContext)
	if err != nil {
		return nil, fmt.Errorf("creating cloned context: %w", err)
	}

	return created, nil
}

func (uc *UseCase) createClonedContext(ctx context.Context, input CloneContextInput, sourceContext *entities.ReconciliationContext, autoMatchOnUpload bool) (*entities.ReconciliationContext, error) {
	newContext := uc.buildClonedContextEntity(input, sourceContext, autoMatchOnUpload)

	created, err := uc.contextRepo.Create(ctx, newContext)
	if err != nil {
		return nil, fmt.Errorf("creating cloned context: %w", err)
	}

	return created, nil
}

func (uc *UseCase) cloneSourcesIntoResult(ctx context.Context, input CloneContextInput, newContextID uuid.UUID, result *entities.CloneResult) error {
	sources, fieldMaps, err := uc.cloneSourcesAndFieldMaps(ctx, nil, input.SourceContextID, newContextID)
	if err != nil {
		return err
	}

	result.SourcesCloned = sources
	result.FieldMapsCloned = fieldMaps

	return nil
}

func (uc *UseCase) cloneSourcesIntoResultWithTx(ctx context.Context, tx *sql.Tx, input CloneContextInput, newContextID uuid.UUID, result *entities.CloneResult) error {
	sources, fieldMaps, err := uc.cloneSourcesAndFieldMaps(ctx, tx, input.SourceContextID, newContextID)
	if err != nil {
		return err
	}

	result.SourcesCloned = sources
	result.FieldMapsCloned = fieldMaps

	return nil
}

func (uc *UseCase) cloneSourcesAndFieldMaps(ctx context.Context, tx *sql.Tx, sourceContextID, newContextID uuid.UUID) (sourcesCloned, fieldMapsCloned int, err error) {
	sources, err := uc.fetchAllSources(ctx, sourceContextID)
	if err != nil {
		return 0, 0, err
	}

	if len(sources) == 0 {
		return 0, 0, nil
	}

	sourceIDs := make([]uuid.UUID, len(sources))

	for i, src := range sources {
		sourceIDs[i] = src.ID
	}

	fieldMapsExist, err := uc.fieldMapRepo.ExistsBySourceIDs(ctx, sourceIDs)
	if err != nil {
		return 0, 0, fmt.Errorf("checking field maps existence: %w", err)
	}

	now := time.Now().UTC()

	for _, src := range sources {
		newSourceID := uuid.New()

		newSource := &entities.ReconciliationSource{
			ID: newSourceID, ContextID: newContextID, Name: src.Name, Type: src.Type, Side: src.Side,
			Config: cloneMap(ctx, src.Config), CreatedAt: now, UpdatedAt: now,
		}

		if createErr := uc.createSourceWithOptionalTx(ctx, tx, newSource); createErr != nil {
			return sourcesCloned, fieldMapsCloned, fmt.Errorf("creating cloned source %q: %w", src.Name, createErr)
		}

		sourcesCloned++

		if fieldMapsExist[src.ID] {
			cloned, cloneErr := uc.cloneFieldMap(ctx, tx, src.ID, newContextID, newSourceID, now)
			if cloneErr != nil {
				return sourcesCloned, fieldMapsCloned, cloneErr
			}

			if cloned {
				fieldMapsCloned++
			}
		}
	}

	return sourcesCloned, fieldMapsCloned, nil
}

func (uc *UseCase) createSourceWithOptionalTx(ctx context.Context, tx *sql.Tx, source *entities.ReconciliationSource) error {
	if tx != nil {
		txCreator, ok := uc.sourceRepo.(sourceTxCreator)
		if !ok {
			return fmt.Errorf("source repository does not support CreateWithTx: %w", ErrCloneProviderRequired)
		}

		_, err := txCreator.CreateWithTx(ctx, tx, source)

		return err
	}

	_, err := uc.sourceRepo.Create(ctx, source)

	return err
}

func (uc *UseCase) cloneFieldMap(ctx context.Context, tx *sql.Tx, oldSourceID, newContextID, newSourceID uuid.UUID, now time.Time) (bool, error) {
	fm, err := uc.fieldMapRepo.FindBySourceID(ctx, oldSourceID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, fmt.Errorf("fetching field map for source %s: %w", oldSourceID, err)
	}

	newFieldMap := &entities.FieldMap{
		ID: uuid.New(), ContextID: newContextID, SourceID: newSourceID,
		Mapping: cloneMap(ctx, fm.Mapping), Version: 1, CreatedAt: now, UpdatedAt: now,
	}

	if tx != nil {
		txCreator, ok := uc.fieldMapRepo.(fieldMapTxCreator)
		if !ok {
			return false, fmt.Errorf("field map repository does not support CreateWithTx: %w", ErrCloneProviderRequired)
		}

		if _, err := txCreator.CreateWithTx(ctx, tx, newFieldMap); err != nil {
			return false, fmt.Errorf("creating cloned field map: %w", err)
		}

		return true, nil
	}

	if _, err := uc.fieldMapRepo.Create(ctx, newFieldMap); err != nil {
		return false, fmt.Errorf("creating cloned field map: %w", err)
	}

	return true, nil
}

func (uc *UseCase) cloneMatchRules(ctx context.Context, sourceContextID, newContextID uuid.UUID) (int, error) {
	return uc.cloneMatchRulesInternal(ctx, nil, sourceContextID, newContextID)
}

func (uc *UseCase) cloneMatchRulesWithTx(ctx context.Context, tx *sql.Tx, sourceContextID, newContextID uuid.UUID) (int, error) {
	return uc.cloneMatchRulesInternal(ctx, tx, sourceContextID, newContextID)
}

func (uc *UseCase) cloneFeeRules(ctx context.Context, sourceContextID, newContextID uuid.UUID) (int, error) {
	return uc.cloneFeeRulesInternal(ctx, nil, sourceContextID, newContextID)
}

func (uc *UseCase) cloneFeeRulesWithTx(ctx context.Context, tx *sql.Tx, sourceContextID, newContextID uuid.UUID) (int, error) {
	return uc.cloneFeeRulesInternal(ctx, tx, sourceContextID, newContextID)
}

func (uc *UseCase) cloneMatchRulesInternal(ctx context.Context, tx *sql.Tx, sourceContextID, newContextID uuid.UUID) (int, error) {
	rules, err := uc.fetchAllRules(ctx, sourceContextID)
	if err != nil {
		return 0, err
	}

	now := time.Now().UTC()
	cloned := 0

	for _, rule := range rules {
		newRule := &entities.MatchRule{
			ID: uuid.New(), ContextID: newContextID, Priority: rule.Priority, Type: rule.Type,
			Config: cloneMap(ctx, rule.Config), CreatedAt: now, UpdatedAt: now,
		}

		if err := uc.createMatchRuleWithOptionalTx(ctx, tx, newRule); err != nil {
			return cloned, fmt.Errorf("creating cloned rule (priority %d): %w", rule.Priority, err)
		}

		cloned++
	}

	return cloned, nil
}

func (uc *UseCase) createMatchRuleWithOptionalTx(ctx context.Context, tx *sql.Tx, rule *entities.MatchRule) error {
	if tx != nil {
		txCreator, ok := uc.matchRuleRepo.(matchRuleTxCreator)
		if !ok {
			return fmt.Errorf("match rule repository does not support CreateWithTx: %w", ErrCloneProviderRequired)
		}

		_, err := txCreator.CreateWithTx(ctx, tx, rule)

		return err
	}

	_, err := uc.matchRuleRepo.Create(ctx, rule)

	return err
}

func (uc *UseCase) cloneFeeRulesInternal(ctx context.Context, tx *sql.Tx, sourceContextID, newContextID uuid.UUID) (int, error) {
	rules, err := uc.feeRuleRepo.FindByContextID(ctx, sourceContextID)
	if err != nil {
		return 0, fmt.Errorf("fetching fee rules: %w", err)
	}

	now := time.Now().UTC()
	cloned := 0

	for _, rule := range rules {
		if rule == nil {
			continue
		}

		newRule := &fee.FeeRule{
			ID:            uuid.New(),
			ContextID:     newContextID,
			Side:          rule.Side,
			FeeScheduleID: rule.FeeScheduleID,
			Name:          rule.Name,
			Priority:      rule.Priority,
			Predicates:    clonePredicates(rule.Predicates),
			CreatedAt:     now,
			UpdatedAt:     now,
		}

		if err := uc.createFeeRuleWithOptionalTx(ctx, tx, newRule); err != nil {
			return cloned, fmt.Errorf("creating cloned fee rule %q: %w", rule.Name, err)
		}

		cloned++
	}

	return cloned, nil
}

func (uc *UseCase) createFeeRuleWithOptionalTx(ctx context.Context, tx *sql.Tx, rule *fee.FeeRule) error {
	if tx != nil {
		txCreator, ok := uc.feeRuleRepo.(feeRuleTxCreator)
		if !ok {
			return fmt.Errorf("fee rule repository does not support CreateWithTx: %w", ErrCloneProviderRequired)
		}

		return txCreator.CreateWithTx(ctx, tx, rule)
	}

	return uc.feeRuleRepo.Create(ctx, rule)
}

func (uc *UseCase) fetchAllSources(ctx context.Context, contextID uuid.UUID) ([]*entities.ReconciliationSource, error) {
	var allSources []*entities.ReconciliationSource

	cursor := ""

	for {
		sources, pagination, err := uc.sourceRepo.FindByContextID(ctx, contextID, cursor, maxClonePaginationLimit)
		if err != nil {
			return nil, fmt.Errorf("fetching sources page: %w", err)
		}

		allSources = append(allSources, sources...)

		if pagination.Next == "" {
			break
		}

		cursor = pagination.Next
	}

	return allSources, nil
}

func (uc *UseCase) fetchAllRules(ctx context.Context, contextID uuid.UUID) (entities.MatchRules, error) {
	var allRules entities.MatchRules

	cursor := ""

	for {
		rules, pagination, err := uc.matchRuleRepo.FindByContextID(ctx, contextID, cursor, maxClonePaginationLimit)
		if err != nil {
			return nil, fmt.Errorf("fetching rules page: %w", err)
		}

		allRules = append(allRules, rules...)

		if pagination.Next == "" {
			break
		}

		cursor = pagination.Next
	}

	return allRules, nil
}

func cloneMap(ctx context.Context, src map[string]any) map[string]any {
	if src == nil {
		return nil
	}

	data, err := json.Marshal(src)
	if err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelWarn, "cloneMap: json.Marshal failed, falling back to shallow copy")

		copied := make(map[string]any, len(src))

		maps.Copy(copied, src)

		return copied
	}

	var copied map[string]any

	if err := json.Unmarshal(data, &copied); err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelWarn, "cloneMap: json.Unmarshal failed, falling back to shallow copy")

		copied = make(map[string]any, len(src))

		maps.Copy(copied, src)
	}

	return copied
}

func clonePredicates(predicates []fee.FieldPredicate) []fee.FieldPredicate {
	if len(predicates) == 0 {
		return nil
	}

	cloned := make([]fee.FieldPredicate, 0, len(predicates))
	for _, predicate := range predicates {
		copyPredicate := predicate
		if len(predicate.Values) > 0 {
			copyPredicate.Values = append([]string(nil), predicate.Values...)
		}

		cloned = append(cloned, copyPredicate)
	}

	return cloned
}
