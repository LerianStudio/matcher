package command

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

func (uc *UseCase) cloneContextTransactional(ctx context.Context, input CloneContextInput, sourceContext *entities.ReconciliationContext, autoMatchOnUpload bool) (*entities.CloneResult, error) {
	tx, cancel, err := beginTenantTx(ctx, uc.infraProvider)
	if err != nil {
		return nil, fmt.Errorf("begin clone transaction: %w", err)
	}

	defer cancel()
	defer func() { _ = tx.Rollback() }()

	// Acquire a shared lock on the source context row to prevent concurrent
	// modifications while we read and clone its children (sources, field maps,
	// match rules, fee rules). Without this lock the reads happen outside the
	// write transaction and a concurrent edit could produce an inconsistent clone.
	if err := lockSourceContextForShare(ctx, tx, input.SourceContextID); err != nil {
		return nil, fmt.Errorf("lock source context for clone: %w", err)
	}

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

func (uc *UseCase) buildClonedContextEntity(input CloneContextInput, sourceContext *entities.ReconciliationContext, autoMatchOnUpload bool) *entities.ReconciliationContext {
	now := time.Now().UTC()

	return &entities.ReconciliationContext{
		ID:                uuid.New(),
		TenantID:          sourceContext.TenantID,
		Name:              strings.TrimSpace(input.NewName),
		Type:              sourceContext.Type,
		Interval:          sourceContext.Interval,
		Status:            value_objects.ContextStatusActive,
		RateID:            sourceContext.RateID,
		FeeToleranceAbs:   sourceContext.FeeToleranceAbs,
		FeeTolerancePct:   sourceContext.FeeTolerancePct,
		FeeNormalization:  sourceContext.FeeNormalization,
		AutoMatchOnUpload: autoMatchOnUpload,
		CreatedAt:         now,
		UpdatedAt:         now,
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
