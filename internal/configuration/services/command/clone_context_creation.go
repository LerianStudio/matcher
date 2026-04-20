package command

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

func (uc *UseCase) cloneContextTransactional(ctx context.Context, input CloneContextInput, sourceContext *entities.ReconciliationContext) (*entities.CloneResult, error) {
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

	lockedSourceContext, err := uc.findSourceContextWithOptionalTx(ctx, tx, input.SourceContextID, sourceContext)
	if err != nil {
		return nil, fmt.Errorf("reload locked source context: %w", err)
	}

	created, err := uc.createClonedContextWithTx(ctx, tx, input, lockedSourceContext, lockedSourceContext.AutoMatchOnUpload)
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

func (uc *UseCase) findSourceContextWithOptionalTx(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	fallback *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if tx == nil {
		return fallback, nil
	}

	txFinder, ok := uc.contextRepo.(contextTxFinder)
	if !ok {
		return fallback, nil
	}

	locked, err := txFinder.FindByIDWithTx(ctx, tx, contextID)
	if err != nil {
		return nil, err
	}

	if locked == nil {
		return nil, ErrContextNotFound
	}

	return locked, nil
}

func (uc *UseCase) buildClonedContextEntity(ctx context.Context, input CloneContextInput, sourceContext *entities.ReconciliationContext, autoMatchOnUpload bool) (*entities.ReconciliationContext, error) {
	newName := strings.TrimSpace(input.NewName)
	autoMatch := autoMatchOnUpload

	clonedContext, err := entities.NewReconciliationContext(ctx, sourceContext.TenantID, entities.CreateReconciliationContextInput{
		Name:              newName,
		Type:              sourceContext.Type,
		Interval:          sourceContext.Interval,
		FeeToleranceAbs:   decimalStringPointer(sourceContext.FeeToleranceAbs),
		FeeTolerancePct:   decimalStringPointer(sourceContext.FeeTolerancePct),
		FeeNormalization:  cloneStringPointer(sourceContext.FeeNormalization),
		AutoMatchOnUpload: &autoMatch,
	})
	if err != nil {
		return nil, fmt.Errorf("build cloned context entity: %w", err)
	}

	if err := clonedContext.Activate(ctx); err != nil {
		return nil, fmt.Errorf("activate cloned context: %w", err)
	}

	return clonedContext, nil
}

func (uc *UseCase) createClonedContextWithTx(ctx context.Context, tx *sql.Tx, input CloneContextInput, sourceContext *entities.ReconciliationContext, autoMatchOnUpload bool) (*entities.ReconciliationContext, error) {
	newContext, err := uc.buildClonedContextEntity(ctx, input, sourceContext, autoMatchOnUpload)
	if err != nil {
		return nil, err
	}

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
	newContext, err := uc.buildClonedContextEntity(ctx, input, sourceContext, autoMatchOnUpload)
	if err != nil {
		return nil, err
	}

	created, err := uc.contextRepo.Create(ctx, newContext)
	if err != nil {
		return nil, fmt.Errorf("creating cloned context: %w", err)
	}

	return created, nil
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}

	cloned := *value

	return &cloned
}

func decimalStringPointer(value interface{ String() string }) *string {
	cloned := value.String()
	return &cloned
}
