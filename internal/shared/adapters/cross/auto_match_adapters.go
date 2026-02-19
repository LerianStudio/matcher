package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	"github.com/LerianStudio/lib-uncommons/v2/uncommons/runtime"

	configRepositories "github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time interface satisfaction checks.
var (
	_ sharedPorts.ContextProvider = (*AutoMatchContextProviderAdapter)(nil)
	_ sharedPorts.MatchTrigger    = (*MatchTriggerAdapter)(nil)
)

// Sentinel errors for auto-match adapters.
var (
	ErrNilMatchingUseCase   = errors.New("matching use case is required")
	ErrNilContextRepository = errors.New("context repository is required")
)

// AutoMatchContextProviderAdapter wraps a configuration ContextRepository
// to implement the ingestion ContextProvider port interface.
type AutoMatchContextProviderAdapter struct {
	repo configRepositories.ContextRepository
}

// NewAutoMatchContextProviderAdapter creates a new adapter.
func NewAutoMatchContextProviderAdapter(
	repo configRepositories.ContextRepository,
) (*AutoMatchContextProviderAdapter, error) {
	if repo == nil {
		return nil, ErrNilContextRepository
	}

	return &AutoMatchContextProviderAdapter{repo: repo}, nil
}

// IsAutoMatchEnabled checks whether auto-match on upload is enabled for the context.
func (adapter *AutoMatchContextProviderAdapter) IsAutoMatchEnabled(
	ctx context.Context,
	contextID uuid.UUID,
) (bool, error) {
	if adapter == nil || adapter.repo == nil {
		return false, ErrContextRepositoryRequired
	}

	ctxEntity, err := adapter.repo.FindByID(ctx, contextID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, fmt.Errorf("check auto-match enabled: %w", err)
	}

	if ctxEntity == nil {
		return false, nil
	}

	return ctxEntity.AutoMatchOnUpload && ctxEntity.IsActive(), nil
}

// MatchTriggerAdapter wraps the matching UseCase to implement the
// ingestion MatchTrigger port interface.
type MatchTriggerAdapter struct {
	matchingUseCase *matchingCommand.UseCase
}

// NewMatchTriggerAdapter creates a new match trigger adapter.
func NewMatchTriggerAdapter(uc *matchingCommand.UseCase) (*MatchTriggerAdapter, error) {
	if uc == nil {
		return nil, ErrNilMatchingUseCase
	}

	return &MatchTriggerAdapter{matchingUseCase: uc}, nil
}

// TriggerMatchForContext starts an asynchronous match run for the given context.
// Errors are logged but do not affect the caller.
func (adapter *MatchTriggerAdapter) TriggerMatchForContext(
	ctx context.Context,
	tenantID, contextID uuid.UUID,
) {
	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed

	runtime.SafeGoWithContextAndComponent(
		ctx,
		logger,
		"ingestion",
		"auto_match_trigger",
		runtime.KeepRunning,
		func(innerCtx context.Context) {
			input := matchingCommand.RunMatchInput{
				TenantID:  tenantID,
				ContextID: contextID,
				Mode:      matchingVO.MatchRunModeCommit,
			}

			_, _, err := adapter.matchingUseCase.RunMatch(innerCtx, input)
			if err != nil {
				innerLogger, _, _, _ := libCommons.NewTrackingFromContext(innerCtx)
				innerLogger.Log(
					innerCtx,
					libLog.LevelWarn,
					fmt.Sprintf("auto-match trigger failed for context %s: %v", contextID.String(), err),
				)
			}
		},
	)
}
