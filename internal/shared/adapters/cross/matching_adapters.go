// Package cross provides adapters for cross-context dependencies.
// These adapters bridge bounded contexts while keeping domain types isolated.
package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configRepositories "github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// MatchRuleProviderAdapter wraps a configuration MatchRuleRepository
// to implement the matching ports.MatchRuleProvider interface.
type MatchRuleProviderAdapter struct {
	repo configRepositories.MatchRuleRepository
}

// NewMatchRuleProviderAdapter creates a new adapter for MatchRuleRepository.
func NewMatchRuleProviderAdapter(
	repo configRepositories.MatchRuleRepository,
) (*MatchRuleProviderAdapter, error) {
	if repo == nil {
		return nil, ErrMatchRuleRepositoryRequired
	}

	return &MatchRuleProviderAdapter{repo: repo}, nil
}

// ListByContextID retrieves match rules and converts them to shared types.
func (adapter *MatchRuleProviderAdapter) ListByContextID(
	ctx context.Context,
	contextID uuid.UUID,
) (shared.MatchRules, error) {
	if adapter == nil || adapter.repo == nil {
		return nil, ErrMatchRuleRepositoryRequired
	}

	rules, err := collectAllMatchRules(ctx, adapter.repo, contextID)
	if err != nil {
		return nil, err
	}

	if len(rules) == 0 {
		return shared.MatchRules{}, nil
	}

	result := make(shared.MatchRules, 0, len(rules))
	for _, rule := range rules {
		if rule == nil {
			continue
		}

		result = append(result, rule)
	}

	return result, nil
}

// ContextProviderAdapter wraps a configuration ContextRepository
// to implement the matching ports.ContextProvider interface.
type ContextProviderAdapter struct {
	repo configRepositories.ContextRepository
}

var (
	// ErrMatchRuleRepositoryRequired is returned when the match rule repository is nil.
	ErrMatchRuleRepositoryRequired = errors.New("match rule repository is required")
	// ErrContextRepositoryRequired is returned when the context repository is nil.
	ErrContextRepositoryRequired = errors.New("context repository is required")
	// ErrSourceRepositoryRequired is returned when the source repository is nil.
	ErrSourceRepositoryRequired = errors.New("source repository is required")
	// ErrFeeRuleRepositoryRequired is returned when the fee rule repository is nil.
	ErrFeeRuleRepositoryRequired = errors.New("fee rule repository is required")
	// ErrMatchRulePaginationCursorDidNotAdvance is returned when a paginated match rule query loops.
	ErrMatchRulePaginationCursorDidNotAdvance = errors.New("match rule pagination cursor did not advance")
	// ErrSourcePaginationCursorDidNotAdvance is returned when a paginated source query loops.
	ErrSourcePaginationCursorDidNotAdvance = errors.New("source pagination cursor did not advance")
)

// NewContextProviderAdapter creates a new adapter for ContextRepository.
func NewContextProviderAdapter(repo configRepositories.ContextRepository) (*ContextProviderAdapter, error) {
	if repo == nil {
		return nil, ErrContextRepositoryRequired
	}

	return &ContextProviderAdapter{repo: repo}, nil
}

// FindByID retrieves a reconciliation context and converts it to matching type.
// Returns (nil, nil) if the context is not found, allowing the caller to differentiate
// between "not found" and "error occurred".
func (adapter *ContextProviderAdapter) FindByID(
	ctx context.Context,
	_ /* tenantID — not needed here; tenant isolation is handled via PostgreSQL schema search_path at the repository layer */, contextID uuid.UUID,
) (*matchingPorts.ReconciliationContextInfo, error) {
	if adapter == nil || adapter.repo == nil {
		return nil, ErrContextRepositoryRequired
	}

	ctxEntity, err := adapter.repo.FindByID(ctx, contextID)
	if err != nil {
		// Context not found is not an error - return nil to signal "not found"
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("find context by id: %w", err)
	}

	if ctxEntity == nil {
		return nil, nil
	}

	return &matchingPorts.ReconciliationContextInfo{
		ID:               ctxEntity.ID,
		Type:             shared.ContextType(ctxEntity.Type.String()),
		Active:           ctxEntity.IsActive(),
		RateID:           ctxEntity.RateID,
		FeeToleranceAbs:  ctxEntity.FeeToleranceAbs,
		FeeTolerancePct:  ctxEntity.FeeTolerancePct,
		FeeNormalization: ctxEntity.FeeNormalization,
	}, nil
}

// SourceProviderAdapter wraps a configuration SourceRepository
// to implement the matching ports.SourceProvider interface.
type SourceProviderAdapter struct {
	repo configRepositories.SourceRepository
}

// NewSourceProviderAdapter creates a new adapter for SourceRepository.
func NewSourceProviderAdapter(repo configRepositories.SourceRepository) (*SourceProviderAdapter, error) {
	if repo == nil {
		return nil, ErrSourceRepositoryRequired
	}

	return &SourceProviderAdapter{repo: repo}, nil
}

// FindByContextID retrieves reconciliation sources and converts them to matching type.
func (adapter *SourceProviderAdapter) FindByContextID(
	ctx context.Context,
	contextID uuid.UUID,
) ([]*matchingPorts.SourceInfo, error) {
	if adapter == nil || adapter.repo == nil {
		return nil, ErrSourceRepositoryRequired
	}

	sources, err := collectAllSources(ctx, adapter.repo, contextID)
	if err != nil {
		return nil, err
	}

	result := make([]*matchingPorts.SourceInfo, 0, len(sources))
	for _, src := range sources {
		if src == nil {
			continue
		}

		// NOTE: configuration SourceType values (LEDGER, BANK, GATEWAY, CUSTOM) are converted
		// directly to matching SourceType via string cast. This is currently benign because the
		// matching context does not validate SourceType values — it is only used as metadata.
		// If matching starts validating SourceType, a proper mapping should be introduced.
		result = append(result, &matchingPorts.SourceInfo{
			ID:   src.ID,
			Type: matchingPorts.SourceType(src.Type.String()),
			Side: src.Side,
		})
	}

	return result, nil
}

// FeeRuleProviderAdapter wraps a configuration FeeRuleRepository
// to implement the matching ports.FeeRuleProvider interface.
type FeeRuleProviderAdapter struct {
	repo configRepositories.FeeRuleRepository
}

// NewFeeRuleProviderAdapter creates a new adapter for FeeRuleRepository.
func NewFeeRuleProviderAdapter(
	repo configRepositories.FeeRuleRepository,
) (*FeeRuleProviderAdapter, error) {
	if repo == nil {
		return nil, ErrFeeRuleRepositoryRequired
	}

	return &FeeRuleProviderAdapter{repo: repo}, nil
}

// FindByContextID retrieves fee rules for a context via the configuration repository.
func (adapter *FeeRuleProviderAdapter) FindByContextID(
	ctx context.Context,
	contextID uuid.UUID,
) ([]*fee.FeeRule, error) {
	if adapter == nil || adapter.repo == nil {
		return nil, ErrFeeRuleRepositoryRequired
	}

	return adapter.repo.FindByContextID(ctx, contextID)
}

func collectAllMatchRules(
	ctx context.Context,
	repo configRepositories.MatchRuleRepository,
	contextID uuid.UUID,
) (shared.MatchRules, error) {
	allRules := make(shared.MatchRules, 0)
	cursor := ""

	for {
		rules, pagination, err := repo.FindByContextID(ctx, contextID, cursor, maxInternalLimit)
		if err != nil {
			return nil, fmt.Errorf("find match rules by context: %w", err)
		}

		for _, rule := range rules {
			if rule == nil {
				continue
			}

			allRules = append(allRules, rule)
		}

		if pagination.Next == "" {
			break
		}

		if pagination.Next == cursor {
			return nil, fmt.Errorf("find match rules by context: %w", ErrMatchRulePaginationCursorDidNotAdvance)
		}

		cursor = pagination.Next
	}

	return allRules, nil
}

func collectAllSources(
	ctx context.Context,
	repo configRepositories.SourceRepository,
	contextID uuid.UUID,
) ([]*configEntities.ReconciliationSource, error) {
	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed here
	allSources := make([]*configEntities.ReconciliationSource, 0)
	cursor := ""

	for {
		sources, pagination, err := repo.FindByContextID(ctx, contextID, cursor, maxInternalLimit)
		if err != nil {
			return nil, fmt.Errorf("find sources by context: %w", err)
		}

		allSources = append(allSources, sources...)

		if pagination.Next == "" {
			break
		}

		if pagination.Next == cursor {
			return nil, fmt.Errorf("find sources by context: %w", ErrSourcePaginationCursorDidNotAdvance)
		}

		logger.Log(ctx, libLog.LevelDebug, fmt.Sprintf(
			"loading additional sources page for context_id=%s with cursor=%s",
			contextID.String(),
			pagination.Next,
		))

		cursor = pagination.Next
	}

	return allSources, nil
}

const (
	maxInternalLimit = constants.MaximumPaginationLimit
)

var (
	_ matchingPorts.MatchRuleProvider = (*MatchRuleProviderAdapter)(nil)
	_ matchingPorts.ContextProvider   = (*ContextProviderAdapter)(nil)
	_ matchingPorts.SourceProvider    = (*SourceProviderAdapter)(nil)
	_ matchingPorts.FeeRuleProvider   = (*FeeRuleProviderAdapter)(nil)
)
