// Package cross provides adapters for cross-context dependencies.
// These adapters bridge bounded contexts while keeping domain types isolated.
package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configRepositories "github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// MatchingConfigurationProvider consolidates configuration-backed lookups used by
// the matching context. A single provider instance satisfies three matching
// ports directly (ContextProvider, SourceProvider, MatchRuleProvider). The
// fourth port (FeeRuleProvider) is exposed via FeeRuleProvider() because its
// FindByContextID method name collides with SourceProvider.FindByContextID on a
// single receiver.
type MatchingConfigurationProvider struct {
	contextRepo   configRepositories.ContextRepository
	sourceRepo    configRepositories.SourceRepository
	matchRuleRepo configRepositories.MatchRuleRepository
	feeRuleRepo   configRepositories.FeeRuleRepository
}

// FeeRuleProviderAdapter exposes fee-rule lookups for matching. This type
// exists solely because FeeRuleProvider.FindByContextID and
// SourceProvider.FindByContextID share a method name, so a single receiver
// cannot satisfy both ports. It is a thin view over MatchingConfigurationProvider.
type FeeRuleProviderAdapter struct {
	provider *MatchingConfigurationProvider
}

var (
	// ErrMatchingConfigurationProviderRequired is returned when no configuration repository was provided.
	ErrMatchingConfigurationProviderRequired = errors.New("at least one configuration repository is required")
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

// Compile-time interface satisfaction checks.
var (
	_ matchingPorts.MatchRuleProvider = (*MatchingConfigurationProvider)(nil)
	_ matchingPorts.ContextProvider   = (*MatchingConfigurationProvider)(nil)
	_ matchingPorts.SourceProvider    = (*MatchingConfigurationProvider)(nil)
	_ matchingPorts.FeeRuleProvider   = (*FeeRuleProviderAdapter)(nil)
)

// NewMatchingConfigurationProvider creates one configuration-backed provider for matching.
func NewMatchingConfigurationProvider(
	contextRepo configRepositories.ContextRepository,
	sourceRepo configRepositories.SourceRepository,
	matchRuleRepo configRepositories.MatchRuleRepository,
	feeRuleRepo configRepositories.FeeRuleRepository,
) (*MatchingConfigurationProvider, error) {
	provider := &MatchingConfigurationProvider{
		contextRepo:   contextRepo,
		sourceRepo:    sourceRepo,
		matchRuleRepo: matchRuleRepo,
		feeRuleRepo:   feeRuleRepo,
	}

	if provider.contextRepo == nil && provider.sourceRepo == nil && provider.matchRuleRepo == nil && provider.feeRuleRepo == nil {
		return nil, ErrMatchingConfigurationProviderRequired
	}

	return provider, nil
}

// FeeRuleProvider returns a thin view that satisfies matchingPorts.FeeRuleProvider.
// This accessor exists because FeeRuleProvider.FindByContextID collides with
// SourceProvider.FindByContextID on the same receiver (same name, different
// return type); Go requires the view type to disambiguate.
func (provider *MatchingConfigurationProvider) FeeRuleProvider() *FeeRuleProviderAdapter {
	if provider == nil {
		return nil
	}

	return &FeeRuleProviderAdapter{provider: provider}
}

// ListByContextID retrieves match rules and converts them to shared types.
// Satisfies matchingPorts.MatchRuleProvider.
func (provider *MatchingConfigurationProvider) ListByContextID(
	ctx context.Context,
	contextID uuid.UUID,
) (shared.MatchRules, error) {
	if provider == nil || provider.matchRuleRepo == nil {
		return nil, ErrMatchRuleRepositoryRequired
	}

	rules, err := collectAllMatchRules(ctx, provider.matchRuleRepo, contextID)
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

// FindByID retrieves a reconciliation context and converts it to matching type.
// Satisfies matchingPorts.ContextProvider.
//
// Returns (nil, nil) if the context is not found, allowing the caller to differentiate
// between "not found" and "error occurred".
//
// tenantID is accepted for port-contract symmetry but is unused here because tenant
// isolation is handled via PostgreSQL schema search_path at the repository layer.
func (provider *MatchingConfigurationProvider) FindByID(
	ctx context.Context,
	_ uuid.UUID,
	contextID uuid.UUID,
) (*matchingPorts.ReconciliationContextInfo, error) {
	if provider == nil || provider.contextRepo == nil {
		return nil, ErrContextRepositoryRequired
	}

	ctxEntity, err := provider.contextRepo.FindByID(ctx, contextID)
	if err != nil {
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
		FeeToleranceAbs:  ctxEntity.FeeToleranceAbs,
		FeeTolerancePct:  ctxEntity.FeeTolerancePct,
		FeeNormalization: ctxEntity.FeeNormalization,
	}, nil
}

// FindByContextID retrieves sources for a context and converts them to matching types.
// Satisfies matchingPorts.SourceProvider.
func (provider *MatchingConfigurationProvider) FindByContextID(
	ctx context.Context,
	contextID uuid.UUID,
) ([]*matchingPorts.SourceInfo, error) {
	if provider == nil || provider.sourceRepo == nil {
		return nil, ErrSourceRepositoryRequired
	}

	sources, err := collectAllSources(ctx, provider.sourceRepo, contextID)
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

// FindByContextID retrieves fee rules for a context via the configuration repository.
// Satisfies matchingPorts.FeeRuleProvider.
func (adapter *FeeRuleProviderAdapter) FindByContextID(
	ctx context.Context,
	contextID uuid.UUID,
) ([]*fee.FeeRule, error) {
	if adapter == nil || adapter.provider == nil || adapter.provider.feeRuleRepo == nil {
		return nil, ErrFeeRuleRepositoryRequired
	}

	return adapter.provider.feeRuleRepo.FindByContextID(ctx, contextID)
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
