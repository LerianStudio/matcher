// Package cross provides adapters for cross-context dependencies.
// These adapters bridge bounded contexts while keeping domain types isolated.
package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"

	configRepositories "github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
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

	rules, _, err := adapter.repo.FindByContextID(ctx, contextID, "", maxInternalLimit)
	if err != nil {
		return nil, fmt.Errorf("find match rules by context: %w", err)
	}

	if len(rules) >= maxInternalLimit {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf(
			"match rules result may be truncated: returned %d items (limit %d) for context_id=%s",
			len(rules), maxInternalLimit, contextID.String(),
		))
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

	sources, _, err := adapter.repo.FindByContextID(ctx, contextID, "", maxInternalLimit)
	if err != nil {
		return nil, fmt.Errorf("find sources by context: %w", err)
	}

	if len(sources) >= maxInternalLimit {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf(
			"sources result may be truncated: returned %d items (limit %d) for context_id=%s",
			len(sources), maxInternalLimit, contextID.String(),
		))
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
			ID:            src.ID,
			Type:          matchingPorts.SourceType(src.Type.String()),
			FeeScheduleID: src.FeeScheduleID,
		})
	}

	return result, nil
}

const (
	maxInternalLimit = 1000
)

var (
	_ matchingPorts.MatchRuleProvider = (*MatchRuleProviderAdapter)(nil)
	_ matchingPorts.ContextProvider   = (*ContextProviderAdapter)(nil)
	_ matchingPorts.SourceProvider    = (*SourceProviderAdapter)(nil)
)
