package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	configRepositories "github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ContextAccessProviderAdapter wraps a configuration ContextRepository to provide
// shared HTTP ownership checks across multiple bounded contexts.
type ContextAccessProviderAdapter struct {
	repo configRepositories.ContextRepository
}

// IngestionContextProviderAdapter reuses shared context access logic for ingestion APIs.
type IngestionContextProviderAdapter = ContextAccessProviderAdapter

// ReportingContextProviderAdapter reuses shared context access logic for reporting APIs.
type ReportingContextProviderAdapter = ContextAccessProviderAdapter

var _ sharedPorts.ContextAccessProvider = (*ContextAccessProviderAdapter)(nil)

// NewContextAccessProviderAdapter creates a new adapter for shared context access.
func NewContextAccessProviderAdapter(repo configRepositories.ContextRepository) *ContextAccessProviderAdapter {
	if repo == nil {
		return nil
	}

	return &ContextAccessProviderAdapter{repo: repo}
}

// NewIngestionContextProviderAdapter creates a new adapter for ContextRepository
// that implements the ingestion HTTP contextProvider interface.
func NewIngestionContextProviderAdapter(repo configRepositories.ContextRepository) *IngestionContextProviderAdapter {
	return NewContextAccessProviderAdapter(repo)
}

// NewReportingContextProviderAdapter creates a new adapter for ContextRepository
// that implements the reporting HTTP contextProvider interface.
func NewReportingContextProviderAdapter(repo configRepositories.ContextRepository) *ReportingContextProviderAdapter {
	return NewContextAccessProviderAdapter(repo)
}

// FindByID retrieves a reconciliation context and converts it to shared access info.
// Returns (nil, nil) if the context is not found, allowing the caller to differentiate
// between "not found" and "error occurred".
func (adapter *ContextAccessProviderAdapter) FindByID(
	ctx context.Context,
	_, contextID uuid.UUID,
) (*sharedPorts.ContextAccessInfo, error) {
	if adapter == nil || adapter.repo == nil {
		return nil, ErrContextRepositoryRequired
	}

	ctxEntity, err := adapter.repo.FindByID(ctx, contextID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("find context by id: %w", err)
	}

	if ctxEntity == nil {
		return nil, nil
	}

	return &sharedPorts.ContextAccessInfo{
		ID:     ctxEntity.ID,
		Active: ctxEntity.IsActive(),
	}, nil
}
