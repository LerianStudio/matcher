package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	configRepositories "github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	ingestionHTTP "github.com/LerianStudio/matcher/internal/ingestion/adapters/http"
	reportingHTTP "github.com/LerianStudio/matcher/internal/reporting/adapters/http"
)

// IngestionContextProviderAdapter wraps a configuration ContextRepository
// to implement the ingestion HTTP contextProvider interface.
// Compile-time check not possible: contextProvider is unexported in ingestion/adapters/http.
type IngestionContextProviderAdapter struct {
	repo configRepositories.ContextRepository
}

// NewIngestionContextProviderAdapter creates a new adapter for ContextRepository
// that implements the ingestion HTTP contextProvider interface.
// Returns nil when repo is missing.
func NewIngestionContextProviderAdapter(
	repo configRepositories.ContextRepository,
) *IngestionContextProviderAdapter {
	if repo == nil {
		return nil
	}

	return &IngestionContextProviderAdapter{repo: repo}
}

// FindByID retrieves a reconciliation context and converts it to ingestion type.
// Returns (nil, nil) if the context is not found, allowing the caller to differentiate
// between "not found" and "error occurred".
func (adapter *IngestionContextProviderAdapter) FindByID(
	ctx context.Context,
	_, contextID uuid.UUID,
) (*ingestionHTTP.ReconciliationContextInfo, error) {
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

	return &ingestionHTTP.ReconciliationContextInfo{
		ID:     ctxEntity.ID,
		Active: ctxEntity.IsActive(),
	}, nil
}

// ReportingContextProviderAdapter wraps a configuration ContextRepository
// to implement the reporting HTTP contextProvider interface.
// Compile-time check not possible: contextProvider is unexported in reporting/adapters/http.
type ReportingContextProviderAdapter struct {
	repo configRepositories.ContextRepository
}

// NewReportingContextProviderAdapter creates a new adapter for ContextRepository
// that implements the reporting HTTP contextProvider interface.
// Returns nil when repo is missing.
func NewReportingContextProviderAdapter(
	repo configRepositories.ContextRepository,
) *ReportingContextProviderAdapter {
	if repo == nil {
		return nil
	}

	return &ReportingContextProviderAdapter{repo: repo}
}

// FindByID retrieves a reconciliation context and converts it to reporting type.
// Returns (nil, nil) if the context is not found, allowing the caller to differentiate
// between "not found" and "error occurred".
func (adapter *ReportingContextProviderAdapter) FindByID(
	ctx context.Context,
	_, contextID uuid.UUID,
) (*reportingHTTP.ReconciliationContextInfo, error) {
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

	return &reportingHTTP.ReconciliationContextInfo{
		ID:     ctxEntity.ID,
		Active: ctxEntity.IsActive(),
	}, nil
}
