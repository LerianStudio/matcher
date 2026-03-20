// Package cross provides adapters for cross-context dependencies.
// These adapters convert between context-specific types and shared kernel types,
// enabling bounded contexts to communicate without direct dependencies.
package cross

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configRepositories "github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	ingestionPorts "github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Compile-time interface satisfaction checks.
var (
	_ ingestionPorts.FieldMapRepository = (*FieldMapRepositoryAdapter)(nil)
	_ ingestionPorts.SourceRepository   = (*SourceRepositoryAdapter)(nil)
)

// Sentinel errors for configuration adapters.
var (
	// ErrNilFieldMapRepository indicates the field map repository is required but was nil.
	ErrNilFieldMapRepository = errors.New("field map repository is required")
	// ErrNilSourceRepository indicates the source repository is required but was nil.
	ErrNilSourceRepository = errors.New("source repository is required")
)

// FieldMapRepositoryAdapter wraps a configuration FieldMapRepository
// to implement the ingestion ports.FieldMapRepository interface.
type FieldMapRepositoryAdapter struct {
	repo configRepositories.FieldMapRepository
}

// NewFieldMapRepositoryAdapter creates a new adapter for FieldMapRepository.
// Returns an error if the repository is nil.
func NewFieldMapRepositoryAdapter(
	repo configRepositories.FieldMapRepository,
) (*FieldMapRepositoryAdapter, error) {
	if repo == nil {
		return nil, ErrNilFieldMapRepository
	}

	return &FieldMapRepositoryAdapter{repo: repo}, nil
}

// FindBySourceID retrieves a field map and converts it to the shared type.
func (adapter *FieldMapRepositoryAdapter) FindBySourceID(
	ctx context.Context,
	sourceID uuid.UUID,
) (*shared.FieldMap, error) {
	if adapter == nil || adapter.repo == nil {
		return nil, ErrNilFieldMapRepository
	}

	fm, err := adapter.repo.FindBySourceID(ctx, sourceID)
	if err != nil {
		return nil, fmt.Errorf("finding field map by source ID: %w", err)
	}

	if fm == nil {
		return nil, nil
	}

	return toSharedFieldMap(fm), nil
}

func toSharedFieldMap(fm *configEntities.FieldMap) *shared.FieldMap {
	return &shared.FieldMap{
		ID:        fm.ID,
		ContextID: fm.ContextID,
		SourceID:  fm.SourceID,
		Mapping:   fm.Mapping,
		Version:   fm.Version,
		CreatedAt: fm.CreatedAt,
		UpdatedAt: fm.UpdatedAt,
	}
}

// SourceRepositoryAdapter wraps a configuration SourceRepository
// to implement the ingestion ports.SourceRepository interface.
type SourceRepositoryAdapter struct {
	repo configRepositories.SourceRepository
}

// NewSourceRepositoryAdapter creates a new adapter for SourceRepository.
// Returns an error if the repository is nil.
func NewSourceRepositoryAdapter(repo configRepositories.SourceRepository) (*SourceRepositoryAdapter, error) {
	if repo == nil {
		return nil, ErrNilSourceRepository
	}

	return &SourceRepositoryAdapter{repo: repo}, nil
}

// FindByID retrieves a reconciliation source and converts it to the shared type.
func (adapter *SourceRepositoryAdapter) FindByID(
	ctx context.Context,
	contextID, id uuid.UUID,
) (*shared.ReconciliationSource, error) {
	if adapter == nil || adapter.repo == nil {
		return nil, ErrNilSourceRepository
	}

	source, err := adapter.repo.FindByID(ctx, contextID, id)
	if err != nil {
		return nil, fmt.Errorf("finding source by ID: %w", err)
	}

	if source == nil {
		return nil, nil
	}

	return toSharedReconciliationSource(source), nil
}

func toSharedReconciliationSource(
	src *configEntities.ReconciliationSource,
) *shared.ReconciliationSource {
	return &shared.ReconciliationSource{
		ID:        src.ID,
		ContextID: src.ContextID,
		Name:      src.Name,
		Type:      string(src.Type),
		Side:      src.Side,
		Config:    src.Config,
		CreatedAt: src.CreatedAt,
		UpdatedAt: src.UpdatedAt,
	}
}
