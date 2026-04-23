package repositories

import (
	"context"

	"github.com/google/uuid"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

//go:generate mockgen -source=field_map_repository.go -destination=mocks/field_map_repository_mock.go -package=mocks

// FieldMapRepository defines persistence operations for field maps.
type FieldMapRepository interface {
	Create(ctx context.Context, entity *shared.FieldMap) (*shared.FieldMap, error)
	FindByID(ctx context.Context, id uuid.UUID) (*shared.FieldMap, error)
	FindBySourceID(ctx context.Context, sourceID uuid.UUID) (*shared.FieldMap, error)
	ExistsBySourceIDs(ctx context.Context, sourceIDs []uuid.UUID) (map[uuid.UUID]bool, error)
	Update(ctx context.Context, entity *shared.FieldMap) (*shared.FieldMap, error)
	Delete(ctx context.Context, id uuid.UUID) error
}
