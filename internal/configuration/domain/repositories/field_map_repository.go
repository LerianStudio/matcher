package repositories

import (
	"context"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

//go:generate mockgen -source=field_map_repository.go -destination=mocks/mock_field_map_repository.go -package=mocks

// FieldMapRepository defines persistence operations for field maps.
type FieldMapRepository interface {
	Create(ctx context.Context, entity *entities.FieldMap) (*entities.FieldMap, error)
	FindByID(ctx context.Context, id uuid.UUID) (*entities.FieldMap, error)
	FindBySourceID(ctx context.Context, sourceID uuid.UUID) (*entities.FieldMap, error)
	ExistsBySourceIDs(ctx context.Context, sourceIDs []uuid.UUID) (map[uuid.UUID]bool, error)
	Update(ctx context.Context, entity *entities.FieldMap) (*entities.FieldMap, error)
	Delete(ctx context.Context, id uuid.UUID) error
}
