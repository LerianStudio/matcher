//go:build unit

package cross

import (
	"context"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

// stubContextRepository implements configRepositories.ContextRepository
// for adapter tests. Shared across ingestion and auto-match adapter tests
// to avoid duplication.
type stubContextRepository struct {
	ctx *configEntities.ReconciliationContext
	err error
}

func (r *stubContextRepository) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*configEntities.ReconciliationContext, error) {
	return r.ctx, r.err
}

func (r *stubContextRepository) FindByName(
	_ context.Context,
	_ string,
) (*configEntities.ReconciliationContext, error) {
	return nil, nil
}

func (r *stubContextRepository) Create(
	_ context.Context,
	_ *configEntities.ReconciliationContext,
) (*configEntities.ReconciliationContext, error) {
	return nil, nil
}

func (r *stubContextRepository) Update(
	_ context.Context,
	_ *configEntities.ReconciliationContext,
) (*configEntities.ReconciliationContext, error) {
	return nil, nil
}

func (r *stubContextRepository) Delete(_ context.Context, _ uuid.UUID) error {
	return nil
}

func (r *stubContextRepository) FindAll(
	_ context.Context,
	_ string,
	_ int,
	_ *configVO.ContextType,
	_ *configVO.ContextStatus,
) ([]*configEntities.ReconciliationContext, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (r *stubContextRepository) Count(_ context.Context) (int64, error) {
	return 0, nil
}
