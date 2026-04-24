// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package repositories

import (
	"context"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

//go:generate mockgen -source=context_repository.go -destination=mocks/context_repository_mock.go -package=mocks

// ContextRepository defines persistence operations for reconciliation contexts.
type ContextRepository interface {
	Create(
		ctx context.Context,
		entity *entities.ReconciliationContext,
	) (*entities.ReconciliationContext, error)
	FindByID(ctx context.Context, id uuid.UUID) (*entities.ReconciliationContext, error)
	FindByName(ctx context.Context, name string) (*entities.ReconciliationContext, error)
	FindAll(
		ctx context.Context,
		cursor string,
		limit int,
		contextType *shared.ContextType,
		status *value_objects.ContextStatus,
	) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error)
	Update(
		ctx context.Context,
		entity *entities.ReconciliationContext,
	) (*entities.ReconciliationContext, error)
	Delete(ctx context.Context, id uuid.UUID) error
	Count(ctx context.Context) (int64, error)
}
