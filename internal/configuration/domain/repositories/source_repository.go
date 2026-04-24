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
)

//go:generate mockgen -source=source_repository.go -destination=mocks/source_repository_mock.go -package=mocks

// SourceRepository defines persistence operations for reconciliation sources.
type SourceRepository interface {
	Create(
		ctx context.Context,
		entity *entities.ReconciliationSource,
	) (*entities.ReconciliationSource, error)
	FindByID(ctx context.Context, contextID, id uuid.UUID) (*entities.ReconciliationSource, error)
	FindByContextID(
		ctx context.Context,
		contextID uuid.UUID,
		cursor string,
		limit int,
	) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error)
	FindByContextIDAndType(
		ctx context.Context,
		contextID uuid.UUID,
		sourceType value_objects.SourceType,
		cursor string,
		limit int,
	) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error)
	Update(
		ctx context.Context,
		entity *entities.ReconciliationSource,
	) (*entities.ReconciliationSource, error)
	Delete(ctx context.Context, contextID, id uuid.UUID) error
}
