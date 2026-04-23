// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package ports

import (
	"context"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// FeeScheduleRepository defines persistence operations for fee schedules.
// Lives in the shared kernel because both the configuration context
// (CRUD via HTTP) and the matching context (read during match execution)
// consume the same contract. Consolidates the previously-duplicated
// matching/domain/repositories.FeeScheduleRepository and
// configuration/ports.FeeScheduleRepository interfaces (T-007 K-13).
type FeeScheduleRepository interface {
	Create(ctx context.Context, schedule *fee.FeeSchedule) (*fee.FeeSchedule, error)
	GetByID(ctx context.Context, id uuid.UUID) (*fee.FeeSchedule, error)
	Update(ctx context.Context, schedule *fee.FeeSchedule) (*fee.FeeSchedule, error)
	Delete(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, limit int) ([]*fee.FeeSchedule, error)
	GetByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*fee.FeeSchedule, error)
}
