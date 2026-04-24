// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package ports

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

// ScheduleRepository defines persistence operations for reconciliation schedules.
type ScheduleRepository interface {
	// Create inserts a new schedule.
	Create(ctx context.Context, schedule *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error)
	// FindByID retrieves a schedule by its ID.
	FindByID(ctx context.Context, id uuid.UUID) (*entities.ReconciliationSchedule, error)
	// FindByContextID retrieves all schedules for a context.
	FindByContextID(ctx context.Context, contextID uuid.UUID) ([]*entities.ReconciliationSchedule, error)
	// FindDueSchedules retrieves all enabled schedules whose next_run_at <= now.
	FindDueSchedules(ctx context.Context, now time.Time) ([]*entities.ReconciliationSchedule, error)
	// Update modifies an existing schedule.
	Update(ctx context.Context, schedule *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error)
	// Delete removes a schedule by ID.
	Delete(ctx context.Context, id uuid.UUID) error
}
