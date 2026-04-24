// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package fee_schedule provides PostgreSQL persistence for fee schedule entities.
package fee_schedule

import (
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	scheduleColumns = "id, tenant_id, name, currency, application_order, rounding_scale, rounding_mode, created_at, updated_at"
	itemColumns     = "id, fee_schedule_id, name, priority, structure_type, structure_data, created_at, updated_at"
)

// Repository persists fee schedules in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new fee schedule repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

var _ ports.FeeScheduleRepository = (*Repository)(nil)
