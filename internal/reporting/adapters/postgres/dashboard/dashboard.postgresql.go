// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dashboard

import (
	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	matchGroupStatusConfirmed = "CONFIRMED"
	exceptionStatusResolved   = "RESOLVED"
	exceptionSeverityCritical = "CRITICAL"

	// matchRatePercentageScale converts a ratio (0.0-1.0) into a percentage
	// (0-100). SummaryMetrics.MatchRate, DailyTrendPoint.MatchRate, and
	// SourceBreakdown.MatchRate are all expressed on the percentage scale
	// to align with MatchRateStats and the console display.
	matchRatePercentageScale = 100.0
)

// Repository persists dashboard data in Postgres.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new dashboard repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

func (repo *Repository) validateFilter(filter *entities.DashboardFilter) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if filter.ContextID == uuid.Nil {
		return ErrContextIDRequired
	}

	return nil
}

var _ repositories.DashboardRepository = (*Repository)(nil)
