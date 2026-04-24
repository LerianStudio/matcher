// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package query provides read operations for configuration management.
package query

import (
	"errors"

	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	configPorts "github.com/LerianStudio/matcher/internal/configuration/ports"
)

// Sentinel errors for use case validation.
var (
	ErrNilContextRepository   = errors.New("context repository is required")
	ErrNilSourceRepository    = errors.New("source repository is required")
	ErrNilFieldMapRepository  = errors.New("field map repository is required")
	ErrNilMatchRuleRepository = errors.New("match rule repository is required")
)

// UseCase provides query operations for configuration entities.
type UseCase struct {
	contextRepo   repositories.ContextRepository
	sourceRepo    repositories.SourceRepository
	fieldMapRepo  repositories.FieldMapRepository
	matchRuleRepo repositories.MatchRuleRepository
	scheduleRepo  configPorts.ScheduleRepository
}

// QueryUseCaseOption configures the query use case.
type QueryUseCaseOption func(*UseCase)

// WithScheduleRepository sets the schedule repository for the query use case.
func WithScheduleRepository(repo configPorts.ScheduleRepository) QueryUseCaseOption {
	return func(uc *UseCase) {
		if repo != nil {
			uc.scheduleRepo = repo
		}
	}
}

// NewUseCase creates a new query use case with the required repositories.
func NewUseCase(
	contextRepo repositories.ContextRepository,
	sourceRepo repositories.SourceRepository,
	fieldMapRepo repositories.FieldMapRepository,
	matchRuleRepo repositories.MatchRuleRepository,
	opts ...QueryUseCaseOption,
) (*UseCase, error) {
	if contextRepo == nil {
		return nil, ErrNilContextRepository
	}

	if sourceRepo == nil {
		return nil, ErrNilSourceRepository
	}

	if fieldMapRepo == nil {
		return nil, ErrNilFieldMapRepository
	}

	if matchRuleRepo == nil {
		return nil, ErrNilMatchRuleRepository
	}

	uc := &UseCase{
		contextRepo:   contextRepo,
		sourceRepo:    sourceRepo,
		fieldMapRepo:  fieldMapRepo,
		matchRuleRepo: matchRuleRepo,
	}

	for _, opt := range opts {
		opt(uc)
	}

	return uc, nil
}
