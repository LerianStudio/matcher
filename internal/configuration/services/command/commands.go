// Package command provides write operations for configuration management.
package command

import (
	"errors"

	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	configPorts "github.com/LerianStudio/matcher/internal/configuration/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for use case validation.
var (
	ErrNilContextRepository   = errors.New("context repository is required")
	ErrNilSourceRepository    = errors.New("source repository is required")
	ErrNilFieldMapRepository  = errors.New("field map repository is required")
	ErrNilMatchRuleRepository = errors.New("match rule repository is required")
	ErrRuleIDsRequired        = errors.New("rule IDs are required")
)

// Deletion guard errors prevent orphan data and referential integrity violations.
var (
	ErrContextHasChildEntities = errors.New("cannot delete context: has associated sources, rules, or schedules that must be removed first")
	ErrSourceHasFieldMap       = errors.New("cannot delete source: has an associated field map that must be removed first")
)

// ErrContextNameAlreadyExists indicates that a context with the given name already exists.
var ErrContextNameAlreadyExists = errors.New("a reconciliation context with this name already exists")

// UseCase provides command operations for configuration entities.
type UseCase struct {
	contextRepo     repositories.ContextRepository
	sourceRepo      repositories.SourceRepository
	fieldMapRepo    repositories.FieldMapRepository
	matchRuleRepo   repositories.MatchRuleRepository
	auditPublisher  configPorts.AuditPublisher
	feeScheduleRepo configPorts.FeeScheduleRepository
	scheduleRepo    configPorts.ScheduleRepository
	infraProvider   sharedPorts.InfrastructureProvider
}

// NewUseCase creates a new command use case with the required repositories.
func NewUseCase(
	contextRepo repositories.ContextRepository,
	sourceRepo repositories.SourceRepository,
	fieldMapRepo repositories.FieldMapRepository,
	matchRuleRepo repositories.MatchRuleRepository,
	opts ...UseCaseOption,
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

// UseCaseOption configures the use case.
type UseCaseOption func(*UseCase)

// WithAuditPublisher sets the audit publisher for the use case.
func WithAuditPublisher(publisher configPorts.AuditPublisher) UseCaseOption {
	return func(uc *UseCase) {
		if publisher != nil {
			uc.auditPublisher = publisher
		}
	}
}

// WithFeeScheduleRepository sets the fee schedule repository for the use case.
func WithFeeScheduleRepository(repo configPorts.FeeScheduleRepository) UseCaseOption {
	return func(uc *UseCase) {
		if repo != nil {
			uc.feeScheduleRepo = repo
		}
	}
}
