package command

import (
	"context"
	"database/sql"
	"errors"
	"time"

	governanceRepositories "github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	outboxEntities "github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	outboxRepositories "github.com/LerianStudio/matcher/internal/outbox/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for UseCase construction.
var (
	ErrNilContextRepository     = errors.New("context repository is required")
	ErrNilSourceRepository      = errors.New("source repository is required")
	ErrNilMatchRuleProvider     = errors.New("match rule provider is required")
	ErrNilTransactionRepository = errors.New("transaction repository is required")
	ErrNilLockManager           = errors.New("lock manager is required")
	ErrNilMatchRunRepository    = errors.New("match run repository is required")
	ErrNilMatchGroupRepository  = errors.New("match group repository is required")
	ErrNilMatchItemRepository   = errors.New("match item repository is required")
	ErrNilExceptionCreator      = errors.New("exception creator is required")
	ErrNilOutboxRepository      = errors.New("outbox repository is required")
	ErrOutboxRepoNotTxCreator   = errors.New(
		"outbox repository does not support transactional creates",
	)
	ErrNilRateRepository         = errors.New("rate repository is required")
	ErrNilFeeVarianceRepository  = errors.New("fee variance repository is required")
	ErrNilAdjustmentRepository   = errors.New("adjustment repository is required")
	ErrNilInfrastructureProvider = errors.New("infrastructure provider is required")
	ErrNilAuditLogRepository     = errors.New("audit log repository is required")
	ErrNilFeeScheduleRepository  = errors.New("fee schedule repository is required")
)

type outboxTxCreator interface {
	CreateWithTx(
		ctx context.Context,
		tx *sql.Tx,
		event *outboxEntities.OutboxEvent,
	) (*outboxEntities.OutboxEvent, error)
}

// UseCase implements matching command operations.
type UseCase struct {
	contextProvider      ports.ContextProvider
	sourceProvider       ports.SourceProvider
	ruleProvider         ports.MatchRuleProvider
	txRepo               ports.TransactionRepository
	lockManager          ports.LockManager
	matchRunRepo         matchingRepositories.MatchRunRepository
	matchGroupRepo       matchingRepositories.MatchGroupRepository
	matchItemRepo        matchingRepositories.MatchItemRepository
	exceptionCreator     ports.ExceptionCreator
	outboxRepo           outboxRepositories.OutboxRepository
	outboxRepoTx         outboxTxCreator
	rateRepo             matchingRepositories.RateRepository
	feeVarianceRepo      matchingRepositories.FeeVarianceRepository
	adjustmentRepo       matchingRepositories.AdjustmentRepository
	infraProvider        sharedPorts.InfrastructureProvider
	auditLogRepo         governanceRepositories.AuditLogRepository
	feeScheduleRepo      matchingRepositories.FeeScheduleRepository
	executeRules         func(context.Context, ExecuteRulesInput) ([]matching.MatchProposal, error)
	executeRulesDetailed func(context.Context, ExecuteRulesInput) (*ExecuteRulesResult, error)
	lockRefreshInterval  time.Duration
	maxLockBatchSize     int
}

// UseCaseDeps groups all dependencies required by the matching UseCase.
type UseCaseDeps struct {
	ContextProvider  ports.ContextProvider
	SourceProvider   ports.SourceProvider
	RuleProvider     ports.MatchRuleProvider
	TxRepo           ports.TransactionRepository
	LockManager      ports.LockManager
	MatchRunRepo     matchingRepositories.MatchRunRepository
	MatchGroupRepo   matchingRepositories.MatchGroupRepository
	MatchItemRepo    matchingRepositories.MatchItemRepository
	ExceptionCreator ports.ExceptionCreator
	OutboxRepo       outboxRepositories.OutboxRepository
	RateRepo         matchingRepositories.RateRepository
	FeeVarianceRepo  matchingRepositories.FeeVarianceRepository
	AdjustmentRepo   matchingRepositories.AdjustmentRepository
	InfraProvider    sharedPorts.InfrastructureProvider
	AuditLogRepo     governanceRepositories.AuditLogRepository
	FeeScheduleRepo  matchingRepositories.FeeScheduleRepository
}

func (deps *UseCaseDeps) validate() error {
	checks := []struct {
		cond bool
		err  error
	}{
		{deps.ContextProvider == nil, ErrNilContextRepository},
		{deps.SourceProvider == nil, ErrNilSourceRepository},
		{deps.RuleProvider == nil, ErrNilMatchRuleProvider},
		{deps.TxRepo == nil, ErrNilTransactionRepository},
		{deps.LockManager == nil, ErrNilLockManager},
		{deps.MatchRunRepo == nil, ErrNilMatchRunRepository},
		{deps.MatchGroupRepo == nil, ErrNilMatchGroupRepository},
		{deps.MatchItemRepo == nil, ErrNilMatchItemRepository},
		{deps.ExceptionCreator == nil, ErrNilExceptionCreator},
		{deps.OutboxRepo == nil, ErrNilOutboxRepository},
		{deps.RateRepo == nil, ErrNilRateRepository},
		{deps.FeeVarianceRepo == nil, ErrNilFeeVarianceRepository},
		{deps.AdjustmentRepo == nil, ErrNilAdjustmentRepository},
		{deps.InfraProvider == nil, ErrNilInfrastructureProvider},
		{deps.AuditLogRepo == nil, ErrNilAuditLogRepository},
		{deps.FeeScheduleRepo == nil, ErrNilFeeScheduleRepository},
	}

	for _, check := range checks {
		if check.cond {
			return check.err
		}
	}

	return nil
}

// New creates a new UseCase with all required dependencies.
func New(deps UseCaseDeps) (*UseCase, error) {
	if err := deps.validate(); err != nil {
		return nil, err
	}

	outboxTx, ok := deps.OutboxRepo.(outboxTxCreator)
	if !ok {
		return nil, ErrOutboxRepoNotTxCreator
	}

	uc := &UseCase{
		contextProvider:     deps.ContextProvider,
		sourceProvider:      deps.SourceProvider,
		ruleProvider:        deps.RuleProvider,
		txRepo:              deps.TxRepo,
		lockManager:         deps.LockManager,
		matchRunRepo:        deps.MatchRunRepo,
		matchGroupRepo:      deps.MatchGroupRepo,
		matchItemRepo:       deps.MatchItemRepo,
		exceptionCreator:    deps.ExceptionCreator,
		outboxRepo:          deps.OutboxRepo,
		outboxRepoTx:        outboxTx,
		rateRepo:            deps.RateRepo,
		feeVarianceRepo:     deps.FeeVarianceRepo,
		adjustmentRepo:      deps.AdjustmentRepo,
		infraProvider:       deps.InfraProvider,
		auditLogRepo:        deps.AuditLogRepo,
		feeScheduleRepo:     deps.FeeScheduleRepo,
		lockRefreshInterval: lockRefreshIntervalDefault,
		maxLockBatchSize:    maxCandidateSet,
	}
	uc.executeRules = uc.ExecuteRules

	return uc, nil
}
