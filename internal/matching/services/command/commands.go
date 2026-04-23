package command

import (
	"context"
	"errors"
	"time"

	governanceRepositories "github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
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
	ErrNilFeeVarianceRepository  = errors.New("fee variance repository is required")
	ErrNilAdjustmentRepository   = errors.New("adjustment repository is required")
	ErrNilInfrastructureProvider = errors.New("infrastructure provider is required")
	ErrNilAuditLogRepository     = errors.New("audit log repository is required")
	ErrNilFeeScheduleRepository  = errors.New("fee schedule repository is required")
	ErrNilFeeRuleProvider        = errors.New("fee rule provider is required")
)

type outboxTxCreator interface {
	CreateWithTx(
		ctx context.Context,
		tx sharedPorts.Tx,
		event *sharedDomain.OutboxEvent,
	) (*sharedDomain.OutboxEvent, error)
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
	outboxRepo           sharedPorts.OutboxRepository
	outboxRepoTx         outboxTxCreator
	feeVarianceRepo      matchingRepositories.FeeVarianceRepository
	adjustmentRepo       matchingRepositories.AdjustmentRepository
	infraProvider        sharedPorts.InfrastructureProvider
	auditLogRepo         governanceRepositories.AuditLogRepository
	feeScheduleRepo      sharedPorts.FeeScheduleRepository
	feeRuleProvider      ports.FeeRuleProvider
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
	OutboxRepo       sharedPorts.OutboxRepository
	FeeVarianceRepo  matchingRepositories.FeeVarianceRepository
	AdjustmentRepo   matchingRepositories.AdjustmentRepository
	InfraProvider    sharedPorts.InfrastructureProvider
	AuditLogRepo     governanceRepositories.AuditLogRepository
	FeeScheduleRepo  sharedPorts.FeeScheduleRepository
	FeeRuleProvider  ports.FeeRuleProvider
}

func (deps *UseCaseDeps) validate() error {
	checks := []struct {
		cond bool
		err  error
	}{
		{sharedPorts.IsNilValue(deps.ContextProvider), ErrNilContextRepository},
		{sharedPorts.IsNilValue(deps.SourceProvider), ErrNilSourceRepository},
		{sharedPorts.IsNilValue(deps.RuleProvider), ErrNilMatchRuleProvider},
		{sharedPorts.IsNilValue(deps.TxRepo), ErrNilTransactionRepository},
		{sharedPorts.IsNilValue(deps.LockManager), ErrNilLockManager},
		{sharedPorts.IsNilValue(deps.MatchRunRepo), ErrNilMatchRunRepository},
		{sharedPorts.IsNilValue(deps.MatchGroupRepo), ErrNilMatchGroupRepository},
		{sharedPorts.IsNilValue(deps.MatchItemRepo), ErrNilMatchItemRepository},
		{sharedPorts.IsNilValue(deps.ExceptionCreator), ErrNilExceptionCreator},
		{sharedPorts.IsNilValue(deps.OutboxRepo), ErrNilOutboxRepository},
		{sharedPorts.IsNilValue(deps.FeeVarianceRepo), ErrNilFeeVarianceRepository},
		{sharedPorts.IsNilValue(deps.AdjustmentRepo), ErrNilAdjustmentRepository},
		{sharedPorts.IsNilValue(deps.InfraProvider), ErrNilInfrastructureProvider},
		{sharedPorts.IsNilValue(deps.AuditLogRepo), ErrNilAuditLogRepository},
		{sharedPorts.IsNilValue(deps.FeeScheduleRepo), ErrNilFeeScheduleRepository},
		{sharedPorts.IsNilValue(deps.FeeRuleProvider), ErrNilFeeRuleProvider},
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
		feeVarianceRepo:     deps.FeeVarianceRepo,
		adjustmentRepo:      deps.AdjustmentRepo,
		infraProvider:       deps.InfraProvider,
		auditLogRepo:        deps.AuditLogRepo,
		feeScheduleRepo:     deps.FeeScheduleRepo,
		feeRuleProvider:     deps.FeeRuleProvider,
		lockRefreshInterval: lockRefreshIntervalDefault,
		maxLockBatchSize:    maxCandidateSet,
	}
	uc.executeRules = uc.ExecuteRules

	return uc, nil
}
