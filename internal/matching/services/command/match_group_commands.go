package command

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

const (
	maxCandidateSet            = 100000
	lockTTL                    = 15 * time.Minute
	minMatchedItemsCount       = 2
	sliceCapMultiplier         = 2
	lockRefreshIntervalDefault = 5 * time.Minute
	statsFieldCount            = 9
)

// Run-match sentinel errors.
var (
	ErrTenantIDRequired                      = errors.New("tenant id is required")
	ErrRunMatchContextIDRequired             = errors.New("context id is required")
	ErrMatchRunModeRequired                  = errors.New("match run mode is required")
	ErrContextNotFound                       = errors.New("context not found")
	ErrContextNotActive                      = errors.New("context is not active")
	ErrNoSourcesConfigured                   = errors.New("no sources configured for context")
	ErrAtLeastTwoSourcesRequired             = errors.New("at least two sources are required")
	ErrSourceSideRequiredForMatching         = errors.New("all sources must declare side LEFT or RIGHT before matching")
	ErrOneToOneRequiresExactlyOneLeftSource  = errors.New("1:1 contexts require exactly one LEFT source")
	ErrOneToOneRequiresExactlyOneRightSource = errors.New("1:1 contexts require exactly one RIGHT source")
	ErrOneToManyRequiresExactlyOneLeftSource = errors.New("1:N contexts require exactly one LEFT source")
	ErrAtLeastOneRightSourceRequired         = errors.New("at least one RIGHT source is required")
	ErrFeeRulesReferenceMissingSchedules     = errors.New("fee rules reference missing fee schedules")
	ErrFeeRulesRequiredForNormalization      = errors.New("fee normalization is enabled but no fee rules are configured")
	ErrMatchRunPersistedNil                  = errors.New(
		"failed to persist match run: created run is nil",
	)
	ErrProposalLeftTransactionNotFound  = errors.New("proposal left transaction not found")
	ErrProposalRightTransactionNotFound = errors.New("proposal right transaction not found")
	ErrMissingBaseAmountForAllocation   = errors.New("missing base amount for allocation")
	ErrMissingBaseCurrencyForAllocation = errors.New("missing base currency for allocation")
	ErrMatchRunLocked                   = errors.New("match run already in progress")
	ErrLockRefreshFailed                = errors.New("lock refresh failed")
	ErrTenantIDMismatch                 = errors.New("tenant id does not match context")
	ErrOutboxRepoNotConfigured          = errors.New("outbox repository is not configured")
	ErrOutboxRequiresSQLTx              = errors.New("outbox requires transaction")
	ErrContextCancelled                 = errors.New("operation cancelled")
	ErrRateNotFound                     = errors.New("rate not found for fee verification")
)

const (
	invalidAllocationMissingBase         = "allocation missing base amount"
	invalidAllocationMissingBaseCurrency = "allocation missing base currency"
)

// RunMatchInput contains the input parameters for running a match.
type RunMatchInput struct {
	TenantID  uuid.UUID
	ContextID uuid.UUID
	Mode      matchingVO.MatchRunMode
	StartDate *time.Time
	EndDate   *time.Time
}

type matchRunContext struct {
	input           RunMatchInput
	ctxInfo         *ports.ReconciliationContextInfo
	sources         []*ports.SourceInfo
	sourceTypeByID  map[uuid.UUID]string
	leftSourceIDs   map[uuid.UUID]struct{}
	rightSourceIDs  map[uuid.UUID]struct{}
	leftCandidates  []*shared.Transaction
	rightCandidates []*shared.Transaction
	unmatchedIDs    []uuid.UUID
	externalTxByID  map[uuid.UUID]*shared.Transaction
	stats           map[string]int
	leftRules       []*fee.FeeRule
	rightRules      []*fee.FeeRule
	allSchedules    map[uuid.UUID]*fee.FeeSchedule
}

type matchExecutionResult struct {
	groups           []*matchingEntities.MatchGroup
	items            []*matchingEntities.MatchItem
	autoMatchedIDs   []uuid.UUID
	pendingReviewIDs []uuid.UUID
	unmatchedIDs     []uuid.UUID
	unmatchedReasons map[uuid.UUID]string
	allTxByID        map[uuid.UUID]*shared.Transaction
	stats            map[string]int
}

type feeVerificationInput struct {
	ctxInfo        *ports.ReconciliationContextInfo
	txByID         map[uuid.UUID]*shared.Transaction
	sourceTypeByID map[uuid.UUID]string
}

type feeFindings struct {
	variances       []*matchingEntities.FeeVariance
	exceptionInputs []ports.ExceptionTransactionInput
}

type feeExtractionError struct {
	reason string
}

type feeItemResult struct {
	variance       *matchingEntities.FeeVariance
	exceptionInput *ports.ExceptionTransactionInput
	fatalErr       error // non-nil signals the caller to fail the run
}

type allocationErrorInfo struct {
	logMessage string
	reason     string
	spanErr    error
}

type proposalProcessingResult struct {
	groups           []*matchingEntities.MatchGroup
	items            []*matchingEntities.MatchItem
	autoMatchedIDs   []uuid.UUID
	pendingReviewIDs []uuid.UUID
	leftMatched      map[uuid.UUID]struct{}
	rightMatched     map[uuid.UUID]struct{}
	leftConfirmed    map[uuid.UUID]struct{}
	rightConfirmed   map[uuid.UUID]struct{}
	leftPending      map[uuid.UUID]struct{}
	rightPending     map[uuid.UUID]struct{}
	unmatchedReasons map[uuid.UUID]string
}

type proposalItemsContext struct {
	txByID               map[uuid.UUID]*shared.Transaction
	allocations          map[uuid.UUID]decimal.Decimal
	allocationCurrencies map[uuid.UUID]string
	allocationUseBase    map[uuid.UUID]bool
	notFoundErr          error
}
