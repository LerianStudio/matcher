package command

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/enums"
	"github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/shared/constants"
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

// Sentinel errors for run match operations.
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
	ErrAtLeastOneLeftSourceRequired          = errors.New("at least one LEFT source is required")
	ErrAtLeastOneRightSourceRequired         = errors.New("at least one RIGHT source is required")
	ErrFeeRulesReferenceMissingSchedules     = errors.New("fee rules reference missing fee schedules")
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
	ErrOutboxRequiresSQLTx              = errors.New("outbox requires *sql.Tx")
	ErrContextCancelled                 = errors.New("operation cancelled")
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

// matchRunContext holds all validated and prepared data for a match run.
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
	stats           map[string]int
	leftRules       []*fee.FeeRule
	rightRules      []*fee.FeeRule
	allSchedules    map[uuid.UUID]*fee.FeeSchedule
}

// RunMatch executes the matching engine for a given context.
//
//nolint:gocyclo,cyclop // Orchestration function with clear phase separation
func (uc *UseCase) RunMatch(
	ctx context.Context,
	in RunMatchInput,
) (*matchingEntities.MatchRun, []*matchingEntities.MatchGroup, error) {
	if err := uc.validateRunMatchDependencies(); err != nil {
		return nil, nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.matching.run_match")
	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(
		span,
		"matcher",
		struct {
			ContextID string `json:"contextId"`
			Mode      string `json:"mode"`
		}{
			ContextID: in.ContextID.String(),
			Mode:      in.Mode.String(),
		},
		nil,
	)

	ctx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	var err error

	ctx, err = uc.validateAndEnrichTenant(ctx, &in)
	if err != nil {
		return nil, nil, err
	}

	if in.ContextID == uuid.Nil {
		return nil, nil, ErrRunMatchContextIDRequired
	}

	if !in.Mode.IsValid() {
		return nil, nil, ErrMatchRunModeRequired
	}

	lock, err := uc.acquireContextLock(ctx, span, in.ContextID)
	if err != nil {
		return nil, nil, err
	}

	var refreshFailed atomic.Bool

	var commitStarted atomic.Bool

	cleanupRefresh := uc.watchLockRefresh(
		ctx,
		span,
		lock,
		logger,
		cancelRun,
		&refreshFailed,
		&commitStarted,
	)
	defer cleanupRefresh()

	mrc, err := uc.prepareMatchRun(ctx, span, logger, in)
	if err != nil {
		return nil, nil, err
	}

	if ctx.Err() != nil {
		return nil, nil, fmt.Errorf("%w: after prepare: %w", ErrContextCancelled, ctx.Err())
	}

	if len(mrc.leftCandidates) == 0 || len(mrc.rightCandidates) == 0 {
		if err := uc.ensureLockFresh(ctx, span, lock, &refreshFailed); err != nil {
			return nil, nil, err
		}

		commitStarted.Store(true)

		return uc.completeEmptyRun(
			ctx,
			in,
			mrc.stats,
			mrc.leftCandidates,
			mrc.rightCandidates,
			mrc.unmatchedIDs,
			mrc.sourceTypeByID,
		)
	}

	if err := uc.ensureLockFresh(ctx, span, lock, &refreshFailed); err != nil {
		return nil, nil, err
	}

	run, err := matchingEntities.NewMatchRun(ctx, in.ContextID, in.Mode)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create match run entity: %w", err)
	}

	createdRun, err := uc.matchRunRepo.Create(ctx, run)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create match run", err)
		return nil, nil, fmt.Errorf("failed to persist match run: %w", err)
	}

	if createdRun == nil {
		return nil, nil, ErrMatchRunPersistedNil
	}

	if ctx.Err() != nil {
		return nil, nil, finalizeRunFailure(
			ctx, uc, createdRun,
			fmt.Errorf("%w: before rule execution: %w", ErrContextCancelled, ctx.Err()),
		)
	}

	execResult, err := uc.executeMatchRules(ctx, span, logger, mrc, createdRun)
	if err != nil {
		return nil, nil, finalizeRunFailure(ctx, uc, createdRun, err)
	}

	if ctx.Err() != nil {
		return nil, nil, finalizeRunFailure(
			ctx, uc, createdRun,
			fmt.Errorf("%w: after rule execution: %w", ErrContextCancelled, ctx.Err()),
		)
	}

	if in.Mode == matchingVO.MatchRunModeDryRun {
		return uc.completeDryRun(
			ctx,
			span,
			createdRun,
			execResult.stats,
			execResult.groups,
			&refreshFailed,
		)
	}

	if err := uc.ensureLockFresh(ctx, span, lock, &refreshFailed); err != nil {
		return nil, nil, finalizeRunFailure(ctx, uc, createdRun, err)
	}

	commitStarted.Store(true)

	feeInput := &feeVerificationInput{
		ctxInfo:        mrc.ctxInfo,
		txByID:         execResult.allTxByID,
		sourceTypeByID: mrc.sourceTypeByID,
	}

	updatedRun, commitErr := uc.commitMatchResults(
		ctx,
		span,
		createdRun,
		execResult.groups,
		execResult.items,
		execResult.autoMatchedIDs,
		execResult.pendingReviewIDs,
		execResult.unmatchedIDs,
		execResult.unmatchedReasons,
		&refreshFailed,
		execResult.stats,
		feeInput,
	)
	if commitErr != nil {
		return nil, nil, commitErr
	}

	return updatedRun, execResult.groups, nil
}

// matchExecutionResult holds the results of rule execution and proposal processing.
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

// prepareMatchRun validates input and loads context, sources, and candidates.
func (uc *UseCase) prepareMatchRun(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	in RunMatchInput,
) (*matchRunContext, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, prepSpan := tracer.Start(ctx, "command.matching.prepare_match_run")
	defer prepSpan.End()

	ctx, err := uc.validateAndEnrichTenant(ctx, &in)
	if err != nil {
		return nil, err
	}

	if in.ContextID == uuid.Nil {
		return nil, ErrRunMatchContextIDRequired
	}

	if !in.Mode.IsValid() {
		return nil, ErrMatchRunModeRequired
	}

	ctxInfo, sources, err := uc.loadContextAndSources(ctx, span, in)
	if err != nil {
		return nil, err
	}

	leftSourceIDs, rightSourceIDs, err := classifySources(ctxInfo.Type, sources)
	if err != nil {
		return nil, err
	}

	sourceTypeByID := buildSourceTypeMap(sources)

	feeNorm := fee.NormalizationModeNone
	if ctxInfo.FeeNormalization != nil {
		feeNorm = fee.NormalizationMode(*ctxInfo.FeeNormalization)
	}

	var (
		leftRules    []*fee.FeeRule
		rightRules   []*fee.FeeRule
		allSchedules map[uuid.UUID]*fee.FeeSchedule
	)

	// Load fee rules and their associated schedules for the context

	if feeNorm != fee.NormalizationModeNone {
		leftRules, rightRules, allSchedules, err = uc.loadFeeRulesAndSchedules(ctx, in.ContextID)
		if err != nil {
			return nil, err
		}
	}

	leftCandidates, rightCandidates, unmatchedIDs, err := uc.loadAndClassifyCandidates(
		ctx,
		span,
		logger,
		in,
		leftSourceIDs,
		rightSourceIDs,
	)
	if err != nil {
		return nil, err
	}

	stats := map[string]int{
		"candidates_left":  len(leftCandidates),
		"candidates_right": len(rightCandidates),
	}

	if span != nil {
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"matcher",
			struct {
				CandidatesLeft  int `json:"candidatesLeft"`
				CandidatesRight int `json:"candidatesRight"`
			}{
				CandidatesLeft:  len(leftCandidates),
				CandidatesRight: len(rightCandidates),
			},
			nil,
		)
	}

	return &matchRunContext{
		input:           in,
		ctxInfo:         ctxInfo,
		sources:         sources,
		sourceTypeByID:  sourceTypeByID,
		leftSourceIDs:   leftSourceIDs,
		rightSourceIDs:  rightSourceIDs,
		leftCandidates:  leftCandidates,
		rightCandidates: rightCandidates,
		unmatchedIDs:    unmatchedIDs,
		stats:           stats,
		leftRules:       leftRules,
		rightRules:      rightRules,
		allSchedules:    allSchedules,
	}, nil
}

// validateAndEnrichTenant validates tenant from context and enriches the input.
func (uc *UseCase) validateAndEnrichTenant(
	ctx context.Context,
	in *RunMatchInput,
) (context.Context, error) {
	ctxTenantID := auth.GetTenantID(ctx)
	if ctxTenantID == "" {
		ctxTenantID = auth.DefaultTenantID
		if strings.TrimSpace(ctxTenantID) == "" {
			return ctx, ErrTenantIDRequired
		}

		ctx = context.WithValue(ctx, auth.TenantIDKey, ctxTenantID)
	}

	ctxTenantUUID, parseErr := uuid.Parse(ctxTenantID)
	if parseErr != nil {
		return ctx, ErrTenantIDRequired
	}

	if in.TenantID == uuid.Nil {
		in.TenantID = ctxTenantUUID
	}

	if in.TenantID != ctxTenantUUID {
		return ctx, ErrTenantIDMismatch
	}

	return ctx, nil
}

// loadContextAndSources loads the reconciliation context and its sources.
func (uc *UseCase) loadContextAndSources(
	ctx context.Context,
	span trace.Span,
	in RunMatchInput,
) (*ports.ReconciliationContextInfo, []*ports.SourceInfo, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, loadSpan := tracer.Start(ctx, "command.matching.load_context_and_sources")
	defer loadSpan.End()

	ctxInfo, err := uc.contextProvider.FindByID(ctx, in.TenantID, in.ContextID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load reconciliation context", err)
		return nil, nil, fmt.Errorf("failed to load reconciliation context: %w", err)
	}

	if ctxInfo == nil {
		return nil, nil, ErrContextNotFound
	}

	if !ctxInfo.Active {
		return nil, nil, ErrContextNotActive
	}

	if ctxInfo.Type != shared.ContextTypeOneToOne && ctxInfo.Type != shared.ContextTypeOneToMany {
		return nil, nil, fmt.Errorf("%w: %s", ErrUnsupportedContextType, ctxInfo.Type)
	}

	sources, err := uc.sourceProvider.FindByContextID(ctx, in.ContextID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load reconciliation sources", err)
		return nil, nil, fmt.Errorf("failed to load reconciliation sources: %w", err)
	}

	if len(sources) == 0 {
		return nil, nil, ErrNoSourcesConfigured
	}

	return ctxInfo, sources, nil
}

// classifySources separates sources into left and right sets using configured source sides.
func classifySources(
	contextType shared.ContextType,
	sources []*ports.SourceInfo,
) (map[uuid.UUID]struct{}, map[uuid.UUID]struct{}, error) {
	leftSourceIDs := make(map[uuid.UUID]struct{})
	rightSourceIDs := make(map[uuid.UUID]struct{})

	// Filter out nil sources
	nonNil := make([]*ports.SourceInfo, 0, len(sources))
	for _, source := range sources {
		if source != nil {
			nonNil = append(nonNil, source)
		}
	}

	if len(nonNil) < 2 { //nolint:mnd // minimum 2 sources for matching
		return nil, nil, ErrAtLeastTwoSourcesRequired
	}

	for _, source := range nonNil {
		if !source.Side.IsExclusive() {
			return nil, nil, ErrSourceSideRequiredForMatching
		}

		if source.Side == fee.MatchingSideLeft {
			leftSourceIDs[source.ID] = struct{}{}
		} else {
			rightSourceIDs[source.ID] = struct{}{}
		}
	}

	if err := validateSourceCountForContextType(contextType, len(leftSourceIDs), len(rightSourceIDs)); err != nil {
		return nil, nil, err
	}

	return leftSourceIDs, rightSourceIDs, nil
}

// validateSourceCountForContextType enforces left/right source cardinality per context type.
func validateSourceCountForContextType(contextType shared.ContextType, leftCount, rightCount int) error {
	switch contextType {
	case shared.ContextTypeOneToOne:
		if leftCount != 1 {
			return ErrOneToOneRequiresExactlyOneLeftSource
		}

		if rightCount != 1 {
			return ErrOneToOneRequiresExactlyOneRightSource
		}
	case shared.ContextTypeOneToMany:
		if leftCount != 1 {
			return ErrOneToManyRequiresExactlyOneLeftSource
		}

		if rightCount == 0 {
			return ErrAtLeastOneRightSourceRequired
		}
	case shared.ContextTypeManyToMany:
		if leftCount == 0 {
			return ErrAtLeastOneLeftSourceRequired
		}

		if rightCount == 0 {
			return ErrAtLeastOneRightSourceRequired
		}
	}

	return nil
}

// loadAndClassifyCandidates loads unmatched transactions and classifies them by source.
func (uc *UseCase) loadAndClassifyCandidates(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	in RunMatchInput,
	leftSourceIDs, rightSourceIDs map[uuid.UUID]struct{},
) ([]*shared.Transaction, []*shared.Transaction, []uuid.UUID, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, loadSpan := tracer.Start(ctx, "command.matching.load_and_classify_candidates")
	defer loadSpan.End()

	candidateLimit := maxCandidateSet
	if uc.maxLockBatchSize > 0 {
		candidateLimit = uc.maxLockBatchSize
	}

	candidates, err := uc.txRepo.ListUnmatchedByContext(
		ctx,
		in.ContextID,
		in.StartDate,
		in.EndDate,
		candidateLimit,
		0,
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load candidate transactions", err)
		return nil, nil, nil, fmt.Errorf("failed to load candidate transactions: %w", err)
	}

	leftCandidates := make([]*shared.Transaction, 0, len(candidates))
	rightCandidates := make([]*shared.Transaction, 0, len(candidates))
	unmatchedIDs := make([]uuid.UUID, 0, len(candidates))

	for _, tx := range candidates {
		if tx == nil {
			continue
		}

		if _, ok := leftSourceIDs[tx.SourceID]; ok {
			leftCandidates = append(leftCandidates, tx)
			continue
		}

		if _, ok := rightSourceIDs[tx.SourceID]; ok {
			rightCandidates = append(rightCandidates, tx)
			continue
		}

		logger.With(libLog.Any("tx.id", tx.ID.String()), libLog.Any("source.id", tx.SourceID.String())).Log(ctx, libLog.LevelWarn, "transaction source not in configured sources")

		unmatchedIDs = append(unmatchedIDs, tx.ID)
	}

	return leftCandidates, rightCandidates, unmatchedIDs, nil
}

// executeMatchRules runs the matching rules and processes proposals.
func (uc *UseCase) executeMatchRules(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	mrc *matchRunContext,
	createdRun *matchingEntities.MatchRun,
) (*matchExecutionResult, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, execSpan := tracer.Start(ctx, "command.matching.execute_match_rules")
	defer execSpan.End()

	executeRulesDetailedFn := uc.executeRulesDetailed
	if executeRulesDetailedFn == nil {
		executeRulesDetailedFn = uc.ExecuteRulesDetailed
	}

	// Determine normalization mode from context
	var feeNorm fee.NormalizationMode
	if mrc.ctxInfo.FeeNormalization != nil {
		feeNorm = fee.NormalizationMode(*mrc.ctxInfo.FeeNormalization)
	}

	rulesResult, err := executeRulesDetailedFn(ctx, ExecuteRulesInput{
		ContextID:        mrc.input.ContextID,
		ContextType:      mrc.ctxInfo.Type,
		Left:             mrc.leftCandidates,
		Right:            mrc.rightCandidates,
		LeftRules:        mrc.leftRules,
		RightRules:       mrc.rightRules,
		AllSchedules:     mrc.allSchedules,
		FeeNormalization: feeNorm,
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to execute match rules", err)
		return nil, err
	}

	proposals := rulesResult.Proposals

	leftMatched := make(map[uuid.UUID]struct{})
	rightMatched := make(map[uuid.UUID]struct{})
	leftConfirmed := make(map[uuid.UUID]struct{})
	rightConfirmed := make(map[uuid.UUID]struct{})
	leftPending := make(map[uuid.UUID]struct{})
	rightPending := make(map[uuid.UUID]struct{})
	autoMatchedIDs := make([]uuid.UUID, 0, len(proposals)*sliceCapMultiplier)
	pendingReviewIDs := make([]uuid.UUID, 0, len(proposals)*sliceCapMultiplier)
	groups := make([]*matchingEntities.MatchGroup, 0, len(proposals))
	items := make([]*matchingEntities.MatchItem, 0, len(proposals)*sliceCapMultiplier)

	leftByID := indexTransactions(mrc.leftCandidates)
	rightByID := indexTransactions(mrc.rightCandidates)

	processOutput := uc.processProposals(
		ctx,
		span,
		logger,
		mrc.input.ContextID,
		createdRun.ID,
		proposals,
		leftByID,
		rightByID,
	)
	groups = append(groups, processOutput.groups...)
	items = append(items, processOutput.items...)
	autoMatchedIDs = append(autoMatchedIDs, processOutput.autoMatchedIDs...)
	pendingReviewIDs = append(pendingReviewIDs, processOutput.pendingReviewIDs...)
	mergeMatched(leftMatched, processOutput.leftMatched)
	mergeMatched(rightMatched, processOutput.rightMatched)
	mergeMatched(leftConfirmed, processOutput.leftConfirmed)
	mergeMatched(rightConfirmed, processOutput.rightConfirmed)
	mergeMatched(leftPending, processOutput.leftPending)
	mergeMatched(rightPending, processOutput.rightPending)
	unmatchedReasons := processOutput.unmatchedReasons

	for txID, failure := range rulesResult.AllocFailures {
		if _, alreadySet := unmatchedReasons[txID]; !alreadySet {
			unmatchedReasons[txID] = string(failure.Code)
		}
	}

	unmatchedLeft := collectUnmatched(mrc.leftCandidates, leftMatched)
	unmatchedRight := collectUnmatched(mrc.rightCandidates, rightMatched)
	externalUnmatchedCount := len(mrc.unmatchedIDs)

	allUnmatchedIDs := make(
		[]uuid.UUID,
		0,
		len(mrc.unmatchedIDs)+len(unmatchedLeft)+len(unmatchedRight),
	)
	allUnmatchedIDs = append(allUnmatchedIDs, mrc.unmatchedIDs...)
	allUnmatchedIDs = append(allUnmatchedIDs, unmatchedLeft...)
	allUnmatchedIDs = append(allUnmatchedIDs, unmatchedRight...)

	if len(unmatchedReasons) == 0 {
		unmatchedReasons = nil
	}

	stats := make(map[string]int, len(mrc.stats)+statsFieldCount)
	maps.Copy(stats, mrc.stats)

	stats["matches"] = len(groups)
	stats["unmatched_left"] = len(unmatchedLeft)
	stats["unmatched_right"] = len(unmatchedRight)
	stats["unmatched_external"] = externalUnmatchedCount
	stats["auto_matched_left"] = len(leftConfirmed)
	stats["auto_matched_right"] = len(rightConfirmed)
	stats["pending_review_left"] = len(leftPending)
	stats["pending_review_right"] = len(rightPending)
	stats["proposed_left"] = len(leftMatched) - len(leftConfirmed)
	stats["proposed_right"] = len(rightMatched) - len(rightConfirmed)

	if span != nil {
		matchedCount := len(
			leftConfirmed,
		) + len(
			rightConfirmed,
		) + len(
			leftPending,
		) + len(
			rightPending,
		)
		unmatchedCount := len(unmatchedLeft) + len(unmatchedRight) + externalUnmatchedCount
		_ = libOpentelemetry.SetSpanAttributesFromValue(
			span,
			"matcher",
			struct {
				GroupsCreated  int `json:"groupsCreated"`
				MatchedCount   int `json:"matchedCount"`
				UnmatchedCount int `json:"unmatchedCount"`
			}{
				GroupsCreated:  len(groups),
				MatchedCount:   matchedCount,
				UnmatchedCount: unmatchedCount,
			},
			nil,
		)
	}

	allTxByID := mergeTransactionMaps(leftByID, rightByID)

	return &matchExecutionResult{
		groups:           groups,
		items:            items,
		autoMatchedIDs:   autoMatchedIDs,
		pendingReviewIDs: pendingReviewIDs,
		unmatchedIDs:     allUnmatchedIDs,
		unmatchedReasons: unmatchedReasons,
		allTxByID:        allTxByID,
		stats:            stats,
	}, nil
}

func (uc *UseCase) commitMatchResults(
	ctx context.Context,
	_ trace.Span,
	createdRun *matchingEntities.MatchRun,
	groups []*matchingEntities.MatchGroup,
	items []*matchingEntities.MatchItem,
	autoMatchedIDs, pendingReviewIDs, unmatchedIDs []uuid.UUID,
	unmatchedReasons map[uuid.UUID]string,
	refreshFailed *atomic.Bool,
	stats map[string]int,
	feeInput *feeVerificationInput,
) (*matchingEntities.MatchRun, error) {
	if refreshFailed != nil && refreshFailed.Load() {
		return nil, finalizeRunFailure(ctx, uc, createdRun, ErrLockRefreshFailed)
	}

	var updatedRun *matchingEntities.MatchRun

	commitErr := uc.matchRunRepo.WithTx(ctx, func(tx repositories.Tx) error {
		if refreshFailed != nil && refreshFailed.Load() {
			return ErrLockRefreshFailed
		}

		if err := uc.persistMatchArtifacts(ctx, tx, createdRun, groups, items, autoMatchedIDs, pendingReviewIDs, unmatchedIDs, unmatchedReasons, feeInput); err != nil {
			return err
		}

		if err := createdRun.Complete(ctx, stats); err != nil {
			return fmt.Errorf("failed to complete match run: %w", err)
		}

		updated, err := uc.matchRunRepo.UpdateWithTx(ctx, tx, createdRun)
		if err != nil {
			return err
		}

		updatedRun = updated

		return nil
	})
	if commitErr != nil {
		return nil, finalizeRunFailure(ctx, uc, createdRun, commitErr)
	}

	if updatedRun == nil {
		return nil, ErrMatchRunPersistedNil
	}

	return updatedRun, nil
}

func (uc *UseCase) completeDryRun(
	ctx context.Context,
	span trace.Span,
	createdRun *matchingEntities.MatchRun,
	stats map[string]int,
	groups []*matchingEntities.MatchGroup,
	refreshFailed *atomic.Bool,
) (*matchingEntities.MatchRun, []*matchingEntities.MatchGroup, error) {
	if refreshFailed != nil && refreshFailed.Load() {
		return nil, nil, finalizeRunFailure(ctx, uc, createdRun, ErrLockRefreshFailed)
	}

	if err := createdRun.Complete(ctx, stats); err != nil {
		return nil, nil, fmt.Errorf("failed to complete match run: %w", err)
	}

	updatedRun, err := uc.matchRunRepo.Update(ctx, createdRun)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to complete match run", err)
		return nil, nil, fmt.Errorf("failed to update match run: %w", err)
	}

	if updatedRun == nil {
		return nil, nil, ErrMatchRunPersistedNil
	}

	return updatedRun, groups, nil
}

type feeVerificationInput struct {
	ctxInfo        *ports.ReconciliationContextInfo
	txByID         map[uuid.UUID]*shared.Transaction
	sourceTypeByID map[uuid.UUID]string
}

func (uc *UseCase) persistMatchArtifacts(
	ctx context.Context,
	tx repositories.Tx,
	createdRun *matchingEntities.MatchRun,
	groups []*matchingEntities.MatchGroup,
	items []*matchingEntities.MatchItem,
	autoMatchedIDs []uuid.UUID,
	pendingReviewIDs []uuid.UUID,
	unmatchedIDs []uuid.UUID,
	unmatchedReasons map[uuid.UUID]string,
	feeInput *feeVerificationInput,
) error {
	if len(groups) > 0 {
		if _, err := uc.matchGroupRepo.CreateBatchWithTx(ctx, tx, groups); err != nil {
			return err
		}

		if _, err := uc.matchItemRepo.CreateBatchWithTx(ctx, tx, items); err != nil {
			return err
		}
	}

	if len(autoMatchedIDs) > 0 {
		if err := uc.txRepo.MarkMatchedWithTx(ctx, tx, createdRun.ContextID, autoMatchedIDs); err != nil {
			return err
		}
	}

	if len(pendingReviewIDs) > 0 {
		if err := uc.txRepo.MarkPendingReviewWithTx(ctx, tx, createdRun.ContextID, pendingReviewIDs); err != nil {
			return err
		}
	}

	if len(unmatchedReasons) == 0 {
		unmatchedReasons = nil
	}

	var txByID map[uuid.UUID]*shared.Transaction

	var sourceTypeByID map[uuid.UUID]string

	if feeInput != nil {
		txByID = feeInput.txByID
		sourceTypeByID = feeInput.sourceTypeByID
	}

	exceptionInputs := buildExceptionInputs(
		unmatchedIDs,
		txByID,
		sourceTypeByID,
		unmatchedReasons,
	)
	if err := uc.exceptionCreator.CreateExceptionsWithTx(ctx, tx, createdRun.ContextID, createdRun.ID, exceptionInputs, nil); err != nil {
		return err
	}

	if err := uc.performFeeVerification(ctx, tx, createdRun, groups, feeInput); err != nil {
		return err
	}

	return uc.enqueueMatchConfirmedEvents(ctx, tx, groups)
}

func (uc *UseCase) enqueueMatchConfirmedEvents(
	ctx context.Context,
	tx repositories.Tx,
	groups []*matchingEntities.MatchGroup,
) error {
	if uc.outboxRepoTx == nil {
		return ErrOutboxRepoNotConfigured
	}

	sqlTx, ok := tx.(*sql.Tx)
	if !ok || sqlTx == nil {
		return ErrOutboxRequiresSQLTx
	}

	tenantIDStr := auth.GetTenantID(ctx)
	if tenantIDStr == "" {
		tenantIDStr = auth.DefaultTenantID
	}

	tenantUUID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return fmt.Errorf("parse tenant id: %w", err)
	}

	tenantSlug := auth.GetTenantSlug(ctx)

	for _, group := range groups {
		if err := uc.enqueueGroupEvent(ctx, sqlTx, group, tenantUUID, tenantSlug); err != nil {
			return err
		}
	}

	return nil
}

func (uc *UseCase) enqueueGroupEvent(
	ctx context.Context,
	sqlTx *sql.Tx,
	group *matchingEntities.MatchGroup,
	tenantUUID uuid.UUID,
	tenantSlug string,
) error {
	if group == nil {
		return nil
	}

	if group.Status != matchingVO.MatchGroupStatusConfirmed {
		return nil
	}

	event, err := matchingEntities.NewMatchConfirmedEvent(
		ctx,
		tenantUUID,
		tenantSlug,
		group,
		time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("build match confirmed event: %w", err)
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal match confirmed event: %w", err)
	}

	outboxEvent, err := shared.NewOutboxEvent(ctx, event.EventType, event.ID(), body)
	if err != nil {
		return fmt.Errorf("create outbox event: %w", err)
	}

	if _, err := uc.outboxRepoTx.CreateWithTx(ctx, sqlTx, outboxEvent); err != nil {
		return fmt.Errorf("create outbox entry: %w", err)
	}

	return nil
}

func (uc *UseCase) performFeeVerification(
	ctx context.Context,
	tx repositories.Tx,
	createdRun *matchingEntities.MatchRun,
	groups []*matchingEntities.MatchGroup,
	feeInput *feeVerificationInput,
) error {
	if feeInput == nil || feeInput.ctxInfo == nil || feeInput.ctxInfo.RateID == nil {
		return nil
	}

	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled
	ctx, span := tracer.Start(ctx, "command.matching.fee_verification")

	defer span.End()

	rate, err := uc.rateRepo.GetByID(ctx, *feeInput.ctxInfo.RateID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load rate for fee verification", err)
		return fmt.Errorf("load rate for fee verification: %w", err)
	}

	tolerance := fee.Tolerance{
		Abs:     feeInput.ctxInfo.FeeToleranceAbs,
		Percent: feeInput.ctxInfo.FeeTolerancePct,
	}

	findings := collectFeeFindings(ctx, span, groups, createdRun, feeInput, rate, tolerance)

	span.SetAttributes(
		attribute.String("fee.currency", rate.Currency),
		attribute.String("fee.structure_type", string(rate.Structure.Type())),
		attribute.Int("fee.items_checked", len(feeInput.txByID)),
		attribute.Int("fee.variances_found", len(findings.variances)),
		attribute.Int("fee.exceptions_created", len(findings.exceptionInputs)),
	)

	return uc.persistFeeFindings(ctx, tx, span, createdRun, findings)
}

// feeFindings holds the collected variances and exceptions from fee verification.
type feeFindings struct {
	variances       []*matchingEntities.FeeVariance
	exceptionInputs []ports.ExceptionTransactionInput
}

// collectFeeFindings iterates through confirmed groups and collects fee variances and exceptions.
func collectFeeFindings(
	ctx context.Context,
	span trace.Span,
	groups []*matchingEntities.MatchGroup,
	createdRun *matchingEntities.MatchRun,
	feeInput *feeVerificationInput,
	rate *fee.Rate,
	tolerance fee.Tolerance,
) *feeFindings {
	findings := &feeFindings{}

	for _, group := range groups {
		if group == nil || group.Status != matchingVO.MatchGroupStatusConfirmed {
			continue
		}

		for _, item := range group.Items {
			result := processFeeForItem(
				ctx,
				span,
				item,
				group,
				createdRun,
				feeInput,
				rate,
				tolerance,
			)
			if result == nil {
				continue
			}

			if result.variance != nil {
				findings.variances = append(findings.variances, result.variance)
			}

			if result.exceptionInput != nil {
				findings.exceptionInputs = append(findings.exceptionInputs, *result.exceptionInput)
			}
		}
	}

	return findings
}

// persistFeeFindings saves the collected variances and creates exceptions.
func (uc *UseCase) persistFeeFindings(
	ctx context.Context,
	tx repositories.Tx,
	span trace.Span,
	createdRun *matchingEntities.MatchRun,
	findings *feeFindings,
) error {
	if len(findings.variances) > 0 {
		if _, err := uc.feeVarianceRepo.CreateBatchWithTx(ctx, tx, findings.variances); err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to persist fee variances", err)
			return fmt.Errorf("persist fee variances: %w", err)
		}
	}

	if len(findings.exceptionInputs) > 0 {
		if err := uc.exceptionCreator.CreateExceptionsWithTx(ctx, tx, createdRun.ContextID, createdRun.ID, findings.exceptionInputs, nil); err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to create fee exceptions", err)
			return fmt.Errorf("create fee exceptions: %w", err)
		}
	}

	return nil
}

type feeExtractionError struct {
	reason string
}

// feeItemResult holds the result of processing a single item for fee verification.
type feeItemResult struct {
	variance       *matchingEntities.FeeVariance
	exceptionInput *ports.ExceptionTransactionInput
}

// processFeeForItem handles fee verification for a single match item.
func processFeeForItem(
	ctx context.Context,
	span trace.Span,
	item *matchingEntities.MatchItem,
	group *matchingEntities.MatchGroup,
	createdRun *matchingEntities.MatchRun,
	feeInput *feeVerificationInput,
	rate *fee.Rate,
	tolerance fee.Tolerance,
) *feeItemResult {
	if item == nil {
		return nil
	}

	txn, ok := feeInput.txByID[item.TransactionID]
	if !ok || txn == nil {
		return nil
	}

	actualFee, feeErr := extractActualFee(txn, rate.Currency)
	if feeErr != nil {
		return &feeItemResult{
			exceptionInput: buildExceptionInputFromTx(txn, feeInput.sourceTypeByID, feeErr.reason),
		}
	}

	txForFee := &fee.TransactionForFee{
		Amount:    fee.Money{Amount: txn.Amount.Abs(), Currency: txn.Currency},
		ActualFee: &actualFee,
	}

	expectedFee, calcErr := fee.CalculateExpectedFee(ctx, txForFee, rate)
	if calcErr != nil {
		if errors.Is(calcErr, fee.ErrCurrencyMismatch) {
			return &feeItemResult{
				exceptionInput: buildExceptionInputFromTx(
					txn,
					feeInput.sourceTypeByID,
					enums.ReasonFeeCurrencyMismatch,
				),
			}
		}

		return nil
	}

	varianceResult, verifyErr := fee.VerifyFee(actualFee, expectedFee, tolerance)
	if verifyErr != nil {
		if errors.Is(verifyErr, fee.ErrCurrencyMismatch) {
			return &feeItemResult{
				exceptionInput: buildExceptionInputFromTx(
					txn,
					feeInput.sourceTypeByID,
					enums.ReasonFeeCurrencyMismatch,
				),
			}
		}

		return nil
	}

	if varianceResult.Type != fee.VarianceMatch {
		fv, fvErr := matchingEntities.NewFeeVariance(
			ctx,
			createdRun.ContextID,
			createdRun.ID,
			group.ID,
			item.TransactionID,
			*feeInput.ctxInfo.RateID,
			rate.Currency,
			expectedFee.Amount,
			actualFee.Amount,
			tolerance.Abs,
			tolerance.Percent,
			string(varianceResult.Type),
		)
		if fvErr != nil {
			libOpentelemetry.HandleSpanError(span, "failed to create fee variance entity", fvErr)
			return nil
		}

		return &feeItemResult{
			variance: fv,
			exceptionInput: buildExceptionInputFromTx(
				txn,
				feeInput.sourceTypeByID,
				enums.ReasonFeeVariance,
			),
		}
	}

	return nil
}

// parseAmount converts a raw value (from metadata) to a decimal.Decimal.
func parseAmount(amountRaw any) (decimal.Decimal, *feeExtractionError) {
	switch amountValue := amountRaw.(type) {
	case string:
		parsed, err := decimal.NewFromString(amountValue)
		if err != nil {
			return decimal.Decimal{}, &feeExtractionError{reason: enums.ReasonFeeDataMissing}
		}

		return parsed, nil
	case float64:
		return decimal.NewFromFloat(amountValue), nil
	case int:
		return decimal.NewFromInt(int64(amountValue)), nil
	case int64:
		return decimal.NewFromInt(amountValue), nil
	default:
		return decimal.Decimal{}, &feeExtractionError{reason: enums.ReasonFeeDataMissing}
	}
}

func extractActualFee(
	txn *shared.Transaction,
	expectedCurrency string,
) (fee.Money, *feeExtractionError) {
	if txn.Metadata == nil {
		return fee.Money{}, &feeExtractionError{reason: enums.ReasonFeeDataMissing}
	}

	feeData, ok := txn.Metadata["fee"]
	if !ok {
		return fee.Money{}, &feeExtractionError{reason: enums.ReasonFeeDataMissing}
	}

	feeMap, ok := feeData.(map[string]any)
	if !ok {
		return fee.Money{}, &feeExtractionError{reason: enums.ReasonFeeDataMissing}
	}

	amountRaw, ok := feeMap["amount"]
	if !ok {
		return fee.Money{}, &feeExtractionError{reason: enums.ReasonFeeDataMissing}
	}

	amount, parseErr := parseAmount(amountRaw)
	if parseErr != nil {
		return fee.Money{}, parseErr
	}

	currency := expectedCurrency

	if currencyRaw, ok := feeMap["currency"]; ok {
		if currencyStr, ok := currencyRaw.(string); ok && strings.TrimSpace(currencyStr) != "" {
			currency = strings.ToUpper(strings.TrimSpace(currencyStr))
		}
	}

	if currency != expectedCurrency {
		return fee.Money{}, &feeExtractionError{reason: enums.ReasonFeeCurrencyMismatch}
	}

	return fee.Money{Amount: amount, Currency: currency}, nil
}

func (uc *UseCase) ensureLockFresh(
	ctx context.Context,
	span trace.Span,
	lock ports.Lock,
	refreshFailed *atomic.Bool,
) error {
	if refreshFailed != nil && refreshFailed.Load() {
		return ErrLockRefreshFailed
	}

	refreshable, ok := lock.(ports.RefreshableLock)
	if !ok {
		return nil
	}

	if err := refreshable.Refresh(ctx, lockTTL); err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to refresh transaction lock", err)
		}

		if refreshFailed != nil {
			refreshFailed.Store(true)
		}

		return ErrLockRefreshFailed
	}

	return nil
}

func (uc *UseCase) validateRunMatchDependencies() error {
	if uc.contextProvider == nil {
		return ErrNilContextRepository
	}

	if uc.sourceProvider == nil {
		return ErrNilSourceRepository
	}

	if uc.ruleProvider == nil {
		return ErrNilMatchRuleProvider
	}

	if uc.txRepo == nil {
		return ErrNilTransactionRepository
	}

	if uc.lockManager == nil {
		return ErrNilLockManager
	}

	if uc.matchRunRepo == nil {
		return ErrNilMatchRunRepository
	}

	if uc.matchGroupRepo == nil {
		return ErrNilMatchGroupRepository
	}

	if uc.matchItemRepo == nil {
		return ErrNilMatchItemRepository
	}

	if uc.exceptionCreator == nil {
		return ErrNilExceptionCreator
	}

	if uc.outboxRepoTx == nil {
		return ErrOutboxRepoNotConfigured
	}

	return nil
}

func (uc *UseCase) acquireContextLock(
	ctx context.Context,
	span trace.Span,
	contextID uuid.UUID,
) (ports.Lock, error) {
	lock, err := uc.lockManager.AcquireContextLock(ctx, contextID, lockTTL)
	if err != nil {
		if errors.Is(err, ports.ErrLockAlreadyHeld) {
			return nil, ErrMatchRunLocked
		}

		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to acquire context lock", err)
		}

		return nil, fmt.Errorf("failed to acquire context lock: %w", err)
	}

	return lock, nil
}

func (uc *UseCase) watchLockRefresh(
	ctx context.Context,
	span trace.Span,
	lock ports.Lock,
	logger libLog.Logger,
	cancelRun context.CancelFunc,
	refreshFailed, commitStarted *atomic.Bool,
) func() {
	if refreshFailed == nil {
		return func() {}
	}

	if lock == nil {
		return func() {}
	}

	refreshable, ok := lock.(ports.RefreshableLock)
	if !ok {
		return func() {
			uc.releaseMatchLock(ctx, span, lock, logger)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	refreshErrs := uc.startLockRefreshLoop(runCtx, span, refreshable, logger, cancel)
	stopWatch := uc.startLockRefreshWatcher(
		runCtx,
		refreshErrs,
		refreshFailed,
		commitStarted,
		cancelRun,
		cancel,
		logger,
	)

	return func() {
		stopWatch()
		cancel()

		uc.releaseMatchLock(ctx, span, lock, logger)
	}
}

func (uc *UseCase) startLockRefreshLoop(
	ctx context.Context,
	span trace.Span,
	lock ports.RefreshableLock,
	logger libLog.Logger,
	cancel context.CancelFunc,
) <-chan error {
	refreshErrs := make(chan error, 1)
	refreshInterval := lockRefreshIntervalDefault

	if uc != nil && uc.lockRefreshInterval > 0 {
		refreshInterval = uc.lockRefreshInterval
	}

	ticker := time.NewTicker(refreshInterval)

	runtime.SafeGoWithContextAndComponent(
		ctx,
		logger,
		constants.ApplicationName,
		"matching.lock_refresh",
		runtime.KeepRunning,
		func(ctx context.Context) {
			defer ticker.Stop()
			defer close(refreshErrs)

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := lock.Refresh(ctx, lockTTL); err != nil {
						if span != nil {
							libOpentelemetry.HandleSpanError(
								span,
								"failed to refresh transaction lock",
								err,
							)
						}

						logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to refresh transaction lock")

						refreshErrs <- err

						cancel()

						return
					}
				}
			}
		},
	)

	return refreshErrs
}

func (uc *UseCase) startLockRefreshWatcher(
	ctx context.Context,
	refreshErrs <-chan error,
	refreshFailed, commitStarted *atomic.Bool,
	cancelRun, cancel context.CancelFunc,
	logger libLog.Logger,
) func() {
	watchCtx, watchCancel := context.WithCancel(ctx) // #nosec G118 -- watchCancel is returned to the caller
	runtime.SafeGoWithContextAndComponent(
		watchCtx,
		logger,
		constants.ApplicationName,
		"matching.lock_refresh_watch",
		runtime.KeepRunning,
		func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				case refreshErr, ok := <-refreshErrs:
					if !ok {
						return
					}

					if refreshErr == nil {
						continue
					}

					refreshFailed.Store(true)

					if commitStarted != nil && !commitStarted.Load() {
						if cancelRun != nil {
							cancelRun()
						}

						cancel()

						return
					}

					logger.With(libLog.Any("error", refreshErr.Error())).Log(ctx, libLog.LevelError, "lock refresh failed")

					return
				}
			}
		},
	)

	return watchCancel
}

func (uc *UseCase) releaseMatchLock(
	ctx context.Context,
	span trace.Span,
	lock ports.Lock,
	logger libLog.Logger,
) {
	if lock == nil {
		return
	}

	releaseCtx := context.WithoutCancel(ctx)

	if releaseErr := lock.Release(releaseCtx); releaseErr != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to release transaction lock", releaseErr)
		}

		logger.With(libLog.Any("error", releaseErr.Error())).Log(ctx, libLog.LevelError, "failed to release transaction lock")
	}
}

func finalizeRunFailure(
	ctx context.Context,
	uc *UseCase,
	run *matchingEntities.MatchRun,
	cause error,
) error {
	if run == nil {
		return cause
	}

	if err := run.Fail(ctx, cause.Error()); err != nil {
		return fmt.Errorf("failed to mark run as failed: %w", err)
	}

	updateCtx := context.WithoutCancel(ctx)
	if _, updateErr := uc.matchRunRepo.Update(updateCtx, run); updateErr != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(updateCtx)

		logger.With(libLog.Any("error", updateErr.Error())).Log(ctx, libLog.LevelError, "failed to update match run after error")

		return fmt.Errorf("updating match run failed: %w; original cause: %w", updateErr, cause)
	}

	return cause
}

func indexTransactions(transactions []*shared.Transaction) map[uuid.UUID]*shared.Transaction {
	indexed := make(map[uuid.UUID]*shared.Transaction, len(transactions))

	for _, tx := range transactions {
		if tx == nil {
			continue
		}

		indexed[tx.ID] = tx
	}

	return indexed
}

func mergeTransactionMaps(
	txMaps ...map[uuid.UUID]*shared.Transaction,
) map[uuid.UUID]*shared.Transaction {
	totalSize := 0
	for _, m := range txMaps {
		totalSize += len(m)
	}

	merged := make(map[uuid.UUID]*shared.Transaction, totalSize)

	for _, m := range txMaps {
		maps.Copy(merged, m)
	}

	return merged
}

func collectUnmatched(
	transactions []*shared.Transaction,
	matched map[uuid.UUID]struct{},
) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(transactions))

	for _, tx := range transactions {
		if tx == nil {
			continue
		}

		if _, ok := matched[tx.ID]; ok {
			continue
		}

		out = append(out, tx.ID)
	}

	return out
}

func allocationMap(allocations []matching.Allocation) map[uuid.UUID]decimal.Decimal {
	out := make(map[uuid.UUID]decimal.Decimal, len(allocations))

	for _, allocation := range allocations {
		out[allocation.TransactionID] = allocation.AllocatedAmount
	}

	return out
}

func allocationCurrencyMap(allocations []matching.Allocation) map[uuid.UUID]string {
	out := make(map[uuid.UUID]string, len(allocations))

	for _, allocation := range allocations {
		out[allocation.TransactionID] = allocation.Currency
	}

	return out
}

func allocationUseBaseMap(allocations []matching.Allocation) map[uuid.UUID]bool {
	out := make(map[uuid.UUID]bool, len(allocations))

	for _, allocation := range allocations {
		out[allocation.TransactionID] = allocation.UseBaseAmount
	}

	return out
}

type allocationErrorInfo struct {
	logMessage string
	reason     string
	spanErr    error
}

func allocationFields(
	tx *shared.Transaction,
	allocations map[uuid.UUID]decimal.Decimal,
	allocationCurrencies map[uuid.UUID]string,
	allocationUseBase map[uuid.UUID]bool,
) (decimal.Decimal, string, decimal.Decimal, *allocationErrorInfo) {
	allocated := tx.Amount
	currency := tx.Currency
	useBase := false

	if value, ok := allocations[tx.ID]; ok {
		allocated = value

		if allocationCurrency, ok := allocationCurrencies[tx.ID]; ok && allocationCurrency != "" {
			currency = allocationCurrency
		}

		if allocationUsesBase, ok := allocationUseBase[tx.ID]; ok {
			useBase = allocationUsesBase
		}
	}

	expected := tx.Amount

	if !useBase {
		return allocated, currency, expected, nil
	}

	if tx.AmountBase == nil {
		return decimal.Zero, "", decimal.Zero, &allocationErrorInfo{
			logMessage: invalidAllocationMissingBase,
			reason:     enums.ReasonMissingBaseAmount,
			spanErr:    ErrMissingBaseAmountForAllocation,
		}
	}

	if tx.BaseCurrency == nil || strings.TrimSpace(*tx.BaseCurrency) == "" {
		return decimal.Zero, "", decimal.Zero, &allocationErrorInfo{
			logMessage: invalidAllocationMissingBaseCurrency,
			reason:     enums.ReasonMissingBaseCurrency,
			spanErr:    ErrMissingBaseCurrencyForAllocation,
		}
	}

	expected = *tx.AmountBase
	currency = *tx.BaseCurrency

	return allocated, currency, expected, nil
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

// proposalItemsContext holds inputs for building match items from a proposal side.
type proposalItemsContext struct {
	txByID               map[uuid.UUID]*shared.Transaction
	allocations          map[uuid.UUID]decimal.Decimal
	allocationCurrencies map[uuid.UUID]string
	allocationUseBase    map[uuid.UUID]bool
	notFoundErr          error
}

// buildProposalItems builds match items for one side of a proposal (left or right).
// Returns the items and whether the proposal should be skipped due to an error.
func buildProposalItems(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	ids []uuid.UUID,
	pic *proposalItemsContext,
	unmatchedReasons map[uuid.UUID]string,
) ([]*matchingEntities.MatchItem, bool) {
	items := make([]*matchingEntities.MatchItem, 0, len(ids))

	for _, id := range ids {
		tx, ok := pic.txByID[id]
		if !ok {
			logProposalError(ctx, logger, span, id, "proposal transaction not found", pic.notFoundErr)
			return nil, true
		}

		allocated, currency, expected, baseErr := allocationFields(
			tx,
			pic.allocations,
			pic.allocationCurrencies,
			pic.allocationUseBase,
		)
		if baseErr != nil {
			logProposalError(ctx, logger, span, id, baseErr.logMessage, baseErr.spanErr)
			unmatchedReasons[tx.ID] = baseErr.reason

			return nil, true
		}

		item, err := matchingEntities.NewMatchItemWithPolicy(
			ctx,
			tx.ID,
			allocated,
			currency,
			expected,
			allocated.LessThan(expected),
		)
		if err != nil {
			logProposalError(ctx, logger, span, id, "match proposal processing failed", err)
			return nil, true
		}

		items = append(items, item)
	}

	return items, false
}

// logProposalError logs an error during proposal processing.
func logProposalError(
	ctx context.Context,
	logger libLog.Logger,
	span trace.Span,
	txID uuid.UUID,
	message string,
	err error,
) {
	libOpentelemetry.HandleSpanError(span, message, err)

	logger.With(libLog.Any("transaction.id", txID.String())).Log(ctx, libLog.LevelError, message)
}

// recordGroupResults updates the processing result with a newly created group.
func recordGroupResults(
	result *proposalProcessingResult,
	group *matchingEntities.MatchGroup,
	leftByID map[uuid.UUID]*shared.Transaction,
	rightByID map[uuid.UUID]*shared.Transaction,
) {
	result.groups = append(result.groups, group)
	result.items = append(result.items, group.Items...)

	canAutoConfirm := group.CanAutoConfirm()

	for _, item := range group.Items {
		if canAutoConfirm {
			result.autoMatchedIDs = append(result.autoMatchedIDs, item.TransactionID)
		} else {
			result.pendingReviewIDs = append(result.pendingReviewIDs, item.TransactionID)
		}

		if _, ok := leftByID[item.TransactionID]; ok {
			result.leftMatched[item.TransactionID] = struct{}{}
			if canAutoConfirm {
				result.leftConfirmed[item.TransactionID] = struct{}{}
			} else {
				result.leftPending[item.TransactionID] = struct{}{}
			}

			continue
		}

		if _, ok := rightByID[item.TransactionID]; ok {
			result.rightMatched[item.TransactionID] = struct{}{}
			if canAutoConfirm {
				result.rightConfirmed[item.TransactionID] = struct{}{}
			} else {
				result.rightPending[item.TransactionID] = struct{}{}
			}
		}
	}
}

func (uc *UseCase) processProposals(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	contextID uuid.UUID,
	runID uuid.UUID,
	proposals []matching.MatchProposal,
	leftByID map[uuid.UUID]*shared.Transaction,
	rightByID map[uuid.UUID]*shared.Transaction,
) proposalProcessingResult {
	result := proposalProcessingResult{
		groups:           make([]*matchingEntities.MatchGroup, 0, len(proposals)),
		items:            make([]*matchingEntities.MatchItem, 0, len(proposals)*sliceCapMultiplier),
		autoMatchedIDs:   make([]uuid.UUID, 0, len(proposals)*sliceCapMultiplier),
		pendingReviewIDs: make([]uuid.UUID, 0, len(proposals)*sliceCapMultiplier),
		leftMatched:      make(map[uuid.UUID]struct{}),
		rightMatched:     make(map[uuid.UUID]struct{}),
		leftConfirmed:    make(map[uuid.UUID]struct{}),
		rightConfirmed:   make(map[uuid.UUID]struct{}),
		leftPending:      make(map[uuid.UUID]struct{}),
		rightPending:     make(map[uuid.UUID]struct{}),
		unmatchedReasons: make(map[uuid.UUID]string),
	}

	for _, proposal := range proposals {
		if ctx.Err() != nil {
			break
		}

		group := uc.processSingleProposal(
			ctx,
			span,
			logger,
			contextID,
			runID,
			proposal,
			leftByID,
			rightByID,
			result.unmatchedReasons,
		)
		if group == nil {
			continue
		}

		recordGroupResults(&result, group, leftByID, rightByID)
	}

	return result
}

// processSingleProposal processes one match proposal and returns a group if successful.
func (uc *UseCase) processSingleProposal(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	contextID uuid.UUID,
	runID uuid.UUID,
	proposal matching.MatchProposal,
	leftByID map[uuid.UUID]*shared.Transaction,
	rightByID map[uuid.UUID]*shared.Transaction,
	unmatchedReasons map[uuid.UUID]string,
) *matchingEntities.MatchGroup {
	confidence, err := matchingVO.ParseConfidenceScore(proposal.Score)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "match proposal processing failed", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "match proposal processing failed")

		return nil
	}

	leftItems, invalid := buildProposalItems(
		ctx,
		span,
		logger,
		proposal.LeftIDs,
		&proposalItemsContext{
			txByID:               leftByID,
			allocations:          allocationMap(proposal.LeftAllocations),
			allocationCurrencies: allocationCurrencyMap(proposal.LeftAllocations),
			allocationUseBase:    allocationUseBaseMap(proposal.LeftAllocations),
			notFoundErr:          ErrProposalLeftTransactionNotFound,
		},
		unmatchedReasons,
	)
	if invalid || len(leftItems) == 0 {
		return nil
	}

	rightItems, invalid := buildProposalItems(
		ctx,
		span,
		logger,
		proposal.RightIDs,
		&proposalItemsContext{
			txByID:               rightByID,
			allocations:          allocationMap(proposal.RightAllocations),
			allocationCurrencies: allocationCurrencyMap(proposal.RightAllocations),
			allocationUseBase:    allocationUseBaseMap(proposal.RightAllocations),
			notFoundErr:          ErrProposalRightTransactionNotFound,
		},
		unmatchedReasons,
	)
	if invalid || len(rightItems) == 0 {
		return nil
	}

	proposalItems := make([]*matchingEntities.MatchItem, 0, len(leftItems)+len(rightItems))
	proposalItems = append(proposalItems, leftItems...)
	proposalItems = append(proposalItems, rightItems...)

	if len(proposalItems) < minMatchedItemsCount {
		return nil
	}

	group, err := matchingEntities.NewMatchGroup(
		ctx,
		contextID,
		runID,
		proposal.RuleID,
		confidence,
		proposalItems,
	)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "match proposal processing failed", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "match proposal processing failed")

		return nil
	}

	if group.CanAutoConfirm() {
		if confirmErr := group.Confirm(ctx); confirmErr != nil {
			libOpentelemetry.HandleSpanError(span, "match proposal confirm failed", confirmErr)

			logger.With(libLog.Any("error", confirmErr.Error())).Log(ctx, libLog.LevelError, "match proposal confirm failed")
		}
	}

	return group
}

func mergeMatched(dest, src map[uuid.UUID]struct{}) {
	if dest == nil || src == nil {
		return
	}

	for id := range src {
		dest[id] = struct{}{}
	}
}

func (uc *UseCase) completeEmptyRun(
	ctx context.Context,
	in RunMatchInput,
	stats map[string]int,
	leftCandidates, rightCandidates []*shared.Transaction,
	externalUnmatched []uuid.UUID,
	sourceTypeByID map[uuid.UUID]string,
) (*matchingEntities.MatchRun, []*matchingEntities.MatchGroup, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "command.matching.complete_empty_run")
	defer span.End()

	stats["matches"] = 0
	stats["unmatched_left"] = len(leftCandidates)
	stats["unmatched_right"] = len(rightCandidates)
	stats["unmatched_external"] = len(externalUnmatched)
	stats["auto_matched_left"] = 0
	stats["auto_matched_right"] = 0
	stats["pending_review_left"] = 0
	stats["pending_review_right"] = 0
	stats["proposed_left"] = 0
	stats["proposed_right"] = 0

	txByID := mergeTransactionMaps(
		indexTransactions(leftCandidates),
		indexTransactions(rightCandidates),
	)

	run, err := matchingEntities.NewMatchRun(ctx, in.ContextID, in.Mode)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create match run entity: %w", err)
	}

	var created *matchingEntities.MatchRun

	var updated *matchingEntities.MatchRun

	commitErr := uc.matchRunRepo.WithTx(ctx, func(tx repositories.Tx) error {
		persisted, err := uc.matchRunRepo.CreateWithTx(ctx, tx, run)
		if err != nil {
			return err
		}

		if persisted == nil {
			return ErrMatchRunPersistedNil
		}

		created = persisted

		if err := created.Complete(ctx, stats); err != nil {
			return fmt.Errorf("failed to complete match run: %w", err)
		}

		updatedRun, err := uc.matchRunRepo.UpdateWithTx(ctx, tx, created)
		if err != nil {
			return err
		}

		updated = updatedRun

		if in.Mode == matchingVO.MatchRunModeCommit {
			unmatchedIDs := collectUnmatched(leftCandidates, map[uuid.UUID]struct{}{})

			unmatchedIDs = append(
				unmatchedIDs,
				collectUnmatched(rightCandidates, map[uuid.UUID]struct{}{})...)
			if len(externalUnmatched) > 0 {
				unmatchedIDs = append(unmatchedIDs, externalUnmatched...)
			}

			exceptionInputs := buildExceptionInputs(unmatchedIDs, txByID, sourceTypeByID, nil)
			if err := uc.exceptionCreator.CreateExceptionsWithTx(ctx, tx, in.ContextID, created.ID, exceptionInputs, nil); err != nil {
				return err
			}
		}

		return nil
	})
	if commitErr != nil {
		libOpentelemetry.HandleSpanError(span, "failed to complete match run", commitErr)
		return nil, nil, commitErr
	}

	if updated == nil {
		return nil, nil, ErrMatchRunPersistedNil
	}

	return updated, []*matchingEntities.MatchGroup{}, nil
}

func buildExceptionInputs(
	txIDs []uuid.UUID,
	txByID map[uuid.UUID]*shared.Transaction,
	sourceTypeByID map[uuid.UUID]string,
	reasons map[uuid.UUID]string,
) []ports.ExceptionTransactionInput {
	if len(txIDs) == 0 {
		return nil
	}

	seen := make(map[uuid.UUID]bool, len(txIDs))
	inputs := make([]ports.ExceptionTransactionInput, 0, len(txIDs))

	for _, txID := range txIDs {
		if seen[txID] {
			continue
		}

		seen[txID] = true

		reason := ""
		if reasons != nil {
			reason = reasons[txID]
		}

		txn, ok := txByID[txID]
		if !ok || txn == nil {
			inputs = append(inputs, ports.ExceptionTransactionInput{
				TransactionID: txID,
				Reason:        reason,
			})

			continue
		}

		input := buildExceptionInputFromTx(txn, sourceTypeByID, reason)
		if input != nil {
			inputs = append(inputs, *input)
		}
	}

	return inputs
}

func buildExceptionInputFromTx(
	txn *shared.Transaction,
	sourceTypeByID map[uuid.UUID]string,
	reason string,
) *ports.ExceptionTransactionInput {
	if txn == nil {
		return nil
	}

	var amountAbsBase decimal.Decimal
	if txn.AmountBase != nil {
		amountAbsBase = txn.AmountBase.Abs()
	} else {
		amountAbsBase = txn.Amount.Abs()
	}

	sourceType := ""
	if sourceTypeByID != nil {
		sourceType = sourceTypeByID[txn.SourceID]
	}

	fxMissing := txn.AmountBase == nil && txn.BaseCurrency != nil

	return &ports.ExceptionTransactionInput{
		TransactionID:   txn.ID,
		AmountAbsBase:   amountAbsBase,
		TransactionDate: txn.Date,
		SourceType:      sourceType,
		FXMissing:       fxMissing,
		Reason:          reason,
	}
}

func (uc *UseCase) loadFeeRulesAndSchedules(
	ctx context.Context,
	contextID uuid.UUID,
) ([]*fee.FeeRule, []*fee.FeeRule, map[uuid.UUID]*fee.FeeSchedule, error) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Bool("fee_rules_configured", true))

	logger, _, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // only logger needed here

	rules, err := uc.feeRuleProvider.FindByContextID(ctx, contextID)
	if err != nil {
		span.SetAttributes(attribute.Bool("fee_rules_load_error", true))
		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to load fee rules")

		return nil, nil, nil, fmt.Errorf("loading fee rules: %w", err)
	}

	if len(rules) == 0 {
		span.SetAttributes(attribute.Bool("fee_rules_configured", false))

		return nil, nil, nil, nil
	}

	// Collect distinct schedule IDs from rules
	scheduleIDSet := make(map[uuid.UUID]struct{})

	for _, rule := range rules {
		if rule != nil {
			scheduleIDSet[rule.FeeScheduleID] = struct{}{}
		}
	}

	scheduleIDs := make([]uuid.UUID, 0, len(scheduleIDSet))
	for id := range scheduleIDSet {
		scheduleIDs = append(scheduleIDs, id)
	}

	if uc.feeScheduleRepo == nil || len(scheduleIDs) == 0 {
		span.SetAttributes(attribute.Bool("fee_rules_load_error", true))

		return nil, nil, nil, ErrNilFeeScheduleRepository
	}

	schedules, err := uc.feeScheduleRepo.GetByIDs(ctx, scheduleIDs)
	if err != nil {
		span.SetAttributes(attribute.Bool("fee_rules_load_error", true))
		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to load fee schedules")

		return nil, nil, nil, fmt.Errorf("loading fee schedules: %w", err)
	}

	if len(schedules) != len(scheduleIDs) {
		span.SetAttributes(attribute.Bool("fee_rules_load_error", true))

		missingCount := len(scheduleIDs) - len(schedules)
		logger.With(libLog.Any("missing_count", missingCount)).Log(ctx, libLog.LevelError, "fee rules reference missing fee schedules")

		return nil, nil, nil, fmt.Errorf(
			"loading fee schedules: %w: %d missing references",
			ErrFeeRulesReferenceMissingSchedules,
			missingCount,
		)
	}

	span.SetAttributes(
		attribute.Bool("fee_rules_configured", true),
		attribute.Int("fee_rules_count", len(rules)),
	)

	leftRules, rightRules := fee.SplitRulesBySide(rules)

	return leftRules, rightRules, schedules, nil
}

func buildSourceTypeMap(sources []*ports.SourceInfo) map[uuid.UUID]string {
	if len(sources) == 0 {
		return nil
	}

	sourceTypes := make(map[uuid.UUID]string, len(sources))

	for _, src := range sources {
		if src == nil {
			continue
		}

		sourceTypes[src.ID] = string(src.Type)
	}

	return sourceTypes
}

const (
	minManualMatchTransactions = 2
	manualMatchConfidence      = 100
)

// ManualMatchInput contains the input parameters for creating a manual match.
type ManualMatchInput struct {
	TenantID       uuid.UUID
	ContextID      uuid.UUID
	TransactionIDs []uuid.UUID
	Notes          string
}

// Sentinel errors for manual match operations.
var (
	ErrMinimumTransactionsRequired  = errors.New("at least two transactions are required")
	ErrDuplicateTransactionIDs      = errors.New("duplicate transaction IDs provided")
	ErrTransactionNotFound          = errors.New("one or more transactions not found")
	ErrTransactionNotUnmatched      = errors.New("one or more transactions are not unmatched")
	ErrManualMatchCreatingRun       = errors.New("failed to create manual match run")
	ErrManualMatchNoGroupCreated    = errors.New("no match group created")
	ErrManualMatchSourcesNotDiverse = errors.New("transactions must come from at least two different sources for reconciliation")
)

// ManualMatch creates a manual match group for the given transactions.
//
//nolint:gocognit,gocyclo,cyclop // transactional operation requires sequential steps
func (uc *UseCase) ManualMatch(
	ctx context.Context,
	in ManualMatchInput,
) (*matchingEntities.MatchGroup, error) {
	if err := uc.validateManualMatchInput(in); err != nil {
		return nil, err
	}

	if err := validateTenantFromContext(ctx, in.TenantID); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.matching.manual_match")
	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(
		span,
		"matcher",
		in,
		nil,
	)

	ctxInfo, err := uc.contextProvider.FindByID(ctx, in.TenantID, in.ContextID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find context", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find context")

		return nil, fmt.Errorf("find context: %w", ErrContextNotFound)
	}

	if ctxInfo == nil {
		return nil, ErrContextNotFound
	}

	if !ctxInfo.Active {
		return nil, ErrContextNotActive
	}

	transactions, err := uc.txRepo.FindByContextAndIDs(ctx, in.ContextID, in.TransactionIDs)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find transactions", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find transactions")

		return nil, fmt.Errorf("find transactions: %w", err)
	}

	if len(transactions) != len(in.TransactionIDs) {
		return nil, ErrTransactionNotFound
	}

	for _, txn := range transactions {
		if txn.Status != shared.TransactionStatusUnmatched {
			return nil, fmt.Errorf(
				"%w: transaction %s has status %s",
				ErrTransactionNotUnmatched,
				txn.ID,
				txn.Status,
			)
		}
	}

	// Validate source diversity: transactions must come from at least 2 different sources
	uniqueSources := make(map[uuid.UUID]struct{}, len(transactions))
	for _, txn := range transactions {
		uniqueSources[txn.SourceID] = struct{}{}
	}

	if len(uniqueSources) < minManualMatchTransactions {
		return nil, ErrManualMatchSourcesNotDiverse
	}

	run, err := matchingEntities.NewMatchRun(ctx, in.ContextID, matchingVO.MatchRunModeCommit)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create match run entity", err)

		return nil, fmt.Errorf("%w: %w", ErrManualMatchCreatingRun, err)
	}

	var createdGroup *matchingEntities.MatchGroup

	err = uc.txRepo.WithTx(ctx, func(tx repositories.Tx) error {
		createdRun, txErr := uc.matchRunRepo.CreateWithTx(ctx, tx, run)
		if txErr != nil {
			return fmt.Errorf("create match run: %w", txErr)
		}

		items := make([]*matchingEntities.MatchItem, 0, len(transactions))
		for _, txn := range transactions {
			amount := txn.Amount
			currency := txn.Currency

			if txn.AmountBase != nil && !txn.AmountBase.IsZero() {
				amount = *txn.AmountBase
			}

			if txn.BaseCurrency != nil && *txn.BaseCurrency != "" {
				currency = *txn.BaseCurrency
			}

			item, itemErr := matchingEntities.NewMatchItem(ctx, txn.ID, amount, currency, amount)
			if itemErr != nil {
				return fmt.Errorf("create match item for transaction %s: %w", txn.ID, itemErr)
			}

			items = append(items, item)
		}

		confidence, confErr := matchingVO.ParseConfidenceScore(manualMatchConfidence)
		if confErr != nil {
			return fmt.Errorf("parse confidence score: %w", confErr)
		}

		group, groupErr := matchingEntities.NewMatchGroup(
			ctx,
			in.ContextID,
			createdRun.ID,
			uuid.Nil,
			confidence,
			items,
		)
		if groupErr != nil {
			return fmt.Errorf("create match group: %w", groupErr)
		}

		if confirmErr := group.Confirm(ctx); confirmErr != nil {
			return fmt.Errorf("confirm match group: %w", confirmErr)
		}

		createdGroups, batchErr := uc.matchGroupRepo.CreateBatchWithTx(
			ctx,
			tx,
			[]*matchingEntities.MatchGroup{group},
		)
		if batchErr != nil {
			return fmt.Errorf("persist match group: %w", batchErr)
		}

		if len(createdGroups) == 0 || createdGroups[0] == nil {
			return ErrManualMatchNoGroupCreated
		}

		createdGroup = createdGroups[0]
		createdGroup.Items = items

		if len(items) > 0 {
			if _, itemsErr := uc.matchItemRepo.CreateBatchWithTx(ctx, tx, items); itemsErr != nil {
				return fmt.Errorf("persist match items: %w", itemsErr)
			}
		}

		if markErr := uc.txRepo.MarkMatchedWithTx(ctx, tx, in.ContextID, in.TransactionIDs); markErr != nil {
			return fmt.Errorf("mark transactions matched: %w", markErr)
		}

		stats := map[string]int{
			"matched_transactions": len(in.TransactionIDs),
			"match_groups":         1,
		}

		if completeErr := createdRun.Complete(ctx, stats); completeErr != nil {
			return fmt.Errorf("complete match run: %w", completeErr)
		}

		if _, updateErr := uc.matchRunRepo.UpdateWithTx(ctx, tx, createdRun); updateErr != nil {
			return fmt.Errorf("update match run: %w", updateErr)
		}

		return nil
	})
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create manual match", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to create manual match")

		return nil, fmt.Errorf("create manual match: %w", err)
	}

	logger.With(
		libLog.String("group_id", createdGroup.ID.String()),
		libLog.Any("transactions", len(in.TransactionIDs)),
	).Log(ctx, libLog.LevelInfo, "manual match created")

	return createdGroup, nil
}

func (uc *UseCase) validateManualMatchInput(in ManualMatchInput) error {
	if in.TenantID == uuid.Nil {
		return ErrTenantIDRequired
	}

	if in.ContextID == uuid.Nil {
		return ErrRunMatchContextIDRequired
	}

	if len(in.TransactionIDs) < minManualMatchTransactions {
		return ErrMinimumTransactionsRequired
	}

	seen := make(map[uuid.UUID]bool, len(in.TransactionIDs))
	for _, id := range in.TransactionIDs {
		if seen[id] {
			return ErrDuplicateTransactionIDs
		}

		seen[id] = true
	}

	return nil
}

// UnmatchInput contains the parameters for breaking a match group.
type UnmatchInput struct {
	TenantID     uuid.UUID
	ContextID    uuid.UUID
	MatchGroupID uuid.UUID
	Reason       string
}

// Sentinel errors for unmatch operations.
var (
	ErrUnmatchContextIDRequired    = errors.New("context id is required")
	ErrUnmatchMatchGroupIDRequired = errors.New("match group id is required")
	ErrUnmatchReasonRequired       = errors.New("reason is required")
	ErrMatchGroupNotFound          = errors.New("match group not found")
)

// Unmatch breaks an existing match group, reverting transaction statuses to UNMATCHED.
// If the group was CONFIRMED, it uses Revoke() and enqueues a compensating event
// so downstream systems can undo any actions taken upon the original confirmation.
func (uc *UseCase) Unmatch(ctx context.Context, input UnmatchInput) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "command.unmatch")

	defer span.End()

	if err := validateUnmatchInput(input); err != nil {
		return err
	}

	if err := validateTenantFromContextStrict(ctx, input.TenantID); err != nil {
		return err
	}

	group, err := uc.loadMatchGroup(ctx, logger, input.ContextID, input.MatchGroupID)
	if err != nil {
		return err
	}

	wasConfirmed := group.Status == matchingVO.MatchGroupStatusConfirmed

	if err := uc.txRepo.WithTx(ctx, func(tx repositories.Tx) error {
		if txErr := uc.rejectOrRevokeGroup(ctx, logger, tx, group, input.Reason, wasConfirmed); txErr != nil {
			return txErr
		}

		if txErr := uc.revertTransactionStatuses(ctx, logger, tx, input.ContextID, input.MatchGroupID); txErr != nil {
			return txErr
		}

		if wasConfirmed {
			if txErr := uc.enqueueUnmatchEvent(ctx, tx, group, input.Reason); txErr != nil {
				return txErr
			}
		}

		return nil
	}); err != nil {
		return err
	}

	logger.With(
		libLog.String("group_id", input.MatchGroupID.String()),
		libLog.String("reason", input.Reason),
	).Log(ctx, libLog.LevelInfo, "successfully unmatched group")

	return nil
}

func validateUnmatchInput(input UnmatchInput) error {
	if input.TenantID == uuid.Nil {
		return ErrTenantIDRequired
	}

	if input.ContextID == uuid.Nil {
		return ErrUnmatchContextIDRequired
	}

	if input.MatchGroupID == uuid.Nil {
		return ErrUnmatchMatchGroupIDRequired
	}

	if input.Reason == "" {
		return ErrUnmatchReasonRequired
	}

	return nil
}

func validateTenantFromContext(ctx context.Context, tenantID uuid.UUID) error {
	if tenantID == uuid.Nil {
		return ErrTenantIDRequired
	}

	rawTenantID, ok := ctx.Value(auth.TenantIDKey).(string)
	if !ok {
		return nil
	}

	ctxTenantID := strings.TrimSpace(rawTenantID)
	if ctxTenantID == "" {
		return nil
	}

	ctxTenantUUID, err := uuid.Parse(ctxTenantID)
	if err != nil {
		return ErrTenantIDRequired
	}

	if tenantID != ctxTenantUUID {
		return ErrTenantIDMismatch
	}

	return nil
}

func validateTenantFromContextStrict(ctx context.Context, tenantID uuid.UUID) error {
	if tenantID == uuid.Nil {
		return ErrTenantIDRequired
	}

	ctxTenantID := strings.TrimSpace(auth.GetTenantID(ctx))
	if ctxTenantID == "" {
		ctxTenantID = strings.TrimSpace(auth.DefaultTenantID)
	}

	if ctxTenantID == "" {
		return ErrTenantIDRequired
	}

	ctxTenantUUID, err := uuid.Parse(ctxTenantID)
	if err != nil {
		return ErrTenantIDRequired
	}

	if tenantID != ctxTenantUUID {
		return ErrTenantIDMismatch
	}

	return nil
}

func (uc *UseCase) loadMatchGroup(
	ctx context.Context,
	logger libLog.Logger,
	contextID, matchGroupID uuid.UUID,
) (*matchingEntities.MatchGroup, error) {
	group, err := uc.matchGroupRepo.FindByID(ctx, contextID, matchGroupID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrMatchGroupNotFound
		}

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to find match group")

		return nil, fmt.Errorf("find match group: %w", err)
	}

	if group == nil {
		return nil, ErrMatchGroupNotFound
	}

	return group, nil
}

func (uc *UseCase) rejectOrRevokeGroup(
	ctx context.Context,
	logger libLog.Logger,
	tx repositories.Tx,
	group *matchingEntities.MatchGroup,
	reason string,
	wasConfirmed bool,
) error {
	var err error

	action := "reject"
	if wasConfirmed {
		action = "revoke"
		err = group.Revoke(ctx, reason)
	} else {
		err = group.Reject(ctx, reason)
	}

	if err != nil {
		logger.With(libLog.String("action", action), libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to update match group status")

		return fmt.Errorf("%s match group: %w", action, err)
	}

	if _, err := uc.matchGroupRepo.UpdateWithTx(ctx, tx, group); err != nil {
		logger.With(libLog.String("action", action), libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to update match group after status change")

		return fmt.Errorf("update match group: %w", err)
	}

	return nil
}

func (uc *UseCase) enqueueUnmatchEvent(
	ctx context.Context,
	tx repositories.Tx,
	group *matchingEntities.MatchGroup,
	reason string,
) error {
	if uc.outboxRepoTx == nil {
		return ErrOutboxRepoNotConfigured
	}

	sqlTx, ok := tx.(*sql.Tx)
	if !ok || sqlTx == nil {
		return ErrOutboxRequiresSQLTx
	}

	// FindByID does not eager-load Items. Load them explicitly so the
	// event constructor receives the transaction IDs it needs.
	if len(group.Items) == 0 {
		items, err := uc.matchItemRepo.ListByMatchGroupID(ctx, group.ID)
		if err != nil {
			return fmt.Errorf("load match items for unmatch event: %w", err)
		}

		group.Items = items
	}

	tenantIDStr := auth.GetTenantID(ctx)
	if tenantIDStr == "" {
		tenantIDStr = auth.DefaultTenantID
	}

	tenantUUID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return fmt.Errorf("parse tenant id: %w", err)
	}

	tenantSlug := auth.GetTenantSlug(ctx)

	event, err := matchingEntities.NewMatchUnmatchedEvent(
		ctx,
		tenantUUID,
		tenantSlug,
		group,
		reason,
		time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("build match unmatched event: %w", err)
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal match unmatched event: %w", err)
	}

	outboxEvent, err := shared.NewOutboxEvent(ctx, event.EventType, event.ID(), body)
	if err != nil {
		return fmt.Errorf("create outbox event: %w", err)
	}

	if _, err := uc.outboxRepoTx.CreateWithTx(ctx, sqlTx, outboxEvent); err != nil {
		return fmt.Errorf("create outbox entry: %w", err)
	}

	return nil
}

func (uc *UseCase) revertTransactionStatuses(
	ctx context.Context,
	logger libLog.Logger,
	tx repositories.Tx,
	contextID, matchGroupID uuid.UUID,
) error {
	items, err := uc.matchItemRepo.ListByMatchGroupID(ctx, matchGroupID)
	if err != nil {
		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to list match items")

		return fmt.Errorf("list match items: %w", err)
	}

	if len(items) == 0 {
		return nil
	}

	transactionIDs := make([]uuid.UUID, 0, len(items))
	for _, item := range items {
		transactionIDs = append(transactionIDs, item.TransactionID)
	}

	if err := uc.txRepo.MarkUnmatchedWithTx(ctx, tx, contextID, transactionIDs); err != nil {
		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to mark transactions unmatched")

		return fmt.Errorf("mark transactions unmatched: %w", err)
	}

	return nil
}
