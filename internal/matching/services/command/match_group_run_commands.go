package command

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

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

	var (
		refreshFailed atomic.Bool
		commitStarted atomic.Bool
	)

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
			mrc.externalTxByID,
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
			ctx,
			uc,
			createdRun,
			fmt.Errorf("%w: before rule execution: %w", ErrContextCancelled, ctx.Err()),
		)
	}

	execResult, err := uc.executeMatchRules(ctx, span, logger, mrc, createdRun)
	if err != nil {
		return nil, nil, finalizeRunFailure(ctx, uc, createdRun, err)
	}

	if ctx.Err() != nil {
		return nil, nil, finalizeRunFailure(
			ctx,
			uc,
			createdRun,
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

	if feeNorm != fee.NormalizationModeNone {
		leftRules, rightRules, allSchedules, err = uc.loadFeeRulesAndSchedules(ctx, in.ContextID)
		if err != nil {
			return nil, err
		}

		if len(leftRules) == 0 && len(rightRules) == 0 {
			return nil, ErrFeeRulesRequiredForNormalization
		}
	}

	leftCandidates, rightCandidates, unmatchedIDs, externalTxByID, err := uc.loadAndClassifyCandidates(
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
		externalTxByID:  externalTxByID,
		stats:           stats,
		leftRules:       leftRules,
		rightRules:      rightRules,
		allSchedules:    allSchedules,
	}, nil
}

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

func (uc *UseCase) loadAndClassifyCandidates(
	ctx context.Context,
	span trace.Span,
	logger libLog.Logger,
	in RunMatchInput,
	leftSourceIDs, rightSourceIDs map[uuid.UUID]struct{},
) ([]*shared.Transaction, []*shared.Transaction, []uuid.UUID, map[uuid.UUID]*shared.Transaction, error) {
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
		return nil, nil, nil, nil, fmt.Errorf("failed to load candidate transactions: %w", err)
	}

	leftCandidates := make([]*shared.Transaction, 0, len(candidates))
	rightCandidates := make([]*shared.Transaction, 0, len(candidates))
	unmatchedIDs := make([]uuid.UUID, 0, len(candidates))
	externalTxByID := make(map[uuid.UUID]*shared.Transaction)

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
		externalTxByID[tx.ID] = tx
	}

	return leftCandidates, rightCandidates, unmatchedIDs, externalTxByID, nil
}
