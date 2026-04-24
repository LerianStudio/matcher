// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

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
	if err := uc.validateCoreRunMatchDependencies(); err != nil {
		return err
	}

	return uc.validateFeeRunMatchDependencies()
}

func (uc *UseCase) validateCoreRunMatchDependencies() error {
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

func (uc *UseCase) validateFeeRunMatchDependencies() error {
	if uc.feeVarianceRepo == nil {
		return ErrNilFeeVarianceRepository
	}

	if uc.feeRuleProvider == nil {
		return ErrNilFeeRuleProvider
	}

	if uc.feeScheduleRepo == nil {
		return ErrNilFeeScheduleRepository
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
	if refreshFailed == nil || lock == nil {
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
							libOpentelemetry.HandleSpanError(span, "failed to refresh transaction lock", err)
						}

						logger.With(libLog.Err(err)).Log(ctx, libLog.LevelError, "failed to refresh transaction lock")

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

					logger.With(libLog.Err(refreshErr)).Log(ctx, libLog.LevelError, "lock refresh failed")

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

		logger.With(libLog.Err(releaseErr)).Log(ctx, libLog.LevelError, "failed to release transaction lock")
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
		logger.With(libLog.Err(updateErr)).Log(ctx, libLog.LevelError, "failed to update match run after error")

		return fmt.Errorf("updating match run failed: %w; original cause: %w", updateErr, cause)
	}

	return cause
}
