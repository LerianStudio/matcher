package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

const defaultWorkerStartWaitTimeout = 30 * time.Second

var (
	errWorkerStartContextCanceled = errors.New("worker start canceled")
	errWorkerPanicked             = errors.New("worker panicked")
	errWorkerStartFuncNil         = errors.New("worker start function is nil")
	errWorkerStartTimeout         = errors.New("worker start timed out")
)

type workerStartEntry struct {
	name          string
	start         func(context.Context) error
	stop          func() error
	critical      bool
	onSoftFailure func()
}

type workerStartResult struct {
	name     string
	err      error
	critical bool
}

func startWorkerEntries(
	ctx context.Context,
	logger libLog.Logger,
	entries []workerStartEntry,
) []workerStartResult {
	results := make(chan workerStartResult, len(entries))

	for _, entry := range entries {
		runtime.SafeGoWithContextAndComponent(
			ctx,
			logger,
			constants.ApplicationName,
			"worker.start."+entry.name,
			runtime.KeepRunning,
			func(workerCtx context.Context) {
				result := workerStartResult{
					name:     entry.name,
					critical: entry.critical,
				}

				defer func() {
					if recovered := recover(); recovered != nil {
						result.err = fmt.Errorf("panic starting %s worker (%v): %w", entry.name, recovered, errWorkerPanicked)
					}

					results <- result
				}()

				if entry.start == nil {
					result.err = errWorkerStartFuncNil

					return
				}

				result.err = entry.start(workerCtx)
			},
		)
	}

	timeout := resolveWorkerStartWaitTimeout(ctx)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	collected := make([]workerStartResult, 0, len(entries))
	for len(collected) < len(entries) {
		select {
		case result := <-results:
			collected = append(collected, result)
		case <-timer.C:
			return appendMissingWorkerResults(
				entries,
				collected,
				fmt.Errorf("worker start timed out after %v: %w", timeout, errWorkerStartTimeout),
			)
		case <-ctx.Done():
			return appendMissingWorkerResults(
				entries,
				collected,
				fmt.Errorf("%w: %w", errWorkerStartContextCanceled, ctx.Err()),
			)
		}
	}

	return collected
}

func resolveWorkerStartWaitTimeout(ctx context.Context) time.Duration {
	timeout := defaultWorkerStartWaitTimeout

	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}

	if timeout <= 0 {
		return time.Millisecond
	}

	return timeout
}

func appendMissingWorkerResults(
	entries []workerStartEntry,
	collected []workerStartResult,
	err error,
) []workerStartResult {
	if len(collected) >= len(entries) {
		return collected
	}

	received := make(map[string]struct{}, len(collected))
	for _, result := range collected {
		received[result.name] = struct{}{}
	}

	for _, entry := range entries {
		if _, ok := received[entry.name]; ok {
			continue
		}

		collected = append(collected, workerStartResult{
			name:     entry.name,
			critical: entry.critical,
			err:      err,
		})
	}

	return collected
}

func collectStartedWorkers(collected []workerStartResult) map[string]struct{} {
	startedWorkers := make(map[string]struct{}, len(collected))
	for _, result := range collected {
		if result.err == nil {
			startedWorkers[result.name] = struct{}{}
		}
	}

	return startedWorkers
}

func collectCriticalWorkerFailures(collected []workerStartResult) []workerStartResult {
	failures := make([]workerStartResult, 0)

	for _, result := range collected {
		if result.critical && result.err != nil {
			failures = append(failures, result)
		}
	}

	return failures
}
