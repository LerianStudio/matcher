//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"time"

	clientpkg "github.com/LerianStudio/matcher/tests/client"
)

// PollOptions configures polling behavior.
type PollOptions struct {
	Interval time.Duration
	Timeout  time.Duration
}

// DefaultPollOptions returns default polling options from config.
func DefaultPollOptions(cfg *E2EConfig) PollOptions {
	return PollOptions{
		Interval: cfg.PollInterval,
		Timeout:  cfg.PollTimeout,
	}
}

// Eventually polls until the condition returns true or timeout expires.
func Eventually(ctx context.Context, opts PollOptions, condition func() (bool, error)) error {
	deadline := time.Now().Add(opts.Timeout)
	ticker := time.NewTicker(opts.Interval)
	defer ticker.Stop()

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("context cancelled: last error: %w", lastErr)
			}
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				if lastErr != nil {
					return fmt.Errorf("timeout after %v: last error: %w", opts.Timeout, lastErr)
				}
				return fmt.Errorf("timeout after %v", opts.Timeout)
			}

			ok, err := condition()
			if err != nil {
				lastErr = err
				continue
			}
			if ok {
				return nil
			}
		}
	}
}

// EventuallyWithResult polls until the condition returns a non-nil result or timeout expires.
func EventuallyWithResult[T any](
	ctx context.Context,
	opts PollOptions,
	fetch func() (*T, error),
) (*T, error) {
	deadline := time.Now().Add(opts.Timeout)
	ticker := time.NewTicker(opts.Interval)
	defer ticker.Stop()

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, fmt.Errorf("context cancelled: last error: %w", lastErr)
			}
			return nil, ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				if lastErr != nil {
					return nil, fmt.Errorf(
						"timeout after %v: last error: %w",
						opts.Timeout,
						lastErr,
					)
				}
				return nil, fmt.Errorf("timeout after %v", opts.Timeout)
			}

			result, err := fetch()
			if err != nil {
				lastErr = err
				continue
			}
			if result != nil {
				return result, nil
			}
		}
	}
}

// WaitForJobComplete polls until a job reaches completed or failed status.
func WaitForJobComplete(
	ctx context.Context,
	tc *TestContext,
	client *Client,
	contextID, jobID string,
) error {
	opts := DefaultPollOptions(tc.Config())
	return Eventually(ctx, opts, func() (bool, error) {
		job, err := client.Ingestion.GetJob(ctx, contextID, jobID)
		if err != nil {
			return false, err
		}
		switch job.Status {
		case "COMPLETED":
			return true, nil
		case "FAILED":
			return false, fmt.Errorf("job failed: %s", job.ID)
		default:
			return false, nil
		}
	})
}

// WaitForMatchRunComplete polls until a match run reaches completed status.
func WaitForMatchRunComplete(
	ctx context.Context,
	tc *TestContext,
	client *Client,
	contextID, runID string,
) error {
	opts := DefaultPollOptions(tc.Config())
	return Eventually(ctx, opts, func() (bool, error) {
		run, err := client.Matching.GetMatchRun(ctx, contextID, runID)
		if err != nil {
			return false, err
		}
		switch run.Status {
		case "COMPLETED":
			return true, nil
		case "FAILED":
			return false, fmt.Errorf("match run failed: %s", run.ID)
		default:
			return false, nil
		}
	})
}

// WaitForExportJobComplete polls until an export job reaches completed status.
func WaitForExportJobComplete(
	ctx context.Context,
	tc *TestContext,
	client *Client,
	jobID string,
) error {
	opts := DefaultPollOptions(tc.Config())
	return Eventually(ctx, opts, func() (bool, error) {
		job, err := client.Reporting.GetExportJob(ctx, jobID)
		if err != nil {
			return false, err
		}
		switch job.Status {
		case "SUCCEEDED":
			return true, nil
		case "FAILED":
			return false, fmt.Errorf("export job failed: %s", job.ID)
		case "CANCELED":
			return false, fmt.Errorf("export job cancelled: %s", job.ID)
		default:
			return false, nil
		}
	})
}

// WaitForAuditLogs polls until audit logs appear for the given entity.
// This accounts for the async outbox dispatch delay.
func WaitForAuditLogs(
	ctx context.Context,
	tc *TestContext,
	client *Client,
	entityType, entityID string,
	minCount int,
) ([]clientpkg.AuditLog, error) {
	opts := DefaultPollOptions(tc.Config())
	var result []clientpkg.AuditLog
	err := Eventually(ctx, opts, func() (bool, error) {
		logs, err := client.Governance.ListAuditLogsByEntity(ctx, entityType, entityID)
		if err != nil {
			return false, err
		}
		result = logs
		return len(logs) >= minCount, nil
	})
	return result, err
}

// WaitForExceptionHistory polls until history entries appear for the given exception.
// This accounts for the async outbox dispatch delay.
func WaitForExceptionHistory(
	ctx context.Context,
	tc *TestContext,
	client *Client,
	exceptionID string,
	minCount int,
) (*clientpkg.ListResponse[clientpkg.ExceptionHistory], error) {
	opts := DefaultPollOptions(tc.Config())
	var result *clientpkg.ListResponse[clientpkg.ExceptionHistory]
	err := Eventually(ctx, opts, func() (bool, error) {
		history, err := client.Exception.GetExceptionHistory(ctx, exceptionID, "", 100)
		if err != nil {
			return false, err
		}
		result = history
		return len(history.Items) >= minCount, nil
	})
	return result, err
}
