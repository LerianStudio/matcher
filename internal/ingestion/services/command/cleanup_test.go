// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
)

func TestConvertParseErrors(t *testing.T) {
	t.Parallel()

	t.Run("nil errors returns nil", func(t *testing.T) {
		t.Parallel()
		result := convertParseErrors(nil)
		require.Nil(t, result)
	})

	t.Run("empty errors returns nil", func(t *testing.T) {
		t.Parallel()
		result := convertParseErrors([]ports.ParseError{})
		require.Nil(t, result)
	})

	t.Run("converts parse errors correctly", func(t *testing.T) {
		t.Parallel()
		errs := []ports.ParseError{
			{Row: 1, Field: "amount", Message: "invalid format"},
			{Row: 5, Field: "date", Message: "cannot parse"},
		}
		result := convertParseErrors(errs)
		require.Len(t, result, 2)
		require.Equal(t, 1, result[0].Row)
		require.Equal(t, "amount", result[0].Field)
		require.Equal(t, "invalid format", result[0].Message)
		require.Equal(t, 5, result[1].Row)
		require.Equal(t, "date", result[1].Field)
	})

	t.Run("limits to maxErrorDetails", func(t *testing.T) {
		t.Parallel()
		errs := make([]ports.ParseError, 100)
		for i := range errs {
			errs[i] = ports.ParseError{Row: i + 1, Field: "field", Message: "error"}
		}
		result := convertParseErrors(errs)
		require.Len(t, result, maxErrorDetails)
		require.Equal(t, 1, result[0].Row)
		require.Equal(t, maxErrorDetails, result[maxErrorDetails-1].Row)
	})
}

type captureLogger struct {
	mu       sync.Mutex
	messages []string
}

func (logger *captureLogger) Log(_ context.Context, _ libLog.Level, msg string, _ ...libLog.Field) {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	logger.messages = append(logger.messages, msg)
}

//nolint:ireturn
func (logger *captureLogger) With(_ ...libLog.Field) libLog.Logger {
	return logger
}

//nolint:ireturn
func (logger *captureLogger) WithGroup(_ string) libLog.Logger {
	return logger
}

func (*captureLogger) Enabled(_ libLog.Level) bool {
	return true
}

func (*captureLogger) Sync(_ context.Context) error {
	return nil
}

func (logger *captureLogger) joinedMessages() string {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	return strings.Join(logger.messages, "\n")
}

type fakeCleanupTxRepo struct {
	calls int
	err   error
}

func (repo *fakeCleanupTxRepo) CleanupFailedJobTransactionsWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ uuid.UUID,
) error {
	repo.calls++

	return repo.err
}

func TestCleanupPartialTransactionsBestEffort_CallsCleanupRepository(t *testing.T) {
	t.Parallel()

	runner := &sequenceTxRunner{}
	cleanupRepo := &fakeCleanupTxRepo{}

	uc := &UseCase{
		jobTxRunner:     runner,
		txCleanupRepoTx: cleanupRepo,
	}

	ctx := libCommons.ContextWithLogger(context.Background(), &captureLogger{})
	uc.cleanupPartialTransactionsBestEffort(ctx, uuid.New())

	require.Equal(t, 1, runner.calls)
	require.Equal(t, 1, cleanupRepo.calls)
}

func TestCleanupPartialTransactionsBestEffort_LogsWarningOnCleanupError(t *testing.T) {
	t.Parallel()

	runner := &sequenceTxRunner{}
	cleanupRepo := &fakeCleanupTxRepo{err: errors.New("cleanup failed")}
	logger := &captureLogger{}

	uc := &UseCase{
		jobTxRunner:     runner,
		txCleanupRepoTx: cleanupRepo,
	}

	ctx := libCommons.ContextWithLogger(context.Background(), logger)
	uc.cleanupPartialTransactionsBestEffort(ctx, uuid.New())

	require.Equal(t, 1, runner.calls)
	require.Equal(t, 1, cleanupRepo.calls)
	require.Contains(t, logger.joinedMessages(), "failed to execute best-effort partial transaction cleanup")
}

func TestCleanupPartialTransactionsBestEffort_NoOpWithoutCleanupRepository(t *testing.T) {
	t.Parallel()

	runner := &sequenceTxRunner{}

	uc := &UseCase{
		jobTxRunner: runner,
	}

	ctx := libCommons.ContextWithLogger(context.Background(), &captureLogger{})
	uc.cleanupPartialTransactionsBestEffort(ctx, uuid.New())

	require.Equal(t, 0, runner.calls)
}

type sequenceTxRunner struct {
	calls     int
	errOnCall map[int]error
}

func (runner *sequenceTxRunner) WithTx(_ context.Context, fn func(*sql.Tx) error) error {
	runner.calls++

	if err, exists := runner.errOnCall[runner.calls]; exists {
		return err
	}

	return fn(&sql.Tx{})
}

type cancelAwareTxRunner struct {
	calls int
}

func (runner *cancelAwareTxRunner) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	runner.calls++

	if ctx.Err() != nil {
		return ctx.Err()
	}

	return fn(&sql.Tx{})
}

func TestFailJob_CleanupRunsBestEffortOutsidePrimaryTransaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 1)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))

	runner := &sequenceTxRunner{
		errOnCall: map[int]error{2: errors.New("cleanup tx failed")},
	}

	uc := &UseCase{
		dedupe:      fakeDedupe{},
		jobTxRunner: runner,
		jobRepoTx: &fakeJobRepo{
			updated: job,
		},
		txCleanupRepoTx: &fakeCleanupTxRepo{},
		outboxRepoTx:    &fakeOutboxRepo{},
	}

	cause := errors.New("ingestion parse failed")
	err = uc.failJob(ctx, job, cause, nil)

	require.ErrorIs(t, err, cause)
	require.Equal(t, 2, runner.calls, "expected one tx for fail persistence and one best-effort cleanup tx")
}

func TestFailJob_UsesDetachedContextForPersistenceAndCleanup(t *testing.T) {
	t.Parallel()

	parentCtx, cancel := context.WithCancel(context.Background())
	cancel()

	contextID := uuid.New()
	sourceID := uuid.New()

	job, err := entities.NewIngestionJob(context.Background(), contextID, sourceID, "file.csv", 1)
	require.NoError(t, err)
	require.NoError(t, job.Start(context.Background()))

	runner := &cancelAwareTxRunner{}
	cleanupRepo := &fakeCleanupTxRepo{}

	uc := &UseCase{
		dedupe:          fakeDedupe{},
		jobTxRunner:     runner,
		jobRepoTx:       &fakeJobRepo{updated: job},
		txCleanupRepoTx: cleanupRepo,
		outboxRepoTx:    &fakeOutboxRepo{},
	}

	cause := errors.New("ingestion parse failed")
	err = uc.failJob(parentCtx, job, cause, nil)

	require.ErrorIs(t, err, cause)
	require.Equal(t, 2, runner.calls, "expected fail persistence tx plus best-effort cleanup tx")
	require.Equal(t, 1, cleanupRepo.calls)
}
