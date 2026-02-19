// Package query provides query use cases for the ingestion service.
package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	ingestionRepositories "github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Query use case errors.
var (
	ErrNilJobRepository         = errors.New("job repository is required")
	ErrNilTransactionRepository = errors.New("transaction repository is required")
	ErrJobNotFound              = errors.New("job not found")
	ErrTransactionNotFound      = errors.New("transaction not found")
	ErrNilUseCase               = errors.New("ingestion query use case is required")
)

// UseCase implements ingestion query operations.
type UseCase struct {
	jobRepo         ingestionRepositories.JobRepository
	transactionRepo ingestionRepositories.TransactionRepository
}

// NewUseCase creates a new query use case with required dependencies.
func NewUseCase(
	jobRepo ingestionRepositories.JobRepository,
	txRepo ingestionRepositories.TransactionRepository,
) (*UseCase, error) {
	if jobRepo == nil {
		return nil, ErrNilJobRepository
	}

	if txRepo == nil {
		return nil, ErrNilTransactionRepository
	}

	return &UseCase{jobRepo: jobRepo, transactionRepo: txRepo}, nil
}

// GetJob retrieves a job by ID without context-scoping.
// TODO(audit): discuss and wire if needed — unscoped variant exists alongside context-scoped GetJobByContext.
// Currently only exercised by unit tests; no HTTP route exposes this method.
func (uc *UseCase) GetJob(ctx context.Context, jobID uuid.UUID) (*entities.IngestionJob, error) {
	if uc == nil {
		return nil, ErrNilUseCase
	}

	//nolint:dogsled // only tracer needed for span management
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.ingestion.get_job")
	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(span, "query", struct {
		JobID string `json:"jobId"`
	}{JobID: jobID.String()}, nil)

	job, err := uc.jobRepo.FindByID(ctx, jobID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find job", err)

		return nil, fmt.Errorf("finding job: %w", err)
	}

	if job == nil {
		return nil, ErrJobNotFound
	}

	return job, nil
}

// GetJobByContext retrieves a job by ID within a context.
func (uc *UseCase) GetJobByContext(
	ctx context.Context,
	contextID, jobID uuid.UUID,
) (*entities.IngestionJob, error) {
	if uc == nil {
		return nil, ErrNilUseCase
	}

	//nolint:dogsled // only tracer needed for span management
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.ingestion.get_job_by_context")
	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(span, "query", struct {
		ContextID string `json:"contextId"`
		JobID     string `json:"jobId"`
	}{ContextID: contextID.String(), JobID: jobID.String()}, nil)

	job, err := uc.jobRepo.FindByID(ctx, jobID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find job", err)

		return nil, fmt.Errorf("finding job: %w", err)
	}

	if job == nil {
		return nil, ErrJobNotFound
	}

	if job.ContextID != contextID {
		return nil, ErrJobNotFound
	}

	return job, nil
}

// ListJobsByContext retrieves jobs for a context with pagination.
func (uc *UseCase) ListJobsByContext(
	ctx context.Context,
	contextID uuid.UUID,
	filter ingestionRepositories.CursorFilter,
) ([]*entities.IngestionJob, libHTTP.CursorPagination, error) {
	if uc == nil {
		return nil, libHTTP.CursorPagination{}, ErrNilUseCase
	}

	//nolint:dogsled // only tracer needed for span management
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.ingestion.list_jobs_by_context")
	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(span, "query", struct {
		ContextID string `json:"contextId"`
	}{ContextID: contextID.String()}, nil)

	jobs, pagination, err := uc.jobRepo.FindByContextID(ctx, contextID, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find jobs by context", err)

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("finding jobs by context: %w", err)
	}

	return jobs, pagination, nil
}

// GetTransaction retrieves a transaction by ID without context-scoping.
// TODO(audit): discuss and wire if needed — standalone orphan with no context-scoped variant.
// Currently only exercised by unit tests; no HTTP route exposes this method.
func (uc *UseCase) GetTransaction(
	ctx context.Context,
	transactionID uuid.UUID,
) (*shared.Transaction, error) {
	if uc == nil {
		return nil, ErrNilUseCase
	}

	//nolint:dogsled // only tracer needed for span management
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.ingestion.get_transaction")
	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(span, "query", struct {
		TransactionID string `json:"transactionId"`
	}{TransactionID: transactionID.String()}, nil)

	tx, err := uc.transactionRepo.FindByID(ctx, transactionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find transaction", err)

		return nil, fmt.Errorf("finding transaction: %w", err)
	}

	if tx == nil {
		return nil, ErrTransactionNotFound
	}

	return tx, nil
}

// ListTransactionsByJob retrieves transactions for a job with pagination without context-scoping.
// TODO(audit): discuss and wire if needed — unscoped variant exists alongside context-scoped ListTransactionsByJobContext.
// Currently only exercised by unit tests; no HTTP route exposes this method.
func (uc *UseCase) ListTransactionsByJob(
	ctx context.Context,
	jobID uuid.UUID,
	filter ingestionRepositories.CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	if uc == nil {
		return nil, libHTTP.CursorPagination{}, ErrNilUseCase
	}

	//nolint:dogsled // only tracer needed for span management
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.ingestion.list_transactions_by_job")
	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(span, "query", struct {
		JobID string `json:"jobId"`
	}{JobID: jobID.String()}, nil)

	txs, pagination, err := uc.transactionRepo.FindByJobID(ctx, jobID, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find transactions by job", err)

		return nil, libHTTP.CursorPagination{}, fmt.Errorf("finding transactions by job: %w", err)
	}

	return txs, pagination, nil
}

// ListTransactionsByJobContext retrieves transactions for a job within a context.
func (uc *UseCase) ListTransactionsByJobContext(
	ctx context.Context,
	jobID, contextID uuid.UUID,
	filter ingestionRepositories.CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	if uc == nil {
		return nil, libHTTP.CursorPagination{}, ErrNilUseCase
	}

	//nolint:dogsled // only tracer needed for span management
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.ingestion.list_transactions_by_job_context")
	defer span.End()

	_ = libOpentelemetry.SetSpanAttributesFromValue(span, "query", struct {
		JobID     string `json:"jobId"`
		ContextID string `json:"contextId"`
	}{JobID: jobID.String(), ContextID: contextID.String()}, nil)

	txs, pagination, err := uc.transactionRepo.FindByJobAndContextID(ctx, jobID, contextID, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(
			span,
			"failed to find transactions by job and context",
			err,
		)

		return nil, libHTTP.CursorPagination{}, fmt.Errorf(
			"finding transactions by job and context: %w",
			err,
		)
	}

	return txs, pagination, nil
}
