package dto

import (
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v4/commons/pointers"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// JobToResponse converts an IngestionJob entity to a JobResponse DTO.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func JobToResponse(job *entities.IngestionJob) JobResponse {
	if job == nil {
		return JobResponse{}
	}

	var startedAt *string

	if !job.StartedAt.IsZero() {
		startedAt = pointers.String(job.StartedAt.Format(time.RFC3339))
	}

	var completedAt *string

	if job.CompletedAt != nil {
		completedAt = pointers.String(job.CompletedAt.Format(time.RFC3339))
	}

	return JobResponse{
		ID:          job.ID.String(),
		ContextID:   job.ContextID.String(),
		SourceID:    job.SourceID.String(),
		Status:      job.Status.String(),
		FileName:    job.Metadata.FileName,
		TotalRows:   job.Metadata.TotalRows,
		FailedRows:  job.Metadata.FailedRows,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		CreatedAt:   job.CreatedAt.Format(time.RFC3339),
	}
}

// JobsToResponse converts a slice of entities to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func JobsToResponse(jobs []*entities.IngestionJob) []JobResponse {
	result := make([]JobResponse, 0, len(jobs))

	for _, job := range jobs {
		if job != nil {
			result = append(result, JobToResponse(job))
		}
	}

	return result
}

// TransactionToResponse converts a Transaction entity to a TransactionResponse DTO.
// The jobID and contextID are passed explicitly since they come from the request context.
// Returns an empty struct for nil input to ensure consistent JSON structure.
func TransactionToResponse(tx *shared.Transaction, jobID, contextID uuid.UUID) TransactionResponse {
	if tx == nil {
		return TransactionResponse{}
	}

	return TransactionResponse{
		ID:               tx.ID.String(),
		JobID:            jobID.String(),
		SourceID:         tx.SourceID.String(),
		ContextID:        contextID.String(),
		ExternalID:       tx.ExternalID,
		Amount:           tx.Amount.String(),
		Currency:         tx.Currency,
		Date:             tx.Date.Format(time.RFC3339),
		Description:      tx.Description,
		Status:           tx.Status.String(),
		ExtractionStatus: tx.ExtractionStatus.String(),
		CreatedAt:        tx.CreatedAt.Format(time.RFC3339),
	}
}

// TransactionsToResponse converts a slice of entities to response DTOs.
// Always returns an initialized slice (never nil) for consistent JSON serialization.
func TransactionsToResponse(
	transactions []*shared.Transaction,
	jobID, contextID uuid.UUID,
) []TransactionResponse {
	result := make([]TransactionResponse, 0, len(transactions))

	for _, tx := range transactions {
		if tx != nil {
			result = append(result, TransactionToResponse(tx, jobID, contextID))
		}
	}

	return result
}

// SearchTransactionsToResponse converts search results to response DTOs.
// Uses each transaction's own IngestionJobID since search results span multiple jobs.
func SearchTransactionsToResponse(
	transactions []*shared.Transaction,
	contextID uuid.UUID,
) []TransactionResponse {
	result := make([]TransactionResponse, 0, len(transactions))

	for _, tx := range transactions {
		if tx != nil {
			result = append(result, TransactionToResponse(tx, tx.IngestionJobID, contextID))
		}
	}

	return result
}
