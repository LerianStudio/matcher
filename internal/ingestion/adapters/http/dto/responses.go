package dto

import (
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

// JobResponse represents an ingestion job in API responses.
// @Description Ingestion job details
type JobResponse struct {
	// Unique identifier for the job
	ID string `json:"id"                    example:"550e8400-e29b-41d4-a716-446655440000"`
	// Context ID this job belongs to
	ContextID string `json:"contextId"             example:"550e8400-e29b-41d4-a716-446655440000"`
	// Source ID this job ingests data into
	SourceID string `json:"sourceId"              example:"550e8400-e29b-41d4-a716-446655440000"`
	// Current status of the job
	Status string `json:"status"                example:"PROCESSING"                           enums:"QUEUED,PROCESSING,COMPLETED,FAILED"`
	// Original file name
	FileName string `json:"fileName,omitempty"    example:"transactions_2024.csv"`
	// Total number of rows in the file
	TotalRows int `json:"totalRows,omitempty"   example:"1000"                                                                            minimum:"0"`
	// Number of rows that failed processing
	FailedRows int `json:"failedRows,omitempty"  example:"5"                                                                               minimum:"0"`
	// When the job started in RFC3339 format (null for QUEUED jobs)
	StartedAt *string `json:"startedAt,omitempty"   example:"2025-01-15T10:30:00Z"`
	// When the job completed in RFC3339 format (if completed)
	CompletedAt *string `json:"completedAt,omitempty" example:"2025-01-15T10:35:00Z"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt"             example:"2025-01-15T10:30:00Z"`
}

// TransactionResponse represents a transaction in API responses.
// @Description Transaction details
type TransactionResponse struct {
	// Unique identifier for the transaction
	ID string `json:"id"                    example:"550e8400-e29b-41d4-a716-446655440000"`
	// Job ID that ingested this transaction
	JobID string `json:"jobId"                 example:"550e8400-e29b-41d4-a716-446655440000"`
	// Source ID this transaction belongs to
	SourceID string `json:"sourceId"              example:"550e8400-e29b-41d4-a716-446655440000"`
	// Context ID this transaction belongs to
	ContextID string `json:"contextId"             example:"550e8400-e29b-41d4-a716-446655440000"`
	// External identifier from the source system
	ExternalID string `json:"externalId"            example:"TXN-12345"`
	// Transaction amount as string
	Amount string `json:"amount"                example:"1000.50"`
	// Currency code
	Currency string `json:"currency"              example:"USD"`
	// Transaction date in RFC3339 format
	Date string `json:"date"                  example:"2025-01-15T00:00:00Z"`
	// Description of the transaction
	Description string `json:"description,omitempty" example:"Wire transfer from ABC Corp"`
	// Current matching status
	Status string `json:"status"                example:"PENDING"                              enums:"PENDING,MATCHED,UNMATCHED"`
	// Extraction status
	ExtractionStatus string `json:"extractionStatus"      example:"EXTRACTED"                            enums:"PENDING,EXTRACTED,FAILED"`
	// Creation timestamp in RFC3339 format
	CreatedAt string `json:"createdAt"             example:"2025-01-15T10:30:00Z"`
}

// ListJobsResponse represents a cursor-paginated list of ingestion jobs.
// @Description Cursor-paginated list of ingestion jobs
type ListJobsResponse struct {
	// List of ingestion jobs
	Items []JobResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	sharedhttp.CursorResponse
}

// ListTransactionsResponse represents a cursor-paginated list of transactions.
// @Description Cursor-paginated list of transactions
type ListTransactionsResponse struct {
	// List of transactions
	Items []TransactionResponse `json:"items" validate:"omitempty,max=200" maxItems:"200"`
	sharedhttp.CursorResponse
}

// IgnoreTransactionResponse represents the response for ignoring a transaction.
// @Description Ignore transaction response
type IgnoreTransactionResponse struct {
	TransactionResponse
}

// FilePreviewResponse contains extracted column names and sample rows from a file.
// @Description File preview result
type FilePreviewResponse struct {
	// Detected column names
	Columns []string `json:"columns" example:"id,amount,currency,date"`
	// Sample data rows (up to maxRows)
	SampleRows [][]string `json:"sampleRows"`
	// Number of sample rows returned
	RowCount int `json:"rowCount" example:"5"`
	// Detected file format
	Format string `json:"format" example:"csv" enums:"csv,json,xml"`
}

// SearchTransactionsResponse represents a paginated list of search results.
// @Description Transaction search results with offset pagination
type SearchTransactionsResponse struct {
	// List of matching transactions
	Items []TransactionResponse `json:"items"`
	// Total number of matching transactions
	Total int64 `json:"total"  example:"42"`
	// Maximum results per page
	Limit int `json:"limit"  example:"20"`
	// Current offset
	Offset int `json:"offset" example:"0"`
}
