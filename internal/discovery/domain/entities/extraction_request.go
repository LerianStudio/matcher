package entities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v4/commons/assert"

	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// ErrInvalidTransition indicates an invalid status transition.
var ErrInvalidTransition = errors.New("invalid status transition")

// ExtractionRequest tracks a data extraction request to Fetcher.
// IngestionJobID is optional and can be set after extraction completes
// when the data is imported into an ingestion job.
type ExtractionRequest struct {
	ID             uuid.UUID
	IngestionJobID uuid.UUID // Nullable: set via SetIngestionJobID after extraction
	FetcherConnID  string
	FetcherJobID   string
	Tables         map[string]any
	Filters        map[string]any
	Status         vo.ExtractionStatus
	ResultPath     string
	ErrorMessage   string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewExtractionRequest creates a new ExtractionRequest with validated invariants.
// The ingestionJobID is not required at creation time; it can be linked later
// via SetIngestionJobID when the extracted data is imported.
func NewExtractionRequest(
	ctx context.Context,
	fetcherConnID string,
	tables map[string]any,
) (*ExtractionRequest, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "discovery.extraction_request.new")

	if err := asserter.NotEmpty(ctx, fetcherConnID, "fetcher connection id required"); err != nil {
		return nil, fmt.Errorf("extraction request fetcher connection id: %w", err)
	}

	// Initialize nil tables to empty map for consistency after DB roundtrip.
	if tables == nil {
		tables = make(map[string]any)
	}

	now := time.Now().UTC()

	return &ExtractionRequest{
		ID:            uuid.New(),
		FetcherConnID: fetcherConnID,
		Tables:        tables,
		Status:        vo.ExtractionStatusPending,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}

// SetIngestionJobID links this extraction request to an ingestion job.
// This is typically set after extraction completes and the data is imported.
func (er *ExtractionRequest) SetIngestionJobID(jobID uuid.UUID) {
	if er == nil {
		return
	}

	er.IngestionJobID = jobID
	er.UpdatedAt = time.Now().UTC()
}

// MarkSubmitted records the Fetcher job ID after successful submission.
// Valid transitions: PENDING → SUBMITTED.
func (er *ExtractionRequest) MarkSubmitted(fetcherJobID string) error {
	if er == nil {
		return nil
	}

	if er.Status != vo.ExtractionStatusPending {
		return fmt.Errorf("%w: cannot transition from %s to SUBMITTED", ErrInvalidTransition, er.Status)
	}

	er.FetcherJobID = fetcherJobID
	er.Status = vo.ExtractionStatusSubmitted
	er.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkExtracting transitions to EXTRACTING status.
// Valid transitions: PENDING/SUBMITTED → EXTRACTING.
func (er *ExtractionRequest) MarkExtracting() error {
	if er == nil {
		return nil
	}

	// Idempotent: already extracting is a no-op (Fetcher reports RUNNING/EXTRACTING
	// multiple times across poll cycles).
	if er.Status == vo.ExtractionStatusExtracting {
		return nil
	}

	if er.Status != vo.ExtractionStatusPending && er.Status != vo.ExtractionStatusSubmitted {
		return fmt.Errorf("%w: cannot transition from %s to EXTRACTING", ErrInvalidTransition, er.Status)
	}

	er.Status = vo.ExtractionStatusExtracting
	er.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkComplete records successful extraction.
// Valid transitions: SUBMITTED/EXTRACTING → COMPLETE.
func (er *ExtractionRequest) MarkComplete(resultPath string) error {
	if er == nil {
		return nil
	}

	if er.Status != vo.ExtractionStatusSubmitted && er.Status != vo.ExtractionStatusExtracting {
		return fmt.Errorf("%w: cannot transition from %s to COMPLETE", ErrInvalidTransition, er.Status)
	}

	er.Status = vo.ExtractionStatusComplete
	er.ResultPath = resultPath
	er.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkFailed records extraction failure.
// Valid transitions: Any non-terminal → FAILED.
func (er *ExtractionRequest) MarkFailed(errMsg string) error {
	if er == nil {
		return nil
	}

	if er.Status.IsTerminal() {
		return fmt.Errorf("%w: cannot fail from terminal state %s", ErrInvalidTransition, er.Status)
	}

	er.Status = vo.ExtractionStatusFailed
	er.ErrorMessage = errMsg
	er.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkCancelled cancels the extraction request.
// Valid transitions: Any non-terminal → CANCELLED.
func (er *ExtractionRequest) MarkCancelled() error {
	if er == nil {
		return nil
	}

	if er.Status.IsTerminal() {
		return fmt.Errorf("%w: cannot cancel from terminal state %s", ErrInvalidTransition, er.Status)
	}

	er.Status = vo.ExtractionStatusCancelled
	er.UpdatedAt = time.Now().UTC()

	return nil
}

// TablesJSON returns the tables config serialized as JSON.
func (er *ExtractionRequest) TablesJSON() ([]byte, error) {
	if er == nil || er.Tables == nil {
		return []byte("{}"), nil
	}

	data, err := json.Marshal(er.Tables)
	if err != nil {
		return nil, fmt.Errorf("marshal tables: %w", err)
	}

	return data, nil
}

// FiltersJSON returns the filters serialized as JSON.
func (er *ExtractionRequest) FiltersJSON() ([]byte, error) {
	if er == nil || er.Filters == nil {
		return []byte("null"), nil // SQL NULL representation for optional filters
	}

	data, err := json.Marshal(er.Filters)
	if err != nil {
		return nil, fmt.Errorf("marshal filters: %w", err)
	}

	return data, nil
}
