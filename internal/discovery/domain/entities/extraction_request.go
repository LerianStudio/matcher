package entities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Sentinel errors for extraction state transitions.
var (
	ErrInvalidTransition       = errors.New("invalid status transition")
	ErrResultPathRequired      = errors.New("result path required")
	ErrResultPathNotAbsolute   = errors.New("result path must be absolute")
	ErrResultPathInvalidFormat = errors.New("result path must not include URL scheme, query, or fragment")
	ErrResultPathPathTraversal = errors.New("result path must not contain traversal segments")
)

// SanitizedExtractionFailureMessage is the client-safe failure detail persisted
// for extraction requests when upstream systems return internal diagnostics.
const SanitizedExtractionFailureMessage = "extraction failed"

// ExtractionRequest tracks a data extraction request to Fetcher.
// IngestionJobID is optional and reserved for downstream ingestion linkage.
type ExtractionRequest struct {
	ID             uuid.UUID
	ConnectionID   uuid.UUID
	IngestionJobID uuid.UUID // Nullable: linked to downstream ingestion when available
	FetcherJobID   string
	Tables         map[string]any
	StartDate      string
	EndDate        string
	Filters        map[string]any
	Status         vo.ExtractionStatus
	ResultPath     string
	ErrorMessage   string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewExtractionRequest creates a new ExtractionRequest with validated invariants.
func NewExtractionRequest(
	ctx context.Context,
	connectionID uuid.UUID,
	tables map[string]any,
	startDate string,
	endDate string,
	filters map[string]any,
) (*ExtractionRequest, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "discovery.extraction_request.new")

	if err := asserter.That(ctx, connectionID != uuid.Nil, "connection id required"); err != nil {
		return nil, fmt.Errorf("extraction request connection id: %w", err)
	}

	clonedTables, err := cloneMap(tables)
	if err != nil {
		return nil, fmt.Errorf("extraction request tables: %w", err)
	}

	var clonedFilters map[string]any
	if filters != nil {
		clonedFilters, err = cloneMap(filters)
		if err != nil {
			return nil, fmt.Errorf("extraction request filters: %w", err)
		}
	}

	now := time.Now().UTC()

	return &ExtractionRequest{
		ID:           uuid.New(),
		ConnectionID: connectionID,
		Tables:       clonedTables,
		StartDate:    strings.TrimSpace(startDate),
		EndDate:      strings.TrimSpace(endDate),
		Filters:      clonedFilters,
		Status:       vo.ExtractionStatusPending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}, nil
}

// MarkSubmitted records the Fetcher job ID after successful submission.
// Valid transitions: PENDING → SUBMITTED.
func (er *ExtractionRequest) MarkSubmitted(fetcherJobID string) error {
	if er == nil {
		return nil
	}

	if strings.TrimSpace(fetcherJobID) == "" {
		return fmt.Errorf("%w: fetcher job id required", ErrInvalidTransition)
	}

	if er.Status != vo.ExtractionStatusPending {
		return fmt.Errorf("%w: cannot transition from %s to SUBMITTED", ErrInvalidTransition, er.Status)
	}

	er.FetcherJobID = fetcherJobID
	er.Status = vo.ExtractionStatusSubmitted
	er.ResultPath = ""
	er.ErrorMessage = ""
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

	if strings.TrimSpace(er.FetcherJobID) == "" {
		return fmt.Errorf("%w: fetcher job id required before extracting", ErrInvalidTransition)
	}

	if er.Status != vo.ExtractionStatusSubmitted {
		return fmt.Errorf("%w: cannot transition from %s to EXTRACTING", ErrInvalidTransition, er.Status)
	}

	er.Status = vo.ExtractionStatusExtracting
	er.ErrorMessage = ""
	er.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkComplete records successful extraction.
// Valid transitions: SUBMITTED/EXTRACTING → COMPLETE.
func (er *ExtractionRequest) MarkComplete(resultPath string) error {
	if er == nil {
		return nil
	}

	if strings.TrimSpace(er.FetcherJobID) == "" {
		return fmt.Errorf("%w: fetcher job id required before completing", ErrInvalidTransition)
	}

	if strings.TrimSpace(resultPath) == "" {
		return fmt.Errorf("%w: result path required", ErrInvalidTransition)
	}

	if err := validateResultPath(resultPath); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidTransition, err)
	}

	if er.Status != vo.ExtractionStatusSubmitted && er.Status != vo.ExtractionStatusExtracting {
		return fmt.Errorf("%w: cannot transition from %s to COMPLETE", ErrInvalidTransition, er.Status)
	}

	er.Status = vo.ExtractionStatusComplete
	er.ResultPath = resultPath
	er.ErrorMessage = ""
	er.UpdatedAt = time.Now().UTC()

	return nil
}

func validateResultPath(resultPath string) error {
	trimmed := strings.TrimSpace(resultPath)
	if trimmed == "" {
		return ErrResultPathRequired
	}

	if !strings.HasPrefix(trimmed, "/") {
		return ErrResultPathNotAbsolute
	}

	if strings.Contains(trimmed, "://") || strings.ContainsAny(trimmed, "?#") {
		return ErrResultPathInvalidFormat
	}

	cleaned := path.Clean(trimmed)
	if cleaned != trimmed || strings.Contains(trimmed, "..") {
		return ErrResultPathPathTraversal
	}

	return nil
}

// MarkFailed records extraction failure.
// Valid transitions: Any non-terminal → FAILED.
func (er *ExtractionRequest) MarkFailed(errMsg string) error {
	if er == nil {
		return nil
	}

	if strings.TrimSpace(errMsg) == "" {
		return fmt.Errorf("%w: error message required", ErrInvalidTransition)
	}

	if er.Status.IsTerminal() {
		return fmt.Errorf("%w: cannot fail from terminal state %s", ErrInvalidTransition, er.Status)
	}

	er.Status = vo.ExtractionStatusFailed
	er.ResultPath = ""
	er.ErrorMessage = errMsg
	er.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkCancelled records extraction cancellation.
// Valid transitions: Any non-terminal state -> CANCELLED.
func (er *ExtractionRequest) MarkCancelled() error {
	if er == nil {
		return nil
	}

	if er.Status.IsTerminal() {
		return fmt.Errorf("%w: cannot cancel from terminal state %s", ErrInvalidTransition, er.Status)
	}

	er.Status = vo.ExtractionStatusCancelled
	er.ResultPath = ""
	er.ErrorMessage = ""
	er.UpdatedAt = time.Now().UTC()

	return nil
}

func cloneMap(src map[string]any) (map[string]any, error) {
	if src == nil {
		return make(map[string]any), nil
	}

	data, err := json.Marshal(src)
	if err != nil {
		return nil, fmt.Errorf("marshal map clone: %w", err)
	}

	cloned := make(map[string]any)
	if err := json.Unmarshal(data, &cloned); err != nil {
		return nil, fmt.Errorf("unmarshal map clone: %w", err)
	}

	return cloned, nil
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
// Nil filters return nil so repositories can persist a real SQL NULL.
func (er *ExtractionRequest) FiltersJSON() ([]byte, error) {
	if er == nil || er.Filters == nil {
		return nil, nil
	}

	data, err := json.Marshal(er.Filters)
	if err != nil {
		return nil, fmt.Errorf("marshal filters: %w", err)
	}

	return data, nil
}
