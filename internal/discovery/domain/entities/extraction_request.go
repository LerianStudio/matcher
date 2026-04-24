// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
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
//
// The bridge_* fields (T-005) describe the Matcher-side bridging pipeline's
// retry-and-failure state. They are independent of Status — Status describes
// the upstream Fetcher pipeline (PENDING → SUBMITTED → EXTRACTING → COMPLETE
// or FAILED/CANCELLED), while BridgeAttempts/BridgeLastError/BridgeFailedAt
// describe what happened when the Matcher worker tried to retrieve, verify,
// custody, ingest, and link the extraction's output. A row can be
// Status=COMPLETE with BridgeLastError set: the upstream succeeded but the
// downstream bridge gave up.
type ExtractionRequest struct {
	ID                     uuid.UUID
	ConnectionID           uuid.UUID
	IngestionJobID         uuid.UUID // Nullable: linked to downstream ingestion when available
	FetcherJobID           string
	Tables                 map[string]any
	StartDate              string
	EndDate                string
	Filters                map[string]any
	Status                 vo.ExtractionStatus
	ResultPath             string
	ErrorMessage           string
	CreatedAt              time.Time
	UpdatedAt              time.Time
	BridgeAttempts         int
	BridgeLastError        vo.BridgeErrorClass // empty when no terminal bridge failure
	BridgeLastErrorMessage string
	BridgeFailedAt         time.Time // zero when no terminal bridge failure
	// CustodyDeletedAt is the UTC timestamp when the custody object for this
	// extraction was deleted (either by the bridge orchestrator's happy-path
	// cleanupCustody hook or by the custody retention worker's sweep). It is
	// a pointer so NULL (custody object may still exist) is distinguishable
	// from a zero-value time. See migration 000027.
	CustodyDeletedAt *time.Time
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

// LinkToIngestion records the downstream ingestion job id that consumed the
// extraction's output. Valid only when the extraction is COMPLETE: linking a
// non-COMPLETE extraction is a state-machine violation.
//
// This method protects the invariants that the raw-field-assign adapter form
// bypassed:
//   - Extractions in PENDING/SUBMITTED/EXTRACTING have no output to link to.
//   - Extractions in FAILED/CANCELLED have no trustworthy output.
//   - Re-linking an already-linked extraction is rejected so the 1:1
//     extraction→ingestion invariant is enforced at the domain layer (in
//     addition to the adapter's atomic SQL guard).
//
// UpdatedAt is bumped so UpdateIfUnchanged-style optimistic concurrency sees
// a real state change. Callers wanting idempotent behavior across retries
// should rely on the adapter's atomic UPDATE ... WHERE ingestion_job_id IS
// NULL guard rather than mutate the entity twice.
func (er *ExtractionRequest) LinkToIngestion(ingestionJobID uuid.UUID) error {
	if er == nil {
		return nil
	}

	if ingestionJobID == uuid.Nil {
		return fmt.Errorf("%w: ingestion job id required", ErrInvalidTransition)
	}

	if er.Status != vo.ExtractionStatusComplete {
		return fmt.Errorf(
			"%w: cannot link ingestion job to extraction in state %s",
			ErrInvalidTransition,
			er.Status,
		)
	}

	if er.IngestionJobID != uuid.Nil && er.IngestionJobID != ingestionJobID {
		// Cross-job collision: extraction is already linked to a DIFFERENT
		// ingestion job. Surface the canonical 1:1 invariant sentinel
		// (sharedPorts.ErrExtractionAlreadyLinked) rather than the generic
		// state-machine sentinel so adapters can errors.Is for it without
		// having to re-classify the cross-job case downstream of the
		// atomic SQL guard.
		return fmt.Errorf(
			"%w: extraction is already linked to ingestion job %s",
			sharedPorts.ErrExtractionAlreadyLinked,
			er.IngestionJobID,
		)
	}

	er.IngestionJobID = ingestionJobID
	er.UpdatedAt = time.Now().UTC()

	return nil
}

// GetID returns the extraction's immutable identifier. Satisfies the
// sharedPorts.LinkableExtraction interface used by the lifecycle link
// writer so the shared kernel does not need to import discovery entities
// directly (that would create an import cycle — this package imports
// shared/ports for ErrExtractionAlreadyLinked).
//
// Kept nil-safe so callers that hold a potentially-nil pointer get back
// uuid.Nil rather than panicking on dereference.
func (er *ExtractionRequest) GetID() uuid.UUID {
	if er == nil {
		return uuid.Nil
	}

	return er.ID
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
