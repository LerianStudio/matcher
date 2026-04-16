// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dto

import (
	"time"

	"github.com/google/uuid"
)

// BridgeReadinessSummaryResponse is the dashboard-facing aggregate of
// extraction readiness for the requesting tenant. Counts are mutually
// exclusive (an extraction is in exactly one bucket) and TotalCount sums to
// the tenant's total extraction count. InFlightCount surfaces upstream
// extractions still running (PENDING/SUBMITTED/EXTRACTING) so the operator
// can distinguish "Fetcher idle" from "Fetcher actively working". The
// previous four-bucket partition silently dropped these rows. StaleThresholdSec
// is echoed back so the dashboard can render "stale after Nm" labels without
// needing a separate config call.
type BridgeReadinessSummaryResponse struct {
	ReadyCount        int64     `json:"readyCount"`
	PendingCount      int64     `json:"pendingCount"`
	StaleCount        int64     `json:"staleCount"`
	FailedCount       int64     `json:"failedCount"`
	InFlightCount     int64     `json:"inFlightCount"`
	TotalCount        int64     `json:"totalCount"`
	StaleThresholdSec int64     `json:"staleThresholdSec"`
	GeneratedAt       time.Time `json:"generatedAt"`
}

// BridgeCandidateResponse is a drill-down row for the dashboard. Carries the
// minimum identifying fields needed to deep-link back to the extraction
// detail view; consumers wanting full extraction state can call the existing
// GET /v1/discovery/extractions/{extractionId} endpoint.
type BridgeCandidateResponse struct {
	ExtractionID   uuid.UUID  `json:"extractionId"`
	ConnectionID   uuid.UUID  `json:"connectionId"`
	Status         string     `json:"status"`
	ReadinessState string     `json:"readinessState"`
	IngestionJobID *uuid.UUID `json:"ingestionJobId,omitempty"`
	FetcherJobID   string     `json:"fetcherJobId,omitempty"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
	AgeSeconds     int64      `json:"ageSeconds"`
}

// ListBridgeCandidatesResponse wraps a single page of drill-down rows. The
// next-page cursor is opaque to the client; passing it back as the cursor
// query parameter resumes paging from where this response ended.
type ListBridgeCandidatesResponse struct {
	Items      []BridgeCandidateResponse `json:"items"`
	NextCursor string                    `json:"nextCursor,omitempty"`
	State      string                    `json:"state"`
	Limit      int                       `json:"limit"`
}

// NewBridgeReadinessSummaryResponse builds a summary response from the
// bare scalar inputs the handler already has in hand. Keeping the
// constructor input-agnostic preserves the dto package's "no service
// imports" boundary. inFlight covers upstream extractions still running
// so TotalCount sums to the tenant's complete extraction population.
func NewBridgeReadinessSummaryResponse(
	ready, pending, stale, failed, inFlight int64,
	staleThresholdSec int64,
	generatedAt time.Time,
) BridgeReadinessSummaryResponse {
	return BridgeReadinessSummaryResponse{
		ReadyCount:        ready,
		PendingCount:      pending,
		StaleCount:        stale,
		FailedCount:       failed,
		InFlightCount:     inFlight,
		TotalCount:        ready + pending + stale + failed + inFlight,
		StaleThresholdSec: staleThresholdSec,
		GeneratedAt:       generatedAt,
	}
}

// NewBridgeCandidateResponse assembles a single drilldown row from the
// fields the handler already holds. ingestionJobID is a pointer so the
// caller can pass nil for unlinked extractions without an extra branch.
func NewBridgeCandidateResponse(
	extractionID uuid.UUID,
	connectionID uuid.UUID,
	status string,
	readinessState string,
	ingestionJobID *uuid.UUID,
	fetcherJobID string,
	createdAt time.Time,
	updatedAt time.Time,
	ageSeconds int64,
) BridgeCandidateResponse {
	return BridgeCandidateResponse{
		ExtractionID:   extractionID,
		ConnectionID:   connectionID,
		Status:         status,
		ReadinessState: readinessState,
		IngestionJobID: ingestionJobID,
		FetcherJobID:   fetcherJobID,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
		AgeSeconds:     ageSeconds,
	}
}
