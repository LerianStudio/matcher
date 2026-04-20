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
// prior partitioning (without InFlight) silently dropped these rows. StaleThresholdSec
// is echoed back so the dashboard can render "stale after Nm" labels without
// needing a separate config call.
//
// Worker liveness fields (C15) — adjacent metadata, not part of the
// Ready/Pending/Stale/Failed/InFlight partition:
//   - WorkerLastTickAt: latest cycle timestamp written by any bridge
//     worker replica. Omitted (nil) when no heartbeat has been observed,
//     which the dashboard must render as "unknown" rather than zero.
//   - WorkerStalenessSeconds: (now - lastTickAt) in seconds. Omitted
//     alongside WorkerLastTickAt so the two fields are consistent.
//   - WorkerHealthy: server-side verdict using the same threshold as
//     staleness partitioning so every client agrees on the fence line.
type BridgeReadinessSummaryResponse struct {
	ReadyCount             int64      `json:"readyCount"`
	PendingCount           int64      `json:"pendingCount"`
	StaleCount             int64      `json:"staleCount"`
	FailedCount            int64      `json:"failedCount"`
	InFlightCount          int64      `json:"inFlightCount"`
	TotalCount             int64      `json:"totalCount"`
	StaleThresholdSec      int64      `json:"staleThresholdSec"`
	GeneratedAt            time.Time  `json:"generatedAt"`
	WorkerLastTickAt       *time.Time `json:"workerLastTickAt,omitempty"`
	WorkerStalenessSeconds *int64     `json:"workerStalenessSeconds,omitempty"`
	WorkerHealthy          bool       `json:"workerHealthy"`
}

// BridgeCandidateResponse is a drill-down row for the dashboard. Carries the
// minimum identifying fields needed to deep-link back to the extraction
// detail view; consumers wanting full extraction state can call the existing
// GET /v1/discovery/extractions/{extractionId} endpoint.
//
// BridgeLastError is surfaced inline so failed-bucket drilldowns can render
// the failure class (integrity_failed / artifact_not_found / source_unresolved
// / max_attempts_exceeded) without a second call — the single field operators
// most often triage against. Omitted via omitempty for non-failed rows.
type BridgeCandidateResponse struct {
	ExtractionID    uuid.UUID  `json:"extractionId"`
	ConnectionID    uuid.UUID  `json:"connectionId"`
	Status          string     `json:"status"`
	ReadinessState  string     `json:"readinessState"`
	IngestionJobID  *uuid.UUID `json:"ingestionJobId,omitempty"`
	FetcherJobID    string     `json:"fetcherJobId,omitempty"`
	CreatedAt       time.Time  `json:"createdAt"`
	UpdatedAt       time.Time  `json:"updatedAt"`
	AgeSeconds      int64      `json:"ageSeconds"`
	BridgeLastError string     `json:"bridgeLastError,omitempty"`
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
//
// workerLastTickAt / workerStalenessSeconds are pointers so the handler
// can pass nil when the worker has never written a heartbeat — omitempty
// then keeps them out of the JSON body. workerHealthy is a scalar bool
// because "unknown" is already communicated by the absence of the
// timestamp and staleness fields. C15.
func NewBridgeReadinessSummaryResponse(
	ready, pending, stale, failed, inFlight int64,
	staleThresholdSec int64,
	generatedAt time.Time,
	workerLastTickAt *time.Time,
	workerStalenessSeconds *int64,
	workerHealthy bool,
) BridgeReadinessSummaryResponse {
	return BridgeReadinessSummaryResponse{
		ReadyCount:             ready,
		PendingCount:           pending,
		StaleCount:             stale,
		FailedCount:            failed,
		InFlightCount:          inFlight,
		TotalCount:             ready + pending + stale + failed + inFlight,
		StaleThresholdSec:      staleThresholdSec,
		GeneratedAt:            generatedAt,
		WorkerLastTickAt:       workerLastTickAt,
		WorkerStalenessSeconds: workerStalenessSeconds,
		WorkerHealthy:          workerHealthy,
	}
}

// NewBridgeCandidateResponse assembles a single drilldown row from the
// fields the handler already holds. ingestionJobID is a pointer so the
// caller can pass nil for unlinked extractions without an extra branch.
// bridgeLastError surfaces the failure class (integrity_failed, etc.) so
// failed-bucket rows carry triage context inline; pass "" for non-failed
// rows and omitempty will keep it out of the response.
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
	bridgeLastError string,
) BridgeCandidateResponse {
	return BridgeCandidateResponse{
		ExtractionID:    extractionID,
		ConnectionID:    connectionID,
		Status:          status,
		ReadinessState:  readinessState,
		IngestionJobID:  ingestionJobID,
		FetcherJobID:    fetcherJobID,
		CreatedAt:       createdAt,
		UpdatedAt:       updatedAt,
		AgeSeconds:      ageSeconds,
		BridgeLastError: bridgeLastError,
	}
}
