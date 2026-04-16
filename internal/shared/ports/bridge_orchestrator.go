// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package ports

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// Sentinel errors for the bridge orchestrator port.
var (
	// ErrNilBridgeOrchestrator indicates a required BridgeOrchestrator
	// dependency was nil at construction time. Returned by worker
	// constructors when wiring is incomplete.
	ErrNilBridgeOrchestrator = errors.New("bridge orchestrator is required")

	// ErrBridgeExtractionIDRequired indicates BridgeExtraction was called
	// with a zero extraction UUID.
	ErrBridgeExtractionIDRequired = errors.New("bridge extraction id is required")

	// ErrBridgeTenantIDRequired indicates BridgeExtraction was called
	// with an empty tenant id. The orchestrator needs tenant context to
	// build the retrieval descriptor and custody key.
	ErrBridgeTenantIDRequired = errors.New("bridge tenant id is required")

	// ErrBridgeExtractionIneligible indicates the orchestrator was asked
	// to bridge an extraction that is no longer eligible (already linked
	// or no longer COMPLETE). Treated as an idempotency signal, not a
	// failure — the worker logs and moves on.
	ErrBridgeExtractionIneligible = errors.New("bridge extraction is no longer eligible")

	// ErrBridgeSourceUnresolvable indicates no reconciliation source was
	// found for the extraction's fetcher connection. The bridge cannot
	// ingest without a configured source + context; the extraction is
	// left unlinked so a later configuration fix re-enables bridging.
	ErrBridgeSourceUnresolvable = errors.New("no reconciliation source wired for fetcher connection")
)

// BridgeExtractionInput carries one unit of bridge work: a specific
// extraction inside a specific tenant. The orchestrator is responsible for
// retrieving, verifying, ingesting, and linking.
type BridgeExtractionInput struct {
	// ExtractionID is the discovery-side extraction lifecycle id. Used
	// to fetch the extraction record, build the retrieval descriptor,
	// and write back the ingestion-job link.
	ExtractionID uuid.UUID
	// TenantID is the tenant namespace the extraction belongs to. Custody
	// writes land under this tenant's prefix. Passed separately (not
	// read from ctx) because the worker may build ctx fresh for each
	// extraction to keep trace spans short.
	TenantID string
}

// BridgeExtractionOutcome reports the terminal state of a bridge call.
// The worker logs these fields and does not surface them to HTTP; they
// exist for testing and for future dashboard projection (T-004).
type BridgeExtractionOutcome struct {
	// IngestionJobID is the downstream ingestion job id. uuid.Nil when
	// the bridge failed before the ingestion step.
	IngestionJobID uuid.UUID
	// TransactionCount is the number of transactions inserted by the
	// ingestion step. Zero when ingestion did not run (e.g. retrieval
	// or verification failed).
	TransactionCount int
	// CustodyDeleted reports whether the custody copy was deleted after
	// successful ingestion (D2: delete-after-ingest). False when
	// ingestion did not succeed or when the delete call itself failed
	// (the latter is non-fatal — a background retention sweep cleans up).
	CustodyDeleted bool
}

// BridgeOrchestrator drives a single extraction through the full
// bridge pipeline: retrieve → verify → custody → ingest → link.
//
// Implementations must:
//   - honour ctx deadlines and cancellation
//   - be safe for concurrent calls on different extractions
//   - return ErrBridgeExtractionIneligible (idempotent signal) when the
//     extraction is no longer COMPLETE/unlinked
//   - return ErrBridgeSourceUnresolvable when no reconciliation source is
//     wired for the extraction's Fetcher connection (config gap, not a
//     transient failure)
//   - wrap transient downstream failures with %w so the worker can
//     errors.Is them for retry classification (T-005 owns the retry
//     taxonomy; T-003 just surfaces the wrapping)
type BridgeOrchestrator interface {
	BridgeExtraction(
		ctx context.Context,
		input BridgeExtractionInput,
	) (*BridgeExtractionOutcome, error)
}

// BridgeSourceResolver maps a Fetcher connection id to the reconciliation
// source + context + tenant that should receive the extraction output.
// Lives in shared ports because the bridge orchestrator (in discovery)
// must not import configuration directly — that would violate the
// cross-context isolation rule enforced by depguard.
//
// Implementations typically JOIN reconciliation_sources.config->>'connection_id'
// against fetcher_connections.id for the current tenant schema.
type BridgeSourceResolver interface {
	// ResolveSourceForConnection returns the source + context ids wired
	// to the given Fetcher connection id. Returns
	// ErrBridgeSourceUnresolvable when no source is configured for the
	// connection in the current tenant scope.
	ResolveSourceForConnection(
		ctx context.Context,
		connectionID uuid.UUID,
	) (BridgeSourceTarget, error)
}

// BridgeSourceTarget is the tuple a BridgeSourceResolver produces.
type BridgeSourceTarget struct {
	SourceID  uuid.UUID
	ContextID uuid.UUID
	// Format is the content format the ingestion pipeline should parse
	// the custody plaintext as. Always "json" for Fetcher-produced
	// artifacts today; parameterised so future Fetcher format evolution
	// (e.g. CSV, Parquet) does not require a second port.
	Format string
}
