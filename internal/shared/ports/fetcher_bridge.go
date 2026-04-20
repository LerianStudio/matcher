// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package ports

import (
	"context"
	"errors"
	"io"

	"github.com/google/uuid"
)

// Sentinel errors for the Fetcher bridge intake / lifecycle-link ports.
// They live alongside the interfaces so callers can assert on them via
// errors.Is without importing the concrete cross-context adapters.
var (
	// ErrNilFetcherBridgeIntake indicates a required FetcherBridgeIntake
	// dependency was nil at construction time.
	ErrNilFetcherBridgeIntake = errors.New("fetcher bridge intake is required")
	// ErrNilExtractionLifecycleLinkWriter indicates a required
	// ExtractionLifecycleLinkWriter dependency was nil at construction time.
	ErrNilExtractionLifecycleLinkWriter = errors.New(
		"extraction lifecycle link writer is required",
	)
	// ErrLinkExtractionIDRequired indicates a lifecycle link was attempted with
	// a zero extraction UUID. Distinct from ErrNilExtractionLifecycleLinkWriter
	// so callers can tell a missing input from an unwired dependency.
	ErrLinkExtractionIDRequired = errors.New("extraction id is required for lifecycle link")
	// ErrLinkExtractionRequired indicates a lifecycle link was attempted with
	// a nil *ExtractionRequest. Distinct from ErrLinkExtractionIDRequired so
	// callers can distinguish a missing entity pointer from a zero-valued id
	// on a populated entity — the former is a programming error (the
	// orchestrator must load the entity before calling the link writer), the
	// latter is a missing input.
	ErrLinkExtractionRequired = errors.New("extraction is required for lifecycle link")
	// ErrLinkIngestionJobIDRequired indicates a lifecycle link was attempted
	// with a zero ingestion-job UUID. Distinct from
	// ErrNilExtractionLifecycleLinkWriter so callers can tell a missing input
	// from an unwired dependency.
	ErrLinkIngestionJobIDRequired = errors.New("ingestion job id is required for lifecycle link")
	// ErrExtractionAlreadyLinked indicates the extraction was already linked
	// to a downstream ingestion job. Returned by link writers to preserve the
	// one-extraction-to-one-ingestion invariant without panicking.
	ErrExtractionAlreadyLinked = errors.New(
		"extraction is already linked to an ingestion job",
	)
)

// TrustedContentInput carries all the information the bridge hands to the
// ingestion boundary. It intentionally mirrors the ingestion UseCase's
// IngestFromTrustedStreamInput but lives in the shared kernel so discovery
// can depend on it without importing ingestion directly (AC-T1).
type TrustedContentInput struct {
	ContextID      uuid.UUID
	SourceID       uuid.UUID
	Format         string
	Content        io.Reader
	SourceMetadata map[string]string
}

// TrustedContentOutcome is the durable result of a trusted intake call.
// IngestionJobID is the identifier the lifecycle link writer persists back
// onto the originating ExtractionRequest so audit tools can walk the chain
// from extraction to ingestion.
type TrustedContentOutcome struct {
	IngestionJobID   uuid.UUID
	TransactionCount int
}

// FetcherBridgeIntake is the inbound port exposed by ingestion to the
// trusted bridge. Implementations must:
//   - honour ctx deadlines and cancellation
//   - reuse the existing ingestion pipeline (dedup + outbox + match-trigger)
//   - return validation-style sentinel errors for caller-side mistakes
//   - wrap downstream failures with %w so callers can errors.Is them
type FetcherBridgeIntake interface {
	IngestTrustedContent(
		ctx context.Context,
		input TrustedContentInput,
	) (TrustedContentOutcome, error)
}

// LinkableExtraction is the minimal view of an ExtractionRequest the
// lifecycle link writer needs: its id (for the atomic UPDATE) and its
// state-machine LinkToIngestion method (for pre-SQL validation).
//
// Declared as a port-local interface rather than the concrete
// *discoveryEntities.ExtractionRequest so the shared kernel does not
// pull in the discovery bounded context (which would create an import
// cycle — the discovery entity imports this package for
// ErrExtractionAlreadyLinked).
//
// The discovery domain's *ExtractionRequest satisfies this interface
// natively; adapters and orchestrators pass it through unchanged.
type LinkableExtraction interface {
	GetID() uuid.UUID
	LinkToIngestion(ingestionJobID uuid.UUID) error
}

// ExtractionLifecycleLinkWriter persists the linkage between an extraction
// lifecycle and the downstream ingestion job it produced. Implementations
// must be idempotent: a link attempt against an already-linked extraction
// must return ErrExtractionAlreadyLinked without mutating the record.
//
// Callers pass the pre-loaded LinkableExtraction rather than just an id so
// the adapter can run state-machine validation (LinkToIngestion) against
// the in-memory entity without issuing a second FindByID — the orchestrator
// has already loaded the row during eligibility verification, and re-loading
// is wasted work plus an extra DB round-trip per bridge outcome.
type ExtractionLifecycleLinkWriter interface {
	LinkExtractionToIngestion(
		ctx context.Context,
		extraction LinkableExtraction,
		ingestionJobID uuid.UUID,
	) error
}
