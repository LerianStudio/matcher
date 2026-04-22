// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package cross

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	discoveryRepositories "github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time interface satisfaction checks for the Fetcher-bridge adapters.
var (
	_ sharedPorts.FetcherBridgeIntake           = (*FetcherBridgeIntakeAdapter)(nil)
	_ sharedPorts.ExtractionLifecycleLinkWriter = (*ExtractionLifecycleLinkWriterAdapter)(nil)
)

// FetcherBridgeIntakeAdapter hands the shared-kernel TrustedContentInput to
// the ingestion UseCase and translates the outcome back into the shared
// TrustedContentOutcome. It exists so the discovery context can ingest
// Fetcher content without importing ingestion directly (preserving AC-T1).
// Since T-007 K-16 collapsed the duplicate input types, the intake path is a
// direct passthrough — the shared input IS the ingestion input.
type FetcherBridgeIntakeAdapter struct {
	uc *ingestionCommand.UseCase
}

// NewFetcherBridgeIntakeAdapter wraps the given ingestion UseCase. The
// ingestion UseCase must be non-nil; otherwise the bridge has no pipeline to
// hand content to and the adapter would silently become a no-op.
func NewFetcherBridgeIntakeAdapter(
	uc *ingestionCommand.UseCase,
) (*FetcherBridgeIntakeAdapter, error) {
	if uc == nil {
		return nil, sharedPorts.ErrNilFetcherBridgeIntake
	}

	return &FetcherBridgeIntakeAdapter{uc: uc}, nil
}

// IngestTrustedContent hands the trusted bridge's content to the ingestion
// command use case and surfaces the persisted ingestion job id.
func (adapter *FetcherBridgeIntakeAdapter) IngestTrustedContent(
	ctx context.Context,
	input sharedPorts.TrustedContentInput,
) (sharedPorts.TrustedContentOutcome, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "cross.fetcher_bridge_intake")
	defer span.End()

	if adapter == nil || adapter.uc == nil {
		err := sharedPorts.ErrNilFetcherBridgeIntake
		libOpentelemetry.HandleSpanError(span, "fetcher bridge intake adapter is not initialised", err)

		return sharedPorts.TrustedContentOutcome{}, err
	}

	output, err := adapter.uc.IngestFromTrustedStream(ctx, input)
	if err != nil {
		wrappedErr := fmt.Errorf("fetcher bridge intake: %w", err)
		libOpentelemetry.HandleSpanError(span, "ingestion from trusted stream failed", wrappedErr)

		if logger != nil {
			logger.With(
				libLog.String("context_id", input.ContextID.String()),
				libLog.String("source_id", input.SourceID.String()),
				libLog.String("error", wrappedErr.Error()),
			).Log(ctx, libLog.LevelError, "fetcher bridge intake failed")
		}

		return sharedPorts.TrustedContentOutcome{}, wrappedErr
	}

	return sharedPorts.TrustedContentOutcome{
		IngestionJobID:   output.IngestionJobID,
		TransactionCount: output.TransactionCount,
	}, nil
}

// ExtractionLifecycleLinkWriterAdapter persists the linkage between an
// extraction lifecycle and the downstream ingestion job id it produced. The
// adapter enforces the one-extraction-to-one-ingestion invariant by refusing
// to overwrite an existing link (returning ErrExtractionAlreadyLinked).
type ExtractionLifecycleLinkWriterAdapter struct {
	repo discoveryRepositories.ExtractionRepository
}

// NewExtractionLifecycleLinkWriterAdapter constructs the adapter. The
// ExtractionRepository is the only downstream dependency; nil repositories
// are rejected up-front so bootstrap failures are visible.
func NewExtractionLifecycleLinkWriterAdapter(
	repo discoveryRepositories.ExtractionRepository,
) (*ExtractionLifecycleLinkWriterAdapter, error) {
	if repo == nil {
		return nil, sharedPorts.ErrNilExtractionLifecycleLinkWriter
	}

	return &ExtractionLifecycleLinkWriterAdapter{repo: repo}, nil
}

// LinkExtractionToIngestion records ingestionJobID on the supplied
// extraction using an atomic conditional UPDATE so concurrent bridge
// invocations cannot both succeed. If the extraction already has an
// ingestion job id (i.e. the bridge is being replayed), the call is
// treated as an idempotency conflict and returns
// ErrExtractionAlreadyLinked unmodified.
//
// The caller passes the pre-loaded *ExtractionRequest. The orchestrator
// has already fetched the row during eligibility verification; re-reading
// it here would cost a second DB round-trip on every bridge outcome. The
// atomic LinkIfUnlinked UPDATE is still the authoritative race guard —
// the in-memory entity is used only for state-machine validation, not as
// the source of truth for the actual write.
//
// Concurrency contract (T-003 P1 hardening):
//   - Under simultaneous link attempts for the same extraction, exactly
//     one UPDATE matches ingestion_job_id IS NULL and succeeds; the rest
//     observe zero rows-affected and get ErrExtractionAlreadyLinked.
//   - A state-machine domain method (LinkToIngestion) is consulted to
//     validate the Status invariant before we touch the DB: linking a
//     FAILED or CANCELLED extraction is a programmer error, not a race.
//
// This replaces the earlier read-check-write implementation that lost
// concurrent writers silently (TOCTOU race flagged by 6 reviewers in
// T-001 Gate 8).
func (adapter *ExtractionLifecycleLinkWriterAdapter) LinkExtractionToIngestion(
	ctx context.Context,
	extraction sharedPorts.LinkableExtraction,
	ingestionJobID uuid.UUID,
) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "cross.extraction_lifecycle_link")
	defer span.End()

	if adapter == nil || adapter.repo == nil {
		err := sharedPorts.ErrNilExtractionLifecycleLinkWriter
		libOpentelemetry.HandleSpanError(span, "extraction lifecycle link writer is not initialised", err)

		return err
	}

	if extraction == nil {
		err := sharedPorts.ErrLinkExtractionRequired
		libOpentelemetry.HandleSpanError(span, "missing extraction", err)

		return err
	}

	extractionID := extraction.GetID()
	if extractionID == uuid.Nil {
		err := sharedPorts.ErrLinkExtractionIDRequired
		libOpentelemetry.HandleSpanError(span, "missing extraction id", err)

		return err
	}

	if ingestionJobID == uuid.Nil {
		err := sharedPorts.ErrLinkIngestionJobIDRequired
		libOpentelemetry.HandleSpanError(span, "missing ingestion job id", err)

		return err
	}

	// Validate domain invariants via the state-machine method BEFORE the
	// atomic SQL so the FAILED/CANCELLED case is rejected even when the
	// row is already NULL-linked. The in-memory entity was loaded by the
	// orchestrator; the actual write uses LinkIfUnlinked which re-checks
	// the row state atomically in SQL.
	if err := extraction.LinkToIngestion(ingestionJobID); err != nil {
		// A domain-level validation failure (FAILED/CANCELLED, wrong state,
		// or cross-job collision wrapped as ErrExtractionAlreadyLinked) is
		// surfaced verbatim so callers can errors.Is on the canonical
		// sentinel. Same-id replays are NOT rejected by LinkToIngestion
		// (the domain treats them as no-ops), so any error here is a real
		// invariant violation and must propagate.
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "domain reject link", err)

		return err //nolint:wrapcheck // Domain sentinel must propagate verbatim so callers can errors.Is.
	}

	if err := adapter.repo.LinkIfUnlinked(ctx, extractionID, ingestionJobID); err != nil {
		if errors.Is(err, sharedPorts.ErrExtractionAlreadyLinked) ||
			errors.Is(err, discoveryRepositories.ErrExtractionNotFound) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "atomic link rejected", err)

			return err
		}

		wrappedErr := fmt.Errorf("persist extraction link: %w", err)
		libOpentelemetry.HandleSpanError(span, "persist extraction link failed", wrappedErr)

		if logger != nil {
			logger.With(
				libLog.String("extraction_id", extractionID.String()),
				libLog.String("ingestion_job_id", ingestionJobID.String()),
				libLog.String("error", wrappedErr.Error()),
			).Log(ctx, libLog.LevelError, "persist extraction link failed")
		}

		return wrappedErr
	}

	return nil
}
