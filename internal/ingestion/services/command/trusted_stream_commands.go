// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
)

// defaultTrustedStreamFileName is used as the synthetic filename recorded on
// the IngestionJob when the caller does not supply an explicit filename via
// SourceMetadata. A non-empty filename is required for downstream visibility
// and ensures the ingestion job is distinguishable from upload-backed runs.
const defaultTrustedStreamFileName = "fetcher-stream"

// trustedStreamFileNameKey is the canonical SourceMetadata key that a bridge
// can use to override the synthetic filename persisted on the IngestionJob.
const trustedStreamFileNameKey = "filename"

// trustedStreamExtractionIDKey is the canonical SourceMetadata key carrying
// the originating Fetcher extraction id (T-005 P1). When present, the intake
// short-circuits on retry — see IngestFromTrustedStream for the lookup-then-
// reuse path.
const trustedStreamExtractionIDKey = "extraction_id"

// Sentinel errors for IngestFromTrustedStream input validation. These are
// returned unwrapped so callers can use errors.Is to distinguish caller-side
// validation errors from downstream pipeline failures.
var (
	// ErrIngestFromTrustedStreamContentRequired indicates the Content reader was nil.
	ErrIngestFromTrustedStreamContentRequired = errors.New(
		"trusted stream content reader is required",
	)
	// ErrIngestFromTrustedStreamSourceRequired indicates the SourceID was the zero UUID.
	ErrIngestFromTrustedStreamSourceRequired = errors.New(
		"trusted stream source id is required",
	)
	// ErrIngestFromTrustedStreamContextRequired indicates the ContextID was the zero UUID.
	ErrIngestFromTrustedStreamContextRequired = errors.New(
		"trusted stream context id is required",
	)
	// ErrIngestFromTrustedStreamFormatRequired indicates the Format string was empty.
	ErrIngestFromTrustedStreamFormatRequired = errors.New(
		"trusted stream format is required",
	)
	// ErrIngestFromTrustedStreamFormatUnsupported indicates the Format is not
	// registered in the parser registry.
	ErrIngestFromTrustedStreamFormatUnsupported = errors.New(
		"trusted stream format is not supported by the parser registry",
	)
)

// IngestFromTrustedStreamInput contains the data required to ingest content
// produced by a trusted internal bridge (e.g. Fetcher) rather than by a
// multipart HTTP upload. SourceMetadata is an open map forwarded from the
// bridge; today only the "filename" key is read (to override the synthetic
// IngestionJob filename — see resolveTrustedStreamFileName). Other keys are
// accepted but ignored pending provenance persistence in a future task (so
// bridges can already plumb e.g. extraction id through without a breaking
// change when the full provenance schema lands).
type IngestFromTrustedStreamInput struct {
	ContextID      uuid.UUID
	SourceID       uuid.UUID
	Format         string
	Content        io.Reader
	SourceMetadata map[string]string
}

// IngestFromTrustedStreamOutput is the durable outcome of a trusted-stream
// intake call. IngestionJobID identifies the persisted IngestionJob so the
// originating extraction lifecycle can be linked to downstream intake.
// TransactionCount reports how many transactions were inserted (dedup and
// existing-row filtering applied); it is the same counter used by the upload
// path and surfaces here so future projections can report it without an
// extra repository round-trip.
type IngestFromTrustedStreamOutput struct {
	IngestionJobID   uuid.UUID
	TransactionCount int
}

// IngestFromTrustedStream accepts content produced by a trusted internal
// bridge (Fetcher) and runs it through the same ingestion pipeline as the
// upload-backed IngestFile path: load source + field map, create and start
// an IngestionJob, parse + dedup + insert, persist completion + outbox, and
// optionally trigger auto-match. The pipeline reuse is intentional — it
// preserves AC-F2 (the intake path reuses existing ingestion business
// behavior rather than inventing a separate pipeline).
func (uc *UseCase) IngestFromTrustedStream(
	ctx context.Context,
	input IngestFromTrustedStreamInput,
) (*IngestFromTrustedStreamOutput, error) {
	if uc == nil {
		return nil, ErrNilUseCase
	}

	//nolint:dogsled // only tracer is needed here; logger is fetched inside helpers.
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.ingestion.ingest_from_trusted_stream")
	defer span.End()

	if err := validateTrustedStreamInput(uc, input); err != nil {
		libOpentelemetry.HandleSpanError(
			span,
			"trusted stream input validation failed",
			err,
		)

		return nil, err
	}

	// T-005 P1 short-circuit: when the bridge stamps an extraction_id in
	// SourceMetadata, look up any prior IngestionJob for the same id. If
	// found, return its identity instead of creating a phantom empty job.
	// This keeps the extraction→ingestion link 1:1 when the bridge retries
	// after a transient link-write failure (the original orphan-job bug).
	if existing, err := uc.findExistingTrustedStreamJob(ctx, input.SourceMetadata); err != nil {
		libOpentelemetry.HandleSpanError(span, "trusted stream short-circuit lookup failed", err)

		return nil, err
	} else if existing != nil {
		return &IngestFromTrustedStreamOutput{
			IngestionJobID:   existing.ID,
			TransactionCount: existing.Metadata.TotalRows,
		}, nil
	}

	fileName := resolveTrustedStreamFileName(input.SourceMetadata)

	job, txCount, err := uc.runTrustedStreamPipeline(ctx, span, input, fileName)
	if err != nil {
		return nil, err
	}

	uc.triggerAutoMatchIfEnabled(ctx, input.ContextID)

	return &IngestFromTrustedStreamOutput{
		IngestionJobID:   job.ID,
		TransactionCount: txCount,
	}, nil
}

// findExistingTrustedStreamJob looks up a prior IngestionJob for the given
// extraction_id (when present in SourceMetadata). Returns (nil, nil) when
// the extraction_id is missing, malformed, or no prior job exists — the
// caller treats either case as "proceed with normal ingest".
//
// Per T-005 P1: this is the orphan-job prevention path. Without it, a
// transient link-write failure causes Tick 2 to create a second
// IngestionJob (empty, because dedup ate all rows in Tick 1) and link the
// extraction to the empty job. Looking up by extraction_id ensures Tick 2
// re-uses Tick 1's job.
func (uc *UseCase) findExistingTrustedStreamJob(
	ctx context.Context,
	metadata map[string]string,
) (*entities.IngestionJob, error) {
	if uc == nil || uc.jobRepo == nil {
		return nil, nil
	}

	if len(metadata) == 0 {
		return nil, nil
	}

	raw, ok := metadata[trustedStreamExtractionIDKey]
	if !ok {
		return nil, nil
	}

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}

	extractionID, parseErr := uuid.Parse(trimmed)
	if parseErr != nil {
		// Malformed extraction_id is a wiring bug at the bridge side, but
		// we return nil to allow ingest to proceed. The bridge's
		// classifier will pick up the resulting non-idempotent linkage
		// failure on retry. Intentionally swallow the parse error to keep
		// the short-circuit fail-open: a malformed metadata key must not
		// block legitimate ingestion of the trusted-stream content.
		//nolint:nilerr // intentional: malformed metadata is non-fatal
		return nil, nil
	}

	job, err := uc.jobRepo.FindLatestByExtractionID(ctx, extractionID)
	if err != nil {
		return nil, fmt.Errorf("trusted stream short-circuit lookup: %w", err)
	}

	return job, nil
}

// validateTrustedStreamInput enforces the domain invariants of a trusted
// intake call before any infrastructure work happens.
func validateTrustedStreamInput(uc *UseCase, input IngestFromTrustedStreamInput) error {
	if input.Content == nil {
		return ErrIngestFromTrustedStreamContentRequired
	}

	if input.SourceID == uuid.Nil {
		return ErrIngestFromTrustedStreamSourceRequired
	}

	if input.ContextID == uuid.Nil {
		return ErrIngestFromTrustedStreamContextRequired
	}

	format := strings.ToLower(strings.TrimSpace(input.Format))
	if format == "" {
		return ErrIngestFromTrustedStreamFormatRequired
	}

	if _, err := uc.parsers.GetParser(format); err != nil {
		return fmt.Errorf("%w: %w", ErrIngestFromTrustedStreamFormatUnsupported, err)
	}

	return nil
}

// canonicalExtractionIDFromMetadata extracts and canonicalizes the extraction
// id from SourceMetadata for the trusted-stream pipeline. Returns the empty
// string when the key is missing, blank, or unparseable — those are the
// cases where no stamp should land on the IngestionJob's metadata. Polish
// Fix 4 + 7: this is the single source of truth so canonicalization stays
// consistent between the pipeline-stamp path and the short-circuit lookup.
func canonicalExtractionIDFromMetadata(metadata map[string]string) string {
	if len(metadata) == 0 {
		return ""
	}

	raw, ok := metadata[trustedStreamExtractionIDKey]
	if !ok {
		return ""
	}

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	parsed, err := uuid.Parse(trimmed)
	if err != nil {
		return ""
	}

	return parsed.String()
}

// stampExtractionIDOnJob applies a pre-validated extraction id directly to
// the job entity. Used by createAndStartJob (Polish Fix 4) so the stamp lands
// atomically inside the initial INSERT rather than via a follow-up Update.
//
// The input string is expected to be either the canonical lowercase UUID
// form OR an unparseable value (in which case we silently skip — the caller
// is the trusted-stream pipeline which has already filtered upstream). We
// re-parse defensively so direct (non-trusted-stream) callers cannot bypass
// the canonical-form invariant.
func stampExtractionIDOnJob(job *entities.IngestionJob, extractionID string) {
	if job == nil {
		return
	}

	trimmed := strings.TrimSpace(extractionID)
	if trimmed == "" {
		return
	}

	parsed, err := uuid.Parse(trimmed)
	if err != nil {
		return
	}

	job.Metadata.ExtractionID = parsed.String()
}

// resolveTrustedStreamFileName picks a filename for the synthetic ingestion
// job. Callers can override via SourceMetadata["filename"]; otherwise the
// pipeline records a stable sentinel value.
func resolveTrustedStreamFileName(metadata map[string]string) string {
	if len(metadata) == 0 {
		return defaultTrustedStreamFileName
	}

	if name, ok := metadata[trustedStreamFileNameKey]; ok {
		trimmed := strings.TrimSpace(name)
		if trimmed != "" {
			return trimmed
		}
	}

	return defaultTrustedStreamFileName
}
