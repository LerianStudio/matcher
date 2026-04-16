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
)

// defaultTrustedStreamFileName is used as the synthetic filename recorded on
// the IngestionJob when the caller does not supply an explicit filename via
// SourceMetadata. A non-empty filename is required for downstream visibility
// and ensures the ingestion job is distinguishable from upload-backed runs.
const defaultTrustedStreamFileName = "fetcher-stream"

// trustedStreamFileNameKey is the canonical SourceMetadata key that a bridge
// can use to override the synthetic filename persisted on the IngestionJob.
const trustedStreamFileNameKey = "filename"

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
