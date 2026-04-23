// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// runTrustedStreamPipeline executes the shared prepare → process → complete
// ingestion pipeline for a trusted stream input. It is the trusted-stream
// sibling of the block inside StartIngestion so both paths share the same
// business behavior (AC-F2) while exposing a typed return for the bridge.
func (uc *UseCase) runTrustedStreamPipeline(
	ctx context.Context,
	span trace.Span,
	input sharedPorts.TrustedContentInput,
	fileName string,
) (*entities.IngestionJob, int, error) {
	// Polish Fix 4: extract the canonical extraction id (if any) from
	// SourceMetadata and pass it via StartIngestionInput so the stamp lands
	// atomically inside the initial INSERT. Previously the stamp was applied
	// via a follow-up Update — a transient failure on that Update reopened
	// the orphan-job window the P1 short-circuit was supposed to close.
	extractionID := canonicalExtractionIDFromMetadata(ctx, input.SourceMetadata)

	startInput := StartIngestionInput{
		ContextID: input.ContextID,
		SourceID:  input.SourceID,
		FileName:  fileName,
		// FileSize is zero for streaming intake; the non-streaming parser
		// threshold warning is the only code path that reads this value and
		// zero is the correct signal that size is unknown.
		FileSize:     0,
		Format:       input.Format,
		Reader:       input.Content,
		ExtractionID: extractionID,
	}

	state, err := uc.prepareIngestion(ctx, startInput, span)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "trusted stream prepare failed", err)

		return nil, 0, fmt.Errorf("prepare trusted stream ingestion: %w", err)
	}

	defer uc.cleanupOnFailure(ctx, state)

	if processErr := uc.processIngestionFile(ctx, state); processErr != nil {
		libOpentelemetry.HandleSpanError(span, "trusted stream processing failed", processErr)

		return nil, 0, uc.failJob(ctx, state.job, processErr, state.markedHashes)
	}

	state.succeeded = true

	completedJob, completeErr := uc.completeIngestionJob(ctx, state, span)
	if completeErr != nil {
		libOpentelemetry.HandleSpanError(span, "trusted stream completion failed", completeErr)

		return nil, 0, fmt.Errorf("complete trusted stream ingestion: %w", completeErr)
	}

	// Clear dedup keys on success so legitimate re-deliveries from the bridge
	// are not silently suppressed beyond the configured TTL window.
	uc.clearDedupKeys(ctx, state)

	return completedJob, state.totalInserted, nil
}
