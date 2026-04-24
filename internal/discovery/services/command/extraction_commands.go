// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	discoveryMetrics "github.com/LerianStudio/matcher/internal/discovery/services/metrics"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// StartExtraction submits a data extraction job to Fetcher.
// It creates an ExtractionRequest record to track the job lifecycle and delegates
// the actual extraction to the Fetcher service via the FetcherClient port.
// The ingestionJobID is not required; extraction requests can exist independently
// and be linked to ingestion jobs later when the data is imported.
//
//nolint:cyclop,gocyclo // workflow branches explicitly on connection ownership, persistence, and remote submission outcomes.
func (uc *UseCase) StartExtraction(
	ctx context.Context,
	connectionID uuid.UUID,
	tables map[string]any,
	params sharedPorts.ExtractionParams,
) (*entities.ExtractionRequest, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "command.discovery.start_extraction")
	defer span.End()

	if err := validateExtractionRequest(tables, params); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "validate extraction request", err)

		return nil, err
	}

	if !uc.fetcherClient.IsHealthy(ctx) {
		libOpentelemetry.HandleSpanError(span, "fetcher unavailable", ErrFetcherUnavailable)

		return nil, ErrFetcherUnavailable
	}

	conn, err := uc.connRepo.FindByID(ctx, connectionID)
	if err != nil {
		if errors.Is(err, repositories.ErrConnectionNotFound) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "connection not found", err)

			return nil, ErrConnectionNotFound
		}

		libOpentelemetry.HandleSpanError(span, "find connection", err)

		return nil, fmt.Errorf("find connection: %w", err)
	}

	if conn == nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "connection not found", ErrConnectionNotFound)

		return nil, ErrConnectionNotFound
	}

	if conn.Status != vo.ConnectionStatusAvailable || !conn.SchemaDiscovered {
		err = fmt.Errorf("%w: connection schema is not available for extraction", ErrInvalidExtractionRequest)
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "connection not ready for extraction", err)

		return nil, err
	}

	schemas, err := uc.schemaRepo.FindByConnectionID(ctx, connectionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "find connection schema", err)

		return nil, fmt.Errorf("find connection schema: %w", err)
	}

	if err := validateExtractionScope(tables, schemas); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "validate extraction scope", err)

		return nil, err
	}

	extractionReq, err := entities.NewExtractionRequest(ctx, connectionID, tables, params.StartDate, params.EndDate, params.Filters.ToMap())
	if err != nil {
		return nil, fmt.Errorf("create extraction request: %w", err)
	}

	if err := uc.extractionRepo.Create(ctx, extractionReq); err != nil {
		return nil, fmt.Errorf("persist pending extraction request: %w", err)
	}

	input, inputErr := buildExtractionJobInput(conn, tables, params)
	if inputErr != nil {
		return nil, inputErr
	}

	jobID, err := uc.fetcherClient.SubmitExtractionJob(ctx, input)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "submit extraction job", err)

		if markErr := extractionReq.MarkFailed(entities.SanitizedExtractionFailureMessage); markErr == nil {
			discoveryMetrics.RecordExtractionState(ctx, extractionReq.Status.String())

			if updateErr := uc.extractionRepo.Update(ctx, extractionReq); updateErr != nil {
				libOpentelemetry.HandleSpanError(span, "persist failed extraction request", updateErr)
			}
		}

		return nil, fmt.Errorf("submit extraction job: %w", err)
	}

	if err := extractionReq.MarkSubmitted(jobID); err != nil {
		return nil, fmt.Errorf("mark extraction submitted: %w", err)
	}

	discoveryMetrics.RecordExtractionState(ctx, extractionReq.Status.String())

	if err := uc.persistSubmittedExtraction(ctx, span, extractionReq); err != nil {
		return nil, fmt.Errorf("persist submitted extraction request: %w", err)
	}

	// Start async polling for extraction completion (if poller configured).
	if uc.extractionPoller != nil {
		// Polling must outlive the originating HTTP request context, otherwise
		// client cancellation aborts extraction tracking mid-flight.
		uc.extractionPoller.PollUntilComplete(context.WithoutCancel(ctx), extractionReq.ID, nil, nil)
	}

	return extractionReq, nil
}
