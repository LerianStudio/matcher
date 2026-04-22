// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

// Test helpers for this file live in trusted_stream_integration_helpers_test.go.
// That file owns the stub types (dedup, field map, source, publisher) so the
// scenario tests here stay focused on the intake flow under test.
package command_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/adapters/parsers"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTransactionRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	ingestionVO "github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/tests/integration"
)

// TestIntegration_UseCase_IngestFromTrustedStream covers Scenario 1 of the
// T-001 integration plan: the trusted-stream bridge entry point drives the
// same real-infrastructure pipeline the upload path uses (real Postgres via
// testcontainers + real outbox repository). It proves AC-F2 at integration
// scope — the trusted stream pipeline reuses ingestion business behavior
// rather than inventing a separate path.
func TestIntegration_UseCase_IngestFromTrustedStream(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		provider := h.Provider()
		jobRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTransactionRepo.NewRepository(provider)
		outboxRepo := integration.NewTestOutboxRepository(t, h.Connection)

		// Minimal field map covering the canonical columns; matches the
		// pattern established by use_case_test.go in tests/integration/ingestion.
		fieldMap := &shared.FieldMap{Mapping: map[string]any{
			"external_id": "id",
			"amount":      "amount",
			"currency":    "currency",
			"date":        "date",
		}}
		fieldMapRepo := &trustedStreamFieldMapStub{fieldMap: fieldMap}

		contextID := h.Seed.ContextID
		sourceID := h.Seed.SourceID
		sourceRepo := newTrustedStreamSourceStub(contextID, sourceID)

		registry := parsers.NewParserRegistry()
		registry.Register(parsers.NewCSVParser())

		useCase, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
			JobRepo:         jobRepo,
			TransactionRepo: txRepo,
			Dedupe:          &trustedStreamFakeDedupe{},
			Publisher:       &trustedStreamPublisher{},
			OutboxRepo:      outboxRepo,
			Parsers:         registry,
			FieldMapRepo:    fieldMapRepo,
			SourceRepo:      sourceRepo,
			DedupeTTL:       0,
		})
		require.NoError(t, err)

		// Two rows drive TotalRows=2 on the persisted IngestionJob and produce
		// a single ingestion.completed outbox event via the shared pipeline.
		csvData := "id,amount,currency,date\n" +
			"trusted-1,12.50,USD,2024-03-01\n" +
			"trusted-2,20.00,USD,2024-03-02\n"

		output, err := useCase.IngestFromTrustedStream(
			context.Background(),
			sharedPorts.TrustedContentInput{
				ContextID: contextID,
				SourceID:  sourceID,
				Format:    "csv",
				Content:   strings.NewReader(csvData),
				SourceMetadata: map[string]string{
					"filename": "trusted-intake.csv",
				},
			},
		)
		require.NoError(t, err)
		require.NotNil(t, output)
		require.NotEmpty(t, output.IngestionJobID)
		require.Equal(t, 2, output.TransactionCount, "both rows must insert as new transactions")

		// IngestionJob is persisted and reached the terminal COMPLETED state.
		persistedJob, err := jobRepo.FindByID(context.Background(), output.IngestionJobID)
		require.NoError(t, err)
		require.NotNil(t, persistedJob)
		require.Equal(t, ingestionVO.JobStatusCompleted, persistedJob.Status)
		require.Equal(t, 2, persistedJob.Metadata.TotalRows)
		require.Equal(t, 0, persistedJob.Metadata.FailedRows)

		// Exactly one ingestion.completed outbox event was written in the
		// same transaction as the job completion.
		pending, err := outboxRepo.ListPending(context.Background(), 10)
		require.NoError(t, err)
		require.Len(t, pending, 1, "trusted stream must produce a single outbox event")
		require.Equal(t, ingestionEntities.EventTypeIngestionCompleted, pending[0].EventType)
		require.Equal(t, output.IngestionJobID, pending[0].AggregateID)
	})
}

// TestIntegration_UseCase_IngestFile_UploadPathStillWorks covers Scenario 3
// of the T-001 integration plan: the pre-existing upload path (StartIngestion)
// continues to function identically after the T-001 refactor that extracted
// trusted_stream_pipeline.go. It guards AC-Q1 — the split of the pipeline
// into a shared helper must not change observable behavior of the upload
// code path.
func TestIntegration_UseCase_IngestFile_UploadPathStillWorks(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		provider := h.Provider()
		jobRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTransactionRepo.NewRepository(provider)
		outboxRepo := integration.NewTestOutboxRepository(t, h.Connection)

		fieldMap := &shared.FieldMap{Mapping: map[string]any{
			"external_id": "id",
			"amount":      "amount",
			"currency":    "currency",
			"date":        "date",
		}}
		fieldMapRepo := &trustedStreamFieldMapStub{fieldMap: fieldMap}

		contextID := h.Seed.ContextID
		sourceID := h.Seed.SourceID
		sourceRepo := newTrustedStreamSourceStub(contextID, sourceID)

		registry := parsers.NewParserRegistry()
		registry.Register(parsers.NewCSVParser())

		useCase, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
			JobRepo:         jobRepo,
			TransactionRepo: txRepo,
			Dedupe:          &trustedStreamFakeDedupe{},
			Publisher:       &trustedStreamPublisher{},
			OutboxRepo:      outboxRepo,
			Parsers:         registry,
			FieldMapRepo:    fieldMapRepo,
			SourceRepo:      sourceRepo,
			DedupeTTL:       0,
		})
		require.NoError(t, err)

		csvData := "id,amount,currency,date\n" +
			"upload-1,50.00,USD,2024-03-05\n"

		job, err := useCase.StartIngestion(
			context.Background(),
			contextID,
			sourceID,
			"upload.csv",
			int64(len(csvData)),
			"csv",
			strings.NewReader(csvData),
		)
		require.NoError(t, err)
		require.NotNil(t, job)
		require.Equal(t, ingestionVO.JobStatusCompleted, job.Status)
		require.Equal(t, 1, job.Metadata.TotalRows)
		require.Equal(t, 0, job.Metadata.FailedRows)

		// Upload path produces the same outbox shape as the trusted-stream path.
		pending, err := outboxRepo.ListPending(context.Background(), 10)
		require.NoError(t, err)
		require.Len(t, pending, 1, "upload path must still produce exactly one outbox event")
		require.Equal(t, ingestionEntities.EventTypeIngestionCompleted, pending[0].EventType)
	})
}
