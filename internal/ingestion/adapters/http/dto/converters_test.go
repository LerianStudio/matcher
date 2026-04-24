// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestJobToResponse(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	completedAt := now.Add(time.Hour)

	tests := []struct {
		name     string
		job      *entities.IngestionJob
		expected JobResponse
	}{
		{
			name:     "nil job returns empty response",
			job:      nil,
			expected: JobResponse{},
		},
		{
			name: "valid job converts all fields correctly",
			job: &entities.IngestionJob{
				ID:          uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID:   uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				SourceID:    uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				Status:      value_objects.JobStatusProcessing,
				StartedAt:   now,
				CompletedAt: &completedAt,
				Metadata: entities.JobMetadata{
					FileName:   "test.csv",
					TotalRows:  100,
					FailedRows: 5,
				},
				CreatedAt: now,
			},
			expected: JobResponse{
				ID:          "11111111-1111-1111-1111-111111111111",
				ContextID:   "22222222-2222-2222-2222-222222222222",
				SourceID:    "33333333-3333-3333-3333-333333333333",
				Status:      "PROCESSING",
				FileName:    "test.csv",
				TotalRows:   100,
				FailedRows:  5,
				StartedAt:   ptrString(now.Format(time.RFC3339)),
				CompletedAt: ptrString(completedAt.Format(time.RFC3339)),
				CreatedAt:   now.Format(time.RFC3339),
			},
		},
		{
			name: "completedAt nil is handled correctly",
			job: &entities.IngestionJob{
				ID:          uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID:   uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				SourceID:    uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				Status:      value_objects.JobStatusQueued,
				StartedAt:   now,
				CompletedAt: nil,
				CreatedAt:   now,
			},
			expected: JobResponse{
				ID:          "11111111-1111-1111-1111-111111111111",
				ContextID:   "22222222-2222-2222-2222-222222222222",
				SourceID:    "33333333-3333-3333-3333-333333333333",
				Status:      "QUEUED",
				StartedAt:   ptrString(now.Format(time.RFC3339)),
				CompletedAt: nil,
				CreatedAt:   now.Format(time.RFC3339),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := JobToResponse(tc.job)
			assert.Equal(t, tc.expected.ID, result.ID)
			assert.Equal(t, tc.expected.ContextID, result.ContextID)
			assert.Equal(t, tc.expected.SourceID, result.SourceID)
			assert.Equal(t, tc.expected.Status, result.Status)
			assert.Equal(t, tc.expected.FileName, result.FileName)
			assert.Equal(t, tc.expected.TotalRows, result.TotalRows)
			assert.Equal(t, tc.expected.FailedRows, result.FailedRows)
			assert.Equal(t, tc.expected.StartedAt, result.StartedAt)
			assert.Equal(t, tc.expected.CompletedAt, result.CompletedAt)
			assert.Equal(t, tc.expected.CreatedAt, result.CreatedAt)
		})
	}
}

func TestJobsToResponse(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		name     string
		jobs     []*entities.IngestionJob
		expected int
	}{
		{
			name:     "empty slice returns empty slice",
			jobs:     []*entities.IngestionJob{},
			expected: 0,
		},
		{
			name:     "nil slice returns empty slice",
			jobs:     nil,
			expected: 0,
		},
		{
			name: "filters out nil jobs",
			jobs: []*entities.IngestionJob{
				{
					ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					SourceID:  uuid.MustParse("33333333-3333-3333-3333-333333333333"),
					Status:    value_objects.JobStatusCompleted,
					StartedAt: now,
					CreatedAt: now,
				},
				nil,
				{
					ID:        uuid.MustParse("44444444-4444-4444-4444-444444444444"),
					ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					SourceID:  uuid.MustParse("33333333-3333-3333-3333-333333333333"),
					Status:    value_objects.JobStatusQueued,
					StartedAt: now,
					CreatedAt: now,
				},
			},
			expected: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := JobsToResponse(tc.jobs)
			assert.NotNil(t, result)
			assert.Len(t, result, tc.expected)
		})
	}
}

func TestTransactionToResponse(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	txDate := now.Add(-24 * time.Hour)

	jobID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	contextID := uuid.MustParse("44444444-4444-4444-4444-444444444444")

	tests := []struct {
		name      string
		tx        *shared.Transaction
		jobID     uuid.UUID
		contextID uuid.UUID
		expected  TransactionResponse
	}{
		{
			name:      "nil transaction returns empty response",
			tx:        nil,
			jobID:     jobID,
			contextID: contextID,
			expected:  TransactionResponse{},
		},
		{
			name: "valid transaction converts all fields correctly",
			tx: &shared.Transaction{
				ID:               uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				SourceID:         uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				ExternalID:       "ext-123",
				Amount:           decimal.NewFromFloat(1234.56),
				Currency:         "USD",
				Date:             txDate,
				Description:      "Test transaction",
				Status:           shared.TransactionStatusUnmatched,
				ExtractionStatus: shared.ExtractionStatusPending,
				CreatedAt:        now,
			},
			jobID:     jobID,
			contextID: contextID,
			expected: TransactionResponse{
				ID:               "11111111-1111-1111-1111-111111111111",
				JobID:            jobID.String(),
				SourceID:         "22222222-2222-2222-2222-222222222222",
				ContextID:        contextID.String(),
				ExternalID:       "ext-123",
				Amount:           "1234.56",
				Currency:         "USD",
				Date:             txDate.Format(time.RFC3339),
				Description:      "Test transaction",
				Status:           "UNMATCHED",
				ExtractionStatus: "PENDING",
				CreatedAt:        now.Format(time.RFC3339),
			},
		},
		{
			name: "amount decimal is converted to string",
			tx: &shared.Transaction{
				ID:               uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				SourceID:         uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				ExternalID:       "ext-456",
				Amount:           decimal.NewFromFloat(9999.99),
				Currency:         "EUR",
				Date:             txDate,
				Status:           shared.TransactionStatusMatched,
				ExtractionStatus: shared.ExtractionStatusComplete,
				CreatedAt:        now,
			},
			jobID:     jobID,
			contextID: contextID,
			expected: TransactionResponse{
				ID:               "11111111-1111-1111-1111-111111111111",
				JobID:            jobID.String(),
				SourceID:         "22222222-2222-2222-2222-222222222222",
				ContextID:        contextID.String(),
				ExternalID:       "ext-456",
				Amount:           "9999.99",
				Currency:         "EUR",
				Date:             txDate.Format(time.RFC3339),
				Status:           "MATCHED",
				ExtractionStatus: "COMPLETE",
				CreatedAt:        now.Format(time.RFC3339),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := TransactionToResponse(tc.tx, tc.jobID, tc.contextID)
			assert.Equal(t, tc.expected.ID, result.ID)
			assert.Equal(t, tc.expected.JobID, result.JobID)
			assert.Equal(t, tc.expected.SourceID, result.SourceID)
			assert.Equal(t, tc.expected.ContextID, result.ContextID)
			assert.Equal(t, tc.expected.ExternalID, result.ExternalID)
			assert.Equal(t, tc.expected.Amount, result.Amount)
			assert.Equal(t, tc.expected.Currency, result.Currency)
			assert.Equal(t, tc.expected.Date, result.Date)
			assert.Equal(t, tc.expected.Description, result.Description)
			assert.Equal(t, tc.expected.Status, result.Status)
			assert.Equal(t, tc.expected.ExtractionStatus, result.ExtractionStatus)
			assert.Equal(t, tc.expected.CreatedAt, result.CreatedAt)
		})
	}
}

func TestTransactionsToResponse(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	jobID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	contextID := uuid.MustParse("44444444-4444-4444-4444-444444444444")

	tests := []struct {
		name     string
		txs      []*shared.Transaction
		expected int
	}{
		{
			name:     "empty slice returns empty slice",
			txs:      []*shared.Transaction{},
			expected: 0,
		},
		{
			name:     "nil slice returns empty slice",
			txs:      nil,
			expected: 0,
		},
		{
			name: "filters out nil transactions",
			txs: []*shared.Transaction{
				{
					ID:               uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					SourceID:         uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					ExternalID:       "ext-1",
					Amount:           decimal.NewFromInt(100),
					Currency:         "USD",
					Date:             now,
					Status:           shared.TransactionStatusUnmatched,
					ExtractionStatus: shared.ExtractionStatusPending,
					CreatedAt:        now,
				},
				nil,
				{
					ID:               uuid.MustParse("55555555-5555-5555-5555-555555555555"),
					SourceID:         uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					ExternalID:       "ext-2",
					Amount:           decimal.NewFromInt(200),
					Currency:         "EUR",
					Date:             now,
					Status:           shared.TransactionStatusMatched,
					ExtractionStatus: shared.ExtractionStatusComplete,
					CreatedAt:        now,
				},
			},
			expected: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := TransactionsToResponse(tc.txs, jobID, contextID)
			assert.NotNil(t, result)
			assert.Len(t, result, tc.expected)
		})
	}
}

func ptrString(s string) *string {
	return &s
}
