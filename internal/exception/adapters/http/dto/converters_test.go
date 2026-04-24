// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	"github.com/LerianStudio/matcher/internal/testutil"
)

func TestExceptionToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *entities.Exception
		expected ExceptionResponse
	}{
		{
			name:     "nil input returns empty struct",
			input:    nil,
			expected: ExceptionResponse{},
		},
		{
			name: "full entity conversion",
			input: &entities.Exception{
				ID:            uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				TransactionID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Severity:      sharedexception.ExceptionSeverityHigh,
				Status:        value_objects.ExceptionStatusOpen,
				CreatedAt:     time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt:     time.Date(2025, 1, 15, 10, 35, 0, 0, time.UTC),
			},
			expected: ExceptionResponse{
				ID:            "11111111-1111-1111-1111-111111111111",
				TransactionID: "22222222-2222-2222-2222-222222222222",
				Severity:      "HIGH",
				Status:        "OPEN",
				CreatedAt:     "2025-01-15T10:30:00Z",
				UpdatedAt:     "2025-01-15T10:35:00Z",
			},
		},
		{
			name: "with optional fields",
			input: &entities.Exception{
				ID:               uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				TransactionID:    uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Severity:         sharedexception.ExceptionSeverityCritical,
				Status:           value_objects.ExceptionStatusResolved,
				ExternalSystem:   testutil.StringPtr("JIRA"),
				ExternalIssueID:  testutil.StringPtr("RECON-123"),
				AssignedTo:       testutil.StringPtr("user@example.com"),
				DueAt:            testutil.TimePtr(time.Date(2025, 1, 20, 10, 30, 0, 0, time.UTC)),
				ResolutionNotes:  testutil.StringPtr("Resolved via force match"),
				ResolutionType:   testutil.StringPtr("FORCE_MATCH"),
				ResolutionReason: testutil.StringPtr("BUSINESS_DECISION"),
				Reason:           testutil.StringPtr("Amount mismatch"),
				CreatedAt:        time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt:        time.Date(2025, 1, 15, 10, 35, 0, 0, time.UTC),
			},
			expected: ExceptionResponse{
				ID:               "11111111-1111-1111-1111-111111111111",
				TransactionID:    "22222222-2222-2222-2222-222222222222",
				Severity:         "CRITICAL",
				Status:           "RESOLVED",
				ExternalSystem:   testutil.StringPtr("JIRA"),
				ExternalIssueID:  testutil.StringPtr("RECON-123"),
				AssignedTo:       testutil.StringPtr("user@example.com"),
				DueAt:            testutil.StringPtr("2025-01-20T10:30:00Z"),
				ResolutionNotes:  testutil.StringPtr("Resolved via force match"),
				ResolutionType:   testutil.StringPtr("FORCE_MATCH"),
				ResolutionReason: testutil.StringPtr("BUSINESS_DECISION"),
				Reason:           testutil.StringPtr("Amount mismatch"),
				CreatedAt:        "2025-01-15T10:30:00Z",
				UpdatedAt:        "2025-01-15T10:35:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := ExceptionToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExceptionsToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []*entities.Exception
		expected []ExceptionResponse
	}{
		{
			name:     "nil slice returns empty slice",
			input:    nil,
			expected: []ExceptionResponse{},
		},
		{
			name:     "empty slice returns empty slice",
			input:    []*entities.Exception{},
			expected: []ExceptionResponse{},
		},
		{
			name: "filters nil elements",
			input: []*entities.Exception{
				{
					ID:            uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					TransactionID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					Severity:      sharedexception.ExceptionSeverityLow,
					Status:        value_objects.ExceptionStatusOpen,
					CreatedAt:     time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
					UpdatedAt:     time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				},
				nil,
			},
			expected: []ExceptionResponse{
				{
					ID:            "11111111-1111-1111-1111-111111111111",
					TransactionID: "22222222-2222-2222-2222-222222222222",
					Severity:      "LOW",
					Status:        "OPEN",
					CreatedAt:     "2025-01-15T10:30:00Z",
					UpdatedAt:     "2025-01-15T10:30:00Z",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := ExceptionsToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvidenceToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *dispute.Evidence
		expected EvidenceResponse
	}{
		{
			name:     "nil input returns empty struct",
			input:    nil,
			expected: EvidenceResponse{},
		},
		{
			name: "full entity conversion",
			input: &dispute.Evidence{
				ID:          uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				DisputeID:   uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Comment:     "Bank statement attached",
				SubmittedBy: "user@example.com",
				FileURL:     testutil.StringPtr("https://storage.example.com/doc.pdf"),
				SubmittedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: EvidenceResponse{
				ID:          "11111111-1111-1111-1111-111111111111",
				DisputeID:   "22222222-2222-2222-2222-222222222222",
				Comment:     "Bank statement attached",
				SubmittedBy: "user@example.com",
				FileURL:     testutil.StringPtr("https://storage.example.com/doc.pdf"),
				SubmittedAt: "2025-01-15T10:30:00Z",
			},
		},
		{
			name: "without file URL",
			input: &dispute.Evidence{
				ID:          uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				DisputeID:   uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Comment:     "Comment only",
				SubmittedBy: "user@example.com",
				SubmittedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: EvidenceResponse{
				ID:          "11111111-1111-1111-1111-111111111111",
				DisputeID:   "22222222-2222-2222-2222-222222222222",
				Comment:     "Comment only",
				SubmittedBy: "user@example.com",
				SubmittedAt: "2025-01-15T10:30:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := EvidenceToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvidenceListToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []dispute.Evidence
		expected []EvidenceResponse
	}{
		{
			name:     "nil slice returns empty slice",
			input:    nil,
			expected: []EvidenceResponse{},
		},
		{
			name:     "empty slice returns empty slice",
			input:    []dispute.Evidence{},
			expected: []EvidenceResponse{},
		},
		{
			name: "converts all elements",
			input: []dispute.Evidence{
				{
					ID:          uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					DisputeID:   uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					Comment:     "Evidence 1",
					SubmittedBy: "user@example.com",
					SubmittedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				},
			},
			expected: []EvidenceResponse{
				{
					ID:          "11111111-1111-1111-1111-111111111111",
					DisputeID:   "22222222-2222-2222-2222-222222222222",
					Comment:     "Evidence 1",
					SubmittedBy: "user@example.com",
					SubmittedAt: "2025-01-15T10:30:00Z",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := EvidenceListToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDisputeToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *dispute.Dispute
		expected DisputeResponse
	}{
		{
			name:  "nil input returns empty struct with empty evidence slice",
			input: nil,
			expected: DisputeResponse{
				Evidence: []EvidenceResponse{},
			},
		},
		{
			name: "full entity conversion",
			input: &dispute.Dispute{
				ID:          uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ExceptionID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Category:    dispute.DisputeCategoryOther,
				State:       dispute.DisputeStateOpen,
				Description: "Transaction amount differs",
				OpenedBy:    "user@example.com",
				Evidence:    []dispute.Evidence{},
				CreatedAt:   time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt:   time.Date(2025, 1, 15, 10, 35, 0, 0, time.UTC),
			},
			expected: DisputeResponse{
				ID:          "11111111-1111-1111-1111-111111111111",
				ExceptionID: "22222222-2222-2222-2222-222222222222",
				Category:    "OTHER",
				State:       "OPEN",
				Description: "Transaction amount differs",
				OpenedBy:    "user@example.com",
				Evidence:    []EvidenceResponse{},
				CreatedAt:   "2025-01-15T10:30:00Z",
				UpdatedAt:   "2025-01-15T10:35:00Z",
			},
		},
		{
			name: "with resolution and evidence",
			input: &dispute.Dispute{
				ID:           uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ExceptionID:  uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Category:     dispute.DisputeCategoryBankFeeError,
				State:        dispute.DisputeStateWon,
				Description:  "Bank fee dispute",
				OpenedBy:     "user@example.com",
				Resolution:   testutil.StringPtr("Bank refunded the fee"),
				ReopenReason: nil,
				Evidence: []dispute.Evidence{
					{
						ID:          uuid.MustParse("33333333-3333-3333-3333-333333333333"),
						DisputeID:   uuid.MustParse("11111111-1111-1111-1111-111111111111"),
						Comment:     "Bank statement",
						SubmittedBy: "user@example.com",
						SubmittedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
					},
				},
				CreatedAt: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt: time.Date(2025, 1, 15, 10, 35, 0, 0, time.UTC),
			},
			expected: DisputeResponse{
				ID:          "11111111-1111-1111-1111-111111111111",
				ExceptionID: "22222222-2222-2222-2222-222222222222",
				Category:    "BANK_FEE_ERROR",
				State:       "WON",
				Description: "Bank fee dispute",
				OpenedBy:    "user@example.com",
				Resolution:  testutil.StringPtr("Bank refunded the fee"),
				Evidence: []EvidenceResponse{
					{
						ID:          "33333333-3333-3333-3333-333333333333",
						DisputeID:   "11111111-1111-1111-1111-111111111111",
						Comment:     "Bank statement",
						SubmittedBy: "user@example.com",
						SubmittedAt: "2025-01-15T10:30:00Z",
					},
				},
				CreatedAt: "2025-01-15T10:30:00Z",
				UpdatedAt: "2025-01-15T10:35:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := DisputeToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDisputesToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []*dispute.Dispute
		expected []DisputeResponse
	}{
		{
			name:     "nil slice returns empty slice",
			input:    nil,
			expected: []DisputeResponse{},
		},
		{
			name:     "empty slice returns empty slice",
			input:    []*dispute.Dispute{},
			expected: []DisputeResponse{},
		},
		{
			name: "filters nil elements",
			input: []*dispute.Dispute{
				{
					ID:          uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					ExceptionID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					Category:    dispute.DisputeCategoryOther,
					State:       dispute.DisputeStateOpen,
					Description: "Test",
					OpenedBy:    "user@example.com",
					Evidence:    []dispute.Evidence{},
					CreatedAt:   time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
					UpdatedAt:   time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
				},
				nil,
			},
			expected: []DisputeResponse{
				{
					ID:          "11111111-1111-1111-1111-111111111111",
					ExceptionID: "22222222-2222-2222-2222-222222222222",
					Category:    "OTHER",
					State:       "OPEN",
					Description: "Test",
					OpenedBy:    "user@example.com",
					Evidence:    []EvidenceResponse{},
					CreatedAt:   "2025-01-15T10:30:00Z",
					UpdatedAt:   "2025-01-15T10:30:00Z",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := DisputesToResponse(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDisputeToResponse_AllCategories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		category dispute.DisputeCategory
		expected string
	}{
		{dispute.DisputeCategoryBankFeeError, "BANK_FEE_ERROR"},
		{dispute.DisputeCategoryUnrecognizedCharge, "UNRECOGNIZED_CHARGE"},
		{dispute.DisputeCategoryDuplicateTransaction, "DUPLICATE_TRANSACTION"},
		{dispute.DisputeCategoryOther, "OTHER"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()

			disputeEntity := &dispute.Dispute{
				ID:          uuid.New(),
				ExceptionID: uuid.New(),
				Category:    tt.category,
				State:       dispute.DisputeStateOpen,
				Description: "Test",
				OpenedBy:    "user@example.com",
				Evidence:    []dispute.Evidence{},
				CreatedAt:   time.Now(),
				UpdatedAt:   time.Now(),
			}

			result := DisputeToResponse(disputeEntity)
			assert.Equal(t, tt.expected, result.Category)
		})
	}
}

func TestDisputeToResponse_AllStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state    dispute.DisputeState
		expected string
	}{
		{dispute.DisputeStateDraft, "DRAFT"},
		{dispute.DisputeStateOpen, "OPEN"},
		{dispute.DisputeStatePendingEvidence, "PENDING_EVIDENCE"},
		{dispute.DisputeStateWon, "WON"},
		{dispute.DisputeStateLost, "LOST"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()

			disputeEntity := &dispute.Dispute{
				ID:          uuid.New(),
				ExceptionID: uuid.New(),
				Category:    dispute.DisputeCategoryOther,
				State:       tt.state,
				Description: "Test",
				OpenedBy:    "user@example.com",
				Evidence:    []dispute.Evidence{},
				CreatedAt:   time.Now(),
				UpdatedAt:   time.Now(),
			}

			result := DisputeToResponse(disputeEntity)
			assert.Equal(t, tt.expected, result.State)
		})
	}
}
