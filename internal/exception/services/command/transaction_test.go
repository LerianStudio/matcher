//go:build unit

package command

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

var errTestBeginTx = errors.New("test: begin transaction failed")

// AdjustEntry transaction tests.
func TestAdjustEntry_BeginTxError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}
	infra := &stubInfraProvider{txErr: errTestBeginTx}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), infra)
	require.NoError(t, err)

	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "test note",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
		EffectiveAt: time.Now().UTC(),
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin transaction")
}

func TestAdjustEntry_CommitError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}

	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit().WillReturnError(errors.New("test: commit failed"))

	infra := &stubInfraProvider{tx: tx}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), infra)
	require.NoError(t, err)

	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "test note",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
		EffectiveAt: time.Now().UTC(),
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit transaction")
}

// ForceMatch transaction tests.
func TestForceMatch_BeginTxError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}
	infra := &stubInfraProvider{txErr: errTestBeginTx}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), infra)
	require.NoError(t, err)

	_, err = uc.ForceMatch(ctx, ForceMatchCommand{
		ExceptionID:    exception.ID,
		OverrideReason: "POLICY_EXCEPTION",
		Notes:          "test note",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin transaction")
}

func TestForceMatch_CommitError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}

	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit().WillReturnError(errors.New("test: commit failed"))

	infra := &stubInfraProvider{tx: tx}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), infra)
	require.NoError(t, err)

	_, err = uc.ForceMatch(ctx, ForceMatchCommand{
		ExceptionID:    exception.ID,
		OverrideReason: "POLICY_EXCEPTION",
		Notes:          "test note",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit transaction")
}

func TestForceMatch_UpdateError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception, updateErr: errTestUpdate}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), &stubInfraProvider{})
	require.NoError(t, err)

	_, err = uc.ForceMatch(ctx, ForceMatchCommand{
		ExceptionID:    exception.ID,
		OverrideReason: "POLICY_EXCEPTION",
		Notes:          "test note",
	})

	require.ErrorIs(t, err, errTestUpdate)
}

// OpenDispute transaction tests.
func TestOpenDispute_BeginTxError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{}
	audit := &stubAuditPublisher{}
	infra := &stubInfraProvider{txErr: errTestBeginTx}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		infra,
	)
	require.NoError(t, err)

	_, err = uc.OpenDispute(ctx, OpenDisputeCommand{
		ExceptionID: exception.ID,
		Category:    "BANK_FEE_ERROR",
		Description: "test description",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin transaction")
}

func TestOpenDispute_CommitError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{}
	audit := &stubAuditPublisher{}

	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit().WillReturnError(errors.New("test: commit failed"))

	infra := &stubInfraProvider{tx: tx}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		infra,
	)
	require.NoError(t, err)

	_, err = uc.OpenDispute(ctx, OpenDisputeCommand{
		ExceptionID: exception.ID,
		Category:    "BANK_FEE_ERROR",
		Description: "test description",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit transaction")
}

// CloseDispute transaction tests.
func TestCloseDispute_BeginTxError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()
	existingDispute := createTestDispute(ctx, t, exceptionID)

	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{dispute: existingDispute}
	audit := &stubAuditPublisher{}
	infra := &stubInfraProvider{txErr: errTestBeginTx}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		infra,
	)
	require.NoError(t, err)

	_, err = uc.CloseDispute(ctx, CloseDisputeCommand{
		DisputeID:  existingDispute.ID,
		Resolution: "resolved",
		Won:        true,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin transaction")
}

func TestCloseDispute_CommitError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()
	existingDispute := createTestDispute(ctx, t, exceptionID)

	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{dispute: existingDispute}
	audit := &stubAuditPublisher{}

	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit().WillReturnError(errors.New("test: commit failed"))

	infra := &stubInfraProvider{tx: tx}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		infra,
	)
	require.NoError(t, err)

	_, err = uc.CloseDispute(ctx, CloseDisputeCommand{
		DisputeID:  existingDispute.ID,
		Resolution: "resolved",
		Won:        true,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit transaction")
}

// SubmitEvidence transaction tests.
func TestSubmitEvidence_BeginTxError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()
	existingDispute := createTestDispute(ctx, t, exceptionID)

	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{dispute: existingDispute}
	audit := &stubAuditPublisher{}
	infra := &stubInfraProvider{txErr: errTestBeginTx}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		infra,
	)
	require.NoError(t, err)

	_, err = uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
		DisputeID: existingDispute.ID,
		Comment:   "test comment",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin transaction")
}

func TestSubmitEvidence_CommitError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()
	existingDispute := createTestDispute(ctx, t, exceptionID)

	exception, err := entities.NewException(
		ctx,
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{dispute: existingDispute}
	audit := &stubAuditPublisher{}

	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit().WillReturnError(errors.New("test: commit failed"))

	infra := &stubInfraProvider{tx: tx}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		infra,
	)
	require.NoError(t, err)

	_, err = uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
		DisputeID: existingDispute.ID,
		Comment:   "test comment",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit transaction")
}

// Test FileURL parsing variations.
func TestSubmitEvidence_FileURLVariations(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		fileURL         *string
		expectInAudit   bool
		expectedFileURL *string
	}{
		{
			name:            "nil file URL",
			fileURL:         nil,
			expectInAudit:   false,
			expectedFileURL: nil,
		},
		{
			name:            "empty file URL",
			fileURL:         ptrString(""),
			expectInAudit:   false,
			expectedFileURL: nil,
		},
		{
			name:            "whitespace file URL",
			fileURL:         ptrString("   "),
			expectInAudit:   false,
			expectedFileURL: nil,
		},
		{
			name:            "valid file URL",
			fileURL:         ptrString("https://example.com/file.pdf"),
			expectInAudit:   true,
			expectedFileURL: ptrString("https://example.com/file.pdf"),
		},
		{
			name:            "file URL with whitespace",
			fileURL:         ptrString("  https://example.com/file.pdf  "),
			expectInAudit:   true,
			expectedFileURL: ptrString("https://example.com/file.pdf"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			exceptionID := uuid.New()
			existingDispute := createTestDispute(ctx, t, exceptionID)

			exception, err := entities.NewException(
				ctx,
				uuid.New(),
				value_objects.ExceptionSeverityHigh,
				nil,
			)
			require.NoError(t, err)

			exceptionRepo := &stubExceptionRepo{exception: exception}
			disputeRepo := &stubDisputeRepo{dispute: existingDispute}
			audit := &stubAuditPublisher{}

			tx, mock, err := newMockTx(ctx)
			require.NoError(t, err)
			mock.ExpectCommit()

			infra := &stubInfraProvider{tx: tx}

			uc, err := NewDisputeUseCase(
				disputeRepo,
				exceptionRepo,
				audit,
				actorExtractor("analyst-1"),
				infra,
			)
			require.NoError(t, err)

			result, err := uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
				DisputeID: existingDispute.ID,
				Comment:   "test comment",
				FileURL:   tc.fileURL,
			})

			require.NoError(t, err)
			require.NotNil(t, result)
			require.NotNil(t, audit.lastEvent)

			if tc.expectInAudit {
				assert.NotEmpty(t, audit.lastEvent.Metadata["file_url"])
			} else {
				assert.Empty(t, audit.lastEvent.Metadata["file_url"])
			}
		})
	}
}

func ptrString(s string) *string {
	return &s
}
