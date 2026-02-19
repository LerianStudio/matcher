//go:build unit

package exception_creator

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/domain/enums"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func setupRepository(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	finish := func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}

	return repo, mock, finish
}

func TestNewRepository(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	require.NotNil(t, repo)
	assert.NotNil(t, repo.provider)
}

func TestNewRepository_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	require.NotNil(t, repo)
	assert.Nil(t, repo.provider)
}

func TestRepository_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ matchingPorts.ExceptionCreator = (*Repository)(nil)
}

func TestRepository_CreateExceptions_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	inputs := []matchingPorts.ExceptionTransactionInput{
		{TransactionID: uuid.New()},
	}

	err := repo.CreateExceptions(context.Background(), uuid.New(), uuid.New(), inputs, nil)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_CreateExceptions_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	inputs := []matchingPorts.ExceptionTransactionInput{
		{TransactionID: uuid.New()},
	}

	err := repo.CreateExceptions(context.Background(), uuid.New(), uuid.New(), inputs, nil)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_CreateExceptions_EmptyInputs(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err := repo.CreateExceptions(context.Background(), uuid.New(), uuid.New(), nil, nil)
	require.NoError(t, err)
}

func TestRepository_CreateExceptions_EmptySlice(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	inputs := []matchingPorts.ExceptionTransactionInput{}

	err := repo.CreateExceptions(context.Background(), uuid.New(), uuid.New(), inputs, nil)
	require.NoError(t, err)
}

func TestRepository_CreateExceptions_SkipsNilTransactionID(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()

	inputs := []matchingPorts.ExceptionTransactionInput{
		{TransactionID: uuid.Nil, Reason: "UNMATCHED"},
	}

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO exceptions (id, transaction_id, severity, status, reason, created_at, updated_at)
			 VALUES ($1,$2,$3,'OPEN',$4,$5,$6)`,
	)

	mock.ExpectBegin()
	mock.ExpectPrepare(insertQuery)
	mock.ExpectCommit()

	err := repo.CreateExceptions(ctx, contextID, runID, inputs, nil)
	require.NoError(t, err)
}

func TestRepository_CreateExceptions_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	txID := uuid.New()
	txDate := time.Now().UTC().Add(-24 * time.Hour)

	inputs := []matchingPorts.ExceptionTransactionInput{
		{
			TransactionID:   txID,
			AmountAbsBase:   decimal.NewFromInt(100),
			TransactionDate: txDate,
			SourceType:      "FILE",
			FXMissing:       false,
			Reason:          "UNMATCHED",
		},
	}

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO exceptions (id, transaction_id, severity, status, reason, created_at, updated_at)
			 VALUES ($1,$2,$3,'OPEN',$4,$5,$6)`,
	)

	mock.ExpectBegin()
	mock.ExpectPrepare(insertQuery)
	mock.ExpectExec(insertQuery).
		WithArgs(
			sqlmock.AnyArg(),
			txID.String(),
			sqlmock.AnyArg(),
			"UNMATCHED",
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := repo.CreateExceptions(ctx, contextID, runID, inputs, nil)
	require.NoError(t, err)
}

func TestRepository_CreateExceptions_MultipleInputs(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	txID1 := uuid.New()
	txID2 := uuid.New()
	txDate := time.Now().UTC()

	inputs := []matchingPorts.ExceptionTransactionInput{
		{
			TransactionID:   txID1,
			AmountAbsBase:   decimal.NewFromInt(100),
			TransactionDate: txDate,
			SourceType:      "FILE",
			FXMissing:       false,
			Reason:          "UNMATCHED",
		},
		{
			TransactionID:   txID2,
			AmountAbsBase:   decimal.NewFromInt(200),
			TransactionDate: txDate,
			SourceType:      "API",
			FXMissing:       true,
			Reason:          "FX_RATE_UNAVAILABLE",
		},
	}

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO exceptions (id, transaction_id, severity, status, reason, created_at, updated_at)
			 VALUES ($1,$2,$3,'OPEN',$4,$5,$6)`,
	)

	mock.ExpectBegin()
	mock.ExpectPrepare(insertQuery)
	mock.ExpectExec(insertQuery).
		WithArgs(sqlmock.AnyArg(), txID1.String(), sqlmock.AnyArg(), "UNMATCHED", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(insertQuery).
		WithArgs(sqlmock.AnyArg(), txID2.String(), sqlmock.AnyArg(), "FX_RATE_UNAVAILABLE", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := repo.CreateExceptions(ctx, contextID, runID, inputs, nil)
	require.NoError(t, err)
}

func TestRepository_CreateExceptions_PrepareError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	txID := uuid.New()

	inputs := []matchingPorts.ExceptionTransactionInput{
		{TransactionID: txID, Reason: "UNMATCHED"},
	}

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO exceptions (id, transaction_id, severity, status, reason, created_at, updated_at)
			 VALUES ($1,$2,$3,'OPEN',$4,$5,$6)`,
	)

	mock.ExpectBegin()
	mock.ExpectPrepare(insertQuery).WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	err := repo.CreateExceptions(ctx, uuid.New(), uuid.New(), inputs, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "prepare insert exception")
}

func TestRepository_CreateExceptions_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	txID := uuid.New()
	txDate := time.Now().UTC()

	inputs := []matchingPorts.ExceptionTransactionInput{
		{
			TransactionID:   txID,
			AmountAbsBase:   decimal.NewFromInt(100),
			TransactionDate: txDate,
			SourceType:      "FILE",
			FXMissing:       false,
			Reason:          "UNMATCHED",
		},
	}

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO exceptions (id, transaction_id, severity, status, reason, created_at, updated_at)
			 VALUES ($1,$2,$3,'OPEN',$4,$5,$6)`,
	)

	mock.ExpectBegin()
	mock.ExpectPrepare(insertQuery)
	mock.ExpectExec(insertQuery).
		WithArgs(sqlmock.AnyArg(), txID.String(), sqlmock.AnyArg(), "UNMATCHED", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	err := repo.CreateExceptions(ctx, uuid.New(), uuid.New(), inputs, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insert exception")
}

func TestRepository_CreateExceptionsWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	inputs := []matchingPorts.ExceptionTransactionInput{
		{TransactionID: uuid.New()},
	}

	err := repo.CreateExceptionsWithTx(
		context.Background(),
		nil,
		uuid.New(),
		uuid.New(),
		inputs,
		nil,
	)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_CreateExceptionsWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	inputs := []matchingPorts.ExceptionTransactionInput{
		{TransactionID: uuid.New()},
	}

	err := repo.CreateExceptionsWithTx(
		context.Background(),
		nil,
		uuid.New(),
		uuid.New(),
		inputs,
		nil,
	)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_CreateExceptionsWithTx_InvalidTxType(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	invalidTx := &invalidTxMock{}
	inputs := []matchingPorts.ExceptionTransactionInput{
		{TransactionID: uuid.New()},
	}

	err := repo.CreateExceptionsWithTx(
		context.Background(),
		invalidTx,
		uuid.New(),
		uuid.New(),
		inputs,
		nil,
	)
	require.ErrorIs(t, err, ErrInvalidTx)
}

func TestRepository_CreateExceptionsWithTx_NilTx(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	txID := uuid.New()
	txDate := time.Now().UTC()

	inputs := []matchingPorts.ExceptionTransactionInput{
		{
			TransactionID:   txID,
			AmountAbsBase:   decimal.NewFromInt(100),
			TransactionDate: txDate,
			SourceType:      "FILE",
			FXMissing:       false,
			Reason:          "UNMATCHED",
		},
	}

	err := repo.CreateExceptionsWithTx(ctx, nil, uuid.New(), uuid.New(), inputs, nil)
	require.ErrorIs(t, err, ErrInvalidTx)
}

func TestRepository_CreateExceptionsWithTx_ValidSqlTx(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
	}()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	txID := uuid.New()
	txDate := time.Now().UTC()

	inputs := []matchingPorts.ExceptionTransactionInput{
		{
			TransactionID:   txID,
			AmountAbsBase:   decimal.NewFromInt(100),
			TransactionDate: txDate,
			SourceType:      "FILE",
			FXMissing:       false,
			Reason:          "UNMATCHED",
		},
	}

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO exceptions (id, transaction_id, severity, status, reason, created_at, updated_at)
			 VALUES ($1,$2,$3,'OPEN',$4,$5,$6)`,
	)

	mock.ExpectBegin()
	mock.ExpectPrepare(insertQuery)
	mock.ExpectExec(insertQuery).
		WithArgs(sqlmock.AnyArg(), txID.String(), sqlmock.AnyArg(), "UNMATCHED", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	sqlTx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	err = repo.CreateExceptionsWithTx(ctx, sqlTx, uuid.New(), uuid.New(), inputs, nil)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_CreateExceptionsWithTx_EmptyInputs(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	err := repo.CreateExceptionsWithTx(context.Background(), nil, uuid.New(), uuid.New(), nil, nil)
	require.ErrorIs(t, err, ErrInvalidTx)
}

func TestSanitizeInputReason_EmptyString(t *testing.T) {
	t.Parallel()

	result := sanitizeInputReason("")
	assert.Equal(t, enums.ReasonUnmatched, result)
}

func TestSanitizeInputReason_WhitespaceOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{"single space", " "},
		{"multiple spaces", "   "},
		{"tab", "\t"},
		{"newline", "\n"},
		{"mixed whitespace", " \t\n "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := sanitizeInputReason(tt.input)
			assert.Equal(t, enums.ReasonUnmatched, result)
		})
	}
}

func TestSanitizeInputReason_ValidReason(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{enums.ReasonUnmatched, enums.ReasonUnmatched},
		{enums.ReasonFXRateUnavailable, enums.ReasonFXRateUnavailable},
		{enums.ReasonMissingBaseAmount, enums.ReasonMissingBaseAmount},
		{enums.ReasonMissingBaseCurrency, enums.ReasonMissingBaseCurrency},
		{enums.ReasonSplitIncomplete, enums.ReasonSplitIncomplete},
		{enums.ReasonValidationFailed, enums.ReasonValidationFailed},
		{enums.ReasonSourceMismatch, enums.ReasonSourceMismatch},
		{enums.ReasonDuplicateTransaction, enums.ReasonDuplicateTransaction},
		{enums.ReasonFeeVariance, enums.ReasonFeeVariance},
		{enums.ReasonFeeDataMissing, enums.ReasonFeeDataMissing},
		{enums.ReasonFeeCurrencyMismatch, enums.ReasonFeeCurrencyMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			result := sanitizeInputReason(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeInputReason_InvalidReason(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{"random string", "RANDOM_REASON"},
		{"lowercase valid", "unmatched"},
		{"mixed case", "Unmatched"},
		{"sql injection", "'; DROP TABLE--"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := sanitizeInputReason(tt.input)
			assert.Equal(t, enums.ReasonUnmatched, result)
		})
	}
}

func TestSanitizeReason_FeeReasons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{enums.ReasonFeeVariance, enums.ReasonFeeVariance},
		{enums.ReasonFeeDataMissing, enums.ReasonFeeDataMissing},
		{enums.ReasonFeeCurrencyMismatch, enums.ReasonFeeCurrencyMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			result := enums.SanitizeReason(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeReason_ExactMaxLength(t *testing.T) {
	t.Parallel()

	input := make([]byte, enums.MaxReasonLength)
	for i := range input {
		input[i] = 'A'
	}

	result := enums.SanitizeReason(string(input))
	assert.Equal(t, enums.ReasonUnmatched, result)
}

func TestSanitizeReason_OneOverMaxLength(t *testing.T) {
	t.Parallel()

	input := make([]byte, enums.MaxReasonLength+1)
	for i := range input {
		input[i] = 'A'
	}

	result := enums.SanitizeReason(string(input))
	assert.Equal(t, enums.ReasonUnmatched, result)
	assert.LessOrEqual(t, len(result), enums.MaxReasonLength)
}

func TestConstants_Values(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "UNMATCHED", enums.ReasonUnmatched)
	assert.Equal(t, "FX_RATE_UNAVAILABLE", enums.ReasonFXRateUnavailable)
	assert.Equal(t, "MISSING_BASE_AMOUNT", enums.ReasonMissingBaseAmount)
	assert.Equal(t, "MISSING_BASE_CURRENCY", enums.ReasonMissingBaseCurrency)
	assert.Equal(t, "SPLIT_INCOMPLETE", enums.ReasonSplitIncomplete)
	assert.Equal(t, "VALIDATION_FAILED", enums.ReasonValidationFailed)
	assert.Equal(t, "SOURCE_MISMATCH", enums.ReasonSourceMismatch)
	assert.Equal(t, "DUPLICATE_TRANSACTION", enums.ReasonDuplicateTransaction)
	assert.Equal(t, "FEE_VARIANCE", enums.ReasonFeeVariance)
	assert.Equal(t, "FEE_DATA_MISSING", enums.ReasonFeeDataMissing)
	assert.Equal(t, "FEE_CURRENCY_MISMATCH", enums.ReasonFeeCurrencyMismatch)
	assert.Equal(t, 64, enums.MaxReasonLength)
}

func TestSentinelErrors_Messages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			"ErrRepoNotInitialized",
			ErrRepoNotInitialized,
			"exception creator repository not initialized",
		},
		{"ErrInvalidTx", ErrInvalidTx, "exception creator repository invalid transaction"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tt.err)
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestSentinelErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, ErrRepoNotInitialized, ErrInvalidTx)
	assert.NotErrorIs(t, ErrRepoNotInitialized, ErrInvalidTx)
	assert.NotErrorIs(t, ErrInvalidTx, ErrRepoNotInitialized)
}

func TestValidReasonsAllowlist_Complete(t *testing.T) {
	t.Parallel()

	expectedReasons := []string{
		enums.ReasonUnmatched,
		enums.ReasonFXRateUnavailable,
		enums.ReasonMissingBaseAmount,
		enums.ReasonMissingBaseCurrency,
		enums.ReasonSplitIncomplete,
		enums.ReasonValidationFailed,
		enums.ReasonSourceMismatch,
		enums.ReasonDuplicateTransaction,
		enums.ReasonFeeVariance,
		enums.ReasonFeeDataMissing,
		enums.ReasonFeeCurrencyMismatch,
	}

	for _, reason := range expectedReasons {
		result := enums.SanitizeReason(reason)
		assert.Equal(t, reason, result, "Reason %s should be in allowlist", reason)
	}
}

type invalidTxMock struct{}

func (m *invalidTxMock) Commit() error   { return nil }
func (m *invalidTxMock) Rollback() error { return nil }
