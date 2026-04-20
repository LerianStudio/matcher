//go:build unit

package transaction

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	sharedpg "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var errScanError = errors.New("scan error")

func TestRepository_PostgreSQLNilConnection(t *testing.T) {
	t.Parallel()

	txRepo := NewRepository(nil)
	ctx := context.Background()

	_, err := txRepo.Create(ctx, &shared.Transaction{})
	require.ErrorIs(t, err, errTxRepoNotInit)

	_, err = txRepo.CreateBatch(ctx, []*shared.Transaction{{}})
	require.ErrorIs(t, err, errTxRepoNotInit)

	_, err = txRepo.FindByID(ctx, uuid.New())
	require.ErrorIs(t, err, errTxRepoNotInit)

	_, _, err = txRepo.FindByJobID(ctx, uuid.New(), repositories.CursorFilter{Limit: 10})
	require.ErrorIs(t, err, errTxRepoNotInit)

	_, err = txRepo.FindBySourceAndExternalID(ctx, uuid.New(), "ext")
	require.ErrorIs(t, err, errTxRepoNotInit)

	_, err = txRepo.ExistsBySourceAndExternalID(ctx, uuid.New(), "ext")
	require.ErrorIs(t, err, errTxRepoNotInit)
}

func TestRepository_PostgreSQLNilEntity(t *testing.T) {
	t.Parallel()

	// Nil connection check happens before entity check
	txRepo := &Repository{}
	_, err := txRepo.Create(context.Background(), nil)
	require.ErrorIs(t, err, errTxRepoNotInit)
}

func TestRepository_PostgreSQLCreateBatchEmpty(t *testing.T) {
	t.Parallel()

	// Nil connection check happens before empty slice check
	txRepo := &Repository{}
	_, err := txRepo.CreateBatch(context.Background(), nil)
	require.ErrorIs(t, err, errTxRepoNotInit)
}

func TestRepository_PostgreSQLSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"errTxEntityRequired", errTxEntityRequired},
		{"errTxModelRequired", errTxModelRequired},
		{"errInvalidExtractionStatus", errInvalidExtractionStatus},
		{"errInvalidTxStatus", errInvalidTxStatus},
		{"errTxRepoNotInit", errTxRepoNotInit},
		{"errContextIDRequired", errContextIDRequired},
		{"errJobIDRequired", errJobIDRequired},
		{"errLimitMustBePositive", errLimitMustBePositive},
		{"errOffsetMustBeNonNegative", errOffsetMustBeNonNegative},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestNormalizeTransactionSortColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"created_at", "created_at"},
		{"CREATED_AT", "created_at"},
		{"date", "date"},
		{"status", "status"},
		{"extraction_status", "extraction_status"},
		{"id", "id"},
		{"", "id"},
		{"unknown", "id"},
		{"  ID  ", "id"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			result := normalizeTransactionSortColumn(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSafeIntToUint64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    int
		expected uint64
	}{
		{0, 0},
		{1, 1},
		{100, 100},
		{-1, 0},
		{-100, 0},
	}

	for _, tt := range tests {
		result := sharedpg.SafeIntToUint64(tt.input)
		require.Equal(t, tt.expected, result)
	}
}

func TestNewTransactionPostgreSQLModel_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	txID := uuid.New()
	jobID := uuid.New()
	sourceID := uuid.New()

	amountBase := decimal.NewFromFloat(125.50)
	baseCurrency := "EUR"
	fxRate := decimal.NewFromFloat(1.255)
	fxRateSource := "ECB"
	fxRateDate := now.Add(-24 * time.Hour)

	entity := &shared.Transaction{
		ID:               txID,
		IngestionJobID:   jobID,
		SourceID:         sourceID,
		ExternalID:       "ext-123",
		Amount:           decimal.NewFromFloat(100.00),
		Currency:         "USD",
		AmountBase:       &amountBase,
		BaseCurrency:     &baseCurrency,
		FXRate:           &fxRate,
		FXRateSource:     &fxRateSource,
		FXRateEffDate:    &fxRateDate,
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusUnmatched,
		Date:             now,
		Description:      "Test transaction",
		Metadata:         map[string]any{"key": "value"},
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	model, err := NewTransactionPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, txID.String(), model.ID)
	assert.Equal(t, jobID.String(), model.IngestionJobID)
	assert.Equal(t, sourceID.String(), model.SourceID)
	assert.Equal(t, "ext-123", model.ExternalID)
	assert.True(t, model.Amount.Equal(decimal.NewFromFloat(100.00)))
	assert.Equal(t, "USD", model.Currency)
	assert.True(t, model.AmountBase.Valid)
	assert.True(t, model.AmountBase.Decimal.Equal(amountBase))
	assert.True(t, model.BaseCurrency.Valid)
	assert.Equal(t, baseCurrency, model.BaseCurrency.String)
	assert.True(t, model.FXRate.Valid)
	assert.True(t, model.FXRate.Decimal.Equal(fxRate))
	assert.True(t, model.FXRateSource.Valid)
	assert.Equal(t, fxRateSource, model.FXRateSource.String)
	assert.True(t, model.FXRateEffectiveDate.Valid)
	assert.Equal(t, fxRateDate, model.FXRateEffectiveDate.Time)
	assert.Equal(t, "COMPLETE", model.ExtractionStatus)
	assert.Equal(t, "UNMATCHED", model.Status)
	assert.True(t, model.Description.Valid)
	assert.Equal(t, "Test transaction", model.Description.String)
}

func TestNewTransactionPostgreSQLModel_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := NewTransactionPostgreSQLModel(nil)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, errTxEntityRequired)
}

func TestNewTransactionPostgreSQLModel_GeneratesIDWhenNil(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &shared.Transaction{
		ID:               uuid.Nil,
		IngestionJobID:   uuid.New(),
		SourceID:         uuid.New(),
		ExternalID:       "ext-456",
		Amount:           decimal.NewFromFloat(50.00),
		Currency:         "GBP",
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusUnmatched,
		Date:             now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	model, err := NewTransactionPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.NotEmpty(t, model.ID)
	parsedID, err := uuid.Parse(model.ID)
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, parsedID)
}

func TestNewTransactionPostgreSQLModel_SetsTimestampsWhenZero(t *testing.T) {
	t.Parallel()

	entity := &shared.Transaction{
		ID:               uuid.New(),
		IngestionJobID:   uuid.New(),
		SourceID:         uuid.New(),
		ExternalID:       "ext-789",
		Amount:           decimal.NewFromFloat(75.00),
		Currency:         "CAD",
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusUnmatched,
		Date:             time.Now().UTC(),
	}

	model, err := NewTransactionPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.False(t, model.CreatedAt.IsZero())
	require.False(t, model.UpdatedAt.IsZero())
	require.True(t, model.UpdatedAt.Equal(model.CreatedAt))
}

func TestNewTransactionPostgreSQLModel_InvalidExtractionStatus(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &shared.Transaction{
		ID:               uuid.New(),
		IngestionJobID:   uuid.New(),
		SourceID:         uuid.New(),
		ExternalID:       "ext-invalid",
		Amount:           decimal.NewFromFloat(25.00),
		Currency:         "USD",
		ExtractionStatus: shared.ExtractionStatus("INVALID_STATUS"),
		Status:           shared.TransactionStatusUnmatched,
		Date:             now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	model, err := NewTransactionPostgreSQLModel(entity)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, errInvalidExtractionStatus)
}

func TestNewTransactionPostgreSQLModel_InvalidTransactionStatus(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &shared.Transaction{
		ID:               uuid.New(),
		IngestionJobID:   uuid.New(),
		SourceID:         uuid.New(),
		ExternalID:       "ext-invalid-status",
		Amount:           decimal.NewFromFloat(35.00),
		Currency:         "USD",
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatus("BOGUS"),
		Date:             now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	model, err := NewTransactionPostgreSQLModel(entity)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, errInvalidTxStatus)
}

func TestNewTransactionPostgreSQLModel_NilOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &shared.Transaction{
		ID:               uuid.New(),
		IngestionJobID:   uuid.New(),
		SourceID:         uuid.New(),
		ExternalID:       "ext-no-optionals",
		Amount:           decimal.NewFromFloat(100.00),
		Currency:         "USD",
		AmountBase:       nil,
		BaseCurrency:     nil,
		FXRate:           nil,
		FXRateSource:     nil,
		FXRateEffDate:    nil,
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusUnmatched,
		Date:             now,
		Metadata:         nil,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	model, err := NewTransactionPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.False(t, model.AmountBase.Valid)
	assert.False(t, model.BaseCurrency.Valid)
	assert.False(t, model.FXRate.Valid)
	assert.False(t, model.FXRateSource.Valid)
	assert.False(t, model.FXRateEffectiveDate.Valid)
	require.NotNil(t, model.Metadata)
	require.JSONEq(t, `{}`, string(model.Metadata))
}

func TestNewTransactionPostgreSQLModel_EmptyDescription(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &shared.Transaction{
		ID:               uuid.New(),
		IngestionJobID:   uuid.New(),
		SourceID:         uuid.New(),
		ExternalID:       "ext-no-desc",
		Amount:           decimal.NewFromFloat(100.00),
		Currency:         "USD",
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusUnmatched,
		Date:             now,
		Description:      "",
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	model, err := NewTransactionPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.False(t, model.Description.Valid)
}

func TestTransactionModelToEntity_Success(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	txID := uuid.New()
	jobID := uuid.New()
	sourceID := uuid.New()

	model := &pgcommon.TransactionPostgreSQLModel{
		ID:             txID.String(),
		IngestionJobID: jobID.String(),
		SourceID:       sourceID.String(),
		ExternalID:     "ext-entity-test",
		Amount:         decimal.NewFromFloat(200.00),
		Currency:       "EUR",
		AmountBase: decimal.NullDecimal{
			Decimal: decimal.NewFromFloat(220.00),
			Valid:   true,
		},
		BaseCurrency:        sql.NullString{String: "USD", Valid: true},
		FXRate:              decimal.NullDecimal{Decimal: decimal.NewFromFloat(1.10), Valid: true},
		FXRateSource:        sql.NullString{String: "OANDA", Valid: true},
		FXRateEffectiveDate: sql.NullTime{Time: now, Valid: true},
		ExtractionStatus:    "COMPLETE",
		Date:                now,
		Description:         sql.NullString{String: "Entity test", Valid: true},
		Status:              "MATCHED",
		Metadata:            []byte(`{"converted":true}`),
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	entity, err := transactionModelToEntity(model)

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, txID, entity.ID)
	assert.Equal(t, jobID, entity.IngestionJobID)
	assert.Equal(t, sourceID, entity.SourceID)
	assert.Equal(t, "ext-entity-test", entity.ExternalID)
	assert.True(t, entity.Amount.Equal(decimal.NewFromFloat(200.00)))
	assert.Equal(t, "EUR", entity.Currency)
	require.NotNil(t, entity.AmountBase)
	assert.True(t, entity.AmountBase.Equal(decimal.NewFromFloat(220.00)))
	require.NotNil(t, entity.BaseCurrency)
	assert.Equal(t, "USD", *entity.BaseCurrency)
	require.NotNil(t, entity.FXRate)
	assert.True(t, entity.FXRate.Equal(decimal.NewFromFloat(1.10)))
	require.NotNil(t, entity.FXRateSource)
	assert.Equal(t, "OANDA", *entity.FXRateSource)
	require.NotNil(t, entity.FXRateEffDate)
	assert.Equal(t, shared.ExtractionStatusComplete, entity.ExtractionStatus)
	assert.Equal(t, shared.TransactionStatusMatched, entity.Status)
	assert.Equal(t, "Entity test", entity.Description)
	assert.Equal(t, true, entity.Metadata["converted"])
}

func TestTransactionModelToEntity_NilModel(t *testing.T) {
	t.Parallel()

	entity, err := transactionModelToEntity(nil)

	require.Error(t, err)
	require.Nil(t, entity)
	require.ErrorIs(t, err, errTxModelRequired)
}

func TestTransactionModelToEntity_InvalidID(t *testing.T) {
	t.Parallel()

	model := &pgcommon.TransactionPostgreSQLModel{
		ID:               "not-a-uuid",
		IngestionJobID:   uuid.New().String(),
		SourceID:         uuid.New().String(),
		ExtractionStatus: "COMPLETE",
		Status:           "UNMATCHED",
	}

	entity, err := transactionModelToEntity(model)

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing ID")
}

func TestTransactionModelToEntity_InvalidIngestionJobID(t *testing.T) {
	t.Parallel()

	model := &pgcommon.TransactionPostgreSQLModel{
		ID:               uuid.New().String(),
		IngestionJobID:   "invalid-job-id",
		SourceID:         uuid.New().String(),
		ExtractionStatus: "COMPLETE",
		Status:           "UNMATCHED",
	}

	entity, err := transactionModelToEntity(model)

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing IngestionJobID")
}

func TestTransactionModelToEntity_InvalidSourceID(t *testing.T) {
	t.Parallel()

	model := &pgcommon.TransactionPostgreSQLModel{
		ID:               uuid.New().String(),
		IngestionJobID:   uuid.New().String(),
		SourceID:         "invalid-source",
		ExtractionStatus: "COMPLETE",
		Status:           "UNMATCHED",
	}

	entity, err := transactionModelToEntity(model)

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing SourceID")
}

func TestTransactionModelToEntity_InvalidExtractionStatus(t *testing.T) {
	t.Parallel()

	model := &pgcommon.TransactionPostgreSQLModel{
		ID:               uuid.New().String(),
		IngestionJobID:   uuid.New().String(),
		SourceID:         uuid.New().String(),
		ExtractionStatus: "INVALID_EXTRACTION",
		Status:           "UNMATCHED",
	}

	entity, err := transactionModelToEntity(model)

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing ExtractionStatus")
}

func TestTransactionModelToEntity_InvalidTransactionStatus(t *testing.T) {
	t.Parallel()

	model := &pgcommon.TransactionPostgreSQLModel{
		ID:               uuid.New().String(),
		IngestionJobID:   uuid.New().String(),
		SourceID:         uuid.New().String(),
		ExtractionStatus: "COMPLETE",
		Status:           "BOGUS_STATUS",
	}

	entity, err := transactionModelToEntity(model)

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing Status")
}

func TestTransactionModelToEntity_InvalidMetadataJSON(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &pgcommon.TransactionPostgreSQLModel{
		ID:               uuid.New().String(),
		IngestionJobID:   uuid.New().String(),
		SourceID:         uuid.New().String(),
		ExternalID:       "ext-bad-json",
		Amount:           decimal.NewFromFloat(50.00),
		Currency:         "USD",
		ExtractionStatus: "COMPLETE",
		Status:           "UNMATCHED",
		Metadata:         []byte(`{invalid json}`),
		Date:             now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	entity, err := transactionModelToEntity(model)

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing Metadata")
}

func TestTransactionModelToEntity_NullOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &pgcommon.TransactionPostgreSQLModel{
		ID:                  uuid.New().String(),
		IngestionJobID:      uuid.New().String(),
		SourceID:            uuid.New().String(),
		ExternalID:          "ext-null-opts",
		Amount:              decimal.NewFromFloat(100.00),
		Currency:            "USD",
		AmountBase:          decimal.NullDecimal{Valid: false},
		BaseCurrency:        sql.NullString{Valid: false},
		FXRate:              decimal.NullDecimal{Valid: false},
		FXRateSource:        sql.NullString{Valid: false},
		FXRateEffectiveDate: sql.NullTime{Valid: false},
		ExtractionStatus:    "PENDING",
		Status:              "UNMATCHED",
		Description:         sql.NullString{Valid: false},
		Date:                now,
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	entity, err := transactionModelToEntity(model)

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Nil(t, entity.AmountBase)
	assert.Nil(t, entity.BaseCurrency)
	assert.Nil(t, entity.FXRate)
	assert.Nil(t, entity.FXRateSource)
	assert.Nil(t, entity.FXRateEffDate)
	assert.Empty(t, entity.Description)
}

func TestValidateListUnmatchedParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		contextID uuid.UUID
		limit     int
		offset    int
		wantErr   error
	}{
		{
			name:      "valid params",
			contextID: uuid.New(),
			limit:     10,
			offset:    0,
			wantErr:   nil,
		},
		{
			name:      "nil context id",
			contextID: uuid.Nil,
			limit:     10,
			offset:    0,
			wantErr:   errContextIDRequired,
		},
		{
			name:      "zero limit",
			contextID: uuid.New(),
			limit:     0,
			offset:    0,
			wantErr:   errLimitMustBePositive,
		},
		{
			name:      "negative limit",
			contextID: uuid.New(),
			limit:     -1,
			offset:    0,
			wantErr:   errLimitMustBePositive,
		},
		{
			name:      "negative offset",
			contextID: uuid.New(),
			limit:     10,
			offset:    -1,
			wantErr:   errOffsetMustBeNonNegative,
		},
		{
			name:      "valid with high offset",
			contextID: uuid.New(),
			limit:     100,
			offset:    1000,
			wantErr:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateListUnmatchedParams(tt.contextID, tt.limit, tt.offset)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBuildUnmatchedByContextQuery(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	now := time.Now().UTC()
	start := now.Add(-7 * 24 * time.Hour)
	end := now

	t.Run("basic query without date filters", func(t *testing.T) {
		t.Parallel()

		query, args, err := buildUnmatchedByContextQuery(contextID, nil, nil, 10, 0)

		require.NoError(t, err)
		require.NotEmpty(t, query)
		require.Contains(t, query, "source_id IN")
		require.Contains(t, query, "extraction_status")
		require.Contains(t, query, "status")
		require.Contains(t, query, "LIMIT 10")
		require.Contains(t, query, "OFFSET 0")
		require.NotEmpty(t, args)
		require.Contains(t, args, "COMPLETE")
		require.Contains(t, args, "UNMATCHED")
	})

	t.Run("query with start date filter", func(t *testing.T) {
		t.Parallel()

		query, args, err := buildUnmatchedByContextQuery(contextID, &start, nil, 20, 5)

		require.NoError(t, err)
		require.NotEmpty(t, query)
		require.Contains(t, query, "date >=")
		require.Greater(t, len(args), 1)
	})

	t.Run("query with end date filter", func(t *testing.T) {
		t.Parallel()

		query, args, err := buildUnmatchedByContextQuery(contextID, nil, &end, 15, 0)

		require.NoError(t, err)
		require.NotEmpty(t, query)
		require.Contains(t, query, "date <=")
		require.Greater(t, len(args), 1)
	})

	t.Run("query with both date filters", func(t *testing.T) {
		t.Parallel()

		query, args, err := buildUnmatchedByContextQuery(contextID, &start, &end, 50, 10)

		require.NoError(t, err)
		require.NotEmpty(t, query)
		require.Contains(t, query, "date >=")
		require.Contains(t, query, "date <=")
		require.Greater(t, len(args), 2)
	})
}

func TestBuildMarkStatusQueries(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}

	t.Run("BuildMarkMatchedQuery", func(t *testing.T) {
		t.Parallel()

		query, err := BuildMarkMatchedQuery(contextID, txIDs)
		require.NoError(t, err)

		sqlStr, args, err := query.ToSql()
		require.NoError(t, err)
		require.Contains(t, sqlStr, "UPDATE transactions")
		require.Contains(t, sqlStr, "status")
		require.Contains(t, sqlStr, "SET")
		require.NotEmpty(t, args)
		require.Contains(t, args, "MATCHED")
	})

	t.Run("BuildMarkPendingReviewQuery", func(t *testing.T) {
		t.Parallel()

		query, err := BuildMarkPendingReviewQuery(contextID, txIDs)
		require.NoError(t, err)

		sqlStr, args, err := query.ToSql()
		require.NoError(t, err)
		require.Contains(t, sqlStr, "UPDATE transactions")
		require.Contains(t, sqlStr, "SET")
		require.NotEmpty(t, args)
		require.Contains(t, args, "PENDING_REVIEW")
	})

	t.Run("BuildMarkUnmatchedQuery", func(t *testing.T) {
		t.Parallel()

		query, err := BuildMarkUnmatchedQuery(contextID, txIDs)
		require.NoError(t, err)

		sqlStr, args, err := query.ToSql()
		require.NoError(t, err)
		require.Contains(t, sqlStr, "UPDATE transactions")
		require.Contains(t, sqlStr, "SET")
		require.NotEmpty(t, args)
		require.Contains(t, args, "UNMATCHED")
	})
}

func TestRepository_CreateWithTx_Validations(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		_, err := repo.CreateWithTx(ctx, nil, &shared.Transaction{})
		require.ErrorIs(t, err, errTxRepoNotInit)
	})
}

func TestRepository_UpdateStatus_Validations(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		_, err := repo.UpdateStatus(ctx, uuid.New(), uuid.New(), shared.TransactionStatusMatched)
		require.ErrorIs(t, err, errTxRepoNotInit)
	})
}

func TestRepository_UpdateStatusWithTx_Validations(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		_, err := repo.UpdateStatusWithTx(
			ctx,
			nil,
			uuid.New(),
			uuid.New(),
			shared.TransactionStatusMatched,
		)
		require.ErrorIs(t, err, errTxRepoNotInit)
	})
}

func TestRepository_CleanupFailedJobTransactionsWithTx_Validations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		err := repo.CleanupFailedJobTransactionsWithTx(ctx, nil, uuid.New())
		require.ErrorIs(t, err, errTxRepoNotInit)
	})

	t.Run("nil tx", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		err := repo.CleanupFailedJobTransactionsWithTx(ctx, nil, uuid.New())
		require.ErrorIs(t, err, errTxRequired)
	})

	t.Run("nil job id", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectBegin()
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		err = repo.CleanupFailedJobTransactionsWithTx(ctx, tx, uuid.Nil)
		require.ErrorIs(t, err, errJobIDRequired)
	})
}

func TestRepository_CleanupFailedJobTransactionsWithTx_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
	})

	jobID := uuid.New()

	mock.ExpectBegin()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	mock.ExpectExec("(?s)UPDATE transactions.+WHERE ingestion_job_id = \\$3 AND status = \\$4").
		WithArgs(shared.TransactionStatusIgnored.String(), sqlmock.AnyArg(), jobID, shared.TransactionStatusUnmatched.String()).
		WillReturnResult(sqlmock.NewResult(0, 2))

	err = repo.CleanupFailedJobTransactionsWithTx(ctx, tx, jobID)
	require.NoError(t, err)

	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_ListUnmatchedByContext_Validations(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		_, err := repo.ListUnmatchedByContext(ctx, uuid.New(), nil, nil, 10, 0)
		require.ErrorIs(t, err, errTxRepoNotInit)
	})
}

func TestRepository_MarkMatched_Validations(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		err := repo.MarkMatched(ctx, uuid.New(), []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errTxRepoNotInit)
	})
}

func TestRepository_MarkPendingReview_Validations(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		err := repo.MarkPendingReview(ctx, uuid.New(), []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errTxRepoNotInit)
	})
}

func TestRepository_FindByJobAndContextID_Validations(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		_, _, err := repo.FindByJobAndContextID(
			ctx,
			uuid.New(),
			uuid.New(),
			repositories.CursorFilter{Limit: 10},
		)
		require.ErrorIs(t, err, errTxRepoNotInit)
	})
}

func TestRepository_ExistsBulkBySourceAndExternalID_Validations(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		keys := []repositories.ExternalIDKey{{SourceID: uuid.New(), ExternalID: "ext"}}
		_, err := repo.ExistsBulkBySourceAndExternalID(ctx, keys)
		require.ErrorIs(t, err, errTxRepoNotInit)
	})
}

func TestRepository_MarkMatchedWithTx_Validations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		err := repo.MarkMatchedWithTx(ctx, nil, uuid.New(), []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errTxRepoNotInit)
	})

	t.Run("nil tx", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		err := repo.MarkMatchedWithTx(ctx, nil, uuid.New(), []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errTxRequired)
	})

	t.Run("nil context id", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		defer db.Close()

		mock.ExpectBegin()

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		err = repo.MarkMatchedWithTx(ctx, tx, uuid.Nil, []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errContextIDRequired)
	})

	t.Run("empty transaction IDs returns nil", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		defer db.Close()

		mock.ExpectBegin()

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		err = repo.MarkMatchedWithTx(ctx, tx, uuid.New(), []uuid.UUID{})
		require.NoError(t, err)
	})
}

func TestRepository_MarkPendingReviewWithTx_Validations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		err := repo.MarkPendingReviewWithTx(ctx, nil, uuid.New(), []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errTxRepoNotInit)
	})

	t.Run("nil tx", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		err := repo.MarkPendingReviewWithTx(ctx, nil, uuid.New(), []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errTxRequired)
	})

	t.Run("nil context id", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		defer db.Close()

		mock.ExpectBegin()

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		err = repo.MarkPendingReviewWithTx(ctx, tx, uuid.Nil, []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errContextIDRequired)
	})

	t.Run("empty transaction IDs returns nil", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		defer db.Close()

		mock.ExpectBegin()

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		err = repo.MarkPendingReviewWithTx(ctx, tx, uuid.New(), []uuid.UUID{})
		require.NoError(t, err)
	})
}

func TestRepository_MarkUnmatched_Validations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		err := repo.MarkUnmatched(ctx, uuid.New(), []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errTxRepoNotInit)
	})

	t.Run("nil context id", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		err := repo.MarkUnmatched(ctx, uuid.Nil, []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errContextIDRequired)
	})

	t.Run("empty transaction IDs returns nil", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		err := repo.MarkUnmatched(ctx, uuid.New(), []uuid.UUID{})
		require.NoError(t, err)
	})
}

func TestRepository_MarkUnmatchedWithTx_Validations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		err := repo.MarkUnmatchedWithTx(ctx, nil, uuid.New(), []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errTxRepoNotInit)
	})

	t.Run("nil tx", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		err := repo.MarkUnmatchedWithTx(ctx, nil, uuid.New(), []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errTxRequired)
	})

	t.Run("nil context id", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		defer db.Close()

		mock.ExpectBegin()

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		err = repo.MarkUnmatchedWithTx(ctx, tx, uuid.Nil, []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errContextIDRequired)
	})

	t.Run("empty transaction IDs returns nil", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		defer db.Close()

		mock.ExpectBegin()

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		err = repo.MarkUnmatchedWithTx(ctx, tx, uuid.New(), []uuid.UUID{})
		require.NoError(t, err)
	})
}

func TestRepository_MarkMatched_ContextIDRequired(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	err := repo.MarkMatched(ctx, uuid.Nil, []uuid.UUID{uuid.New()})
	require.ErrorIs(t, err, errContextIDRequired)
}

func TestRepository_MarkMatched_EmptyTransactionIDs(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	err := repo.MarkMatched(ctx, uuid.New(), []uuid.UUID{})
	require.NoError(t, err)
}

func TestRepository_MarkPendingReview_ContextIDRequired(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	err := repo.MarkPendingReview(ctx, uuid.Nil, []uuid.UUID{uuid.New()})
	require.ErrorIs(t, err, errContextIDRequired)
}

func TestRepository_MarkPendingReview_EmptyTransactionIDs(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	err := repo.MarkPendingReview(ctx, uuid.New(), []uuid.UUID{})
	require.NoError(t, err)
}

func TestRepository_FindByContextAndIDs_Validations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("nil repository provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		_, err := repo.FindByContextAndIDs(ctx, uuid.New(), []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errTxRepoNotInit)
	})

	t.Run("nil context id", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		_, err := repo.FindByContextAndIDs(ctx, uuid.Nil, []uuid.UUID{uuid.New()})
		require.ErrorIs(t, err, errContextIDRequired)
	})

	t.Run("empty transaction IDs returns empty slice", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		result, err := repo.FindByContextAndIDs(ctx, uuid.New(), []uuid.UUID{})
		require.NoError(t, err)
		require.Empty(t, result)
		require.NotNil(t, result)
	})
}

func TestRepository_ExistsBulkBySourceAndExternalID_EmptyKeys(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.ExistsBulkBySourceAndExternalID(ctx, []repositories.ExternalIDKey{})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Empty(t, result)
}

func TestRepository_CreateWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	entity := createValidTransactionEntity()
	_, err := repo.CreateWithTx(ctx, nil, entity)
	require.ErrorIs(t, err, errTxRequired)
}

func TestRepository_CreateWithTx_NilEntity(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = repo.CreateWithTx(ctx, tx, nil)
	require.ErrorIs(t, err, errTxEntityRequired)
}

func TestRepository_UpdateStatusWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	_, err := repo.UpdateStatusWithTx(
		ctx,
		nil,
		uuid.New(),
		uuid.New(),
		shared.TransactionStatusMatched,
	)
	require.ErrorIs(t, err, errTxRequired)
}

func TestRepository_UpdateStatusWithTx_NilContextID(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = repo.UpdateStatusWithTx(ctx, tx, uuid.New(), uuid.Nil, shared.TransactionStatusMatched)
	require.ErrorIs(t, err, errContextIDRequired)
}

func TestRepository_UpdateStatus_NilContextID(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	_, err := repo.UpdateStatus(ctx, uuid.New(), uuid.Nil, shared.TransactionStatusMatched)
	require.ErrorIs(t, err, errContextIDRequired)
}

func TestRepository_ListUnmatchedByContext_InvalidParams(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	t.Run("nil context id", func(t *testing.T) {
		t.Parallel()

		_, err := repo.ListUnmatchedByContext(ctx, uuid.Nil, nil, nil, 10, 0)
		require.ErrorIs(t, err, errContextIDRequired)
	})

	t.Run("zero limit", func(t *testing.T) {
		t.Parallel()

		_, err := repo.ListUnmatchedByContext(ctx, uuid.New(), nil, nil, 0, 0)
		require.ErrorIs(t, err, errLimitMustBePositive)
	})

	t.Run("negative limit", func(t *testing.T) {
		t.Parallel()

		_, err := repo.ListUnmatchedByContext(ctx, uuid.New(), nil, nil, -1, 0)
		require.ErrorIs(t, err, errLimitMustBePositive)
	})

	t.Run("negative offset", func(t *testing.T) {
		t.Parallel()

		_, err := repo.ListUnmatchedByContext(ctx, uuid.New(), nil, nil, 10, -1)
		require.ErrorIs(t, err, errOffsetMustBeNonNegative)
	})
}

func TestScanRowsToTransactions_Empty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id"})
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	mockScan := func(scanner interface{ Scan(dest ...any) error }) (*shared.Transaction, error) {
		return &shared.Transaction{}, nil
	}

	result, err := scanRowsToTransactions(sqlRows, mockScan)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestScanRowsToTransactions_ScanError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id"}).AddRow("test")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	mockScan := func(scanner interface{ Scan(dest ...any) error }) (*shared.Transaction, error) {
		return nil, errScanError
	}

	result, err := scanRowsToTransactions(sqlRows, mockScan)
	require.Error(t, err)
	require.ErrorIs(t, err, errScanError)
	require.Nil(t, result)
}

func TestBuildMarkStatusQueries_EmptyIDs(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	emptyIDs := []uuid.UUID{}

	t.Run("BuildMarkMatchedQuery with empty IDs", func(t *testing.T) {
		t.Parallel()

		_, err := BuildMarkMatchedQuery(contextID, emptyIDs)
		require.Error(t, err)
	})

	t.Run("BuildMarkPendingReviewQuery with empty IDs", func(t *testing.T) {
		t.Parallel()

		_, err := BuildMarkPendingReviewQuery(contextID, emptyIDs)
		require.Error(t, err)
	})

	t.Run("BuildMarkUnmatchedQuery with empty IDs", func(t *testing.T) {
		t.Parallel()

		_, err := BuildMarkUnmatchedQuery(contextID, emptyIDs)
		require.Error(t, err)
	})
}

func TestBuildMarkStatusQueries_SingleID(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	singleID := []uuid.UUID{uuid.New()}

	query, err := BuildMarkMatchedQuery(contextID, singleID)
	require.NoError(t, err)

	sqlStr, args, err := query.ToSql()
	require.NoError(t, err)
	require.Contains(t, sqlStr, "UPDATE transactions")
	require.Contains(t, sqlStr, "SET")
	require.NotEmpty(t, args)
}

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("creates repository with valid provider", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)
		require.NotNil(t, repo)
		assert.Equal(t, provider, repo.provider)
	})

	t.Run("creates repository with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		require.NotNil(t, repo)
		assert.Nil(t, repo.provider)
	})
}

func TestRepository_Create_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()
	entity := createValidTransactionEntity()

	result, err := repo.Create(ctx, entity)
	require.Nil(t, result)
	require.ErrorIs(t, err, errTxRepoNotInit)
}

func TestRepository_Create_NilEntity(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.Create(ctx, nil)
	require.Nil(t, result)
	require.ErrorIs(t, err, errTxEntityRequired)
}

func TestRepository_CreateBatch_EmptySlice(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.CreateBatch(ctx, []*shared.Transaction{})
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestRepository_CreateBatch_NilSlice(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.CreateBatch(ctx, nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestRepository_CreateBatchWithTx_EmptySlice(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	result, err := repo.CreateBatchWithTx(ctx, nil, []*shared.Transaction{})
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestRepository_FindByID_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()

	result, err := repo.FindByID(ctx, uuid.New())
	require.Nil(t, result)
	require.ErrorIs(t, err, errTxRepoNotInit)
}

func TestRepository_FindByJobID_CursorPaginationWithNonIDSort(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn}
	repo := NewRepository(provider)
	ctx := context.Background()
	jobID := uuid.New()

	filter := repositories.CursorFilter{
		Limit:     10,
		SortBy:    "created_at",
		SortOrder: "ASC",
		Cursor:    "somecursor",
	}

	_, _, err = repo.FindByJobID(ctx, jobID, filter)
	require.Error(t, err)
	require.ErrorIs(t, err, pkgHTTP.ErrInvalidCursor)
}

func TestRepository_FindByJobAndContextID_InvalidSortCursor(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn}
	repo := NewRepository(provider)
	ctx := context.Background()
	jobID := uuid.New()
	contextID := uuid.New()

	filter := repositories.CursorFilter{
		Limit:     10,
		SortBy:    "date",
		SortOrder: "DESC",
		Cursor:    "somecursor",
	}

	_, _, err = repo.FindByJobAndContextID(ctx, jobID, contextID, filter)
	require.Error(t, err)
	require.ErrorIs(t, err, pkgHTTP.ErrInvalidCursor)
}

func TestRepository_FindByJobID_DefaultLimit(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()

	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{
		"id", "ingestion_job_id", "source_id", "external_id", "amount", "currency",
		"amount_base", "base_currency", "fx_rate", "fx_rate_source", "fx_rate_effective_date",
		"extraction_status", "date", "description", "status", "metadata", "created_at", "updated_at",
	}))

	filter := repositories.CursorFilter{
		Limit: 0,
	}

	result, _, err := repo.FindByJobID(ctx, jobID, filter)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestRepository_FindByJobAndContextID_DefaultLimit(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()
	contextID := uuid.New()

	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{
		"id", "ingestion_job_id", "source_id", "external_id", "amount", "currency",
		"amount_base", "base_currency", "fx_rate", "fx_rate_source", "fx_rate_effective_date",
		"extraction_status", "date", "description", "status", "metadata", "created_at", "updated_at",
	}))

	filter := repositories.CursorFilter{
		Limit: -5,
	}

	result, _, err := repo.FindByJobAndContextID(ctx, jobID, contextID, filter)
	require.NoError(t, err)
	require.Empty(t, result)
}

func createValidTransactionEntity() *shared.Transaction {
	now := time.Now().UTC()

	return &shared.Transaction{
		ID:               uuid.New(),
		IngestionJobID:   uuid.New(),
		SourceID:         uuid.New(),
		ExternalID:       "ext-test",
		Amount:           decimal.NewFromFloat(100.00),
		Currency:         "USD",
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusUnmatched,
		Date:             now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

func setupRepositoryWithMock(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)
	provider := &testutil.MockInfrastructureProvider{PostgresConn: conn}
	repo := NewRepository(provider)

	finish := func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}

	return repo, mock, finish
}

func testTransactionColumns() []string {
	return []string{
		"id", "ingestion_job_id", "source_id", "external_id", "amount", "currency",
		"amount_base", "base_currency", "fx_rate", "fx_rate_source", "fx_rate_effective_date",
		"extraction_status", "date", "description", "status", "metadata", "created_at", "updated_at",
	}
}

func createTransactionRow(entity *shared.Transaction) []driver.Value {
	return []driver.Value{
		entity.ID.String(),
		entity.IngestionJobID.String(),
		entity.SourceID.String(),
		entity.ExternalID,
		entity.Amount,
		entity.Currency,
		nil,
		nil,
		nil,
		nil,
		nil,
		entity.ExtractionStatus.String(),
		entity.Date,
		entity.Description,
		entity.Status.String(),
		[]byte(`{}`),
		entity.CreatedAt,
		entity.UpdatedAt,
	}
}

func TestRepository_Create_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	entity := createValidTransactionEntity()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO transactions").
		WithArgs(
			sqlmock.AnyArg(),
			entity.IngestionJobID.String(),
			entity.SourceID.String(),
			entity.ExternalID,
			entity.Amount,
			entity.Currency,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			entity.ExtractionStatus.String(),
			entity.Date,
			sqlmock.AnyArg(),
			entity.Status.String(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery("SELECT .* FROM transactions WHERE id").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))
	mock.ExpectCommit()

	result, err := repo.Create(ctx, entity)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, entity.ExternalID, result.ExternalID)
}

func TestRepository_CreateBatch_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	entity1 := createValidTransactionEntity()
	entity1.ExternalID = "batch-ext-1"
	entity2 := createValidTransactionEntity()
	entity2.ExternalID = "batch-ext-2"
	txs := []*shared.Transaction{entity1, entity2}

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO transactions")
	mock.ExpectExec("INSERT INTO transactions").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO transactions").WillReturnResult(sqlmock.NewResult(2, 1))

	rows := sqlmock.NewRows(testTransactionColumns()).
		AddRow(createTransactionRow(entity1)...).
		AddRow(createTransactionRow(entity2)...)
	mock.ExpectQuery("SELECT .* FROM transactions WHERE id").WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.CreateBatch(ctx, txs)
	require.NoError(t, err)
	require.Len(t, result, 2)
}

func TestRepository_FindByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	entity := createValidTransactionEntity()

	mock.ExpectQuery("SELECT .* FROM transactions WHERE id").
		WithArgs(entity.ID.String()).
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))

	result, err := repo.FindByID(ctx, entity.ID)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, entity.ID, result.ID)
	assert.Equal(t, entity.ExternalID, result.ExternalID)
}

func TestRepository_FindByID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()

	mock.ExpectQuery("SELECT .* FROM transactions WHERE id").
		WithArgs(id.String()).
		WillReturnError(sql.ErrNoRows)

	result, err := repo.FindByID(ctx, id)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to find transaction")
}

func TestRepository_FindByJobID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()
	entity := createValidTransactionEntity()
	entity.IngestionJobID = jobID

	mock.ExpectQuery("SELECT .* FROM transactions WHERE ingestion_job_id").
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))

	filter := repositories.CursorFilter{
		Limit:     10,
		SortBy:    "id",
		SortOrder: "ASC",
	}

	result, pagination, err := repo.FindByJobID(ctx, jobID, filter)
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, entity.ExternalID, result[0].ExternalID)
	assert.NotNil(t, pagination)
}

func TestRepository_FindByJobID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()

	mock.ExpectQuery("SELECT .* FROM transactions").
		WillReturnError(errors.New("database error"))

	filter := repositories.CursorFilter{Limit: 10}
	_, _, err := repo.FindByJobID(ctx, jobID, filter)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to list transactions by job")
}

func TestRepository_UpdateStatus_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()
	contextID := uuid.New()
	entity := createValidTransactionEntity()
	entity.ID = id
	entity.Status = shared.TransactionStatusMatched

	mock.ExpectBegin()
	mock.ExpectQuery("UPDATE transactions SET status").
		WithArgs(shared.TransactionStatusMatched.String(), id.String(), contextID.String()).
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))
	mock.ExpectCommit()

	result, err := repo.UpdateStatus(ctx, id, contextID, shared.TransactionStatusMatched)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestRepository_UpdateStatus_DBError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	id := uuid.New()
	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("UPDATE transactions SET status").
		WithArgs(shared.TransactionStatusMatched.String(), id.String(), contextID.String()).
		WillReturnError(errors.New("db error"))
	mock.ExpectRollback()

	result, err := repo.UpdateStatus(ctx, id, contextID, shared.TransactionStatusMatched)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to update transaction status")
}

func TestRepository_MarkMatched_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New(), uuid.New()}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE transactions SET").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	err := repo.MarkMatched(ctx, contextID, txIDs)
	require.NoError(t, err)
}

func TestRepository_MarkMatched_DBError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New()}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE transactions SET").WillReturnError(errors.New("db error"))
	mock.ExpectRollback()

	err := repo.MarkMatched(ctx, contextID, txIDs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to mark transactions matched")
}

func TestRepository_MarkUnmatched_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New(), uuid.New()}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE transactions SET").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	err := repo.MarkUnmatched(ctx, contextID, txIDs)
	require.NoError(t, err)
}

func TestRepository_MarkUnmatched_DBError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New()}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE transactions SET").WillReturnError(errors.New("db error"))
	mock.ExpectRollback()

	err := repo.MarkUnmatched(ctx, contextID, txIDs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to mark transactions unmatched")
}

func TestRepository_MarkPendingReview_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New(), uuid.New()}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE transactions SET").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	err := repo.MarkPendingReview(ctx, contextID, txIDs)
	require.NoError(t, err)
}

func TestRepository_MarkPendingReview_DBError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New()}

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE transactions SET").WillReturnError(errors.New("db error"))
	mock.ExpectRollback()

	err := repo.MarkPendingReview(ctx, contextID, txIDs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to mark transactions pending review")
}

func TestRepository_ExistsBulkBySourceAndExternalID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	sourceID := uuid.New()
	keys := []repositories.ExternalIDKey{
		{SourceID: sourceID, ExternalID: "ext-1"},
		{SourceID: sourceID, ExternalID: "ext-2"},
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT t.source_id, t.external_id FROM transactions").
		WillReturnRows(sqlmock.NewRows([]string{"source_id", "external_id"}).
			AddRow(sourceID.String(), "ext-1"))
	mock.ExpectCommit()

	result, err := repo.ExistsBulkBySourceAndExternalID(ctx, keys)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result[repositories.ExternalIDKey{SourceID: sourceID, ExternalID: "ext-1"}])
	assert.False(t, result[repositories.ExternalIDKey{SourceID: sourceID, ExternalID: "ext-2"}])
}

func TestRepository_ExistsBulkBySourceAndExternalID_DBError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	sourceID := uuid.New()
	keys := []repositories.ExternalIDKey{
		{SourceID: sourceID, ExternalID: "ext-1"},
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT t.source_id, t.external_id FROM transactions").
		WillReturnError(errors.New("db error"))
	mock.ExpectRollback()

	result, err := repo.ExistsBulkBySourceAndExternalID(ctx, keys)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to check bulk transaction existence")
}

func TestRepository_ListUnmatchedByContext_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	entity := createValidTransactionEntity()
	entity.Status = shared.TransactionStatusUnmatched

	mock.ExpectQuery("SELECT .* FROM transactions WHERE").
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))

	result, err := repo.ListUnmatchedByContext(ctx, contextID, nil, nil, 10, 0)
	require.NoError(t, err)
	require.Len(t, result, 1)
}

func TestRepository_ListUnmatchedByContext_DBError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	mock.ExpectQuery("SELECT .* FROM transactions WHERE").
		WillReturnError(errors.New("db error"))

	result, err := repo.ListUnmatchedByContext(ctx, contextID, nil, nil, 10, 0)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to list unmatched transactions")
}

func TestRepository_FindBySourceAndExternalID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	sourceID := uuid.New()
	externalID := "ext-123"
	entity := createValidTransactionEntity()
	entity.SourceID = sourceID
	entity.ExternalID = externalID

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM transactions WHERE source_id").
		WithArgs(sourceID.String(), externalID).
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))
	mock.ExpectCommit()

	result, err := repo.FindBySourceAndExternalID(ctx, sourceID, externalID)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, externalID, result.ExternalID)
}

func TestRepository_FindBySourceAndExternalID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	sourceID := uuid.New()
	externalID := "not-found"

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM transactions WHERE source_id").
		WithArgs(sourceID.String(), externalID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.FindBySourceAndExternalID(ctx, sourceID, externalID)
	require.Error(t, err)
	require.Nil(t, result)
}

func TestRepository_ExistsBySourceAndExternalID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	sourceID := uuid.New()
	externalID := "ext-123"

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT EXISTS").
		WithArgs(sourceID.String(), externalID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectCommit()

	result, err := repo.ExistsBySourceAndExternalID(ctx, sourceID, externalID)
	require.NoError(t, err)
	assert.True(t, result)
}

func TestRepository_ExistsBySourceAndExternalID_NotExists(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	sourceID := uuid.New()
	externalID := "not-exists"

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT EXISTS").
		WithArgs(sourceID.String(), externalID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectCommit()

	result, err := repo.ExistsBySourceAndExternalID(ctx, sourceID, externalID)
	require.NoError(t, err)
	assert.False(t, result)
}

func TestRepository_ExistsBySourceAndExternalID_DBError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	sourceID := uuid.New()
	externalID := "error"

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT EXISTS").
		WillReturnError(errors.New("db error"))
	mock.ExpectRollback()

	result, err := repo.ExistsBySourceAndExternalID(ctx, sourceID, externalID)
	require.Error(t, err)
	assert.False(t, result)
	require.Contains(t, err.Error(), "failed to check transaction existence")
}

func TestRepository_FindByJobAndContextID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()
	contextID := uuid.New()
	entity := createValidTransactionEntity()
	entity.IngestionJobID = jobID

	mock.ExpectQuery("SELECT .* FROM transactions WHERE ingestion_job_id").
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))

	filter := repositories.CursorFilter{
		Limit:     10,
		SortBy:    "id",
		SortOrder: "ASC",
	}

	result, pagination, err := repo.FindByJobAndContextID(ctx, jobID, contextID, filter)
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.NotNil(t, pagination)
}

func TestRepository_FindByJobAndContextID_DBError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()
	contextID := uuid.New()

	mock.ExpectQuery("SELECT .* FROM transactions").
		WillReturnError(errors.New("database error"))

	filter := repositories.CursorFilter{Limit: 10}
	_, _, err := repo.FindByJobAndContextID(ctx, jobID, contextID, filter)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to list transactions by job and context")
}

func TestRepository_Create_InsertError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	entity := createValidTransactionEntity()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO transactions").
		WillReturnError(errors.New("insert error"))
	mock.ExpectRollback()

	result, err := repo.Create(ctx, entity)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to create transaction")
}

func TestRepository_CreateBatch_PrepareError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	txs := []*shared.Transaction{createValidTransactionEntity()}

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO transactions").WillReturnError(errors.New("prepare error"))
	mock.ExpectRollback()

	result, err := repo.CreateBatch(ctx, txs)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to create batch")
}
