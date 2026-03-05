//go:build unit

//nolint:dupl
package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libPostgres "github.com/LerianStudio/lib-uncommons/v2/uncommons/postgres"
	libRedis "github.com/LerianStudio/lib-uncommons/v2/uncommons/redis"

	sharedhttp "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	governanceEntities "github.com/LerianStudio/matcher/internal/governance/domain/entities"
	governanceRepositories "github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var (
	errAdjTestDatabase = errors.New("database error")
	errAdjTestNotFound = errors.New("not found")
)

// stubInfraProvider implements sharedPorts.InfrastructureProvider for testing.
// Uses sqlmock to create a working mock PostgresConnection for WithTenantTxProvider.
type stubInfraProvider struct {
	connErr error
	db      *sql.DB
}

func (s *stubInfraProvider) Close() {
	if s.db != nil {
		_ = s.db.Close()
	}
}

func (s *stubInfraProvider) GetPostgresConnection(
	_ context.Context,
) (*libPostgres.Client, error) {
	if s.connErr != nil {
		return nil, s.connErr
	}

	if s.db != nil {
		_ = s.db.Close()
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, fmt.Errorf("create sqlmock: %w", err)
	}

	s.db = db

	mock.ExpectBegin()
	mock.ExpectCommit()
	mock.ExpectRollback()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	conn := testutil.NewClientWithResolver(resolver)

	return conn, nil
}

func (s *stubInfraProvider) GetRedisConnection(
	_ context.Context,
) (*libRedis.Client, error) {
	return nil, nil
}

func (s *stubInfraProvider) BeginTx(_ context.Context) (*sql.Tx, error) {
	return nil, nil
}

func (s *stubInfraProvider) GetReplicaDB(_ context.Context) (*sql.DB, error) {
	return nil, nil
}

// stubAuditLogRepo implements governanceRepositories.AuditLogRepository for testing.
type stubAuditLogRepo struct {
	createErr error
}

func (s *stubAuditLogRepo) Create(
	_ context.Context,
	auditLog *governanceEntities.AuditLog,
) (*governanceEntities.AuditLog, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}

	return auditLog, nil
}

func (s *stubAuditLogRepo) CreateWithTx(
	_ context.Context,
	_ governanceRepositories.Tx,
	auditLog *governanceEntities.AuditLog,
) (*governanceEntities.AuditLog, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}

	return auditLog, nil
}

func (s *stubAuditLogRepo) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*governanceEntities.AuditLog, error) {
	return nil, nil
}

func (s *stubAuditLogRepo) ListByEntity(
	_ context.Context,
	_ string,
	_ uuid.UUID,
	_ *sharedhttp.TimestampCursor,
	_ int,
) ([]*governanceEntities.AuditLog, string, error) {
	return nil, "", nil
}

func (s *stubAuditLogRepo) List(
	_ context.Context,
	_ governanceEntities.AuditLogFilter,
	_ *sharedhttp.TimestampCursor,
	_ int,
) ([]*governanceEntities.AuditLog, string, error) {
	return nil, "", nil
}

func TestValidateCreateAdjustmentInput(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000020001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000020002")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000020003")
	transactionID := uuid.MustParse("00000000-0000-0000-0000-000000020004")

	validInput := func() CreateAdjustmentInput {
		return CreateAdjustmentInput{
			TenantID:     tenantID,
			ContextID:    contextID,
			MatchGroupID: &matchGroupID,
			Type:         string(matchingEntities.AdjustmentTypeBankFee),
			Direction:    string(matchingEntities.AdjustmentDirectionDebit),
			Amount:       decimal.NewFromInt(10),
			Currency:     "USD",
			Description:  "Test adjustment",
			Reason:       "Test reason",
			CreatedBy:    "user@test.com",
		}
	}

	tests := []struct {
		name    string
		modify  func(*CreateAdjustmentInput)
		wantErr error
	}{
		{
			name:    "valid input with match group",
			modify:  func(in *CreateAdjustmentInput) {},
			wantErr: nil,
		},
		{
			name: "valid input with transaction id",
			modify: func(in *CreateAdjustmentInput) {
				in.MatchGroupID = nil
				in.TransactionID = &transactionID
			},
			wantErr: nil,
		},
		{
			name: "valid input with both targets",
			modify: func(in *CreateAdjustmentInput) {
				in.TransactionID = &transactionID
			},
			wantErr: nil,
		},
		{
			name: "missing tenant id",
			modify: func(in *CreateAdjustmentInput) {
				in.TenantID = uuid.Nil
			},
			wantErr: ErrAdjustmentTenantIDRequired,
		},
		{
			name: "missing context id",
			modify: func(in *CreateAdjustmentInput) {
				in.ContextID = uuid.Nil
			},
			wantErr: ErrAdjustmentContextIDRequired,
		},
		{
			name: "missing target",
			modify: func(in *CreateAdjustmentInput) {
				in.MatchGroupID = nil
				in.TransactionID = nil
			},
			wantErr: ErrAdjustmentTargetRequired,
		},
		{
			name: "missing type",
			modify: func(in *CreateAdjustmentInput) {
				in.Type = ""
			},
			wantErr: ErrAdjustmentTypeRequired,
		},
		{
			name: "missing direction",
			modify: func(in *CreateAdjustmentInput) {
				in.Direction = ""
			},
			wantErr: ErrAdjustmentDirectionRequired,
		},
		{
			name: "zero amount",
			modify: func(in *CreateAdjustmentInput) {
				in.Amount = decimal.Zero
			},
			wantErr: ErrAdjustmentAmountNotPositive,
		},
		{
			name: "negative amount",
			modify: func(in *CreateAdjustmentInput) {
				in.Amount = decimal.NewFromInt(-10)
			},
			wantErr: ErrAdjustmentAmountNotPositive,
		},
		{
			name: "missing currency",
			modify: func(in *CreateAdjustmentInput) {
				in.Currency = ""
			},
			wantErr: ErrAdjustmentCurrencyRequired,
		},
		{
			name: "missing description",
			modify: func(in *CreateAdjustmentInput) {
				in.Description = ""
			},
			wantErr: ErrAdjustmentDescriptionRequired,
		},
		{
			name: "missing reason",
			modify: func(in *CreateAdjustmentInput) {
				in.Reason = ""
			},
			wantErr: ErrAdjustmentReasonRequired,
		},
		{
			name: "missing created by",
			modify: func(in *CreateAdjustmentInput) {
				in.CreatedBy = ""
			},
			wantErr: ErrAdjustmentCreatedByRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			input := validInput()
			tt.modify(&input)

			uc := &UseCase{}
			err := uc.validateCreateAdjustmentInput(input)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateAdjustment_ContextValidation(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000021001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000021002")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000021003")

	tests := []struct {
		name        string
		ctxProvider stubContextProvider
		wantErr     error
	}{
		{
			name: "context not found - provider error",
			ctxProvider: stubContextProvider{
				contextInfo: nil,
				err:         errAdjTestDatabase,
			},
			wantErr: errAdjTestDatabase,
		},
		{
			name: "context not found - nil result",
			ctxProvider: stubContextProvider{
				contextInfo: nil,
				err:         nil,
			},
			wantErr: ErrAdjustmentContextNotFound,
		},
		{
			name: "context not active",
			ctxProvider: stubContextProvider{
				contextInfo: &ports.ReconciliationContextInfo{
					ID:     contextID,
					Active: false,
				},
			},
			wantErr: ErrAdjustmentContextNotActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uc := &UseCase{
				contextProvider: tt.ctxProvider,
			}

			input := CreateAdjustmentInput{
				TenantID:     tenantID,
				ContextID:    contextID,
				MatchGroupID: &matchGroupID,
				Type:         string(matchingEntities.AdjustmentTypeBankFee),
				Direction:    string(matchingEntities.AdjustmentDirectionDebit),
				Amount:       decimal.NewFromInt(10),
				Currency:     "USD",
				Description:  "Test adjustment",
				Reason:       "Test reason",
				CreatedBy:    "user@test.com",
			}

			result, err := uc.CreateAdjustment(context.Background(), input)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, result)
		})
	}
}

type stubMatchGroupRepoForAdjustment struct {
	stubMatchGroupRepo
	findByIDResult *matchingEntities.MatchGroup
	findByIDErr    error
}

func (s *stubMatchGroupRepoForAdjustment) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*matchingEntities.MatchGroup, error) {
	return s.findByIDResult, s.findByIDErr
}

type stubTxRepoForAdjustment struct {
	stubTxRepo
	findByContextErr error
}

func (s *stubTxRepoForAdjustment) FindByContextAndIDs(
	_ context.Context,
	_ uuid.UUID,
	_ []uuid.UUID,
) ([]*shared.Transaction, error) {
	if s.findByContextErr != nil {
		return nil, s.findByContextErr
	}

	return s.transactions, nil
}

func TestCreateAdjustment_TargetValidation(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000022001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000022002")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000022003")
	transactionID := uuid.MustParse("00000000-0000-0000-0000-000000022004")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000022005")

	tests := []struct {
		name           string
		input          CreateAdjustmentInput
		matchGroupRepo *stubMatchGroupRepoForAdjustment
		txRepo         *stubTxRepoForAdjustment
		wantErr        error
	}{
		{
			name: "match group not found - db error",
			input: CreateAdjustmentInput{
				TenantID:     tenantID,
				ContextID:    contextID,
				MatchGroupID: &matchGroupID,
				Type:         string(matchingEntities.AdjustmentTypeBankFee),
				Direction:    string(matchingEntities.AdjustmentDirectionDebit),
				Amount:       decimal.NewFromInt(10),
				Currency:     "USD",
				Description:  "Test adjustment",
				Reason:       "Test reason",
				CreatedBy:    "user@test.com",
			},
			matchGroupRepo: &stubMatchGroupRepoForAdjustment{findByIDErr: errAdjTestNotFound},
			txRepo:         &stubTxRepoForAdjustment{},
			wantErr:        errAdjTestNotFound,
		},
		{
			name: "match group nil result",
			input: CreateAdjustmentInput{
				TenantID:     tenantID,
				ContextID:    contextID,
				MatchGroupID: &matchGroupID,
				Type:         string(matchingEntities.AdjustmentTypeBankFee),
				Direction:    string(matchingEntities.AdjustmentDirectionDebit),
				Amount:       decimal.NewFromInt(10),
				Currency:     "USD",
				Description:  "Test adjustment",
				Reason:       "Test reason",
				CreatedBy:    "user@test.com",
			},
			matchGroupRepo: &stubMatchGroupRepoForAdjustment{findByIDResult: nil},
			txRepo:         &stubTxRepoForAdjustment{},
			wantErr:        ErrAdjustmentMatchGroupNotFound,
		},
		{
			name: "transaction not found - db error",
			input: CreateAdjustmentInput{
				TenantID:      tenantID,
				ContextID:     contextID,
				TransactionID: &transactionID,
				Type:          string(matchingEntities.AdjustmentTypeBankFee),
				Direction:     string(matchingEntities.AdjustmentDirectionDebit),
				Amount:        decimal.NewFromInt(10),
				Currency:      "USD",
				Description:   "Test adjustment",
				Reason:        "Test reason",
				CreatedBy:     "user@test.com",
			},
			matchGroupRepo: &stubMatchGroupRepoForAdjustment{},
			txRepo:         &stubTxRepoForAdjustment{findByContextErr: errAdjTestNotFound},
			wantErr:        errAdjTestNotFound,
		},
		{
			name: "transaction empty result",
			input: CreateAdjustmentInput{
				TenantID:      tenantID,
				ContextID:     contextID,
				TransactionID: &transactionID,
				Type:          string(matchingEntities.AdjustmentTypeBankFee),
				Direction:     string(matchingEntities.AdjustmentDirectionDebit),
				Amount:        decimal.NewFromInt(10),
				Currency:      "USD",
				Description:   "Test adjustment",
				Reason:        "Test reason",
				CreatedBy:     "user@test.com",
			},
			matchGroupRepo: &stubMatchGroupRepoForAdjustment{},
			txRepo: &stubTxRepoForAdjustment{
				stubTxRepo: stubTxRepo{transactions: []*shared.Transaction{}},
			},
			wantErr: ErrAdjustmentTransactionNotFound,
		},
		{
			name: "valid match group target",
			input: CreateAdjustmentInput{
				TenantID:     tenantID,
				ContextID:    contextID,
				MatchGroupID: &matchGroupID,
				Type:         string(matchingEntities.AdjustmentTypeBankFee),
				Direction:    string(matchingEntities.AdjustmentDirectionDebit),
				Amount:       decimal.NewFromInt(10),
				Currency:     "USD",
				Description:  "Test adjustment",
				Reason:       "Test reason",
				CreatedBy:    "user@test.com",
			},
			matchGroupRepo: &stubMatchGroupRepoForAdjustment{
				findByIDResult: &matchingEntities.MatchGroup{
					ID:        matchGroupID,
					ContextID: contextID,
				},
			},
			txRepo:  &stubTxRepoForAdjustment{},
			wantErr: nil,
		},
		{
			name: "valid transaction target",
			input: CreateAdjustmentInput{
				TenantID:      tenantID,
				ContextID:     contextID,
				TransactionID: &transactionID,
				Type:          string(matchingEntities.AdjustmentTypeBankFee),
				Direction:     string(matchingEntities.AdjustmentDirectionDebit),
				Amount:        decimal.NewFromInt(10),
				Currency:      "USD",
				Description:   "Test adjustment",
				Reason:        "Test reason",
				CreatedBy:     "user@test.com",
			},
			matchGroupRepo: &stubMatchGroupRepoForAdjustment{},
			txRepo: &stubTxRepoForAdjustment{
				stubTxRepo: stubTxRepo{
					transactions: []*shared.Transaction{
						{
							ID:       transactionID,
							SourceID: sourceID,
							Amount:   decimal.NewFromInt(100),
							Currency: "USD",
						},
					},
				},
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			infraProv := &stubInfraProvider{}
			t.Cleanup(infraProv.Close)

			uc := &UseCase{
				contextProvider: stubContextProvider{
					contextInfo: &ports.ReconciliationContextInfo{
						ID:     contextID,
						Active: true,
					},
				},
				matchGroupRepo: tt.matchGroupRepo,
				txRepo:         tt.txRepo,
				adjustmentRepo: &stubAdjustmentRepo{},
				infraProvider:  infraProv,
				auditLogRepo:   &stubAuditLogRepo{},
			}

			result, err := uc.CreateAdjustment(context.Background(), tt.input)

			if tt.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
			}
		})
	}
}

func TestCreateAdjustment_InvalidType(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000023001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000023002")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000023003")

	uc := &UseCase{
		contextProvider: stubContextProvider{
			contextInfo: &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			},
		},
		matchGroupRepo: &stubMatchGroupRepoForAdjustment{
			findByIDResult: &matchingEntities.MatchGroup{
				ID:        matchGroupID,
				ContextID: contextID,
			},
		},
		txRepo:         &stubTxRepoForAdjustment{},
		adjustmentRepo: &stubAdjustmentRepo{},
	}

	input := CreateAdjustmentInput{
		TenantID:     tenantID,
		ContextID:    contextID,
		MatchGroupID: &matchGroupID,
		Type:         "INVALID_TYPE",
		Direction:    string(matchingEntities.AdjustmentDirectionDebit),
		Amount:       decimal.NewFromInt(10),
		Currency:     "USD",
		Description:  "Test adjustment",
		Reason:       "Test reason",
		CreatedBy:    "user@test.com",
	}

	result, err := uc.CreateAdjustment(context.Background(), input)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdjustmentTypeInvalid)
	require.Nil(t, result)
}

func TestCreateAdjustment_AllValidTypes(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000024001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000024002")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000024003")

	types := []matchingEntities.AdjustmentType{
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentTypeFXDifference,
		matchingEntities.AdjustmentTypeRounding,
		matchingEntities.AdjustmentTypeWriteOff,
		matchingEntities.AdjustmentTypeMiscellaneous,
	}

	for _, adjType := range types {
		t.Run(string(adjType), func(t *testing.T) {
			t.Parallel()

			infraProv := &stubInfraProvider{}
			t.Cleanup(infraProv.Close)

			uc := &UseCase{
				contextProvider: stubContextProvider{
					contextInfo: &ports.ReconciliationContextInfo{
						ID:     contextID,
						Active: true,
					},
				},
				matchGroupRepo: &stubMatchGroupRepoForAdjustment{
					findByIDResult: &matchingEntities.MatchGroup{
						ID:        matchGroupID,
						ContextID: contextID,
					},
				},
				txRepo:         &stubTxRepoForAdjustment{},
				adjustmentRepo: &stubAdjustmentRepo{},
				infraProvider:  infraProv,
				auditLogRepo:   &stubAuditLogRepo{},
			}

			input := CreateAdjustmentInput{
				TenantID:     tenantID,
				ContextID:    contextID,
				MatchGroupID: &matchGroupID,
				Type:         string(adjType),
				Direction:    string(matchingEntities.AdjustmentDirectionDebit),
				Amount:       decimal.NewFromInt(10),
				Currency:     "USD",
				Description:  "Test adjustment",
				Reason:       "Test reason",
				CreatedBy:    "user@test.com",
			}

			result, err := uc.CreateAdjustment(context.Background(), input)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, adjType, result.Type)
		})
	}
}

func TestCreateAdjustment_Success(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000025001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000025002")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000025003")

	infraProv := &stubInfraProvider{}
	t.Cleanup(infraProv.Close)

	uc := &UseCase{
		contextProvider: stubContextProvider{
			contextInfo: &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			},
		},
		matchGroupRepo: &stubMatchGroupRepoForAdjustment{
			findByIDResult: &matchingEntities.MatchGroup{
				ID:        matchGroupID,
				ContextID: contextID,
			},
		},
		txRepo:         &stubTxRepoForAdjustment{},
		adjustmentRepo: &stubAdjustmentRepo{},
		infraProvider:  infraProv,
		auditLogRepo:   &stubAuditLogRepo{},
	}

	input := CreateAdjustmentInput{
		TenantID:     tenantID,
		ContextID:    contextID,
		MatchGroupID: &matchGroupID,
		Type:         string(matchingEntities.AdjustmentTypeBankFee),
		Direction:    string(matchingEntities.AdjustmentDirectionDebit),
		Amount:       decimal.NewFromFloat(15.50),
		Currency:     "EUR",
		Description:  "Bank wire fee",
		Reason:       "Variance due to wire transfer fee",
		CreatedBy:    "admin@example.com",
	}

	result, err := uc.CreateAdjustment(context.Background(), input)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.NotEqual(t, uuid.Nil, result.ID)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, &matchGroupID, result.MatchGroupID)
	assert.Nil(t, result.TransactionID)
	assert.Equal(t, matchingEntities.AdjustmentTypeBankFee, result.Type)
	assert.Equal(t, matchingEntities.AdjustmentDirectionDebit, result.Direction)
	assert.True(t, result.Amount.Equal(decimal.NewFromFloat(15.50)))
	assert.Equal(t, "EUR", result.Currency)
	assert.Equal(t, "Bank wire fee", result.Description)
	assert.Equal(t, "Variance due to wire transfer fee", result.Reason)
	assert.Equal(t, "admin@example.com", result.CreatedBy)
	assert.False(t, result.CreatedAt.IsZero())
	assert.False(t, result.UpdatedAt.IsZero())
}

type stubAdjustmentRepoWithError struct {
	stubAdjustmentRepo
	err error
}

func (s *stubAdjustmentRepoWithError) Create(
	_ context.Context,
	_ *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	return nil, s.err
}

func (s *stubAdjustmentRepoWithError) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	return nil, s.err
}

func (s *stubAdjustmentRepoWithError) CreateWithAuditLog(
	_ context.Context,
	_ *matchingEntities.Adjustment,
	_ *shared.AuditLog,
) (*matchingEntities.Adjustment, error) {
	return nil, s.err
}

func (s *stubAdjustmentRepoWithError) CreateWithAuditLogWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ *matchingEntities.Adjustment,
	_ *shared.AuditLog,
) (*matchingEntities.Adjustment, error) {
	return nil, s.err
}

func TestCreateAdjustment_RepositoryError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000026001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000026002")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000026003")

	infraProv := &stubInfraProvider{}
	t.Cleanup(infraProv.Close)

	uc := &UseCase{
		contextProvider: stubContextProvider{
			contextInfo: &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			},
		},
		matchGroupRepo: &stubMatchGroupRepoForAdjustment{
			findByIDResult: &matchingEntities.MatchGroup{
				ID:        matchGroupID,
				ContextID: contextID,
			},
		},
		txRepo:         &stubTxRepoForAdjustment{},
		adjustmentRepo: &stubAdjustmentRepoWithError{err: errAdjTestDatabase},
		infraProvider:  infraProv,
		auditLogRepo:   &stubAuditLogRepo{},
	}

	input := CreateAdjustmentInput{
		TenantID:     tenantID,
		ContextID:    contextID,
		MatchGroupID: &matchGroupID,
		Type:         string(matchingEntities.AdjustmentTypeBankFee),
		Direction:    string(matchingEntities.AdjustmentDirectionDebit),
		Amount:       decimal.NewFromInt(10),
		Currency:     "USD",
		Description:  "Test adjustment",
		Reason:       "Test reason",
		CreatedBy:    "user@test.com",
	}

	result, err := uc.CreateAdjustment(context.Background(), input)
	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "persist adjustment")
}

func TestCreateAdjustment_ZeroAmount(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000027001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000027002")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000027003")

	uc := &UseCase{}

	input := CreateAdjustmentInput{
		TenantID:     tenantID,
		ContextID:    contextID,
		MatchGroupID: &matchGroupID,
		Type:         string(matchingEntities.AdjustmentTypeRounding),
		Direction:    string(matchingEntities.AdjustmentDirectionDebit),
		Amount:       decimal.Zero,
		Currency:     "USD",
		Description:  "Zero rounding adjustment",
		Reason:       "Rounding difference",
		CreatedBy:    "user@test.com",
	}

	result, err := uc.CreateAdjustment(context.Background(), input)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdjustmentAmountNotPositive)
	require.Nil(t, result)
}

func TestCreateAdjustment_NegativeAmount(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000028001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000028002")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000028003")

	uc := &UseCase{}

	input := CreateAdjustmentInput{
		TenantID:     tenantID,
		ContextID:    contextID,
		MatchGroupID: &matchGroupID,
		Type:         string(matchingEntities.AdjustmentTypeFXDifference),
		Direction:    string(matchingEntities.AdjustmentDirectionCredit),
		Amount:       decimal.NewFromFloat(-5.25),
		Currency:     "USD",
		Description:  "Negative FX adjustment",
		Reason:       "FX loss",
		CreatedBy:    "user@test.com",
	}

	result, err := uc.CreateAdjustment(context.Background(), input)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdjustmentAmountNotPositive)
	require.Nil(t, result)
}

func TestCreateAdjustment_WithBothTargets(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000029001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000029002")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000029003")
	transactionID := uuid.MustParse("00000000-0000-0000-0000-000000029004")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000029005")

	confidence, _ := matchingVO.ParseConfidenceScore(90)

	infraProv := &stubInfraProvider{}
	t.Cleanup(infraProv.Close)

	uc := &UseCase{
		contextProvider: stubContextProvider{
			contextInfo: &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			},
		},
		matchGroupRepo: &stubMatchGroupRepoForAdjustment{
			findByIDResult: &matchingEntities.MatchGroup{
				ID:         matchGroupID,
				ContextID:  contextID,
				Confidence: confidence,
			},
		},
		txRepo: &stubTxRepoForAdjustment{
			stubTxRepo: stubTxRepo{
				transactions: []*shared.Transaction{
					{
						ID:       transactionID,
						SourceID: sourceID,
						Amount:   decimal.NewFromInt(100),
						Currency: "USD",
					},
				},
			},
		},
		adjustmentRepo: &stubAdjustmentRepo{},
		infraProvider:  infraProv,
		auditLogRepo:   &stubAuditLogRepo{},
	}

	input := CreateAdjustmentInput{
		TenantID:      tenantID,
		ContextID:     contextID,
		MatchGroupID:  &matchGroupID,
		TransactionID: &transactionID,
		Type:          string(matchingEntities.AdjustmentTypeBankFee),
		Direction:     string(matchingEntities.AdjustmentDirectionDebit),
		Amount:        decimal.NewFromInt(10),
		Currency:      "USD",
		Description:   "Test adjustment",
		Reason:        "Test reason",
		CreatedBy:     "user@test.com",
	}

	result, err := uc.CreateAdjustment(context.Background(), input)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, &matchGroupID, result.MatchGroupID)
	assert.Equal(t, &transactionID, result.TransactionID)
}
