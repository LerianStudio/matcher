//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	repoMocks "github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	configPorts "github.com/LerianStudio/matcher/internal/configuration/ports"
	portMocks "github.com/LerianStudio/matcher/internal/configuration/ports/mocks"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// Sentinel errors for stub repositories.
var (
	errCreateFailed                    = errors.New("create failed")
	errCreateNotImplemented            = errors.New("create not implemented")
	errFindByIDNotImplemented          = errors.New("find by id not implemented")
	errFindByNameNotImplemented        = errors.New("find by name not implemented")
	errFindAllNotImplemented           = errors.New("find all not implemented")
	errUpdateNotImplemented            = errors.New("update not implemented")
	errDeleteNotImplemented            = errors.New("delete not implemented")
	errCountNotImplemented             = errors.New("count not implemented")
	errFindByContextNotImplemented     = errors.New("find by context not implemented")
	errFindByContextTypeNotImplemented = errors.New("find by context and type not implemented")
	errFindBySourceNotImplemented      = errors.New("find by source not implemented")
	errFindByPriorityNotImplemented    = errors.New("find by priority not implemented")
	errReorderNotImplemented           = errors.New("reorder not implemented")
	errFindFeeRulesNotImplemented      = errors.New("find fee rules not implemented")
)

type contextRepoStub struct {
	createFn     func(context.Context, *entities.ReconciliationContext) (*entities.ReconciliationContext, error)
	findByIDFn   func(context.Context, uuid.UUID) (*entities.ReconciliationContext, error)
	findByNameFn func(context.Context, string) (*entities.ReconciliationContext, error)
	updateFn     func(context.Context, *entities.ReconciliationContext) (*entities.ReconciliationContext, error)
	deleteFn     func(context.Context, uuid.UUID) error
}

func (stub *contextRepoStub) Create(
	ctx context.Context,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if stub.createFn != nil {
		return stub.createFn(ctx, entity)
	}

	return nil, errCreateNotImplemented
}

func (stub *contextRepoStub) FindByID(
	ctx context.Context,
	identifier uuid.UUID,
) (*entities.ReconciliationContext, error) {
	if stub.findByIDFn != nil {
		return stub.findByIDFn(ctx, identifier)
	}

	return nil, errFindByIDNotImplemented
}

func (stub *contextRepoStub) FindByName(
	ctx context.Context,
	name string,
) (*entities.ReconciliationContext, error) {
	if stub.findByNameFn != nil {
		return stub.findByNameFn(ctx, name)
	}

	return nil, errFindByNameNotImplemented
}

func (stub *contextRepoStub) FindAll(
	_ context.Context,
	_ string,
	_ int,
	_ *value_objects.ContextType,
	_ *value_objects.ContextStatus,
) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, errFindAllNotImplemented
}

func (stub *contextRepoStub) Update(
	ctx context.Context,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if stub.updateFn != nil {
		return stub.updateFn(ctx, entity)
	}

	return nil, errUpdateNotImplemented
}

func (stub *contextRepoStub) Delete(ctx context.Context, identifier uuid.UUID) error {
	if stub.deleteFn != nil {
		return stub.deleteFn(ctx, identifier)
	}

	return errDeleteNotImplemented
}

func (stub *contextRepoStub) Count(_ context.Context) (int64, error) {
	return 0, errCountNotImplemented
}

type sourceRepoStub struct {
	createFn                 func(context.Context, *entities.ReconciliationSource) (*entities.ReconciliationSource, error)
	findByIDFn               func(context.Context, uuid.UUID, uuid.UUID) (*entities.ReconciliationSource, error)
	findByContextIDFn        func(context.Context, uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error)
	findByContextIDAndTypeFn func(context.Context, uuid.UUID, value_objects.SourceType) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error)
	updateFn                 func(context.Context, *entities.ReconciliationSource) (*entities.ReconciliationSource, error)
	deleteFn                 func(context.Context, uuid.UUID, uuid.UUID) error
}

func (stub *sourceRepoStub) Create(
	ctx context.Context,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if stub.createFn != nil {
		return stub.createFn(ctx, entity)
	}

	return nil, errCreateNotImplemented
}

func (stub *sourceRepoStub) FindByID(
	ctx context.Context,
	contextID, identifier uuid.UUID,
) (*entities.ReconciliationSource, error) {
	if stub.findByIDFn != nil {
		return stub.findByIDFn(ctx, contextID, identifier)
	}

	return nil, errFindByIDNotImplemented
}

func (stub *sourceRepoStub) FindByContextID(
	ctx context.Context,
	contextID uuid.UUID,
	_ string,
	_ int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	if stub.findByContextIDFn != nil {
		return stub.findByContextIDFn(ctx, contextID)
	}

	return nil, libHTTP.CursorPagination{}, errFindByContextNotImplemented
}

func (stub *sourceRepoStub) FindByContextIDAndType(
	ctx context.Context,
	contextID uuid.UUID,
	sourceType value_objects.SourceType,
	_ string,
	_ int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	if stub.findByContextIDAndTypeFn != nil {
		return stub.findByContextIDAndTypeFn(ctx, contextID, sourceType)
	}

	return nil, libHTTP.CursorPagination{}, errFindByContextTypeNotImplemented
}

func (stub *sourceRepoStub) Update(
	ctx context.Context,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if stub.updateFn != nil {
		return stub.updateFn(ctx, entity)
	}

	return nil, errUpdateNotImplemented
}

func (stub *sourceRepoStub) Delete(ctx context.Context, contextID, identifier uuid.UUID) error {
	if stub.deleteFn != nil {
		return stub.deleteFn(ctx, contextID, identifier)
	}

	return errDeleteNotImplemented
}

type fieldMapRepoStub struct {
	createFn         func(context.Context, *entities.FieldMap) (*entities.FieldMap, error)
	findByIDFn       func(context.Context, uuid.UUID) (*entities.FieldMap, error)
	findBySourceIDFn func(context.Context, uuid.UUID) (*entities.FieldMap, error)
	updateFn         func(context.Context, *entities.FieldMap) (*entities.FieldMap, error)
	deleteFn         func(context.Context, uuid.UUID) error
}

func (stub *fieldMapRepoStub) Create(
	ctx context.Context,
	entity *entities.FieldMap,
) (*entities.FieldMap, error) {
	if stub.createFn != nil {
		return stub.createFn(ctx, entity)
	}

	return nil, errCreateNotImplemented
}

func (stub *fieldMapRepoStub) FindByID(
	ctx context.Context,
	identifier uuid.UUID,
) (*entities.FieldMap, error) {
	if stub.findByIDFn != nil {
		return stub.findByIDFn(ctx, identifier)
	}

	return nil, errFindByIDNotImplemented
}

func (stub *fieldMapRepoStub) FindBySourceID(
	ctx context.Context,
	sourceID uuid.UUID,
) (*entities.FieldMap, error) {
	if stub.findBySourceIDFn != nil {
		return stub.findBySourceIDFn(ctx, sourceID)
	}

	return nil, errFindBySourceNotImplemented
}

func (stub *fieldMapRepoStub) Update(
	ctx context.Context,
	entity *entities.FieldMap,
) (*entities.FieldMap, error) {
	if stub.updateFn != nil {
		return stub.updateFn(ctx, entity)
	}

	return nil, errUpdateNotImplemented
}

func (stub *fieldMapRepoStub) ExistsBySourceIDs(
	_ context.Context,
	sourceIDs []uuid.UUID,
) (map[uuid.UUID]bool, error) {
	return make(map[uuid.UUID]bool, len(sourceIDs)), nil
}

func (stub *fieldMapRepoStub) Delete(ctx context.Context, identifier uuid.UUID) error {
	if stub.deleteFn != nil {
		return stub.deleteFn(ctx, identifier)
	}

	return errDeleteNotImplemented
}

type matchRuleRepoStub struct {
	createFn                 func(context.Context, *entities.MatchRule) (*entities.MatchRule, error)
	findByIDFn               func(context.Context, uuid.UUID, uuid.UUID) (*entities.MatchRule, error)
	findByContextIDFn        func(context.Context, uuid.UUID, string, int) (entities.MatchRules, libHTTP.CursorPagination, error)
	findByContextIDAndTypeFn func(context.Context, uuid.UUID, value_objects.RuleType, string, int) (entities.MatchRules, libHTTP.CursorPagination, error)
	findByPriorityFn         func(context.Context, uuid.UUID, int) (*entities.MatchRule, error)
	updateFn                 func(context.Context, *entities.MatchRule) (*entities.MatchRule, error)
	deleteFn                 func(context.Context, uuid.UUID, uuid.UUID) error
	reorderFn                func(context.Context, uuid.UUID, []uuid.UUID) error
}

type feeRuleRepoStub struct {
	findByContextIDFn func(context.Context, uuid.UUID) ([]*sharedfee.FeeRule, error)
}

func (stub *feeRuleRepoStub) Create(_ context.Context, _ *sharedfee.FeeRule) error {
	return errCreateNotImplemented
}

func (stub *feeRuleRepoStub) CreateWithTx(_ context.Context, _ *sql.Tx, _ *sharedfee.FeeRule) error {
	return errCreateNotImplemented
}

func (stub *feeRuleRepoStub) FindByID(_ context.Context, _ uuid.UUID) (*sharedfee.FeeRule, error) {
	return nil, errFindByIDNotImplemented
}

func (stub *feeRuleRepoStub) FindByContextID(ctx context.Context, contextID uuid.UUID) ([]*sharedfee.FeeRule, error) {
	if stub.findByContextIDFn != nil {
		return stub.findByContextIDFn(ctx, contextID)
	}

	return nil, errFindFeeRulesNotImplemented
}

func (stub *feeRuleRepoStub) Update(_ context.Context, _ *sharedfee.FeeRule) error {
	return errUpdateNotImplemented
}

func (stub *feeRuleRepoStub) UpdateWithTx(_ context.Context, _ *sql.Tx, _ *sharedfee.FeeRule) error {
	return errUpdateNotImplemented
}

func (stub *feeRuleRepoStub) Delete(_ context.Context, _, _ uuid.UUID) error {
	return errDeleteNotImplemented
}

func (stub *feeRuleRepoStub) DeleteWithTx(_ context.Context, _ *sql.Tx, _, _ uuid.UUID) error {
	return errDeleteNotImplemented
}

func (stub *matchRuleRepoStub) Create(
	ctx context.Context,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if stub.createFn != nil {
		return stub.createFn(ctx, entity)
	}

	return nil, errCreateNotImplemented
}

func (stub *matchRuleRepoStub) FindByID(
	ctx context.Context,
	contextID, identifier uuid.UUID,
) (*entities.MatchRule, error) {
	if stub.findByIDFn != nil {
		return stub.findByIDFn(ctx, contextID, identifier)
	}

	return nil, errFindByIDNotImplemented
}

func (stub *matchRuleRepoStub) FindByContextID(
	ctx context.Context,
	contextID uuid.UUID,
	cursor string,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	if stub.findByContextIDFn != nil {
		return stub.findByContextIDFn(ctx, contextID, cursor, limit)
	}

	return nil, libHTTP.CursorPagination{}, errFindByContextNotImplemented
}

func (stub *matchRuleRepoStub) FindByContextIDAndType(
	ctx context.Context,
	contextID uuid.UUID,
	ruleType value_objects.RuleType,
	cursor string,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	if stub.findByContextIDAndTypeFn != nil {
		return stub.findByContextIDAndTypeFn(ctx, contextID, ruleType, cursor, limit)
	}

	return nil, libHTTP.CursorPagination{}, errFindByContextTypeNotImplemented
}

func (stub *matchRuleRepoStub) FindByPriority(
	ctx context.Context,
	contextID uuid.UUID,
	priority int,
) (*entities.MatchRule, error) {
	if stub.findByPriorityFn != nil {
		return stub.findByPriorityFn(ctx, contextID, priority)
	}

	return nil, errFindByPriorityNotImplemented
}

func (stub *matchRuleRepoStub) Update(
	ctx context.Context,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if stub.updateFn != nil {
		return stub.updateFn(ctx, entity)
	}

	return nil, errUpdateNotImplemented
}

func (stub *matchRuleRepoStub) Delete(ctx context.Context, contextID, identifier uuid.UUID) error {
	if stub.deleteFn != nil {
		return stub.deleteFn(ctx, contextID, identifier)
	}

	return errDeleteNotImplemented
}

func (stub *matchRuleRepoStub) ReorderPriorities(
	ctx context.Context,
	contextID uuid.UUID,
	ruleIDs []uuid.UUID,
) error {
	if stub.reorderFn != nil {
		return stub.reorderFn(ctx, contextID, ruleIDs)
	}

	return errReorderNotImplemented
}

type contextRepoTxStub struct {
	*contextRepoStub
	createWithTxFn   func(context.Context, *sql.Tx, *entities.ReconciliationContext) (*entities.ReconciliationContext, error)
	findByIDWithTxFn func(context.Context, *sql.Tx, uuid.UUID) (*entities.ReconciliationContext, error)
}

func (stub *contextRepoTxStub) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if stub.createWithTxFn != nil {
		return stub.createWithTxFn(ctx, tx, entity)
	}

	return nil, errCreateNotImplemented
}

func (stub *contextRepoTxStub) FindByIDWithTx(
	ctx context.Context,
	tx *sql.Tx,
	identifier uuid.UUID,
) (*entities.ReconciliationContext, error) {
	if stub.findByIDWithTxFn != nil {
		return stub.findByIDWithTxFn(ctx, tx, identifier)
	}

	return nil, errFindByIDNotImplemented
}

type sourceRepoTxStub struct {
	*sourceRepoStub
	createWithTxFn func(context.Context, *sql.Tx, *entities.ReconciliationSource) (*entities.ReconciliationSource, error)
}

func (stub *sourceRepoTxStub) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if stub.createWithTxFn != nil {
		return stub.createWithTxFn(ctx, tx, entity)
	}

	return nil, errCreateNotImplemented
}

type fieldMapRepoTxStub struct {
	*fieldMapRepoStub
	createWithTxFn func(context.Context, *sql.Tx, *entities.FieldMap) (*entities.FieldMap, error)
}

func (stub *fieldMapRepoTxStub) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	entity *entities.FieldMap,
) (*entities.FieldMap, error) {
	if stub.createWithTxFn != nil {
		return stub.createWithTxFn(ctx, tx, entity)
	}

	return nil, errCreateNotImplemented
}

type matchRuleRepoTxStub struct {
	*matchRuleRepoStub
	createWithTxFn func(context.Context, *sql.Tx, *entities.MatchRule) (*entities.MatchRule, error)
}

func (stub *matchRuleRepoTxStub) CreateWithTx(
	ctx context.Context,
	tx *sql.Tx,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if stub.createWithTxFn != nil {
		return stub.createWithTxFn(ctx, tx, entity)
	}

	return nil, errCreateNotImplemented
}

func setupInfraProviderWithSQLMock(t *testing.T) (*testutil.MockInfrastructureProvider, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)

	return provider, mock, db
}

func TestCreateContext_Command(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tenantID  uuid.UUID
		input     entities.CreateReconciliationContextInput
		createErr error
		wantErr   error
	}{
		{
			name:     "valid input",
			tenantID: uuid.New(),
			input: entities.CreateReconciliationContextInput{
				Name:     "Ledger vs Bank",
				Type:     value_objects.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
		},
		{
			name:     "empty name",
			tenantID: uuid.New(),
			input: entities.CreateReconciliationContextInput{
				Name:     "",
				Type:     value_objects.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
			wantErr: entities.ErrContextNameRequired,
		},
		{
			name:     "invalid type",
			tenantID: uuid.New(),
			input: entities.CreateReconciliationContextInput{
				Name:     "Invalid",
				Type:     value_objects.ContextType("INVALID"),
				Interval: "0 0 * * *",
			},
			wantErr: entities.ErrContextTypeInvalid,
		},
		{
			name:     "empty interval",
			tenantID: uuid.New(),
			input: entities.CreateReconciliationContextInput{
				Name:     "No Interval",
				Type:     value_objects.ContextTypeOneToOne,
				Interval: "",
			},
			wantErr: entities.ErrContextIntervalRequired,
		},
		{
			name:     "nil tenant",
			tenantID: uuid.Nil,
			input: entities.CreateReconciliationContextInput{
				Name:     "Tenant Missing",
				Type:     value_objects.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
			wantErr: entities.ErrContextTenantRequired,
		},
		{
			name:     "repository error",
			tenantID: uuid.New(),
			input: entities.CreateReconciliationContextInput{
				Name:     "Repo Error",
				Type:     value_objects.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			},
			createErr: errCreateFailed,
			wantErr:   errCreateFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &contextRepoStub{
				findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
					return nil, nil // No duplicate
				},
				createFn: func(ctx context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
					if tt.createErr != nil {
						return nil, tt.createErr
					}

					return entity, nil
				},
			}
			useCase, err := NewUseCase(
				repo,
				&sourceRepoStub{},
				&fieldMapRepoStub{},
				&matchRuleRepoStub{},
			)
			require.NoError(t, err)

			contextEntity, err := useCase.CreateContext(context.Background(), tt.tenantID, tt.input)
			if tt.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErr, "expected error %v, got %v", tt.wantErr, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.tenantID, contextEntity.TenantID)
			assert.Equal(t, tt.input.Name, contextEntity.Name)
		})
	}
}

func TestCreateContext_InlineCreateWithoutInfrastructureProvider(t *testing.T) {
	t.Parallel()

	createCalled := false
	repo := &contextRepoStub{
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return nil, nil
		},
		createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			createCalled = true
			return entity, nil
		},
	}

	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Inline Context",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{Name: "Bank", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideLeft},
		},
	})
	require.ErrorIs(t, err, ErrInlineCreateRequiresInfrastructure)
	assert.False(t, createCalled, "contextRepo.Create must not run when inline create is invalid")
}

func TestCreateContext_WithInfrastructureProviderAndNoInlineResources_UsesRegularCreate(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	createCalled := 0

	repo := &contextRepoStub{
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return nil, nil
		},
		createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			createCalled++
			return entity, nil
		},
	}

	useCase, err := NewUseCase(
		repo,
		&sourceRepoStub{},
		&fieldMapRepoStub{},
		&matchRuleRepoStub{},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	created, err := useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Regular Create",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	})
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.Equal(t, 1, createCalled)
}

func TestCreateContext_TransactionalInlineCreate_Success(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	contextCreateWithTxCalls := 0
	sourceCreateWithTxCalls := 0
	fieldMapCreateWithTxCalls := 0
	ruleCreateWithTxCalls := 0

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			contextCreateWithTxCalls++
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			sourceCreateWithTxCalls++
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoTxStub{
		fieldMapRepoStub: &fieldMapRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.FieldMap) (*entities.FieldMap, error) {
			fieldMapCreateWithTxCalls++
			return entity, nil
		},
	}

	ruleRepo := &matchRuleRepoTxStub{
		matchRuleRepoStub: &matchRuleRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.MatchRule) (*entities.MatchRule, error) {
			ruleCreateWithTxCalls++
			return entity, nil
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		fmRepo,
		ruleRepo,
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	created, err := useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Transactional Context",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{
				Name:    "Bank",
				Type:    value_objects.SourceTypeBank,
				Side:    sharedfee.MatchingSideLeft,
				Mapping: map[string]any{"amount": "col_amount"},
			},
		},
		Rules: []entities.CreateMatchRuleInput{
			{
				Priority: 1,
				Type:     value_objects.RuleTypeExact,
				Config:   map[string]any{"matchAmount": true},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.Equal(t, "Transactional Context", created.Name)
	assert.Equal(t, value_objects.ContextTypeOneToOne, created.Type)
	assert.Equal(t, value_objects.ContextStatusDraft, created.Status)
	assert.Equal(t, 1, contextCreateWithTxCalls)
	assert.Equal(t, 1, sourceCreateWithTxCalls)
	assert.Equal(t, 1, fieldMapCreateWithTxCalls)
	assert.Equal(t, 1, ruleCreateWithTxCalls)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_EmptyMappingSkipsFieldMap(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	fieldMapCreateWithTxCalls := 0

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoTxStub{
		fieldMapRepoStub: &fieldMapRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.FieldMap) (*entities.FieldMap, error) {
			fieldMapCreateWithTxCalls++
			return entity, nil
		},
	}

	ruleRepo := &matchRuleRepoTxStub{
		matchRuleRepoStub: &matchRuleRepoStub{},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		fmRepo,
		ruleRepo,
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Empty Mapping Context",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{
				Name:    "Bank",
				Type:    value_objects.SourceTypeBank,
				Side:    sharedfee.MatchingSideLeft,
				Mapping: map[string]any{},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, fieldMapCreateWithTxCalls)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_DuplicateRulePriority(t *testing.T) {
	t.Parallel()

	provider, _, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	contextCreateWithTxCalls := 0

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			contextCreateWithTxCalls++
			return entity, nil
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		&sourceRepoTxStub{sourceRepoStub: &sourceRepoStub{}},
		&fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}},
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Duplicate Priorities",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Rules: []entities.CreateMatchRuleInput{
			{Priority: 1, Type: value_objects.RuleTypeExact, Config: map[string]any{"matchAmount": true}},
			{Priority: 1, Type: value_objects.RuleTypeTolerance, Config: map[string]any{"absTolerance": 0.01}},
		},
	})
	require.ErrorIs(t, err, entities.ErrRulePriorityConflict)
	assert.Equal(t, 0, contextCreateWithTxCalls)
}

func TestCreateContext_TransactionalInlineCreate_SourceCreateError(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, _ *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return nil, errCreateFailed
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		&fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}},
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Source Create Failure",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{{
			Name: "Bank",
			Type: value_objects.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "creating source")
	require.ErrorIs(t, err, errCreateFailed)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_FieldMapCreateError(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoTxStub{
		fieldMapRepoStub: &fieldMapRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, _ *entities.FieldMap) (*entities.FieldMap, error) {
			return nil, errCreateFailed
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		fmRepo,
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Field Map Create Failure",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{{
			Name:    "Bank",
			Type:    value_objects.SourceTypeBank,
			Side:    sharedfee.MatchingSideLeft,
			Mapping: map[string]any{"amount": "col_amount"},
		}},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "creating field map")
	require.ErrorIs(t, err, errCreateFailed)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_RuleCreateError(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	ruleRepo := &matchRuleRepoTxStub{
		matchRuleRepoStub: &matchRuleRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, _ *entities.MatchRule) (*entities.MatchRule, error) {
			return nil, errCreateFailed
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		&sourceRepoTxStub{sourceRepoStub: &sourceRepoStub{}},
		&fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}},
		ruleRepo,
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Rule Create Failure",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Rules: []entities.CreateMatchRuleInput{{
			Priority: 1,
			Type:     value_objects.RuleTypeExact,
			Config:   map[string]any{"matchAmount": true},
		}},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "creating rule")
	require.ErrorIs(t, err, errCreateFailed)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_CommitError(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(errCreateFailed)

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	ruleRepo := &matchRuleRepoTxStub{
		matchRuleRepoStub: &matchRuleRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.MatchRule) (*entities.MatchRule, error) {
			return entity, nil
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		&sourceRepoTxStub{sourceRepoStub: &sourceRepoStub{}},
		&fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}},
		ruleRepo,
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Commit Failure",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Rules: []entities.CreateMatchRuleInput{{
			Priority: 1,
			Type:     value_objects.RuleTypeExact,
			Config:   map[string]any{"matchAmount": true},
		}},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "commit create transaction")
	require.ErrorIs(t, err, errCreateFailed)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_MixedMappings(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	sourceCreateWithTxCalls := 0
	fieldMapCreateWithTxCalls := 0

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			sourceCreateWithTxCalls++
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoTxStub{
		fieldMapRepoStub: &fieldMapRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.FieldMap) (*entities.FieldMap, error) {
			fieldMapCreateWithTxCalls++
			return entity, nil
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		fmRepo,
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Mixed Mapping Context",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{Name: "Bank A", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideLeft, Mapping: map[string]any{"amount": "col_amount"}},
			{Name: "Bank B", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideRight, Mapping: map[string]any{}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 2, sourceCreateWithTxCalls)
	assert.Equal(t, 1, fieldMapCreateWithTxCalls)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_ContextRepoReturnsNilEntity(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, _ *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return nil, nil
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		&sourceRepoTxStub{sourceRepoStub: &sourceRepoStub{}},
		&fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}},
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Nil Context",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources:  []entities.CreateContextSourceInput{{Name: "Bank", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideLeft}},
	})
	require.ErrorIs(t, err, ErrCreateContextReturnedNil)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_SourceRepoReturnsNilEntity(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, _ *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return nil, nil
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		&fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}},
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Nil Source",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources:  []entities.CreateContextSourceInput{{Name: "Bank", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideLeft}},
	})
	require.ErrorIs(t, err, ErrCreateSourceReturnedNil)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_MissingTxSupport(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	mockCtxRepo.EXPECT().FindByName(gomock.Any(), "Tx Unsupported").Return(nil, nil)

	provider := &testutil.MockInfrastructureProvider{}
	useCase, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Tx Unsupported",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources:  []entities.CreateContextSourceInput{{Name: "Bank", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideLeft}},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCreateContextTxSupportRequired)
}

func TestCreateContext_DatabaseUniqueViolationMapsToContextNameAlreadyExists(t *testing.T) {
	t.Parallel()

	repo := &contextRepoStub{
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return nil, nil
		},
		createFn: func(_ context.Context, _ *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return nil, &pgconn.PgError{Code: postgresUniqueViolationCode, ConstraintName: "uq_context_tenant_name"}
		},
	}

	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Race Context",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	})
	require.ErrorIs(t, err, ErrContextNameAlreadyExists)
}

func TestCreateContext_WithAuditPublisher_IncludesInlineCounts(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	ctrl := gomock.NewController(t)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}}
	ruleRepo := &matchRuleRepoTxStub{
		matchRuleRepoStub: &matchRuleRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.MatchRule) (*entities.MatchRule, error) {
			return entity, nil
		},
	}

	mockAuditPub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, event configPorts.AuditEvent) error {
			require.Equal(t, "context", event.EntityType)
			require.Equal(t, "create", event.Action)
			assert.Equal(t, 1, event.Changes["sources_count"])
			assert.Equal(t, 1, event.Changes["rules_count"])
			return nil
		})

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		fmRepo,
		ruleRepo,
		WithInfrastructureProvider(provider),
		WithAuditPublisher(mockAuditPub),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Audited Context",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources:  []entities.CreateContextSourceInput{{Name: "Bank", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideLeft}},
		Rules: []entities.CreateMatchRuleInput{{
			Priority: 1,
			Type:     value_objects.RuleTypeExact,
			Config:   map[string]any{"matchAmount": true},
		}},
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateContext_CommandValidation(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	ctx := context.Background()
	existing, err := entities.NewReconciliationContext(
		ctx,
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Context",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	repo := &contextRepoStub{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return existing, nil
		},
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return nil, nil // No duplicate
		},
		updateFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	emptyName := ""
	_, err = useCase.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &emptyName},
	)
	require.Error(t, err)
	assert.Equal(t, entities.ErrContextNameRequired, err)
}

func TestUpdateContext_CommandSuccess(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	ctx := context.Background()
	existing, err := entities.NewReconciliationContext(
		ctx,
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Original",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	repo := &contextRepoStub{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return existing, nil
		},
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return nil, nil // No duplicate
		},
		updateFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	newName := "Updated Name"
	updated, err := useCase.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &newName},
	)
	require.NoError(t, err)
	assert.Equal(t, newName, updated.Name)
}

func TestCreateContext_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.CreateContext(
		context.Background(),
		uuid.New(),
		entities.CreateReconciliationContextInput{},
	)
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestCreateContext_NilContextRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo: nil,
	}

	_, err := uc.CreateContext(
		context.Background(),
		uuid.New(),
		entities.CreateReconciliationContextInput{},
	)
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestCreateContext_WithAuditPublisher_SimpleCreate(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	tenantID := uuid.New()
	input := entities.CreateReconciliationContextInput{
		Name:     "Test Context",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
	}

	mockCtxRepo.EXPECT().
		FindByName(gomock.Any(), input.Name).
		Return(nil, sql.ErrNoRows)

	mockCtxRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		})

	mockAuditPub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(nil)

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithAuditPublisher(mockAuditPub),
	)
	require.NoError(t, err)

	result, err := uc.CreateContext(context.Background(), tenantID, input)
	require.NoError(t, err)
	assert.Equal(t, input.Name, result.Name)
}

func TestUpdateContext_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.UpdateContext(
		context.Background(),
		uuid.New(),
		entities.UpdateReconciliationContextInput{},
	)
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestUpdateContext_NilContextRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo: nil,
	}

	_, err := uc.UpdateContext(
		context.Background(),
		uuid.New(),
		entities.UpdateReconciliationContextInput{},
	)
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestUpdateContext_FindByIDError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	findErr := errors.New("context not found")

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), contextID).
		Return(nil, findErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	_, err = uc.UpdateContext(
		context.Background(),
		contextID,
		entities.UpdateReconciliationContextInput{},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "finding reconciliation context")
}

func TestUpdateContext_RepositoryUpdateError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Original",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	updateErr := errors.New("update failed")

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	mockCtxRepo.EXPECT().
		FindByName(gomock.Any(), "Updated Name").
		Return(nil, sql.ErrNoRows)

	mockCtxRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		Return(nil, updateErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	newName := "Updated Name"
	_, err = uc.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &newName},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "updating reconciliation context")
}

func TestUpdateContext_WithAuditPublisher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Original",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	mockCtxRepo.EXPECT().
		FindByName(gomock.Any(), "Updated Name").
		Return(nil, sql.ErrNoRows)

	mockCtxRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		})

	mockAuditPub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(nil)

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithAuditPublisher(mockAuditPub),
	)
	require.NoError(t, err)

	newName := "Updated Name"
	result, err := uc.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &newName},
	)
	require.NoError(t, err)
	assert.Equal(t, newName, result.Name)
}

func TestDeleteContext_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	err := uc.DeleteContext(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestDeleteContext_NilContextRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo: nil,
	}

	err := uc.DeleteContext(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestDeleteContext_FindByIDError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	findErr := errors.New("context not found")

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), contextID).
		Return(nil, findErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), contextID)
	require.Error(t, err)
	require.ErrorContains(t, err, "finding reconciliation context")
}

func TestDeleteContext_DeleteError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "To Delete",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	deleteErr := errors.New("delete failed")

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	// Child entity checks: no children exist.
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockCtxRepo.EXPECT().
		Delete(gomock.Any(), existing.ID).
		Return(deleteErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.Error(t, err)
	require.ErrorContains(t, err, "deleting reconciliation context")
}

func TestDeleteContext_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "To Delete",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	// Child entity checks: no children exist.
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockCtxRepo.EXPECT().
		Delete(gomock.Any(), existing.ID).
		Return(nil)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.NoError(t, err)
}

func TestDeleteContext_WithAuditPublisher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "To Delete",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	// Child entity checks: no children exist.
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockCtxRepo.EXPECT().
		Delete(gomock.Any(), existing.ID).
		Return(nil)

	mockAuditPub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(nil)

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithAuditPublisher(mockAuditPub),
	)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.NoError(t, err)
}

func TestDeleteContext_BlockedBySources(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Has Sources",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	childSource := &entities.ReconciliationSource{ID: uuid.New(), ContextID: existing.ID}
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return([]*entities.ReconciliationSource{childSource}, libHTTP.CursorPagination{}, nil)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.ErrorIs(t, err, ErrContextHasChildEntities)
}

func TestDeleteContext_BlockedByMatchRules(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Has Rules",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	childRule := &entities.MatchRule{ID: uuid.New()}
	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(entities.MatchRules{childRule}, libHTTP.CursorPagination{}, nil)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.ErrorIs(t, err, ErrContextHasChildEntities)
}

func TestDeleteContext_SourceCheckError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Source Error",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	checkErr := errors.New("source repo unavailable")
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, checkErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.Error(t, err)
	require.ErrorContains(t, err, "checking context sources")
	require.ErrorIs(t, err, checkErr)
}

func TestCreateContext_DuplicateName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		findByNameFn func(context.Context, string) (*entities.ReconciliationContext, error)
		wantErr      error
		wantContains string
	}{
		{
			name: "duplicate name returns existing entity",
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return &entities.ReconciliationContext{
					ID:   uuid.New(),
					Name: "Ledger vs Bank",
				}, nil
			},
			wantErr: ErrContextNameAlreadyExists,
		},
		{
			name: "FindByName transient error propagates",
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, errors.New("connection refused")
			},
			wantContains: "checking context name uniqueness",
		},
		{
			name: "FindByName returns sql.ErrNoRows allows creation",
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, sql.ErrNoRows
			},
		},
		{
			name: "FindByName returns nil nil allows creation",
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &contextRepoStub{
				findByNameFn: tt.findByNameFn,
				createFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
					return entity, nil
				},
			}
			useCase, err := NewUseCase(
				repo,
				&sourceRepoStub{},
				&fieldMapRepoStub{},
				&matchRuleRepoStub{},
			)
			require.NoError(t, err)

			result, err := useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
				Name:     "Ledger vs Bank",
				Type:     value_objects.ContextTypeOneToOne,
				Interval: "0 0 * * *",
			})

			if tt.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			if tt.wantContains != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantContains)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "Ledger vs Bank", result.Name)
		})
	}
}

func TestUpdateContext_RenameToDuplicateName(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Original",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	otherEntity := &entities.ReconciliationContext{
		ID:   uuid.New(), // Different ID → name collision
		Name: "Taken Name",
	}

	repo := &contextRepoStub{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return existing, nil
		},
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return otherEntity, nil
		},
		updateFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	newName := "Taken Name"
	_, err = useCase.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &newName},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrContextNameAlreadyExists)
}

func TestUpdateContext_RenameToSameName(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Original",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	// FindByName returns the same entity (same ID) — not a collision.
	repo := &contextRepoStub{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return existing, nil
		},
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return existing, nil // Same ID → allowed
		},
		updateFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	// Different name string but FindByName returns the same entity → no error.
	newName := "Renamed"
	updated, err := useCase.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &newName},
	)
	require.NoError(t, err)
	assert.Equal(t, newName, updated.Name)
}

func TestUpdateContext_NameUnchanged(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Original",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	findByNameCalled := false

	repo := &contextRepoStub{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return existing, nil
		},
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			findByNameCalled = true

			return nil, nil
		},
		updateFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	// input.Name equals entity.Name → uniqueness check should be skipped.
	sameName := "Original"
	updated, err := useCase.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &sameName},
	)
	require.NoError(t, err)
	assert.Equal(t, "Original", updated.Name)
	assert.False(t, findByNameCalled, "FindByName should NOT be called when name is unchanged")
}

func TestUpdateContext_FindByNameTransientError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Original",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	transientErr := errors.New("connection refused")

	repo := &contextRepoStub{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return existing, nil
		},
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return nil, transientErr
		},
		updateFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	newName := "New Name"
	_, err = useCase.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &newName},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "checking context name uniqueness")
	require.ErrorIs(t, err, transientErr)
}

func TestDeleteContext_BlockedBySchedules(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Has Schedules",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	// No sources or rules — those checks pass.
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	scheduleRepo := &mockScheduleRepo{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSchedule, error) {
			return []*entities.ReconciliationSchedule{
				{ID: uuid.New(), ContextID: existing.ID},
			}, nil
		},
	}

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithScheduleRepository(scheduleRepo),
	)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.ErrorIs(t, err, ErrContextHasChildEntities)
}

func TestDeleteContext_BlockedByFeeRules(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Has Fee Rules",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	feeRuleRepo := &feeRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*sharedfee.FeeRule, error) {
			return []*sharedfee.FeeRule{{ID: uuid.New(), ContextID: existing.ID}}, nil
		},
	}

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithFeeRuleRepository(feeRuleRepo),
	)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.ErrorIs(t, err, ErrContextHasChildEntities)
}

func TestDeleteContext_FeeRuleCheckError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Fee Rule Error",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	checkErr := errors.New("fee rule repo unavailable")
	feeRuleRepo := &feeRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*sharedfee.FeeRule, error) {
			return nil, checkErr
		},
	}

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithFeeRuleRepository(feeRuleRepo),
	)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.Error(t, err)
	require.ErrorContains(t, err, "checking context fee rules")
	require.ErrorIs(t, err, checkErr)
}

func TestDeleteContext_ScheduleCheckError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Schedule Error",
			Type:     value_objects.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	// No sources or rules — those checks pass.
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	scheduleCheckErr := errors.New("schedule repo unavailable")
	scheduleRepo := &mockScheduleRepo{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSchedule, error) {
			return nil, scheduleCheckErr
		},
	}

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithScheduleRepository(scheduleRepo),
	)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.Error(t, err)
	require.ErrorContains(t, err, "checking context schedules")
	require.ErrorIs(t, err, scheduleCheckErr)
}

func TestCreateContext_TransactionalInlineCreate_AllSourcesWithMappings(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	sourceCreateWithTxCalls := 0
	fieldMapCreateWithTxCalls := 0

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			sourceCreateWithTxCalls++
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoTxStub{
		fieldMapRepoStub: &fieldMapRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.FieldMap) (*entities.FieldMap, error) {
			fieldMapCreateWithTxCalls++
			return entity, nil
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		fmRepo,
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "All Mapped Context",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{Name: "Bank A", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideLeft, Mapping: map[string]any{"amount": "col_amount"}},
			{Name: "Bank B", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideRight, Mapping: map[string]any{"date": "col_date"}},
			{Name: "Bank C", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideRight, Mapping: map[string]any{"ref": "col_ref"}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 3, sourceCreateWithTxCalls)
	assert.Equal(t, 3, fieldMapCreateWithTxCalls)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_BeginTxError(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	beginErr := errors.New("database unavailable")
	mock.ExpectBegin().WillReturnError(beginErr)

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		&sourceRepoTxStub{sourceRepoStub: &sourceRepoStub{}},
		&fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}},
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Begin Tx Error Context",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{{
			Name: "Bank",
			Type: value_objects.SourceTypeBank,
			Side: sharedfee.MatchingSideLeft,
		}},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "begin create transaction")
	require.ErrorIs(t, err, beginErr)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_NilMappingSkipsFieldMap(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	fieldMapCreateWithTxCalls := 0

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoTxStub{
		fieldMapRepoStub: &fieldMapRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.FieldMap) (*entities.FieldMap, error) {
			fieldMapCreateWithTxCalls++
			return entity, nil
		},
	}

	ruleRepo := &matchRuleRepoTxStub{
		matchRuleRepoStub: &matchRuleRepoStub{},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		fmRepo,
		ruleRepo,
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Nil Mapping Context",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{
				Name:    "Bank",
				Type:    value_objects.SourceTypeBank,
				Side:    sharedfee.MatchingSideLeft,
				Mapping: nil, // explicitly nil — distinct from map[string]any{}
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, fieldMapCreateWithTxCalls, "field map creator must NOT be called when Mapping is nil")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateContext_TransactionalInlineCreate_SecondSourceCreateError(t *testing.T) {
	t.Parallel()

	provider, mock, db := setupInfraProviderWithSQLMock(t)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	ctxRepo := &contextRepoTxStub{
		contextRepoStub: &contextRepoStub{
			findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
				return nil, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}

	sourceCallCount := 0
	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			sourceCallCount++
			if sourceCallCount == 2 {
				return nil, errCreateFailed
			}

			return entity, nil
		},
	}

	useCase, err := NewUseCase(
		ctxRepo,
		srcRepo,
		&fieldMapRepoTxStub{fieldMapRepoStub: &fieldMapRepoStub{}},
		&matchRuleRepoTxStub{matchRuleRepoStub: &matchRuleRepoStub{}},
		WithInfrastructureProvider(provider),
	)
	require.NoError(t, err)

	_, err = useCase.CreateContext(context.Background(), uuid.New(), entities.CreateReconciliationContextInput{
		Name:     "Two Sources Second Fails",
		Type:     value_objects.ContextTypeOneToOne,
		Interval: "0 0 * * *",
		Sources: []entities.CreateContextSourceInput{
			{Name: "Bank A", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideLeft},
			{Name: "Bank B", Type: value_objects.SourceTypeBank, Side: sharedfee.MatchingSideRight},
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "creating source")
	require.ErrorIs(t, err, errCreateFailed)
	assert.Equal(t, 2, sourceCallCount, "source creator must be called twice: once for success, once for failure")
	require.NoError(t, mock.ExpectationsWereMet())
}
