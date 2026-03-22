//go:build unit

package cross

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configRepositories "github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// errTestRepo is a sentinel error used for testing repository failure scenarios.
var errTestRepo = errors.New("database error")

type feeRuleRepositoryStub struct {
	rules []*fee.FeeRule
	err   error
}

var _ configRepositories.FeeRuleRepository = (*feeRuleRepositoryStub)(nil)

func (stub *feeRuleRepositoryStub) Create(context.Context, *fee.FeeRule) error { return nil }
func (stub *feeRuleRepositoryStub) CreateWithTx(context.Context, *sql.Tx, *fee.FeeRule) error {
	return nil
}
func (stub *feeRuleRepositoryStub) FindByID(context.Context, uuid.UUID) (*fee.FeeRule, error) {
	return nil, nil
}
func (stub *feeRuleRepositoryStub) FindByContextID(context.Context, uuid.UUID) ([]*fee.FeeRule, error) {
	if stub.err != nil {
		return nil, stub.err
	}

	return stub.rules, nil
}
func (stub *feeRuleRepositoryStub) Update(context.Context, *fee.FeeRule) error { return nil }
func (stub *feeRuleRepositoryStub) UpdateWithTx(context.Context, *sql.Tx, *fee.FeeRule) error {
	return nil
}
func (stub *feeRuleRepositoryStub) Delete(context.Context, uuid.UUID, uuid.UUID) error { return nil }
func (stub *feeRuleRepositoryStub) DeleteWithTx(context.Context, *sql.Tx, uuid.UUID, uuid.UUID) error {
	return nil
}

func TestNewMatchRuleProviderAdapter_NilRepo(t *testing.T) {
	t.Parallel()

	adapter, err := NewMatchRuleProviderAdapter(nil)
	require.ErrorIs(t, err, ErrMatchRuleRepositoryRequired)
	assert.Nil(t, adapter)
}

func TestNewMatchRuleProviderAdapter_ValidRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockMatchRuleRepository(ctrl)
	adapter, err := NewMatchRuleProviderAdapter(mockRepo)

	require.NoError(t, err)
	require.NotNil(t, adapter)
	assert.Equal(t, mockRepo, adapter.repo)
}

func TestMatchRuleProviderAdapter_ListByContextID_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *MatchRuleProviderAdapter

	ctx := context.Background()
	contextID := uuid.New()

	result, err := adapter.ListByContextID(ctx, contextID)

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrMatchRuleRepositoryRequired)
}

func TestMatchRuleProviderAdapter_ListByContextID_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockMatchRuleRepository(ctrl)
	adapter, err := NewMatchRuleProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	now := time.Now().UTC()

	rules := configEntities.MatchRules{
		&shared.MatchRule{
			ID:        uuid.New(),
			ContextID: contextID,
			Priority:  1,
			Type:      shared.RuleTypeExact,
			Config:    map[string]any{"field": "amount"},
			CreatedAt: now,
			UpdatedAt: now,
		},
		&shared.MatchRule{
			ID:        uuid.New(),
			ContextID: contextID,
			Priority:  2,
			Type:      shared.RuleTypeTolerance,
			Config:    map[string]any{"tolerance": 0.01},
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return(rules, libHTTP.CursorPagination{}, nil)

	result, err := adapter.ListByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, rules[0].ID, result[0].ID)
	assert.Equal(t, rules[1].ID, result[1].ID)
}

func TestMatchRuleProviderAdapter_ListByContextID_EmptyRules(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockMatchRuleRepository(ctrl)
	adapter, err := NewMatchRuleProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return(configEntities.MatchRules{}, libHTTP.CursorPagination{}, nil)

	result, err := adapter.ListByContextID(ctx, contextID)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestMatchRuleProviderAdapter_ListByContextID_PaginatesAllRules(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockMatchRuleRepository(ctrl)
	adapter, err := NewMatchRuleProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	now := time.Now().UTC()

	pageOne := configEntities.MatchRules{
		&shared.MatchRule{ID: uuid.New(), ContextID: contextID, Priority: 1, Type: shared.RuleTypeExact, CreatedAt: now, UpdatedAt: now},
	}
	pageTwo := configEntities.MatchRules{
		&shared.MatchRule{ID: uuid.New(), ContextID: contextID, Priority: 2, Type: shared.RuleTypeTolerance, CreatedAt: now, UpdatedAt: now},
	}

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return(pageOne, libHTTP.CursorPagination{Next: "cursor-2"}, nil)
	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "cursor-2", maxInternalLimit).
		Return(pageTwo, libHTTP.CursorPagination{}, nil)

	result, err := adapter.ListByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, pageOne[0].ID, result[0].ID)
	assert.Equal(t, pageTwo[0].ID, result[1].ID)
}

func TestMatchRuleProviderAdapter_ListByContextID_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockMatchRuleRepository(ctrl)
	adapter, err := NewMatchRuleProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return(nil, libHTTP.CursorPagination{}, errTestRepo)

	result, err := adapter.ListByContextID(ctx, contextID)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "find match rules by context")
	require.ErrorIs(t, err, errTestRepo)
}

func TestNewContextProviderAdapter_NilRepo(t *testing.T) {
	t.Parallel()

	adapter, err := NewContextProviderAdapter(nil)
	require.ErrorIs(t, err, ErrContextRepositoryRequired)
	assert.Nil(t, adapter)
}

func TestContextProviderAdapter_FindByID_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *ContextProviderAdapter

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()

	result, err := adapter.FindByID(ctx, tenantID, contextID)

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrContextRepositoryRequired)
}

func TestContextProviderAdapter_FindByID_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockContextRepository(ctrl)
	adapter, err := NewContextProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()

	ctxEntity := &configEntities.ReconciliationContext{
		ID:        contextID,
		TenantID:  tenantID,
		Name:      "Test Context",
		Type:      value_objects.ContextTypeOneToOne,
		Interval:  "daily",
		Status:    value_objects.ContextStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}

	mockRepo.EXPECT().
		FindByID(ctx, contextID).
		Return(ctxEntity, nil)

	result, err := adapter.FindByID(ctx, tenantID, contextID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, contextID, result.ID)
	assert.Equal(t, shared.ContextType(ctxEntity.Type.String()), result.Type)
	assert.True(t, result.Active)
}

func TestContextProviderAdapter_FindByID_NilResult(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockContextRepository(ctrl)
	adapter, err := NewContextProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByID(ctx, contextID).
		Return(nil, nil)

	result, err := adapter.FindByID(ctx, tenantID, contextID)

	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestNewSourceProviderAdapter_NilRepo(t *testing.T) {
	t.Parallel()

	adapter, err := NewSourceProviderAdapter(nil)
	require.ErrorIs(t, err, ErrSourceRepositoryRequired)
	assert.Nil(t, adapter)
}

func TestNewSourceProviderAdapter_ValidRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	adapter, err := NewSourceProviderAdapter(mockRepo)

	require.NoError(t, err)
	require.NotNil(t, adapter)
	assert.Equal(t, mockRepo, adapter.repo)
}

func TestNewFeeRuleProviderAdapter_NilRepo(t *testing.T) {
	t.Parallel()

	adapter, err := NewFeeRuleProviderAdapter(nil)
	require.ErrorIs(t, err, ErrFeeRuleRepositoryRequired)
	assert.Nil(t, adapter)
}

func TestFeeRuleProviderAdapter_FindByContextID_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	rules := []*fee.FeeRule{{ID: uuid.New(), ContextID: contextID, Name: "fee-rule"}}
	adapter, err := NewFeeRuleProviderAdapter(&feeRuleRepositoryStub{rules: rules})
	require.NoError(t, err)

	result, err := adapter.FindByContextID(context.Background(), contextID)
	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, rules[0].ID, result[0].ID)
}

func TestFeeRuleProviderAdapter_FindByContextID_Empty(t *testing.T) {
	t.Parallel()

	adapter, err := NewFeeRuleProviderAdapter(&feeRuleRepositoryStub{rules: nil})
	require.NoError(t, err)

	result, err := adapter.FindByContextID(context.Background(), uuid.New())
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestFeeRuleProviderAdapter_FindByContextID_Error(t *testing.T) {
	t.Parallel()

	adapter, err := NewFeeRuleProviderAdapter(&feeRuleRepositoryStub{err: errTestRepo})
	require.NoError(t, err)

	result, err := adapter.FindByContextID(context.Background(), uuid.New())
	require.ErrorIs(t, err, errTestRepo)
	assert.Nil(t, result)
}

func TestFeeRuleProviderAdapter_FindByContextID_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *FeeRuleProviderAdapter

	result, err := adapter.FindByContextID(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrFeeRuleRepositoryRequired)
	assert.Nil(t, result)
}

func TestSourceProviderAdapter_FindByContextID_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *SourceProviderAdapter

	ctx := context.Background()
	contextID := uuid.New()

	result, err := adapter.FindByContextID(ctx, contextID)

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrSourceRepositoryRequired)
}

func TestSourceProviderAdapter_FindByContextID_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	adapter, err := NewSourceProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	now := time.Now().UTC()

	sources := []*configEntities.ReconciliationSource{
		{
			ID:        uuid.New(),
			ContextID: contextID,
			Name:      "Source 1",
			Type:      value_objects.SourceTypeLedger,
			Side:      fee.MatchingSideLeft,
			Config:    map[string]any{},
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			ID:        uuid.New(),
			ContextID: contextID,
			Name:      "Source 2",
			Type:      value_objects.SourceTypeBank,
			Side:      fee.MatchingSideRight,
			Config:    map[string]any{},
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return(sources, libHTTP.CursorPagination{}, nil)

	result, err := adapter.FindByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, sources[0].ID, result[0].ID)
	assert.Equal(t, matchingPorts.SourceType(sources[0].Type.String()), result[0].Type)
	assert.Equal(t, sources[0].Side, result[0].Side)
	assert.Equal(t, sources[1].ID, result[1].ID)
	assert.Equal(t, matchingPorts.SourceType(sources[1].Type.String()), result[1].Type)
	assert.Equal(t, sources[1].Side, result[1].Side)
}

func TestSourceProviderAdapter_FindByContextID_PaginatesAllSources(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	adapter, err := NewSourceProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	now := time.Now().UTC()

	pageOne := []*configEntities.ReconciliationSource{{
		ID:        uuid.New(),
		ContextID: contextID,
		Name:      "Source 1",
		Type:      value_objects.SourceTypeLedger,
		Side:      fee.MatchingSideLeft,
		Config:    map[string]any{},
		CreatedAt: now,
		UpdatedAt: now,
	}}
	pageTwo := []*configEntities.ReconciliationSource{{
		ID:        uuid.New(),
		ContextID: contextID,
		Name:      "Source 2",
		Type:      value_objects.SourceTypeBank,
		Side:      fee.MatchingSideRight,
		Config:    map[string]any{},
		CreatedAt: now,
		UpdatedAt: now,
	}}

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return(pageOne, libHTTP.CursorPagination{Next: "cursor-2"}, nil)
	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "cursor-2", maxInternalLimit).
		Return(pageTwo, libHTTP.CursorPagination{}, nil)

	result, err := adapter.FindByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, pageOne[0].ID, result[0].ID)
	assert.Equal(t, pageOne[0].Side, result[0].Side)
	assert.Equal(t, pageTwo[0].ID, result[1].ID)
	assert.Equal(t, pageTwo[0].Side, result[1].Side)
}

func TestSourceProviderAdapter_FindByContextID_SkipsNilSources(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	adapter, err := NewSourceProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	now := time.Now().UTC()

	sources := []*configEntities.ReconciliationSource{
		{
			ID:        uuid.New(),
			ContextID: contextID,
			Name:      "Source 1",
			Type:      value_objects.SourceTypeLedger,
			Config:    map[string]any{},
			CreatedAt: now,
			UpdatedAt: now,
		},
		nil,
		{
			ID:        uuid.New(),
			ContextID: contextID,
			Name:      "Source 2",
			Type:      value_objects.SourceTypeBank,
			Config:    map[string]any{},
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return(sources, libHTTP.CursorPagination{}, nil)

	result, err := adapter.FindByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, sources[0].ID, result[0].ID)
	assert.Equal(t, sources[2].ID, result[1].ID)
}

func TestSourceProviderAdapter_FindByContextID_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	adapter, err := NewSourceProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return(nil, libHTTP.CursorPagination{}, errTestRepo)

	result, err := adapter.FindByContextID(ctx, contextID)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "find sources by context")
	require.ErrorIs(t, err, errTestRepo)
}

func TestSourceProviderAdapter_FindByContextID_EmptySources(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	adapter, err := NewSourceProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return([]*configEntities.ReconciliationSource{}, libHTTP.CursorPagination{}, nil)

	result, err := adapter.FindByContextID(ctx, contextID)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestContextProviderAdapter_FindByID_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockContextRepository(ctrl)
	adapter, err := NewContextProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByID(ctx, contextID).
		Return(nil, errTestRepo)

	result, err := adapter.FindByID(ctx, tenantID, contextID)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "find context by id")
	require.ErrorIs(t, err, errTestRepo)
}

func TestContextProviderAdapter_FindByID_ErrNoRows(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockContextRepository(ctrl)
	adapter, err := NewContextProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByID(ctx, contextID).
		Return(nil, sql.ErrNoRows)

	result, err := adapter.FindByID(ctx, tenantID, contextID)

	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestNewContextProviderAdapter_ValidRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockContextRepository(ctrl)
	adapter, err := NewContextProviderAdapter(mockRepo)

	require.NoError(t, err)
	require.NotNil(t, adapter)
	assert.Equal(t, mockRepo, adapter.repo)
}

func TestMatchRuleProviderAdapter_ListByContextID_SkipsNilRules(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockMatchRuleRepository(ctrl)
	adapter, err := NewMatchRuleProviderAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	now := time.Now().UTC()

	rules := configEntities.MatchRules{
		&shared.MatchRule{
			ID:        uuid.New(),
			ContextID: contextID,
			Priority:  1,
			Type:      shared.RuleTypeExact,
			Config:    map[string]any{"field": "amount"},
			CreatedAt: now,
			UpdatedAt: now,
		},
		nil,
		&shared.MatchRule{
			ID:        uuid.New(),
			ContextID: contextID,
			Priority:  2,
			Type:      shared.RuleTypeTolerance,
			Config:    map[string]any{"tolerance": 0.01},
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return(rules, libHTTP.CursorPagination{}, nil)

	result, err := adapter.ListByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, rules[0].ID, result[0].ID)
	assert.Equal(t, rules[2].ID, result[1].ID)
}
