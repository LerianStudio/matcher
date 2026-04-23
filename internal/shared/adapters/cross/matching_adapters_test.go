//go:build unit

package cross

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

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

func newTestProviderWithMatchRuleRepo(repo configRepositories.MatchRuleRepository) (*MatchingConfigurationProvider, error) {
	if repo == nil {
		return nil, ErrMatchRuleRepositoryRequired
	}

	return NewMatchingConfigurationProvider(nil, nil, repo, nil)
}

func newTestProviderWithContextRepo(repo configRepositories.ContextRepository) (*MatchingConfigurationProvider, error) {
	if repo == nil {
		return nil, ErrContextRepositoryRequired
	}

	return NewMatchingConfigurationProvider(repo, nil, nil, nil)
}

func newTestProviderWithSourceRepo(repo configRepositories.SourceRepository) (*MatchingConfigurationProvider, error) {
	if repo == nil {
		return nil, ErrSourceRepositoryRequired
	}

	return NewMatchingConfigurationProvider(nil, repo, nil, nil)
}

func newTestFeeRuleProviderAdapter(repo configRepositories.FeeRuleRepository) (*FeeRuleProviderAdapter, error) {
	if repo == nil {
		return nil, ErrFeeRuleRepositoryRequired
	}

	provider, err := NewMatchingConfigurationProvider(nil, nil, nil, repo)
	if err != nil {
		return nil, err
	}

	return provider.FeeRuleProvider(), nil
}

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

// -----------------------------------------------------------------------------
// MatchRuleProvider satisfaction (provider implements port directly)
// -----------------------------------------------------------------------------

func TestProvider_MatchRuleRepo_Nil(t *testing.T) {
	t.Parallel()

	provider, err := newTestProviderWithMatchRuleRepo(nil)
	require.ErrorIs(t, err, ErrMatchRuleRepositoryRequired)
	assert.Nil(t, provider)
}

func TestProvider_MatchRuleRepo_Valid(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockMatchRuleRepository(ctrl)
	provider, err := newTestProviderWithMatchRuleRepo(mockRepo)

	require.NoError(t, err)
	require.NotNil(t, provider)
	assert.Equal(t, mockRepo, provider.matchRuleRepo)
}

func TestProvider_ListByContextID_NilProvider(t *testing.T) {
	t.Parallel()

	var provider *MatchingConfigurationProvider

	result, err := provider.ListByContextID(context.Background(), uuid.New())

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrMatchRuleRepositoryRequired)
}

func TestProvider_ListByContextID_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockMatchRuleRepository(ctrl)
	provider, err := newTestProviderWithMatchRuleRepo(mockRepo)
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

	result, err := provider.ListByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, rules[0].ID, result[0].ID)
	assert.Equal(t, rules[1].ID, result[1].ID)
}

func TestProvider_ListByContextID_EmptyRules(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockMatchRuleRepository(ctrl)
	provider, err := newTestProviderWithMatchRuleRepo(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return(configEntities.MatchRules{}, libHTTP.CursorPagination{}, nil)

	result, err := provider.ListByContextID(ctx, contextID)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestProvider_ListByContextID_PaginatesAllRules(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockMatchRuleRepository(ctrl)
	provider, err := newTestProviderWithMatchRuleRepo(mockRepo)
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

	result, err := provider.ListByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, pageOne[0].ID, result[0].ID)
	assert.Equal(t, pageTwo[0].ID, result[1].ID)
}

func TestProvider_ListByContextID_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockMatchRuleRepository(ctrl)
	provider, err := newTestProviderWithMatchRuleRepo(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return(nil, libHTTP.CursorPagination{}, errTestRepo)

	result, err := provider.ListByContextID(ctx, contextID)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "find match rules by context")
	require.ErrorIs(t, err, errTestRepo)
}

func TestProvider_ListByContextID_SkipsNilRules(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockMatchRuleRepository(ctrl)
	provider, err := newTestProviderWithMatchRuleRepo(mockRepo)
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

	result, err := provider.ListByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, rules[0].ID, result[0].ID)
	assert.Equal(t, rules[2].ID, result[1].ID)
}

// -----------------------------------------------------------------------------
// ContextProvider satisfaction (provider implements port directly)
// -----------------------------------------------------------------------------

func TestProvider_ContextRepo_Nil(t *testing.T) {
	t.Parallel()

	provider, err := newTestProviderWithContextRepo(nil)
	require.ErrorIs(t, err, ErrContextRepositoryRequired)
	assert.Nil(t, provider)
}

func TestProvider_ContextRepo_Valid(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockContextRepository(ctrl)
	provider, err := newTestProviderWithContextRepo(mockRepo)

	require.NoError(t, err)
	require.NotNil(t, provider)
	assert.Equal(t, mockRepo, provider.contextRepo)
}

func TestProvider_FindByID_NilProvider(t *testing.T) {
	t.Parallel()

	var provider *MatchingConfigurationProvider

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()

	result, err := provider.FindByID(ctx, tenantID, contextID)

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrContextRepositoryRequired)
}

func TestProvider_FindByID_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockContextRepository(ctrl)
	provider, err := newTestProviderWithContextRepo(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()
	now := time.Now().UTC()
	feeNormalization := "NET"

	ctxEntity := &configEntities.ReconciliationContext{
		ID:               contextID,
		TenantID:         tenantID,
		Name:             "Test Context",
		Type:             shared.ContextTypeOneToOne,
		Interval:         "daily",
		Status:           value_objects.ContextStatusActive,
		FeeToleranceAbs:  decimal.RequireFromString("0.10"),
		FeeTolerancePct:  decimal.RequireFromString("0.05"),
		FeeNormalization: &feeNormalization,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	mockRepo.EXPECT().
		FindByID(ctx, contextID).
		Return(ctxEntity, nil)

	result, err := provider.FindByID(ctx, tenantID, contextID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, contextID, result.ID)
	assert.Equal(t, shared.ContextType(ctxEntity.Type.String()), result.Type)
	assert.True(t, result.Active)
	assert.True(t, ctxEntity.FeeToleranceAbs.Equal(result.FeeToleranceAbs))
	assert.True(t, ctxEntity.FeeTolerancePct.Equal(result.FeeTolerancePct))
	require.NotNil(t, result.FeeNormalization)
	assert.Equal(t, *ctxEntity.FeeNormalization, *result.FeeNormalization)
}

func TestProvider_FindByID_NilResult(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockContextRepository(ctrl)
	provider, err := newTestProviderWithContextRepo(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByID(ctx, contextID).
		Return(nil, nil)

	result, err := provider.FindByID(ctx, tenantID, contextID)

	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestProvider_FindByID_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockContextRepository(ctrl)
	provider, err := newTestProviderWithContextRepo(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByID(ctx, contextID).
		Return(nil, errTestRepo)

	result, err := provider.FindByID(ctx, tenantID, contextID)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "find context by id")
	require.ErrorIs(t, err, errTestRepo)
}

func TestProvider_FindByID_ErrNoRows(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockContextRepository(ctrl)
	provider, err := newTestProviderWithContextRepo(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	tenantID := uuid.New()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByID(ctx, contextID).
		Return(nil, sql.ErrNoRows)

	result, err := provider.FindByID(ctx, tenantID, contextID)

	require.NoError(t, err)
	assert.Nil(t, result)
}

// -----------------------------------------------------------------------------
// SourceProvider satisfaction (provider implements port directly)
// -----------------------------------------------------------------------------

func TestProvider_SourceRepo_Nil(t *testing.T) {
	t.Parallel()

	provider, err := newTestProviderWithSourceRepo(nil)
	require.ErrorIs(t, err, ErrSourceRepositoryRequired)
	assert.Nil(t, provider)
}

func TestProvider_SourceRepo_Valid(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	provider, err := newTestProviderWithSourceRepo(mockRepo)

	require.NoError(t, err)
	require.NotNil(t, provider)
	assert.Equal(t, mockRepo, provider.sourceRepo)
}

func TestProvider_FindSourcesByContextID_NilProvider(t *testing.T) {
	t.Parallel()

	var provider *MatchingConfigurationProvider

	ctx := context.Background()
	contextID := uuid.New()

	result, err := provider.FindByContextID(ctx, contextID)

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrSourceRepositoryRequired)
}

func TestProvider_FindSourcesByContextID_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	provider, err := newTestProviderWithSourceRepo(mockRepo)
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

	result, err := provider.FindByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, sources[0].ID, result[0].ID)
	assert.Equal(t, matchingPorts.SourceType(sources[0].Type.String()), result[0].Type)
	assert.Equal(t, sources[0].Side, result[0].Side)
	assert.Equal(t, sources[1].ID, result[1].ID)
	assert.Equal(t, matchingPorts.SourceType(sources[1].Type.String()), result[1].Type)
	assert.Equal(t, sources[1].Side, result[1].Side)
}

func TestProvider_FindSourcesByContextID_PaginatesAllSources(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	provider, err := newTestProviderWithSourceRepo(mockRepo)
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

	result, err := provider.FindByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, pageOne[0].ID, result[0].ID)
	assert.Equal(t, pageOne[0].Side, result[0].Side)
	assert.Equal(t, pageTwo[0].ID, result[1].ID)
	assert.Equal(t, pageTwo[0].Side, result[1].Side)
}

func TestProvider_FindSourcesByContextID_SkipsNilSources(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	provider, err := newTestProviderWithSourceRepo(mockRepo)
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

	result, err := provider.FindByContextID(ctx, contextID)

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, sources[0].ID, result[0].ID)
	assert.Equal(t, sources[2].ID, result[1].ID)
}

func TestProvider_FindSourcesByContextID_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	provider, err := newTestProviderWithSourceRepo(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return(nil, libHTTP.CursorPagination{}, errTestRepo)

	result, err := provider.FindByContextID(ctx, contextID)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "find sources by context")
	require.ErrorIs(t, err, errTestRepo)
}

func TestProvider_FindSourcesByContextID_EmptySources(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	provider, err := newTestProviderWithSourceRepo(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()

	mockRepo.EXPECT().
		FindByContextID(ctx, contextID, "", maxInternalLimit).
		Return([]*configEntities.ReconciliationSource{}, libHTTP.CursorPagination{}, nil)

	result, err := provider.FindByContextID(ctx, contextID)

	require.NoError(t, err)
	assert.Empty(t, result)
}

// -----------------------------------------------------------------------------
// FeeRuleProviderAdapter tests — this adapter is kept because its
// FindByContextID collides method-name-wise with SourceProvider.FindByContextID
// on a single receiver.
// -----------------------------------------------------------------------------

func TestFeeRuleProviderAdapter_NilRepo(t *testing.T) {
	t.Parallel()

	adapter, err := newTestFeeRuleProviderAdapter(nil)
	require.ErrorIs(t, err, ErrFeeRuleRepositoryRequired)
	assert.Nil(t, adapter)
}

func TestFeeRuleProviderAdapter_ValidRepo(t *testing.T) {
	t.Parallel()

	repo := &feeRuleRepositoryStub{}
	adapter, err := newTestFeeRuleProviderAdapter(repo)

	require.NoError(t, err)
	require.NotNil(t, adapter)
	assert.Equal(t, repo, adapter.provider.feeRuleRepo)
}

func TestFeeRuleProviderAdapter_FindByContextID_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	rules := []*fee.FeeRule{{ID: uuid.New(), ContextID: contextID, Name: "fee-rule"}}
	adapter, err := newTestFeeRuleProviderAdapter(&feeRuleRepositoryStub{rules: rules})
	require.NoError(t, err)

	result, err := adapter.FindByContextID(context.Background(), contextID)
	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, rules[0].ID, result[0].ID)
}

func TestFeeRuleProviderAdapter_FindByContextID_Empty(t *testing.T) {
	t.Parallel()

	adapter, err := newTestFeeRuleProviderAdapter(&feeRuleRepositoryStub{rules: nil})
	require.NoError(t, err)

	result, err := adapter.FindByContextID(context.Background(), uuid.New())
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestFeeRuleProviderAdapter_FindByContextID_Error(t *testing.T) {
	t.Parallel()

	adapter, err := newTestFeeRuleProviderAdapter(&feeRuleRepositoryStub{err: errTestRepo})
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

// -----------------------------------------------------------------------------
// FeeRuleProvider accessor tests
// -----------------------------------------------------------------------------

func TestProvider_FeeRuleProvider_NilProvider(t *testing.T) {
	t.Parallel()

	var provider *MatchingConfigurationProvider

	adapter := provider.FeeRuleProvider()
	assert.Nil(t, adapter)
}

func TestProvider_FeeRuleProvider_ReturnsAdapter(t *testing.T) {
	t.Parallel()

	provider, err := NewMatchingConfigurationProvider(nil, nil, nil, &feeRuleRepositoryStub{})
	require.NoError(t, err)

	adapter := provider.FeeRuleProvider()
	require.NotNil(t, adapter)
	assert.Equal(t, provider, adapter.provider)
}

// -----------------------------------------------------------------------------
// Port-interface satisfaction (compile-time checks are in production file; these
// tests exercise the constructor path and confirm implicit satisfaction at
// runtime through the port type assertions).
// -----------------------------------------------------------------------------

func TestProvider_SatisfiesPorts(t *testing.T) {
	t.Parallel()

	provider, err := NewMatchingConfigurationProvider(
		mocks.NewMockContextRepository(gomock.NewController(t)),
		mocks.NewMockSourceRepository(gomock.NewController(t)),
		mocks.NewMockMatchRuleRepository(gomock.NewController(t)),
		&feeRuleRepositoryStub{},
	)
	require.NoError(t, err)

	var (
		_ matchingPorts.ContextProvider   = provider
		_ matchingPorts.SourceProvider    = provider
		_ matchingPorts.MatchRuleProvider = provider
		_ matchingPorts.FeeRuleProvider   = provider.FeeRuleProvider()
	)
}
