//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ---------------------------------------------------------------------------
// Transactional-read stub for sources (FindByContextIDWithTx)
// ---------------------------------------------------------------------------

type sourceRepoTxFinderStub struct {
	*sourceRepoTxStub
	findByContextIDWithTxFn func(context.Context, *sql.Tx, uuid.UUID, string, int) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error)
}

var _ sourceTxFinder = (*sourceRepoTxFinderStub)(nil)

func (stub *sourceRepoTxFinderStub) FindByContextIDWithTx(
	ctx context.Context,
	tx *sql.Tx,
	contextID uuid.UUID,
	cursor string,
	limit int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	if stub.findByContextIDWithTxFn != nil {
		return stub.findByContextIDWithTxFn(ctx, tx, contextID, cursor, limit)
	}

	return nil, libHTTP.CursorPagination{}, errFindByContextNotImplemented
}

// ---------------------------------------------------------------------------
// Transactional-read stub for field map existence check
// ---------------------------------------------------------------------------

type fieldMapRepoTxExistsStub struct {
	*fieldMapRepoTxStub
	existsBySourceIDsWithTxFn func(context.Context, *sql.Tx, []uuid.UUID) (map[uuid.UUID]bool, error)
}

var _ fieldMapTxExistsChecker = (*fieldMapRepoTxExistsStub)(nil)

func (stub *fieldMapRepoTxExistsStub) ExistsBySourceIDsWithTx(
	ctx context.Context,
	tx *sql.Tx,
	sourceIDs []uuid.UUID,
) (map[uuid.UUID]bool, error) {
	if stub.existsBySourceIDsWithTxFn != nil {
		return stub.existsBySourceIDsWithTxFn(ctx, tx, sourceIDs)
	}

	return make(map[uuid.UUID]bool), nil
}

// ---------------------------------------------------------------------------
// Transactional-read stub for field map FindBySourceIDWithTx
// ---------------------------------------------------------------------------

type fieldMapRepoTxFinderStub struct {
	*fieldMapRepoTxStub
	findBySourceIDWithTxFn func(context.Context, *sql.Tx, uuid.UUID) (*shared.FieldMap, error)
}

var _ fieldMapTxFinder = (*fieldMapRepoTxFinderStub)(nil)

func (stub *fieldMapRepoTxFinderStub) FindBySourceIDWithTx(
	ctx context.Context,
	tx *sql.Tx,
	sourceID uuid.UUID,
) (*shared.FieldMap, error) {
	if stub.findBySourceIDWithTxFn != nil {
		return stub.findBySourceIDWithTxFn(ctx, tx, sourceID)
	}

	return nil, errFindBySourceNotImplemented
}

// ===========================================================================
// cloneSourcesAndFieldMaps (non-transactional, tx=nil)
// ===========================================================================

func TestCloneSourcesAndFieldMaps_NoSources(t *testing.T) {
	t.Parallel()

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return nil, libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{
		sourceRepo:   srcRepo,
		fieldMapRepo: &fieldMapRepoStub{},
	}

	sources, fieldMaps, err := uc.cloneSourcesAndFieldMaps(context.Background(), nil, uuid.New(), uuid.New())

	require.NoError(t, err)
	assert.Equal(t, 0, sources)
	assert.Equal(t, 0, fieldMaps)
}

func TestCloneSourcesAndFieldMaps_SourcesWithoutFieldMaps(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	newCtxID := uuid.New()
	srcID1 := uuid.New()
	srcID2 := uuid.New()

	var createdSources []*entities.ReconciliationSource

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, ctxID uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return []*entities.ReconciliationSource{
				{ID: srcID1, ContextID: sourceCtxID, Name: "Source A"},
				{ID: srcID2, ContextID: sourceCtxID, Name: "Source B"},
			}, libHTTP.CursorPagination{}, nil
		},
		createFn: func(_ context.Context, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			createdSources = append(createdSources, entity)
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoStub{} // ExistsBySourceIDs returns empty map by default

	uc := &UseCase{
		sourceRepo:   srcRepo,
		fieldMapRepo: fmRepo,
	}

	sources, fieldMaps, err := uc.cloneSourcesAndFieldMaps(context.Background(), nil, sourceCtxID, newCtxID)

	require.NoError(t, err)
	assert.Equal(t, 2, sources)
	assert.Equal(t, 0, fieldMaps)
	require.Len(t, createdSources, 2)

	for _, src := range createdSources {
		assert.Equal(t, newCtxID, src.ContextID)
		assert.NotEqual(t, uuid.Nil, src.ID)
	}
}

func TestCloneSourcesAndFieldMaps_SourcesWithFieldMaps(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	newCtxID := uuid.New()
	srcID := uuid.New()

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return []*entities.ReconciliationSource{
				{ID: srcID, ContextID: sourceCtxID, Name: "Source With FM", Config: map[string]any{"key": "val"}},
			}, libHTTP.CursorPagination{}, nil
		},
		createFn: func(_ context.Context, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}

	fmCreateCalled := false
	fmRepo := &fieldMapRepoStub{
		findBySourceIDFn: func(_ context.Context, sourceID uuid.UUID) (*shared.FieldMap, error) {
			if sourceID == srcID {
				return &shared.FieldMap{
					ID:        uuid.New(),
					ContextID: sourceCtxID,
					SourceID:  srcID,
					Mapping:   map[string]any{"amount": "$.amount"},
					Version:   5,
				}, nil
			}

			return nil, sql.ErrNoRows
		},
		createFn: func(_ context.Context, entity *shared.FieldMap) (*shared.FieldMap, error) {
			fmCreateCalled = true
			assert.Equal(t, newCtxID, entity.ContextID)
			assert.NotEqual(t, srcID, entity.SourceID) // New source ID
			assert.Equal(t, 1, entity.Version)         // Reset to 1
			assert.Equal(t, map[string]any{"amount": "$.amount"}, entity.Mapping)
			return entity, nil
		},
	}

	// Override ExistsBySourceIDs to return true for srcID.
	origExistsBySourceIDs := fmRepo.findBySourceIDFn
	_ = origExistsBySourceIDs

	// We need a custom field map repo that supports ExistsBySourceIDs properly.
	type existsFieldMapRepoStub struct {
		*fieldMapRepoStub
	}

	customFMRepo := &fieldMapRepoStub{
		findBySourceIDFn: fmRepo.findBySourceIDFn,
		createFn:         fmRepo.createFn,
	}

	// Monkey-patch: we can't override ExistsBySourceIDs on fieldMapRepoStub
	// because it always returns empty map. Use a wrapper pattern instead.
	type fieldMapExistsOverrideStub struct {
		*fieldMapRepoStub
	}

	fmExistsStub := &fieldMapExistsOverrideStub{
		fieldMapRepoStub: customFMRepo,
	}
	_ = fmExistsStub

	// Actually, let's just build a custom repo. The fieldMapRepoStub.ExistsBySourceIDs
	// always returns an empty map. For this test to work we need an override approach.
	// Looking at the pattern more carefully: `fieldMapRepoStub` has a hardcoded
	// ExistsBySourceIDs returning empty. We need to embed it and override.

	// Simplest approach: use the feeRuleMockRepo pattern — a custom struct.
	type fieldMapRepoWithExistsFn struct {
		*fieldMapRepoStub
		existsFn func(context.Context, []uuid.UUID) (map[uuid.UUID]bool, error)
	}

	overrideRepo := &fieldMapRepoWithExistsFn{
		fieldMapRepoStub: customFMRepo,
		existsFn: func(_ context.Context, ids []uuid.UUID) (map[uuid.UUID]bool, error) {
			result := make(map[uuid.UUID]bool)
			for _, id := range ids {
				if id == srcID {
					result[id] = true
				}
			}

			return result, nil
		},
	}
	_ = overrideRepo

	// Actually the fieldMapRepoStub.ExistsBySourceIDs is NOT configurable (no fn hook).
	// However, looking at the code: `existsBySourceIDsWithOptionalTx` with nil tx calls
	// `uc.fieldMapRepo.ExistsBySourceIDs`. The stub returns `make(map[uuid.UUID]bool, len(sourceIDs)), nil`.
	// An empty map means no field maps exist, so cloneFieldMap won't be called.
	//
	// To test the field-map cloning path, we need the exists check to return true.
	// The cleanest way is a minimal wrapper that satisfies the FieldMapRepository
	// interface but overrides ExistsBySourceIDs. Since we're in the same package:

	fmRepoOverride := &fieldMapRepoExistsStub{
		fieldMapRepoStub: &fieldMapRepoStub{
			findBySourceIDFn: func(_ context.Context, sourceID uuid.UUID) (*shared.FieldMap, error) {
				if sourceID == srcID {
					return &shared.FieldMap{
						ID:        uuid.New(),
						ContextID: sourceCtxID,
						SourceID:  srcID,
						Mapping:   map[string]any{"amount": "$.amount"},
						Version:   5,
					}, nil
				}

				return nil, sql.ErrNoRows
			},
			createFn: func(_ context.Context, entity *shared.FieldMap) (*shared.FieldMap, error) {
				fmCreateCalled = true
				assert.Equal(t, newCtxID, entity.ContextID)
				assert.Equal(t, 1, entity.Version)
				return entity, nil
			},
		},
		existsFn: func(_ context.Context, ids []uuid.UUID) (map[uuid.UUID]bool, error) {
			result := make(map[uuid.UUID]bool)
			for _, id := range ids {
				if id == srcID {
					result[id] = true
				}
			}

			return result, nil
		},
	}

	uc := &UseCase{
		sourceRepo:   srcRepo,
		fieldMapRepo: fmRepoOverride,
	}

	sources, fieldMaps, err := uc.cloneSourcesAndFieldMaps(context.Background(), nil, sourceCtxID, newCtxID)

	require.NoError(t, err)
	assert.Equal(t, 1, sources)
	assert.Equal(t, 1, fieldMaps)
	assert.True(t, fmCreateCalled)
}

// fieldMapRepoExistsStub wraps fieldMapRepoStub to override ExistsBySourceIDs.
type fieldMapRepoExistsStub struct {
	*fieldMapRepoStub
	existsFn func(context.Context, []uuid.UUID) (map[uuid.UUID]bool, error)
}

func (stub *fieldMapRepoExistsStub) ExistsBySourceIDs(ctx context.Context, sourceIDs []uuid.UUID) (map[uuid.UUID]bool, error) {
	if stub.existsFn != nil {
		return stub.existsFn(ctx, sourceIDs)
	}

	return stub.fieldMapRepoStub.ExistsBySourceIDs(ctx, sourceIDs)
}

func TestCloneSourcesAndFieldMaps_FetchSourcesError(t *testing.T) {
	t.Parallel()

	errFetch := errors.New("source fetch failed")

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return nil, libHTTP.CursorPagination{}, errFetch
		},
	}

	uc := &UseCase{
		sourceRepo:   srcRepo,
		fieldMapRepo: &fieldMapRepoStub{},
	}

	_, _, err := uc.cloneSourcesAndFieldMaps(context.Background(), nil, uuid.New(), uuid.New())

	require.Error(t, err)
	assert.ErrorIs(t, err, errFetch)
}

func TestCloneSourcesAndFieldMaps_ExistsBySourceIDsError_Propagates(t *testing.T) {
	t.Parallel()

	errExists := errors.New("exists check failed")
	sourceCtxID := uuid.New()

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return []*entities.ReconciliationSource{
				{ID: uuid.New(), ContextID: sourceCtxID, Name: "Source"},
			}, libHTTP.CursorPagination{}, nil
		},
	}

	fmRepo := &fieldMapRepoExistsStub{
		fieldMapRepoStub: &fieldMapRepoStub{},
		existsFn: func(_ context.Context, _ []uuid.UUID) (map[uuid.UUID]bool, error) {
			return nil, errExists
		},
	}

	uc := &UseCase{
		sourceRepo:   srcRepo,
		fieldMapRepo: fmRepo,
	}

	_, _, err := uc.cloneSourcesAndFieldMaps(context.Background(), nil, sourceCtxID, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "checking field maps existence")
	assert.ErrorIs(t, err, errExists)
}

func TestCloneSourcesAndFieldMaps_SourceCreateError_PropagatesWrapped(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	errCreate := errors.New("source insert failed")

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return []*entities.ReconciliationSource{
				{ID: uuid.New(), ContextID: sourceCtxID, Name: "Bad Source"},
			}, libHTTP.CursorPagination{}, nil
		},
		createFn: func(_ context.Context, _ *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return nil, errCreate
		},
	}

	uc := &UseCase{
		sourceRepo:   srcRepo,
		fieldMapRepo: &fieldMapRepoStub{},
	}

	sources, fieldMaps, err := uc.cloneSourcesAndFieldMaps(context.Background(), nil, sourceCtxID, uuid.New())

	require.Error(t, err)
	assert.Equal(t, 0, sources)
	assert.Equal(t, 0, fieldMaps)
	assert.ErrorIs(t, err, errCreate)
	assert.Contains(t, err.Error(), "creating cloned source")
}

func TestCloneSourcesAndFieldMaps_FieldMapCloneError(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	srcID := uuid.New()
	errFMCreate := errors.New("field map insert failed")

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return []*entities.ReconciliationSource{
				{ID: srcID, ContextID: sourceCtxID, Name: "Source"},
			}, libHTTP.CursorPagination{}, nil
		},
		createFn: func(_ context.Context, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}

	fmRepo := &fieldMapRepoExistsStub{
		fieldMapRepoStub: &fieldMapRepoStub{
			findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
				return &shared.FieldMap{
					ID:      uuid.New(),
					Mapping: map[string]any{"a": "b"},
				}, nil
			},
			createFn: func(_ context.Context, _ *shared.FieldMap) (*shared.FieldMap, error) {
				return nil, errFMCreate
			},
		},
		existsFn: func(_ context.Context, ids []uuid.UUID) (map[uuid.UUID]bool, error) {
			result := make(map[uuid.UUID]bool)
			for _, id := range ids {
				result[id] = true
			}

			return result, nil
		},
	}

	uc := &UseCase{
		sourceRepo:   srcRepo,
		fieldMapRepo: fmRepo,
	}

	sources, fieldMaps, err := uc.cloneSourcesAndFieldMaps(context.Background(), nil, sourceCtxID, uuid.New())

	require.Error(t, err)
	assert.Equal(t, 1, sources) // Source was created before field map failed
	assert.Equal(t, 0, fieldMaps)
	assert.Contains(t, err.Error(), "creating cloned field map")
}

func TestCloneSourcesAndFieldMaps_ConfigDeepCopy(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	originalConfig := map[string]any{
		"nested": map[string]any{"key": "value"},
	}

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return []*entities.ReconciliationSource{
				{ID: uuid.New(), ContextID: sourceCtxID, Name: "DeepCopy Source", Config: originalConfig},
			}, libHTTP.CursorPagination{}, nil
		},
		createFn: func(_ context.Context, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			// Mutate the cloned config
			entity.Config["nested"].(map[string]any)["key"] = "mutated"
			return entity, nil
		},
	}

	uc := &UseCase{
		sourceRepo:   srcRepo,
		fieldMapRepo: &fieldMapRepoStub{},
	}

	_, _, err := uc.cloneSourcesAndFieldMaps(context.Background(), nil, sourceCtxID, uuid.New())

	require.NoError(t, err)
	// Original must be untouched
	assert.Equal(t, "value", originalConfig["nested"].(map[string]any)["key"])
}

// ===========================================================================
// createSourceWithOptionalTx
// ===========================================================================

func TestCreateSourceWithOptionalTx_NilTxUsesCreate(t *testing.T) {
	t.Parallel()

	createCalled := false
	srcRepo := &sourceRepoStub{
		createFn: func(_ context.Context, _ *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			createCalled = true
			return &entities.ReconciliationSource{}, nil
		},
	}

	uc := &UseCase{sourceRepo: srcRepo}

	err := uc.createSourceWithOptionalTx(context.Background(), nil, &entities.ReconciliationSource{})

	require.NoError(t, err)
	assert.True(t, createCalled)
}

func TestCreateSourceWithOptionalTx_TxUsesCreateWithTx(t *testing.T) {
	t.Parallel()

	txCreateCalled := false
	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, _ *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			txCreateCalled = true
			return &entities.ReconciliationSource{}, nil
		},
	}

	uc := &UseCase{sourceRepo: srcRepo}
	fakeTx := &sql.Tx{}

	err := uc.createSourceWithOptionalTx(context.Background(), fakeTx, &entities.ReconciliationSource{})

	require.NoError(t, err)
	assert.True(t, txCreateCalled)
}

func TestCreateSourceWithOptionalTx_TxNoSupportReturnsError(t *testing.T) {
	t.Parallel()

	// sourceRepoStub does NOT implement sourceTxCreator.
	srcRepo := &sourceRepoStub{}

	uc := &UseCase{sourceRepo: srcRepo}
	fakeTx := &sql.Tx{}

	err := uc.createSourceWithOptionalTx(context.Background(), fakeTx, &entities.ReconciliationSource{})

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCloneProviderRequired)
}

// ===========================================================================
// cloneFieldMap
// ===========================================================================

func TestCloneFieldMap_NilFieldMapReturnsFalse(t *testing.T) {
	t.Parallel()

	fmRepo := &fieldMapRepoStub{
		findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
			return nil, nil
		},
	}

	uc := &UseCase{fieldMapRepo: fmRepo}

	cloned, err := uc.cloneFieldMap(context.Background(), nil, uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())

	require.NoError(t, err)
	assert.False(t, cloned)
}

func TestCloneFieldMap_ErrNoRowsReturnsFalse(t *testing.T) {
	t.Parallel()

	fmRepo := &fieldMapRepoStub{
		findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
			return nil, sql.ErrNoRows
		},
	}

	uc := &UseCase{fieldMapRepo: fmRepo}

	cloned, err := uc.cloneFieldMap(context.Background(), nil, uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())

	require.NoError(t, err)
	assert.False(t, cloned)
}

func TestCloneFieldMap_FindError_Propagated(t *testing.T) {
	t.Parallel()

	errFind := errors.New("field map lookup failed")

	fmRepo := &fieldMapRepoStub{
		findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
			return nil, errFind
		},
	}

	uc := &UseCase{fieldMapRepo: fmRepo}

	cloned, err := uc.cloneFieldMap(context.Background(), nil, uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())

	require.Error(t, err)
	assert.False(t, cloned)
	assert.ErrorIs(t, err, errFind)
}

func TestCloneFieldMap_NonTxCreateSuccess(t *testing.T) {
	t.Parallel()

	oldSourceID := uuid.New()
	newCtxID := uuid.New()
	newSourceID := uuid.New()
	now := time.Now().UTC()

	fmRepo := &fieldMapRepoStub{
		findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
			return &shared.FieldMap{
				ID:        uuid.New(),
				ContextID: uuid.New(),
				SourceID:  oldSourceID,
				Mapping:   map[string]any{"amount": "col_a"},
				Version:   7,
			}, nil
		},
		createFn: func(_ context.Context, entity *shared.FieldMap) (*shared.FieldMap, error) {
			assert.Equal(t, newCtxID, entity.ContextID)
			assert.Equal(t, newSourceID, entity.SourceID)
			assert.Equal(t, 1, entity.Version)
			assert.Equal(t, now, entity.CreatedAt)
			return entity, nil
		},
	}

	uc := &UseCase{fieldMapRepo: fmRepo}

	cloned, err := uc.cloneFieldMap(context.Background(), nil, oldSourceID, newCtxID, newSourceID, now)

	require.NoError(t, err)
	assert.True(t, cloned)
}

func TestCloneFieldMap_NonTxCreateError(t *testing.T) {
	t.Parallel()

	errCreate := errors.New("field map create failed")

	fmRepo := &fieldMapRepoStub{
		findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
			return &shared.FieldMap{ID: uuid.New(), Mapping: map[string]any{}}, nil
		},
		createFn: func(_ context.Context, _ *shared.FieldMap) (*shared.FieldMap, error) {
			return nil, errCreate
		},
	}

	uc := &UseCase{fieldMapRepo: fmRepo}

	cloned, err := uc.cloneFieldMap(context.Background(), nil, uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())

	require.Error(t, err)
	assert.False(t, cloned)
	assert.ErrorIs(t, err, errCreate)
}

func TestCloneFieldMap_TxCreateSuccess(t *testing.T) {
	t.Parallel()

	txCreateCalled := false
	fmRepo := &fieldMapRepoTxStub{
		fieldMapRepoStub: &fieldMapRepoStub{
			findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
				return &shared.FieldMap{ID: uuid.New(), Mapping: map[string]any{"x": "y"}}, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *shared.FieldMap) (*shared.FieldMap, error) {
			txCreateCalled = true
			return entity, nil
		},
	}

	uc := &UseCase{fieldMapRepo: fmRepo}
	fakeTx := &sql.Tx{}

	cloned, err := uc.cloneFieldMap(context.Background(), fakeTx, uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())

	require.NoError(t, err)
	assert.True(t, cloned)
	assert.True(t, txCreateCalled)
}

func TestCloneFieldMap_TxRepoDoesNotSupportTxCreate(t *testing.T) {
	t.Parallel()

	// fieldMapRepoStub does NOT implement fieldMapTxCreator.
	fmRepo := &fieldMapRepoStub{
		findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
			return &shared.FieldMap{ID: uuid.New(), Mapping: map[string]any{}}, nil
		},
	}

	uc := &UseCase{fieldMapRepo: fmRepo}
	fakeTx := &sql.Tx{}

	cloned, err := uc.cloneFieldMap(context.Background(), fakeTx, uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())

	require.Error(t, err)
	assert.False(t, cloned)
	assert.ErrorIs(t, err, ErrCloneProviderRequired)
}

func TestCloneFieldMap_TxCreateError(t *testing.T) {
	t.Parallel()

	errTxCreate := errors.New("tx field map create failed")

	fmRepo := &fieldMapRepoTxStub{
		fieldMapRepoStub: &fieldMapRepoStub{
			findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
				return &shared.FieldMap{ID: uuid.New(), Mapping: map[string]any{}}, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, _ *shared.FieldMap) (*shared.FieldMap, error) {
			return nil, errTxCreate
		},
	}

	uc := &UseCase{fieldMapRepo: fmRepo}
	fakeTx := &sql.Tx{}

	cloned, err := uc.cloneFieldMap(context.Background(), fakeTx, uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())

	require.Error(t, err)
	assert.False(t, cloned)
	assert.ErrorIs(t, err, errTxCreate)
}

// ===========================================================================
// fetchAllSources (pagination)
// ===========================================================================

func TestFetchAllSources_SinglePage(t *testing.T) {
	t.Parallel()

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return []*entities.ReconciliationSource{
				{ID: uuid.New(), Name: "A"},
			}, libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{sourceRepo: srcRepo}

	sources, err := uc.fetchAllSources(context.Background(), uuid.New())

	require.NoError(t, err)
	assert.Len(t, sources, 1)
}

func TestFetchAllSources_MultiplePages(t *testing.T) {
	t.Parallel()

	callCount := 0

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			callCount++
			if callCount == 1 {
				return []*entities.ReconciliationSource{{ID: uuid.New(), Name: "A"}},
					libHTTP.CursorPagination{Next: "page2"}, nil
			}

			return []*entities.ReconciliationSource{{ID: uuid.New(), Name: "B"}},
				libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{sourceRepo: srcRepo}

	sources, err := uc.fetchAllSources(context.Background(), uuid.New())

	require.NoError(t, err)
	assert.Len(t, sources, 2)
	assert.Equal(t, 2, callCount)
}

func TestFetchAllSources_FetchError(t *testing.T) {
	t.Parallel()

	errFetch := errors.New("sources page failed")

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return nil, libHTTP.CursorPagination{}, errFetch
		},
	}

	uc := &UseCase{sourceRepo: srcRepo}

	sources, err := uc.fetchAllSources(context.Background(), uuid.New())

	require.Error(t, err)
	assert.Nil(t, sources)
	assert.ErrorIs(t, err, errFetch)
}

// ===========================================================================
// fetchAllSourcesWithOptionalTx
// ===========================================================================

func TestFetchAllSourcesWithOptionalTx_NilTxFallsBack(t *testing.T) {
	t.Parallel()

	called := false

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			called = true
			return nil, libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{sourceRepo: srcRepo}

	_, err := uc.fetchAllSourcesWithOptionalTx(context.Background(), nil, uuid.New())

	require.NoError(t, err)
	assert.True(t, called)
}

func TestFetchAllSourcesWithOptionalTx_TxFinderUsed(t *testing.T) {
	t.Parallel()

	txCalled := false

	repo := &sourceRepoTxFinderStub{
		sourceRepoTxStub: &sourceRepoTxStub{
			sourceRepoStub: &sourceRepoStub{},
		},
		findByContextIDWithTxFn: func(_ context.Context, _ *sql.Tx, _ uuid.UUID, _ string, _ int) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			txCalled = true
			return nil, libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{sourceRepo: repo}
	fakeTx := &sql.Tx{}

	_, err := uc.fetchAllSourcesWithOptionalTx(context.Background(), fakeTx, uuid.New())

	require.NoError(t, err)
	assert.True(t, txCalled)
}

func TestFetchAllSourcesWithOptionalTx_TxWithoutFinderFallsBack(t *testing.T) {
	t.Parallel()

	// sourceRepoStub does NOT implement sourceTxFinder → fallback.
	fallbackCalled := false

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			fallbackCalled = true
			return nil, libHTTP.CursorPagination{}, nil
		},
	}

	uc := &UseCase{sourceRepo: srcRepo}
	fakeTx := &sql.Tx{}

	_, err := uc.fetchAllSourcesWithOptionalTx(context.Background(), fakeTx, uuid.New())

	require.NoError(t, err)
	assert.True(t, fallbackCalled)
}

func TestFetchAllSourcesWithOptionalTx_TxFinderError(t *testing.T) {
	t.Parallel()

	errTxFetch := errors.New("tx fetch failed")

	repo := &sourceRepoTxFinderStub{
		sourceRepoTxStub: &sourceRepoTxStub{
			sourceRepoStub: &sourceRepoStub{},
		},
		findByContextIDWithTxFn: func(_ context.Context, _ *sql.Tx, _ uuid.UUID, _ string, _ int) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return nil, libHTTP.CursorPagination{}, errTxFetch
		},
	}

	uc := &UseCase{sourceRepo: repo}
	fakeTx := &sql.Tx{}

	_, err := uc.fetchAllSourcesWithOptionalTx(context.Background(), fakeTx, uuid.New())

	require.Error(t, err)
	assert.ErrorIs(t, err, errTxFetch)
}

// ===========================================================================
// existsBySourceIDsWithOptionalTx
// ===========================================================================

func TestExistsBySourceIDsWithOptionalTx_NilTxFallsBack(t *testing.T) {
	t.Parallel()

	fmRepo := &fieldMapRepoStub{} // Returns empty map
	uc := &UseCase{fieldMapRepo: fmRepo}

	result, err := uc.existsBySourceIDsWithOptionalTx(context.Background(), nil, []uuid.UUID{uuid.New()})

	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestExistsBySourceIDsWithOptionalTx_TxCheckerUsed(t *testing.T) {
	t.Parallel()

	txCalled := false
	expectedID := uuid.New()

	repo := &fieldMapRepoTxExistsStub{
		fieldMapRepoTxStub: &fieldMapRepoTxStub{
			fieldMapRepoStub: &fieldMapRepoStub{},
		},
		existsBySourceIDsWithTxFn: func(_ context.Context, _ *sql.Tx, ids []uuid.UUID) (map[uuid.UUID]bool, error) {
			txCalled = true
			result := make(map[uuid.UUID]bool)
			for _, id := range ids {
				result[id] = id == expectedID
			}

			return result, nil
		},
	}

	uc := &UseCase{fieldMapRepo: repo}
	fakeTx := &sql.Tx{}

	result, err := uc.existsBySourceIDsWithOptionalTx(context.Background(), fakeTx, []uuid.UUID{expectedID})

	require.NoError(t, err)
	assert.True(t, txCalled)
	assert.True(t, result[expectedID])
}

func TestExistsBySourceIDsWithOptionalTx_TxWithoutCheckerFallsBack(t *testing.T) {
	t.Parallel()

	fmRepo := &fieldMapRepoStub{} // No tx support → fallback
	uc := &UseCase{fieldMapRepo: fmRepo}
	fakeTx := &sql.Tx{}

	result, err := uc.existsBySourceIDsWithOptionalTx(context.Background(), fakeTx, []uuid.UUID{uuid.New()})

	require.NoError(t, err)
	assert.NotNil(t, result)
}

// ===========================================================================
// findBySourceIDWithOptionalTx
// ===========================================================================

func TestFindBySourceIDWithOptionalTx_NilTxFallsBack(t *testing.T) {
	t.Parallel()

	called := false
	fmRepo := &fieldMapRepoStub{
		findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
			called = true
			return nil, nil
		},
	}

	uc := &UseCase{fieldMapRepo: fmRepo}

	_, err := uc.findBySourceIDWithOptionalTx(context.Background(), nil, uuid.New())

	require.NoError(t, err)
	assert.True(t, called)
}

func TestFindBySourceIDWithOptionalTx_TxFinderUsed(t *testing.T) {
	t.Parallel()

	txCalled := false
	repo := &fieldMapRepoTxFinderStub{
		fieldMapRepoTxStub: &fieldMapRepoTxStub{
			fieldMapRepoStub: &fieldMapRepoStub{},
		},
		findBySourceIDWithTxFn: func(_ context.Context, _ *sql.Tx, _ uuid.UUID) (*shared.FieldMap, error) {
			txCalled = true
			return &shared.FieldMap{}, nil
		},
	}

	uc := &UseCase{fieldMapRepo: repo}
	fakeTx := &sql.Tx{}

	_, err := uc.findBySourceIDWithOptionalTx(context.Background(), fakeTx, uuid.New())

	require.NoError(t, err)
	assert.True(t, txCalled)
}

func TestFindBySourceIDWithOptionalTx_TxWithoutFinderFallsBack(t *testing.T) {
	t.Parallel()

	fallbackCalled := false
	fmRepo := &fieldMapRepoStub{
		findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*shared.FieldMap, error) {
			fallbackCalled = true
			return nil, nil
		},
	}

	uc := &UseCase{fieldMapRepo: fmRepo}
	fakeTx := &sql.Tx{}

	_, err := uc.findBySourceIDWithOptionalTx(context.Background(), fakeTx, uuid.New())

	require.NoError(t, err)
	assert.True(t, fallbackCalled)
}

// ===========================================================================
// cloneMap / cloneValue / cloneSlice / cloneSliceValue
// ===========================================================================

func TestCloneMap_NilReturnsNil(t *testing.T) {
	t.Parallel()

	assert.Nil(t, cloneMap(context.Background(), nil))
}

func TestCloneMap_EmptyReturnsEmpty(t *testing.T) {
	t.Parallel()

	result := cloneMap(context.Background(), map[string]any{})
	assert.NotNil(t, result)
	assert.Empty(t, result)
}

func TestCloneMap_DeepCopiesNestedStructures(t *testing.T) {
	t.Parallel()

	original := map[string]any{
		"config": map[string]any{
			"tags": []any{"a", "b"},
		},
		"scalar": float64(42),
		"bool":   true,
		"str":    "hello",
	}

	copied := cloneMap(context.Background(), original)

	require.Equal(t, original, copied)

	// Mutate copy, verify original is untouched.
	tags := copied["config"].(map[string]any)["tags"].([]any)
	tags[0] = "mutated"

	originalTags := original["config"].(map[string]any)["tags"].([]any)
	assert.Equal(t, "a", originalTags[0])
}

func TestCloneValue_PrimitiveTypes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name  string
		input any
	}{
		{"nil", nil},
		{"bool", true},
		{"string", "hello"},
		{"float32", float32(1.5)},
		{"float64", float64(2.5)},
		{"int", int(10)},
		{"int8", int8(8)},
		{"int16", int16(16)},
		{"int32", int32(32)},
		{"int64", int64(64)},
		{"uint", uint(10)},
		{"uint8", uint8(8)},
		{"uint16", uint16(16)},
		{"uint32", uint32(32)},
		{"uint64", uint64(64)},
		{"uintptr", uintptr(99)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := cloneValue(ctx, tt.input)
			assert.Equal(t, tt.input, result)
		})
	}
}

func TestCloneValue_NilSlice(t *testing.T) {
	t.Parallel()

	assert.Nil(t, cloneSlice(context.Background(), nil))
}

func TestCloneValue_EmptySlice(t *testing.T) {
	t.Parallel()

	result := cloneSlice(context.Background(), []any{})
	assert.NotNil(t, result)
	assert.Empty(t, result)
}

func TestCloneValue_IntSliceViaReflect(t *testing.T) {
	t.Parallel()

	original := []int{1, 2, 3}
	result := cloneValue(context.Background(), original)

	resultSlice, ok := result.([]any)
	require.True(t, ok)
	assert.Len(t, resultSlice, 3)
}

// ===========================================================================
// cloneSourcesIntoResult / cloneSourcesIntoResultWithTx
// ===========================================================================

func TestCloneSourcesIntoResult_Success(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	newCtxID := uuid.New()

	srcRepo := &sourceRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
			return []*entities.ReconciliationSource{
				{ID: uuid.New(), ContextID: sourceCtxID, Name: "Source"},
			}, libHTTP.CursorPagination{}, nil
		},
		createFn: func(_ context.Context, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}

	uc := &UseCase{
		sourceRepo:   srcRepo,
		fieldMapRepo: &fieldMapRepoStub{},
	}

	result := &entities.CloneResult{
		Context: &entities.ReconciliationContext{ID: newCtxID},
	}

	err := uc.cloneSourcesIntoResult(
		context.Background(),
		CloneContextInput{SourceContextID: sourceCtxID},
		newCtxID,
		result,
	)

	require.NoError(t, err)
	assert.Equal(t, 1, result.SourcesCloned)
	assert.Equal(t, 0, result.FieldMapsCloned)
}

func TestCloneSourcesIntoResultWithTx_Success(t *testing.T) {
	t.Parallel()

	sourceCtxID := uuid.New()
	newCtxID := uuid.New()

	srcRepo := &sourceRepoTxStub{
		sourceRepoStub: &sourceRepoStub{
			// Fallback for fetchAllSources (non-tx finder fallback)
			findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
				return []*entities.ReconciliationSource{
					{ID: uuid.New(), ContextID: sourceCtxID, Name: "Source"},
				}, libHTTP.CursorPagination{}, nil
			},
		},
		createWithTxFn: func(_ context.Context, _ *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return entity, nil
		},
	}

	uc := &UseCase{
		sourceRepo:   srcRepo,
		fieldMapRepo: &fieldMapRepoStub{},
	}

	result := &entities.CloneResult{
		Context: &entities.ReconciliationContext{ID: newCtxID},
	}

	fakeTx := &sql.Tx{}

	err := uc.cloneSourcesIntoResultWithTx(
		context.Background(),
		fakeTx,
		CloneContextInput{SourceContextID: sourceCtxID},
		newCtxID,
		result,
	)

	require.NoError(t, err)
	assert.Equal(t, 1, result.SourcesCloned)
}
