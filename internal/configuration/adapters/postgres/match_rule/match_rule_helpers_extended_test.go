//go:build unit

package match_rule

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// --- parseCursor Tests ---

func TestParseCursorExt_EmptyString(t *testing.T) {
	t.Parallel()

	decoded, cursorID, err := parseCursor("")
	require.NoError(t, err)
	assert.Equal(t, libHTTP.CursorDirectionNext, decoded.Direction)
	assert.Equal(t, uuid.Nil, cursorID)
}

func TestParseCursorExt_InvalidBase64(t *testing.T) {
	t.Parallel()

	_, _, err := parseCursor("not-valid-cursor")
	require.Error(t, err)
}

// --- safeUint64 Tests ---

func TestSafeUint64Ext_PositiveValue(t *testing.T) {
	t.Parallel()

	result := safeUint64(10)
	assert.Equal(t, uint64(10), result)
}

func TestSafeUint64Ext_ZeroValue(t *testing.T) {
	t.Parallel()

	result := safeUint64(0)
	assert.Equal(t, uint64(0), result)
}

func TestSafeUint64Ext_NegativeValue(t *testing.T) {
	t.Parallel()

	result := safeUint64(-5)
	assert.Equal(t, uint64(0), result)
}

func TestSafeUint64Ext_LargePositive(t *testing.T) {
	t.Parallel()

	result := safeUint64(1000000)
	assert.Equal(t, uint64(1000000), result)
}

// --- buildReorderQuery Tests ---

func TestBuildReorderQueryExt_SingleRule(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	ruleIDs := []uuid.UUID{uuid.MustParse("22222222-2222-2222-2222-222222222222")}

	query, args := buildReorderQuery(contextID, ruleIDs)

	assert.Contains(t, query, "UPDATE match_rules SET priority = CASE id")
	assert.Contains(t, query, "WHERE context_id = $1")
	assert.Contains(t, query, "AND id IN (")
	// contextID + (ruleID, priority) + ruleID in IN clause = 1 + 2 + 1 = 4
	assert.Len(t, args, 4)
	assert.Equal(t, contextID.String(), args[0])
}

func TestBuildReorderQueryExt_MultipleRules(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	ruleIDs := []uuid.UUID{
		uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		uuid.MustParse("44444444-4444-4444-4444-444444444444"),
	}

	query, args := buildReorderQuery(contextID, ruleIDs)

	assert.Contains(t, query, "WHEN $2 THEN $3")
	assert.Contains(t, query, "WHEN $4 THEN $5")
	assert.Contains(t, query, "WHEN $6 THEN $7")
	// contextID + 3*(ruleID, priority) + 3*ruleID = 1 + 6 + 3 = 10
	assert.Len(t, args, 10)
	// Check priorities are sequential starting from 1.
	assert.Equal(t, 1, args[2])
	assert.Equal(t, 2, args[4])
	assert.Equal(t, 3, args[6])
}

// --- paginateAndCalculateCursor Tests ---

func TestPaginateAndCalculateCursorExt_EmptyRules(t *testing.T) {
	t.Parallel()

	rules, pagination, err := paginateAndCalculateCursor(
		"",
		libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext},
		entities.MatchRules{},
		10,
	)

	require.NoError(t, err)
	assert.Empty(t, rules)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestPaginateAndCalculateCursorExt_SinglePage(t *testing.T) {
	t.Parallel()

	id1 := testutil.DeterministicUUID("paginate-single-page-rule-1")
	id2 := testutil.DeterministicUUID("paginate-single-page-rule-2")

	rules := entities.MatchRules{
		{ID: id1, Priority: 1},
		{ID: id2, Priority: 2},
	}

	resultRules, _, err := paginateAndCalculateCursor(
		"",
		libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext},
		rules,
		10,
	)

	require.NoError(t, err)
	assert.Len(t, resultRules, 2)
}

func TestPaginateAndCalculateCursorExt_HasMoreResults(t *testing.T) {
	t.Parallel()

	paginationIDs := testutil.DeterministicUUIDs("paginate-has-more-rule", 3)

	rules := make(entities.MatchRules, 0, 3)
	for i := 0; i < 3; i++ {
		rules = append(rules, &entities.MatchRule{ID: paginationIDs[i], Priority: i + 1})
	}

	// Limit is 2 but we have 3 results, indicating there are more pages.
	resultRules, pagination, err := paginateAndCalculateCursor(
		"",
		libHTTP.Cursor{Direction: libHTTP.CursorDirectionNext},
		rules,
		2,
	)

	require.NoError(t, err)
	assert.Len(t, resultRules, 2)
	assert.NotEmpty(t, pagination.Next)
}

// --- NewMatchRulePostgreSQLModel Tests ---

func TestNewMatchRulePostgreSQLModelCov_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := NewMatchRulePostgreSQLModel(nil)
	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrMatchRuleEntityRequired)
}

func TestNewMatchRulePostgreSQLModelCov_NilContextID(t *testing.T) {
	t.Parallel()

	entity := &entities.MatchRule{
		ID:        testutil.DeterministicUUID("model-nil-context-rule"),
		ContextID: uuid.Nil,
	}

	model, err := NewMatchRulePostgreSQLModel(entity)
	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrMatchRuleContextIDRequired)
}

// --- ToEntity Tests ---

func TestToEntityCov_NilModel(t *testing.T) {
	t.Parallel()

	var model *MatchRulePostgreSQLModel
	entity, err := model.ToEntity()
	require.Error(t, err)
	require.Nil(t, entity)
	require.ErrorIs(t, err, ErrMatchRuleModelRequired)
}

func TestToEntityCov_InvalidID(t *testing.T) {
	t.Parallel()

	model := &MatchRulePostgreSQLModel{
		ID:        "not-a-uuid",
		ContextID: testutil.DeterministicUUID("toentity-invalid-id-context").String(),
		Type:      "EXACT",
		Config:    []byte(`{}`),
	}

	entity, err := model.ToEntity()
	require.Error(t, err)
	require.Nil(t, entity)
}

func TestToEntityCov_InvalidContextID(t *testing.T) {
	t.Parallel()

	model := &MatchRulePostgreSQLModel{
		ID:        testutil.DeterministicUUID("toentity-invalid-context-id").String(),
		ContextID: "not-a-uuid",
		Type:      "EXACT",
		Config:    []byte(`{}`),
	}

	entity, err := model.ToEntity()
	require.Error(t, err)
	require.Nil(t, entity)
}

func TestToEntityCov_InvalidRuleType(t *testing.T) {
	t.Parallel()

	model := &MatchRulePostgreSQLModel{
		ID:        testutil.DeterministicUUID("toentity-invalid-type-rule").String(),
		ContextID: testutil.DeterministicUUID("toentity-invalid-type-context").String(),
		Type:      "INVALID_TYPE",
		Config:    []byte(`{}`),
	}

	entity, err := model.ToEntity()
	require.Error(t, err)
	require.Nil(t, entity)
}

func TestToEntityCov_InvalidConfig(t *testing.T) {
	t.Parallel()

	model := &MatchRulePostgreSQLModel{
		ID:        testutil.DeterministicUUID("toentity-invalid-config-rule").String(),
		ContextID: testutil.DeterministicUUID("toentity-invalid-config-context").String(),
		Type:      "EXACT",
		Config:    []byte(`{invalid json`),
	}

	entity, err := model.ToEntity()
	require.Error(t, err)
	require.Nil(t, entity)
}

func TestToEntityCov_EmptyConfig(t *testing.T) {
	t.Parallel()

	model := &MatchRulePostgreSQLModel{
		ID:        testutil.DeterministicUUID("toentity-empty-config-rule").String(),
		ContextID: testutil.DeterministicUUID("toentity-empty-config-context").String(),
		Type:      "EXACT",
		Config:    nil,
	}

	entity, err := model.ToEntity()
	require.NoError(t, err)
	require.NotNil(t, entity)
	require.NotNil(t, entity.Config)
}
