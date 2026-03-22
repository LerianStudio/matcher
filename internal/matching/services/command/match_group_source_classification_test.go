//go:build unit

package command

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestClassifySources_OneToOne_ValidSetup(t *testing.T) {
	t.Parallel()

	leftID := uuid.MustParse("00000000-0000-0000-0000-000000310001")
	rightID := uuid.MustParse("00000000-0000-0000-0000-000000310002")

	sources := []*ports.SourceInfo{
		{ID: leftID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	left, right, err := classifySources(shared.ContextTypeOneToOne, sources)
	require.NoError(t, err)
	assert.Len(t, left, 1)
	assert.Len(t, right, 1)
	_, leftOk := left[leftID]
	assert.True(t, leftOk)
	_, rightOk := right[rightID]
	assert.True(t, rightOk)
}

func TestClassifySources_OneToOne_TwoLeftSources_Error(t *testing.T) {
	t.Parallel()

	sources := []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: uuid.New(), Type: ports.SourceTypeFile, Side: fee.MatchingSideLeft},
	}

	_, _, err := classifySources(shared.ContextTypeOneToOne, sources)
	require.ErrorIs(t, err, ErrOneToOneRequiresExactlyOneLeftSource)
}

func TestClassifySources_OneToOne_TwoRightSources_Error(t *testing.T) {
	t.Parallel()

	sources := []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger, Side: fee.MatchingSideRight},
		{ID: uuid.New(), Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	_, _, err := classifySources(shared.ContextTypeOneToOne, sources)
	require.ErrorIs(t, err, ErrOneToOneRequiresExactlyOneLeftSource)
}

func TestClassifySources_OneToMany_ValidSetup(t *testing.T) {
	t.Parallel()

	leftID := uuid.MustParse("00000000-0000-0000-0000-000000310010")
	rightID1 := uuid.MustParse("00000000-0000-0000-0000-000000310011")
	rightID2 := uuid.MustParse("00000000-0000-0000-0000-000000310012")

	sources := []*ports.SourceInfo{
		{ID: leftID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightID1, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
		{ID: rightID2, Type: ports.SourceTypeAPI, Side: fee.MatchingSideRight},
	}

	left, right, err := classifySources(shared.ContextTypeOneToMany, sources)
	require.NoError(t, err)
	assert.Len(t, left, 1)
	assert.Len(t, right, 2)
}

func TestClassifySources_OneToMany_MultipleLeftSources_Error(t *testing.T) {
	t.Parallel()

	sources := []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: uuid.New(), Type: ports.SourceTypeFile, Side: fee.MatchingSideLeft},
		{ID: uuid.New(), Type: ports.SourceTypeAPI, Side: fee.MatchingSideRight},
	}

	_, _, err := classifySources(shared.ContextTypeOneToMany, sources)
	require.ErrorIs(t, err, ErrOneToManyRequiresExactlyOneLeftSource)
}

func TestClassifySources_OneToMany_NoRightSources_Error(t *testing.T) {
	t.Parallel()

	sources := []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: uuid.New(), Type: ports.SourceTypeFile, Side: fee.MatchingSideLeft},
	}

	_, _, err := classifySources(shared.ContextTypeOneToMany, sources)
	require.ErrorIs(t, err, ErrOneToManyRequiresExactlyOneLeftSource)
}

func TestClassifySources_LessThanTwoSources_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sources []*ports.SourceInfo
	}{
		{"empty", []*ports.SourceInfo{}},
		{"single source", []*ports.SourceInfo{
			{ID: uuid.New(), Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		}},
		{"all nil", []*ports.SourceInfo{nil, nil}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := classifySources(shared.ContextTypeOneToOne, tt.sources)
			require.ErrorIs(t, err, ErrAtLeastTwoSourcesRequired)
		})
	}
}

func TestClassifySources_NilSourcesFiltered(t *testing.T) {
	t.Parallel()

	leftID := uuid.MustParse("00000000-0000-0000-0000-000000310020")
	rightID := uuid.MustParse("00000000-0000-0000-0000-000000310021")

	sources := []*ports.SourceInfo{
		nil,
		{ID: leftID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		nil,
		{ID: rightID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
		nil,
	}

	left, right, err := classifySources(shared.ContextTypeOneToOne, sources)
	require.NoError(t, err)
	assert.Len(t, left, 1)
	assert.Len(t, right, 1)
}

func TestClassifySources_MissingSide_Error(t *testing.T) {
	t.Parallel()

	sources := []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: uuid.New(), Type: ports.SourceTypeFile},
	}

	_, _, err := classifySources(shared.ContextTypeOneToOne, sources)
	require.ErrorIs(t, err, ErrSourceSideRequiredForMatching)
}

func TestClassifySources_ManyToMany_Unsupported(t *testing.T) {
	t.Parallel()

	sources := []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: uuid.New(), Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	_, _, err := classifySources(shared.ContextTypeManyToMany, sources)
	require.ErrorIs(t, err, ErrUnsupportedContextType)
}

func TestValidateSourceCountForContextType_OneToOne_Valid(t *testing.T) {
	t.Parallel()

	err := validateSourceCountForContextType(shared.ContextTypeOneToOne, 1, 1)
	require.NoError(t, err)
}

func TestValidateSourceCountForContextType_OneToOne_InvalidLeft(t *testing.T) {
	t.Parallel()

	err := validateSourceCountForContextType(shared.ContextTypeOneToOne, 2, 1)
	require.ErrorIs(t, err, ErrOneToOneRequiresExactlyOneLeftSource)
}

func TestValidateSourceCountForContextType_OneToOne_InvalidRight(t *testing.T) {
	t.Parallel()

	err := validateSourceCountForContextType(shared.ContextTypeOneToOne, 1, 2)
	require.ErrorIs(t, err, ErrOneToOneRequiresExactlyOneRightSource)
}

func TestValidateSourceCountForContextType_OneToMany_Valid(t *testing.T) {
	t.Parallel()

	err := validateSourceCountForContextType(shared.ContextTypeOneToMany, 1, 3)
	require.NoError(t, err)
}

func TestValidateSourceCountForContextType_OneToMany_ZeroRight(t *testing.T) {
	t.Parallel()

	err := validateSourceCountForContextType(shared.ContextTypeOneToMany, 1, 0)
	require.ErrorIs(t, err, ErrAtLeastOneRightSourceRequired)
}

func TestValidateSourceCountForContextType_ManyToMany_Error(t *testing.T) {
	t.Parallel()

	err := validateSourceCountForContextType(shared.ContextTypeManyToMany, 2, 2)
	require.ErrorIs(t, err, ErrUnsupportedContextType)
}

func TestValidateSourceCountForContextType_UnknownType_NoError(t *testing.T) {
	t.Parallel()

	// Unknown types pass through without validation
	err := validateSourceCountForContextType(shared.ContextType("custom"), 5, 5)
	require.NoError(t, err)
}
