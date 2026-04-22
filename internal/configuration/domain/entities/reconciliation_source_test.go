//go:build unit

package entities_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// These tests verify that the type aliases in entities correctly re-export
// the canonical shared.ReconciliationSource and its companions. Full behavior
// tests for the underlying type live at internal/shared/domain/reconciliation_source_test.go.
//
// See docs/handoffs/simplify/T-007-followup.md for the consolidation rationale.

func TestReconciliationSourceAlias_IsSharedType(t *testing.T) {
	t.Parallel()

	// Round-trip through the alias: a value created via entities.NewReconciliationSource
	// is assignable to *shared.ReconciliationSource with zero conversion, because
	// entities.ReconciliationSource is a type alias for shared.ReconciliationSource.
	input := entities.CreateReconciliationSourceInput{
		Name: "Primary Bank",
		Type: shared.SourceTypeBank,
		Side: sharedfee.MatchingSideLeft,
	}

	got, err := entities.NewReconciliationSource(context.Background(), uuid.New(), input)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Pointer-identity preserved: the same value satisfies both names.
	var asShared *shared.ReconciliationSource = got
	assert.Equal(t, got.ID, asShared.ID)
	assert.Equal(t, got.Name, asShared.Name)
	assert.Equal(t, shared.SourceTypeBank, asShared.Type)
}

func TestReconciliationSourceAlias_SentinelsMatch(t *testing.T) {
	t.Parallel()

	// Sentinels are re-exported as var assignments, so they are the same
	// error instance as the shared kernel version — errors.Is and equality
	// both hold.
	assert.Same(t, shared.ErrNilReconciliationSource, entities.ErrNilReconciliationSource)
	assert.Same(t, shared.ErrSourceNameRequired, entities.ErrSourceNameRequired)
	assert.Same(t, shared.ErrSourceNameTooLong, entities.ErrSourceNameTooLong)
	assert.Same(t, shared.ErrSourceTypeInvalid, entities.ErrSourceTypeInvalid)
	assert.Same(t, shared.ErrSourceContextRequired, entities.ErrSourceContextRequired)
	assert.Same(t, shared.ErrSourceSideRequired, entities.ErrSourceSideRequired)
	assert.Same(t, shared.ErrSourceSideInvalid, entities.ErrSourceSideInvalid)
}
