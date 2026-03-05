//go:build unit

package entities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCloneResult(t *testing.T) {
	t.Parallel()

	ctx := &ReconciliationContext{}

	result, err := NewCloneResult(context.Background(), ctx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, ctx, result.Context)
	assert.Zero(t, result.SourcesCloned)
	assert.Zero(t, result.RulesCloned)
	assert.Zero(t, result.FieldMapsCloned)
	assert.Zero(t, result.FeeSchedulesCloned)
}

func TestNewCloneResult_NilContext(t *testing.T) {
	t.Parallel()

	result, err := NewCloneResult(context.Background(), nil)

	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrCloneResultContextRequired)
}
