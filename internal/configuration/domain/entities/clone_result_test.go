// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
	assert.Zero(t, result.FeeRulesCloned)
	assert.Zero(t, result.FieldMapsCloned)
}

func TestNewCloneResult_NilContext(t *testing.T) {
	t.Parallel()

	result, err := NewCloneResult(context.Background(), nil)

	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrCloneResultContextRequired)
}
