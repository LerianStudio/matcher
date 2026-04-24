// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubContextAccessProvider struct {
	result *ContextAccessInfo
	err    error
}

func (stub stubContextAccessProvider) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*ContextAccessInfo, error) {
	return stub.result, stub.err
}

func TestContextAccessInfo_HoldsMinimalContextState(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	info := ContextAccessInfo{
		ID:     contextID,
		Active: true,
	}

	assert.Equal(t, contextID, info.ID)
	assert.True(t, info.Active)
}

func TestContextAccessProvider_FindByID_ReturnsContextAccessInfo(t *testing.T) {
	t.Parallel()

	want := &ContextAccessInfo{ID: uuid.New(), Active: true}
	provider := stubContextAccessProvider{result: want}

	got, err := provider.FindByID(context.Background(), uuid.New())

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want, got)
}
