// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestNewRepository_ReturnsNonNil(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	require.NotNil(t, repo)
	assert.Equal(t, provider, repo.provider)
}

func TestNewRepository_AcceptsNilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)

	require.NotNil(t, repo, "NewRepository must return a non-nil *Repository even with nil provider")
	assert.Nil(t, repo.provider)
}
