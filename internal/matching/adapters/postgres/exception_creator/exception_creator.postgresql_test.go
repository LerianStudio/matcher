// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package exception_creator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRepository_NilProvider_ReturnsNonNil(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	require.NotNil(t, repo)
}
