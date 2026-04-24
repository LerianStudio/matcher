// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/governance/domain/repositories/mocks"
)

func TestNewUseCase(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		uc, err := NewUseCase(repo)

		require.NoError(t, err)
		require.NotNil(t, uc)
	})

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		uc, err := NewUseCase(nil)

		require.ErrorIs(t, err, ErrQueryRepoRequired)
		require.Nil(t, uc)
	})
}
