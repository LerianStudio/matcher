// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package match_rule

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestRepository_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, err := repo.Create(ctx, &entities.MatchRule{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.FindByID(ctx, uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, _, err = repo.FindByContextID(ctx, uuid.New(), "", 10)
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, _, err = repo.FindByContextIDAndType(ctx, uuid.New(), shared.RuleTypeExact, "", 10)
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.FindByPriority(ctx, uuid.New(), 1)
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.Update(ctx, &entities.MatchRule{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	err = repo.Delete(ctx, uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	err = repo.ReorderPriorities(ctx, uuid.New(), []uuid.UUID{uuid.New()})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_NilEntity(t *testing.T) {
	t.Parallel()

	// Nil connection check happens before entity check
	repo := &Repository{}
	_, err := repo.Create(context.Background(), nil)
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.Update(context.Background(), nil)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_ReorderPriorities_EmptyList(t *testing.T) {
	t.Parallel()

	// Nil connection check happens before ruleIDs check
	repo := &Repository{}
	err := repo.ReorderPriorities(context.Background(), uuid.New(), []uuid.UUID{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepositorySentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrMatchRuleEntityRequired", ErrMatchRuleEntityRequired},
		{"ErrMatchRuleModelRequired", ErrMatchRuleModelRequired},
		{"ErrMatchRuleContextIDRequired", ErrMatchRuleContextIDRequired},
		{"ErrRepoNotInitialized", ErrRepoNotInitialized},
		{"ErrRuleIDsRequired", ErrRuleIDsRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}
