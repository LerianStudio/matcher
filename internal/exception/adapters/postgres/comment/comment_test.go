// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package comment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRepository_ReturnsNonNil(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	assert.NotNil(t, repo)
}

func TestRepository_Create_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	_, err := repo.Create(context.Background(), nil)
	assert.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_FindByID_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	_, err := repo.FindByID(context.Background(), [16]byte{})
	assert.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_FindByExceptionID_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	_, err := repo.FindByExceptionID(context.Background(), [16]byte{})
	assert.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_Delete_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.Delete(context.Background(), [16]byte{})
	assert.ErrorIs(t, err, ErrRepoNotInitialized)
}
