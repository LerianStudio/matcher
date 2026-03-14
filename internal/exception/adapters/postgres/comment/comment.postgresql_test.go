//go:build unit

package comment

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubProvider is a minimal stub satisfying ports.InfrastructureProvider
// so that the nil-comment guard (which sits behind the provider check) can be reached.
type stubProvider struct{}

func (p *stubProvider) GetPostgresConnection(_ context.Context) (*ports.PostgresConnectionLease, error) {
	return nil, nil
}

func (p *stubProvider) GetRedisConnection(_ context.Context) (*ports.RedisConnectionLease, error) {
	return nil, nil
}

func (p *stubProvider) BeginTx(_ context.Context) (*ports.TxLease, error) {
	return nil, nil
}

func (p *stubProvider) GetReplicaDB(_ context.Context) (*ports.ReplicaDBLease, error) {
	return nil, nil
}

// --- Constructor ---

func TestNewCommentRepository(t *testing.T) {
	t.Parallel()

	provider := &stubProvider{}
	repo := NewRepository(provider)

	require.NotNil(t, repo)
	assert.Equal(t, provider, repo.provider)
}

// --- Create ---

func TestCommentRepository_Create_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.Create(context.Background(), &entities.ExceptionComment{})

	require.ErrorIs(t, err, ErrRepoNotInitialized)
	assert.Nil(t, result)
}

func TestCommentRepository_Create_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)

	result, err := repo.Create(context.Background(), &entities.ExceptionComment{})

	require.ErrorIs(t, err, ErrRepoNotInitialized)
	assert.Nil(t, result)
}

func TestCommentRepository_Create_NilComment(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&stubProvider{})

	result, err := repo.Create(context.Background(), nil)

	require.ErrorIs(t, err, ErrCommentNil)
	assert.Nil(t, result)
}

// --- FindByExceptionID ---

func TestCommentRepository_FindByExceptionID_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.FindByExceptionID(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrRepoNotInitialized)
	assert.Nil(t, result)
}

func TestCommentRepository_FindByExceptionID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)

	result, err := repo.FindByExceptionID(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrRepoNotInitialized)
	assert.Nil(t, result)
}

// --- FindByID ---

func TestCommentRepository_FindByID_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.FindByID(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrRepoNotInitialized)
	assert.Nil(t, result)
}

func TestCommentRepository_FindByID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)

	result, err := repo.FindByID(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrRepoNotInitialized)
	assert.Nil(t, result)
}

// --- Delete ---

func TestCommentRepository_Delete_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository

	err := repo.Delete(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestCommentRepository_Delete_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)

	err := repo.Delete(context.Background(), uuid.New())

	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// --- Interface Satisfaction ---

func TestCommentRepository_InterfaceSatisfaction(t *testing.T) {
	t.Parallel()

	var _ repositories.CommentRepository = (*Repository)(nil)
}
