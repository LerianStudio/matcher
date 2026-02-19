//go:build unit

package match_group

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestNewRepository_NilProvider_ReturnsNonNil(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	require.NotNil(t, repo)
}

func TestNewRepository_WithProvider_ReturnsValidRepo(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	require.NotNil(t, repo)
	assert.Equal(t, provider, repo.provider)
}

func TestFindByID_ConstructsCorrectSQL(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	contextID := uuid.New()
	groupID := uuid.New()

	expectedColumns := "id, context_id, run_id, rule_id, confidence, status, rejected_reason, confirmed_at, created_at, updated_at"
	mock.ExpectQuery("SELECT "+expectedColumns+" FROM match_groups WHERE context_id=\\$1 AND id=\\$2").
		WithArgs(contextID.String(), groupID.String()).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "context_id", "run_id", "rule_id", "confidence", "status",
			"rejected_reason", "confirmed_at", "created_at", "updated_at",
		}))

	_, _ = repo.FindByID(ctx, contextID, groupID)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListByRunID_ConstructsCorrectSQL(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()

	mock.ExpectQuery("SELECT (.+) FROM match_groups WHERE (.+)").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "context_id", "run_id", "rule_id", "confidence", "status",
			"rejected_reason", "confirmed_at", "created_at", "updated_at",
		}))

	filter := matchingRepos.CursorFilter{Limit: 10}
	_, _, _ = repo.ListByRunID(ctx, contextID, runID, filter)

	require.NoError(t, mock.ExpectationsWereMet())
}
