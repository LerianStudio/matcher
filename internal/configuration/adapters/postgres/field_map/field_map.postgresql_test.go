//go:build unit

package field_map

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestRepository_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, err := repo.Create(ctx, &shared.FieldMap{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.FindByID(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.FindBySourceID(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	_, err = repo.Update(ctx, &shared.FieldMap{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	err = repo.Delete(ctx, uuid.New())
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

func TestRepositorySentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrFieldMapEntityRequired", ErrFieldMapEntityRequired},
		{"ErrFieldMapModelRequired", ErrFieldMapModelRequired},
		{"ErrRepoNotInitialized", ErrRepoNotInitialized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestDedupeSourceIDs(t *testing.T) {
	t.Parallel()

	t.Run("empty input returns empty slice", func(t *testing.T) {
		t.Parallel()

		result := dedupeSourceIDs(nil)
		require.Empty(t, result)
	})

	t.Run("single ID returns single ID", func(t *testing.T) {
		t.Parallel()

		id := testutil.MustDeterministicUUID("single-id")
		result := dedupeSourceIDs([]uuid.UUID{id})
		require.Len(t, result, 1)
		require.Equal(t, id, result[0])
	})

	t.Run("removes duplicates preserving order", func(t *testing.T) {
		t.Parallel()

		id1 := testutil.MustDeterministicUUID("dedupe-id1")
		id2 := testutil.MustDeterministicUUID("dedupe-id2")
		id3 := testutil.MustDeterministicUUID("dedupe-id3")
		input := []uuid.UUID{id1, id2, id1, id3, id2, id1}
		result := dedupeSourceIDs(input)
		require.Len(t, result, 3)
		require.Equal(t, id1, result[0])
		require.Equal(t, id2, result[1])
		require.Equal(t, id3, result[2])
	})

	t.Run("no duplicates returns same length", func(t *testing.T) {
		t.Parallel()

		ids := []uuid.UUID{
			testutil.MustDeterministicUUID("unique-id1"),
			testutil.MustDeterministicUUID("unique-id2"),
			testutil.MustDeterministicUUID("unique-id3"),
		}
		result := dedupeSourceIDs(ids)
		require.Len(t, result, 3)
	})
}

func TestExistsBySourceIDsBatchSize(t *testing.T) {
	t.Parallel()

	require.Equal(
		t,
		1000,
		existsBySourceIDsBatchSize,
		"batch size should be 1000 to protect against Postgres parameter limits",
	)
}

func TestJoinPlaceholders(t *testing.T) {
	t.Parallel()

	t.Run("zero count returns empty string", func(t *testing.T) {
		t.Parallel()

		result := joinPlaceholders(0)
		require.Empty(t, result)
	})

	t.Run("negative count returns empty string", func(t *testing.T) {
		t.Parallel()

		result := joinPlaceholders(-5)
		require.Empty(t, result)
	})

	t.Run("count of 1 returns single placeholder", func(t *testing.T) {
		t.Parallel()

		result := joinPlaceholders(1)
		require.Equal(t, "$1", result)
	})

	t.Run("count of 3 returns comma-separated placeholders", func(t *testing.T) {
		t.Parallel()

		result := joinPlaceholders(3)
		require.Equal(t, "$1, $2, $3", result)
	})

	t.Run("count of 5 returns correct sequence", func(t *testing.T) {
		t.Parallel()

		result := joinPlaceholders(5)
		require.Equal(t, "$1, $2, $3, $4, $5", result)
	})

	t.Run("batch size boundary returns correct sequence", func(t *testing.T) {
		t.Parallel()

		var builder strings.Builder

		for i := 1; i <= existsBySourceIDsBatchSize; i++ {
			if i > 1 {
				builder.WriteString(", ")
			}

			builder.WriteString("$")
			builder.WriteString(strconv.Itoa(i))
		}

		result := joinPlaceholders(existsBySourceIDsBatchSize)
		require.Equal(t, builder.String(), result)
	})
}
