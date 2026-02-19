//go:build unit

package export_job

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

func TestRepository_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, uuid.New())
	require.ErrorIs(t, err, pgcommon.ErrConnectionRequired)
}
