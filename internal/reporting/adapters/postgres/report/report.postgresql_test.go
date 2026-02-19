//go:build unit

package report

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestRepository_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)

	filter := entities.ReportFilter{
		ContextID: uuid.New(),
	}

	err := repo.validateFilter(&filter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}
