//go:build unit

package repositories_test

import (
	"testing"

	"github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	"github.com/LerianStudio/matcher/internal/matching/domain/repositories/mocks"
)

func TestAdjustmentRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ repositories.AdjustmentRepository = (*mocks.MockAdjustmentRepository)(nil)
}
