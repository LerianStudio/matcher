//go:build unit

package repositories

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories/mocks"
)

func TestDashboardRepository_MockImplementsInterface(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var _ DashboardRepository = mocks.NewMockDashboardRepository(ctrl)
}

func TestDashboardRepository_InterfaceNotNil(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := mocks.NewMockDashboardRepository(ctrl)
	assert.NotNil(t, mock)
	assert.NotNil(t, mock.EXPECT())
}

func TestDashboardRepository_ReadOnlyInterface(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*DashboardRepository)(nil)).Elem()

	allowedMethods := map[string]bool{
		"GetVolumeStats":       true,
		"GetSLAStats":          true,
		"GetSummaryMetrics":    true,
		"GetTrendMetrics":      true,
		"GetBreakdownMetrics":  true,
		"GetSourceBreakdown":   true,
		"GetCashImpactSummary": true,
	}

	forbiddenPatterns := []string{
		"Create",
		"Update",
		"Delete",
		"Remove",
		"Insert",
		"Save",
	}

	for i := 0; i < repoType.NumMethod(); i++ {
		method := repoType.Method(i)
		methodName := method.Name

		if !allowedMethods[methodName] {
			t.Errorf(
				"unexpected method %q in DashboardRepository interface - should be read-only",
				methodName,
			)
		}

		for _, pattern := range forbiddenPatterns {
			if containsSubstring(methodName, pattern) {
				t.Errorf(
					"method %q contains forbidden pattern %q - dashboard repository must be read-only",
					methodName,
					pattern,
				)
			}
		}
	}
}

func TestDashboardRepository_MethodCount(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*DashboardRepository)(nil)).Elem()

	const expectedMethodCount = 7

	actualCount := repoType.NumMethod()

	assert.Equal(t, expectedMethodCount, actualCount,
		"DashboardRepository should have exactly %d methods - found %d",
		expectedMethodCount, actualCount)
}

func TestDashboardRepository_InterfaceContract(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*DashboardRepository)(nil)).Elem()

	t.Run("GetVolumeStats method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("GetVolumeStats")
		assert.True(t, exists, "GetVolumeStats method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "GetVolumeStats should accept context and filter")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "GetVolumeStats should return stats and error")
	})

	t.Run("GetSLAStats method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("GetSLAStats")
		assert.True(t, exists, "GetSLAStats method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "GetSLAStats should accept context and filter")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "GetSLAStats should return stats and error")
	})
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}
