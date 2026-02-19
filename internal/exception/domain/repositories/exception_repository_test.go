//go:build unit

package repositories_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories/mocks"
)

func TestExceptionRepository_MockImplementsInterface(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var _ repositories.ExceptionRepository = mocks.NewMockExceptionRepository(ctrl)
}

func TestExceptionRepository_InterfaceNotNil(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := mocks.NewMockExceptionRepository(ctrl)
	assert.NotNil(t, mock)
	assert.NotNil(t, mock.EXPECT())
}

func TestExceptionRepository_MethodCount(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.ExceptionRepository)(nil)).Elem()

	const expectedMethodCount = 4

	actualCount := repoType.NumMethod()

	assert.Equal(
		t,
		expectedMethodCount,
		actualCount,
		"ExceptionRepository should have exactly %d methods (FindByID, List, Update, UpdateWithTx) - found %d",
		expectedMethodCount,
		actualCount,
	)
}

func TestExceptionRepository_InterfaceContract(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.ExceptionRepository)(nil)).Elem()

	t.Run("FindByID method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("FindByID")
		assert.True(t, exists, "FindByID method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "FindByID should accept context and id")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "FindByID should return exception and error")

		firstParam := method.Type.In(0)
		assert.Equal(t, "context.Context", firstParam.String(),
			"first parameter should be context.Context")
	})

	t.Run("Update method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("Update")
		assert.True(t, exists, "Update method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "Update should accept context and exception")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "Update should return exception and error")

		firstParam := method.Type.In(0)
		assert.Equal(t, "context.Context", firstParam.String(),
			"first parameter should be context.Context")
	})
}

func TestExceptionRepository_AllowedMethods(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.ExceptionRepository)(nil)).Elem()

	allowedMethods := map[string]bool{
		"FindByID":     true,
		"List":         true,
		"Update":       true,
		"UpdateWithTx": true,
	}

	for i := 0; i < repoType.NumMethod(); i++ {
		method := repoType.Method(i)
		methodName := method.Name

		if !allowedMethods[methodName] {
			t.Errorf("unexpected method %q in ExceptionRepository interface", methodName)
		}
	}
}
