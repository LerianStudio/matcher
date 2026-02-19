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

func TestDisputeRepository_MockImplementsInterface(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var _ repositories.DisputeRepository = mocks.NewMockDisputeRepository(ctrl)
}

func TestDisputeRepository_InterfaceNotNil(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := mocks.NewMockDisputeRepository(ctrl)
	assert.NotNil(t, mock)
	assert.NotNil(t, mock.EXPECT())
}

func TestDisputeRepository_MethodCount(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.DisputeRepository)(nil)).Elem()

	const expectedMethodCount = 7

	actualCount := repoType.NumMethod()

	assert.Equal(
		t,
		expectedMethodCount,
		actualCount,
		"DisputeRepository should have exactly %d methods (Create, CreateWithTx, FindByID, FindByExceptionID, List, Update, UpdateWithTx) - found %d",
		expectedMethodCount,
		actualCount,
	)
}

func TestDisputeRepository_InterfaceContract(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.DisputeRepository)(nil)).Elem()

	t.Run("Create method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("Create")
		assert.True(t, exists, "Create method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "Create should accept context and dispute")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "Create should return dispute and error")

		firstParam := method.Type.In(0)
		assert.Equal(t, "context.Context", firstParam.String(),
			"first parameter should be context.Context")
	})

	t.Run("FindByID method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("FindByID")
		assert.True(t, exists, "FindByID method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "FindByID should accept context and id")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "FindByID should return dispute and error")

		firstParam := method.Type.In(0)
		assert.Equal(t, "context.Context", firstParam.String(),
			"first parameter should be context.Context")
	})

	t.Run("FindByExceptionID method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("FindByExceptionID")
		assert.True(t, exists, "FindByExceptionID method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "FindByExceptionID should accept context and exceptionID")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "FindByExceptionID should return dispute and error")

		firstParam := method.Type.In(0)
		assert.Equal(t, "context.Context", firstParam.String(),
			"first parameter should be context.Context")
	})

	t.Run("List method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("List")
		assert.True(t, exists, "List method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 3, numIn, "List should accept context, filter, and cursor")

		numOut := method.Type.NumOut()
		assert.Equal(t, 3, numOut, "List should return disputes, pagination, and error")

		firstParam := method.Type.In(0)
		assert.Equal(t, "context.Context", firstParam.String(),
			"first parameter should be context.Context")
	})

	t.Run("Update method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("Update")
		assert.True(t, exists, "Update method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "Update should accept context and dispute")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "Update should return dispute and error")

		firstParam := method.Type.In(0)
		assert.Equal(t, "context.Context", firstParam.String(),
			"first parameter should be context.Context")
	})
}

func TestDisputeRepository_AllowedMethods(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.DisputeRepository)(nil)).Elem()

	allowedMethods := map[string]bool{
		"Create":            true,
		"CreateWithTx":      true,
		"FindByID":          true,
		"FindByExceptionID": true,
		"List":              true,
		"Update":            true,
		"UpdateWithTx":      true,
	}

	for i := 0; i < repoType.NumMethod(); i++ {
		method := repoType.Method(i)
		methodName := method.Name

		if !allowedMethods[methodName] {
			t.Errorf("unexpected method %q in DisputeRepository interface", methodName)
		}
	}
}
