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

func TestCallbackIdempotencyRepository_MockImplementsInterface(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var _ repositories.CallbackIdempotencyRepository = mocks.NewMockCallbackIdempotencyRepository(ctrl)
}

func TestCallbackIdempotencyRepository_InterfaceNotNil(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := mocks.NewMockCallbackIdempotencyRepository(ctrl)
	assert.NotNil(t, mock)
	assert.NotNil(t, mock.EXPECT())
}

func TestCallbackIdempotencyRepository_MethodCount(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.CallbackIdempotencyRepository)(nil)).Elem()

	const expectedMethodCount = 4

	actualCount := repoType.NumMethod()

	assert.Equal(
		t,
		expectedMethodCount,
		actualCount,
		"CallbackIdempotencyRepository should have exactly %d methods (TryAcquire, MarkComplete, MarkFailed, GetCachedResult) - found %d",
		expectedMethodCount,
		actualCount,
	)
}

func TestCallbackIdempotencyRepository_InterfaceContract(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.CallbackIdempotencyRepository)(nil)).Elem()

	t.Run("TryAcquire method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("TryAcquire")
		assert.True(t, exists, "TryAcquire method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "TryAcquire should accept context and key")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "TryAcquire should return acquired bool and error")

		firstParam := method.Type.In(0)
		assert.Equal(t, "context.Context", firstParam.String(),
			"first parameter should be context.Context")

		firstReturn := method.Type.Out(0)
		assert.Equal(t, "bool", firstReturn.String(),
			"first return should be bool (acquired)")
	})

	t.Run("MarkComplete method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("MarkComplete")
		assert.True(t, exists, "MarkComplete method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(
			t,
			4,
			numIn,
			"MarkComplete should accept context, key, response, and httpStatus",
		)

		numOut := method.Type.NumOut()
		assert.Equal(t, 1, numOut, "MarkComplete should return only error")

		firstParam := method.Type.In(0)
		assert.Equal(t, "context.Context", firstParam.String(),
			"first parameter should be context.Context")
	})

	t.Run("GetCachedResult method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("GetCachedResult")
		assert.True(t, exists, "GetCachedResult method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "GetCachedResult should accept context and key")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "GetCachedResult should return *IdempotencyResult and error")

		firstParam := method.Type.In(0)
		assert.Equal(t, "context.Context", firstParam.String(),
			"first parameter should be context.Context")
	})

	t.Run("MarkFailed method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("MarkFailed")
		assert.True(t, exists, "MarkFailed method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "MarkFailed should accept context and key")

		numOut := method.Type.NumOut()
		assert.Equal(t, 1, numOut, "MarkFailed should return only error")

		firstParam := method.Type.In(0)
		assert.Equal(t, "context.Context", firstParam.String(),
			"first parameter should be context.Context")
	})
}

func TestCallbackIdempotencyRepository_AllowedMethods(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.CallbackIdempotencyRepository)(nil)).Elem()

	allowedMethods := map[string]bool{
		"TryAcquire":      true,
		"MarkComplete":    true,
		"MarkFailed":      true,
		"GetCachedResult": true,
	}

	for i := 0; i < repoType.NumMethod(); i++ {
		method := repoType.Method(i)
		methodName := method.Name

		if !allowedMethods[methodName] {
			t.Errorf("unexpected method %q in CallbackIdempotencyRepository interface", methodName)
		}
	}
}
