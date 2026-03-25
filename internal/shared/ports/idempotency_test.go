//go:build unit

package ports

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIdempotencyRepositoryInterfaceShape(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*IdempotencyRepository)(nil)).Elem()
	assert.Equal(t, reflect.Interface, repoType.Kind())
	assert.Equal(t, 5, repoType.NumMethod())

	_, hasTryAcquire := repoType.MethodByName("TryAcquire")
	_, hasTryReacquire := repoType.MethodByName("TryReacquireFromFailed")
	_, hasMarkComplete := repoType.MethodByName("MarkComplete")
	_, hasMarkFailed := repoType.MethodByName("MarkFailed")
	_, hasGetCachedResult := repoType.MethodByName("GetCachedResult")

	assert.True(t, hasTryAcquire)
	assert.True(t, hasTryReacquire)
	assert.True(t, hasMarkComplete)
	assert.True(t, hasMarkFailed)
	assert.True(t, hasGetCachedResult)
}
