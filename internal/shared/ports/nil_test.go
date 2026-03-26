//go:build unit

package ports

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type nilTestStruct struct{}

func TestIsNilValue(t *testing.T) {
	t.Parallel()

	var nilPointer *nilTestStruct
	var nilMap map[string]string
	var nilFunc func()

	assert.True(t, IsNilValue(nil))
	assert.True(t, IsNilValue(nilPointer))
	assert.True(t, IsNilValue(nilMap))
	assert.True(t, IsNilValue(nilFunc))
	assert.False(t, IsNilValue(&nilTestStruct{}))
	assert.False(t, IsNilValue(42))
}
