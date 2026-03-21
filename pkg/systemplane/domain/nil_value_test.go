// Copyright 2025 Lerian Studio.

//go:build unit

package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsNilValue_UntypedNil(t *testing.T) {
	t.Parallel()

	assert.True(t, IsNilValue(nil))
}

func TestIsNilValue_TypedNilPointer(t *testing.T) {
	t.Parallel()

	var p *string

	assert.True(t, IsNilValue(p))
}

func TestIsNilValue_TypedNilSlice(t *testing.T) {
	t.Parallel()

	var s []string

	assert.True(t, IsNilValue(s))
}

func TestIsNilValue_TypedNilMap(t *testing.T) {
	t.Parallel()

	var m map[string]string

	assert.True(t, IsNilValue(m))
}

func TestIsNilValue_NonNilInt(t *testing.T) {
	t.Parallel()

	assert.False(t, IsNilValue(42))
}

func TestIsNilValue_NonNilString(t *testing.T) {
	t.Parallel()

	assert.False(t, IsNilValue("hello"))
}

func TestIsNilValue_NonNilPointer(t *testing.T) {
	t.Parallel()

	s := "hello"

	assert.False(t, IsNilValue(&s))
}

func TestIsNilValue_EmptyStruct(t *testing.T) {
	t.Parallel()

	assert.False(t, IsNilValue(struct{}{}))
}
