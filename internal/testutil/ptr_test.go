//go:build unit

package testutil

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestStringPtr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"simple string", "hello"},
		{"string with spaces", "hello world"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := StringPtr(tt.input)
			assert.NotNil(t, result)
			assert.Equal(t, tt.input, *result)
		})
	}
}

func TestTimePtr(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	result := TimePtr(now)

	assert.NotNil(t, result)
	assert.Equal(t, now, *result)
}

func TestUUIDPtr(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	result := UUIDPtr(id)

	assert.NotNil(t, result)
	assert.Equal(t, id, *result)
}

func TestIntPtr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input int
	}{
		{"zero", 0},
		{"positive", 42},
		{"negative", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := IntPtr(tt.input)
			assert.NotNil(t, result)
			assert.Equal(t, tt.input, *result)
		})
	}
}

func TestDecimalFromInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input int64
	}{
		{"zero", 0},
		{"positive", 50000},
		{"negative", -100},
		{"large value", 1000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := DecimalFromInt(tt.input)
			assert.NotNil(t, result)

			expected := decimal.NewFromInt(tt.input)
			assert.True(t, expected.Equal(*result))
		})
	}
}

func TestPtr(t *testing.T) {
	t.Parallel()

	t.Run("int", func(t *testing.T) {
		t.Parallel()

		result := Ptr(42)
		assert.NotNil(t, result)
		assert.Equal(t, 42, *result)
	})

	t.Run("string", func(t *testing.T) {
		t.Parallel()

		result := Ptr("hello")
		assert.NotNil(t, result)
		assert.Equal(t, "hello", *result)
	})

	t.Run("bool", func(t *testing.T) {
		t.Parallel()

		result := Ptr(true)
		assert.NotNil(t, result)
		assert.Equal(t, true, *result)
	})

	t.Run("custom type", func(t *testing.T) {
		t.Parallel()

		type myType string

		val := myType("test")
		result := Ptr(val)
		assert.NotNil(t, result)
		assert.Equal(t, val, *result)
	})
}

func TestMustDeterministicUUID(t *testing.T) {
	t.Parallel()

	t.Run("same seed produces same UUID", func(t *testing.T) {
		t.Parallel()

		uuid1 := MustDeterministicUUID(t, "test-seed")
		uuid2 := MustDeterministicUUID(t, "test-seed")
		assert.Equal(t, uuid1, uuid2)
	})

	t.Run("different seeds produce different UUIDs", func(t *testing.T) {
		t.Parallel()

		uuid1 := MustDeterministicUUID(t, "seed1")
		uuid2 := MustDeterministicUUID(t, "seed2")
		assert.NotEqual(t, uuid1, uuid2)
	})

	t.Run("returns valid UUID format", func(t *testing.T) {
		t.Parallel()

		result := MustDeterministicUUID(t, "any-seed")
		assert.NotEqual(t, uuid.Nil, result)
	})
}
