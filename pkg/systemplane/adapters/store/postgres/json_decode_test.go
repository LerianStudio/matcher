// Copyright 2025 Lerian Studio.

//go:build unit

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeJSONValue_IntegerPreservedAsInt(t *testing.T) {
	t.Parallel()

	value, err := decodeJSONValue([]byte(`1000`))

	require.NoError(t, err)
	assert.IsType(t, int(0), value)
	assert.Equal(t, 1000, value)
}

func TestDecodeJSONValue_FloatPreservedAsFloat64(t *testing.T) {
	t.Parallel()

	value, err := decodeJSONValue([]byte(`12.5`))

	require.NoError(t, err)
	assert.IsType(t, float64(0), value)
	assert.Equal(t, 12.5, value)
}

func TestDecodeJSONValue_NestedStructures(t *testing.T) {
	t.Parallel()

	value, err := decodeJSONValue([]byte(`{"n":5,"arr":[1,2.5]}`))

	require.NoError(t, err)

	obj, ok := value.(map[string]any)
	require.True(t, ok)

	assert.Equal(t, 5, obj["n"])
	arr, ok := obj["arr"].([]any)
	require.True(t, ok)
	assert.Equal(t, 1, arr[0])
	assert.Equal(t, 2.5, arr[1])
}

func TestDecodeJSONValue_NilRaw(t *testing.T) {
	t.Parallel()

	value, err := decodeJSONValue(nil)

	require.ErrorIs(t, err, errNilJSONPayload)
	assert.Nil(t, value)
}
