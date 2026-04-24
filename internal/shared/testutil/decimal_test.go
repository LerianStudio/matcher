// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package testutil

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestDecimalPtr(t *testing.T) {
	t.Parallel()

	t.Run("returns pointer to value", func(t *testing.T) {
		t.Parallel()

		value := decimal.NewFromFloat(123.45)
		ptr := DecimalPtr(value)

		require.NotNil(t, ptr)
	})

	t.Run("pointer value equals input", func(t *testing.T) {
		t.Parallel()

		value := decimal.NewFromFloat(99.99)
		ptr := DecimalPtr(value)

		require.True(t, ptr.Equal(value))
	})

	t.Run("different values produce different pointers", func(t *testing.T) {
		t.Parallel()

		value1 := decimal.NewFromFloat(100.00)
		value2 := decimal.NewFromFloat(200.00)

		ptr1 := DecimalPtr(value1)
		ptr2 := DecimalPtr(value2)

		require.False(t, ptr1.Equal(*ptr2))
		require.NotSame(t, ptr1, ptr2)
	})

	t.Run("zero value works", func(t *testing.T) {
		t.Parallel()

		value := decimal.Zero
		ptr := DecimalPtr(value)

		require.NotNil(t, ptr)
		require.True(t, ptr.IsZero())
	})

	t.Run("negative value works", func(t *testing.T) {
		t.Parallel()

		value := decimal.NewFromFloat(-50.25)
		ptr := DecimalPtr(value)

		require.NotNil(t, ptr)
		require.True(t, ptr.Equal(value))
		require.True(t, ptr.IsNegative())
	})

	t.Run("large value works", func(t *testing.T) {
		t.Parallel()

		value := decimal.NewFromFloat(9999999999.999999)
		ptr := DecimalPtr(value)

		require.NotNil(t, ptr)
		require.True(t, ptr.Equal(value))
	})

	t.Run("from string works", func(t *testing.T) {
		t.Parallel()

		value, err := decimal.NewFromString("12345.67890")
		require.NoError(t, err)

		ptr := DecimalPtr(value)

		require.NotNil(t, ptr)
		require.True(t, ptr.Equal(value))
	})
}
