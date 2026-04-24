// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package shared_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestSourceType_String(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		input    shared.SourceType
		expected string
	}{
		{name: "ledger", input: shared.SourceTypeLedger, expected: "LEDGER"},
		{name: "bank", input: shared.SourceTypeBank, expected: "BANK"},
		{name: "gateway", input: shared.SourceTypeGateway, expected: "GATEWAY"},
		{name: "custom", input: shared.SourceTypeCustom, expected: "CUSTOM"},
		{name: "fetcher", input: shared.SourceTypeFetcher, expected: "FETCHER"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expected, tc.input.String())
		})
	}
}

func TestSourceType_Valid(t *testing.T) {
	t.Parallel()

	validTypes := []shared.SourceType{
		shared.SourceTypeLedger,
		shared.SourceTypeBank,
		shared.SourceTypeGateway,
		shared.SourceTypeCustom,
		shared.SourceTypeFetcher,
	}

	for _, st := range validTypes {
		t.Run(st.String(), func(t *testing.T) {
			t.Parallel()

			assert.True(t, st.Valid())
			assert.True(t, st.IsValid())
		})
	}

	t.Run("empty is invalid", func(t *testing.T) {
		t.Parallel()

		var st shared.SourceType
		assert.False(t, st.Valid())
	})

	t.Run("unknown value is invalid", func(t *testing.T) {
		t.Parallel()

		st := shared.SourceType("UNKNOWN")
		assert.False(t, st.Valid())
	})

	t.Run("lowercase value is invalid", func(t *testing.T) {
		t.Parallel()

		// Valid() does not normalize — only ParseSourceType does.
		st := shared.SourceType("ledger")
		assert.False(t, st.Valid())
	})
}

func TestParseSourceType(t *testing.T) {
	t.Parallel()

	t.Run("parses all uppercase values", func(t *testing.T) {
		t.Parallel()

		cases := map[string]shared.SourceType{
			"LEDGER":  shared.SourceTypeLedger,
			"BANK":    shared.SourceTypeBank,
			"GATEWAY": shared.SourceTypeGateway,
			"CUSTOM":  shared.SourceTypeCustom,
			"FETCHER": shared.SourceTypeFetcher,
		}

		for input, expected := range cases {
			got, err := shared.ParseSourceType(input)
			require.NoError(t, err)
			assert.Equal(t, expected, got)
		}
	})

	t.Run("normalizes lowercase to uppercase", func(t *testing.T) {
		t.Parallel()

		got, err := shared.ParseSourceType("ledger")
		require.NoError(t, err)
		assert.Equal(t, shared.SourceTypeLedger, got)
	})

	t.Run("normalizes mixed case", func(t *testing.T) {
		t.Parallel()

		got, err := shared.ParseSourceType("Gateway")
		require.NoError(t, err)
		assert.Equal(t, shared.SourceTypeGateway, got)
	})

	t.Run("rejects unknown input", func(t *testing.T) {
		t.Parallel()

		_, err := shared.ParseSourceType("NOT_A_TYPE")
		require.Error(t, err)
		assert.True(t, errors.Is(err, shared.ErrInvalidSourceType))
	})
}
