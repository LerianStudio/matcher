// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientExtraction_ParseOptionalRFC3339(t *testing.T) {
	t.Parallel()

	parsed := parseOptionalRFC3339("2026-04-15T20:00:00Z")

	assert.Equal(t, time.Date(2026, 4, 15, 20, 0, 0, 0, time.UTC), parsed)
	assert.True(t, parseOptionalRFC3339("").IsZero())
	assert.True(t, parseOptionalRFC3339("not-a-date").IsZero())
}

func TestClientExtraction_ParseOptionalRFC3339Ptr(t *testing.T) {
	t.Parallel()

	parsed := parseOptionalRFC3339Ptr("2026-04-15T20:00:00Z")

	require.NotNil(t, parsed)
	assert.Equal(t, time.Date(2026, 4, 15, 20, 0, 0, 0, time.UTC), *parsed)
	assert.Nil(t, parseOptionalRFC3339Ptr(""))
	assert.Nil(t, parseOptionalRFC3339Ptr("not-a-date"))
}

func TestClientExtraction_ConvertPortFiltersToTypedFilters_ScalarFallback(t *testing.T) {
	t.Parallel()

	converted := convertPortFiltersToTypedFilters(map[string]map[string]map[string]any{
		"prod-db": {
			"transactions": {
				"currency": "USD",
			},
		},
	})

	require.Contains(t, converted, "prod-db")
	require.Contains(t, converted["prod-db"], "transactions")
	assert.Equal(t, []any{"USD"}, converted["prod-db"]["transactions"]["currency"].Eq)
}
