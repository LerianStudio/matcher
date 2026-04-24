// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestStartExtractionRequest_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	req := StartExtractionRequest{
		Tables: map[string]ExtractionTableRequest{
			"transactions": {Columns: []string{"id", "amount", "date"}},
		},
		StartDate: "2025-01-01",
		EndDate:   "2025-12-31",
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded StartExtractionRequest
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, req.Tables["transactions"].Columns, decoded.Tables["transactions"].Columns)
	assert.Equal(t, req.StartDate, decoded.StartDate)
	assert.Equal(t, req.EndDate, decoded.EndDate)
}

func TestStartExtractionRequest_WithFilters_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	filters := &sharedPorts.ExtractionFilters{}

	req := StartExtractionRequest{
		Tables: map[string]ExtractionTableRequest{
			"payments": {Columns: []string{"id"}},
		},
		Filters: filters,
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded StartExtractionRequest
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.NotNil(t, decoded.Filters)
}

func TestExtractionTableRequest_EmptyColumns_OmittedInJSON(t *testing.T) {
	t.Parallel()

	req := ExtractionTableRequest{}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	assert.Equal(t, "{}", string(data))
}
