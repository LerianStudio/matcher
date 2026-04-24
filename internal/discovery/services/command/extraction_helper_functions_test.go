// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestBuildTypedFilterConditions_NilFilters(t *testing.T) {
	t.Parallel()

	result := buildTypedFilterConditions(nil)
	assert.Nil(t, result)
}

func TestBuildTypedFilterConditions_EmptyEquals(t *testing.T) {
	t.Parallel()

	result := buildTypedFilterConditions(&sharedPorts.ExtractionFilters{Equals: map[string]string{}})
	assert.Nil(t, result)
}

func TestBuildTypedFilterConditions_SingleEquality(t *testing.T) {
	t.Parallel()

	filters := &sharedPorts.ExtractionFilters{Equals: map[string]string{"currency": "USD"}}
	result := buildTypedFilterConditions(filters)

	require.Contains(t, result, "currency")

	cond, ok := result["currency"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, []any{"USD"}, cond["eq"])
}

func TestBuildTypedFilterConditions_MultipleEquals(t *testing.T) {
	t.Parallel()

	filters := &sharedPorts.ExtractionFilters{Equals: map[string]string{"currency": "USD", "status": "active"}}
	result := buildTypedFilterConditions(filters)

	require.Len(t, result, 2)
	require.Contains(t, result, "currency")
	require.Contains(t, result, "status")

	currCond, ok := result["currency"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, []any{"USD"}, currCond["eq"])

	statCond, ok := result["status"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, []any{"active"}, statCond["eq"])
}

func TestExtractSubmittedColumns_NilTables(t *testing.T) {
	t.Parallel()

	result := extractSubmittedColumns(nil)
	assert.Nil(t, result)
}

func TestExtractSubmittedColumns_ValidTables(t *testing.T) {
	t.Parallel()

	tables := map[string]any{"transactions": map[string]any{"columns": []string{"id", "amount"}}, "accounts": map[string]any{"columns": []any{"id", "name"}}}

	result := extractSubmittedColumns(tables)

	require.Len(t, result, 2)
	assert.Equal(t, []string{"id", "amount"}, result["transactions"])
	assert.Equal(t, []string{"id", "name"}, result["accounts"])
}

func TestExtractSubmittedColumns_SkipsUnparseable(t *testing.T) {
	t.Parallel()

	tables := map[string]any{"transactions": map[string]any{"columns": []string{"id"}}, "bad": "not-a-map"}

	result := extractSubmittedColumns(tables)

	require.Len(t, result, 1)
	assert.Equal(t, []string{"id"}, result["transactions"])
}

func TestTableColumnsEqual_BothNil(t *testing.T) {
	t.Parallel()

	assert.True(t, tableColumnsEqual(nil, nil))
}

func TestTableColumnsEqual_DifferentLengths(t *testing.T) {
	t.Parallel()

	a := map[string][]string{"t1": {"a"}}
	b := map[string][]string{"t1": {"a"}, "t2": {"b"}}

	assert.False(t, tableColumnsEqual(a, b))
}

func TestTableColumnsEqual_MissingTable(t *testing.T) {
	t.Parallel()

	a := map[string][]string{"t1": {"a"}}
	b := map[string][]string{"t2": {"a"}}

	assert.False(t, tableColumnsEqual(a, b))
}

func TestTableColumnsEqual_DifferentColumns(t *testing.T) {
	t.Parallel()

	a := map[string][]string{"t1": {"a", "b"}}
	b := map[string][]string{"t1": {"a", "c"}}

	assert.False(t, tableColumnsEqual(a, b))
}

func TestTableColumnsEqual_ColumnCountMismatch(t *testing.T) {
	t.Parallel()

	a := map[string][]string{"t1": {"a", "b"}}
	b := map[string][]string{"t1": {"a"}}

	assert.False(t, tableColumnsEqual(a, b))
}

func TestTableColumnsEqual_Identical(t *testing.T) {
	t.Parallel()

	a := map[string][]string{"t1": {"a", "b"}, "t2": {"x"}}
	b := map[string][]string{"t1": {"a", "b"}, "t2": {"x"}}

	assert.True(t, tableColumnsEqual(a, b))
}

func TestLogMappedFieldsDivergence_NoEchoData_DoesNotLog(t *testing.T) {
	t.Parallel()

	submitted := map[string][]string{"transactions": {"id"}}
	assert.False(t, logMappedFieldsDivergence(context.Background(), submitted, nil, "job-1"))
}

func TestLogMappedFieldsDivergence_NilSubmitted_DoesNotLog(t *testing.T) {
	t.Parallel()

	returned := map[string]map[string][]string{"cfg": {"t": {"id"}}}
	assert.False(t, logMappedFieldsDivergence(context.Background(), nil, returned, "job-1"))
}

func TestLogMappedFieldsDivergence_NoDivergence_DoesNotLog(t *testing.T) {
	t.Parallel()

	submitted := map[string][]string{"transactions": {"id", "amount"}}
	returned := map[string]map[string][]string{"prod-db": {"transactions": {"id", "amount"}}}

	assert.False(t, logMappedFieldsDivergence(context.Background(), submitted, returned, "job-1"))
}

func TestLogMappedFieldsDivergence_Divergence_Logs(t *testing.T) {
	t.Parallel()

	submitted := map[string][]string{"transactions": {"id", "amount"}}
	returned := map[string]map[string][]string{"prod-db": {"public.transactions": {"id", "amount"}}}

	assert.True(t, logMappedFieldsDivergence(context.Background(), submitted, returned, "job-1"))
}
