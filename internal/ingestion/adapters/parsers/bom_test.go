// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package parsers

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestStripBOM_WithBOM(t *testing.T) {
	t.Parallel()

	bom := []byte{0xEF, 0xBB, 0xBF}
	content := []byte("id,amount,currency")
	input := append(bom, content...)

	result, err := StripBOM(bytes.NewReader(input))
	require.NoError(t, err)

	output, err := io.ReadAll(result)
	require.NoError(t, err)
	assert.Equal(t, string(content), string(output))
}

func TestStripBOM_WithoutBOM(t *testing.T) {
	t.Parallel()

	content := "id,amount,currency"

	result, err := StripBOM(strings.NewReader(content))
	require.NoError(t, err)

	output, err := io.ReadAll(result)
	require.NoError(t, err)
	assert.Equal(t, content, string(output))
}

func TestStripBOM_EmptyReader(t *testing.T) {
	t.Parallel()

	result, err := StripBOM(strings.NewReader(""))
	require.NoError(t, err)

	output, err := io.ReadAll(result)
	require.NoError(t, err)
	assert.Empty(t, output)
}

func TestStripBOM_ShorterThanBOM_OneByte(t *testing.T) {
	t.Parallel()

	content := "a"

	result, err := StripBOM(strings.NewReader(content))
	require.NoError(t, err)

	output, err := io.ReadAll(result)
	require.NoError(t, err)
	assert.Equal(t, content, string(output))
}

func TestStripBOM_ShorterThanBOM_TwoBytes(t *testing.T) {
	t.Parallel()

	content := "ab"

	result, err := StripBOM(strings.NewReader(content))
	require.NoError(t, err)

	output, err := io.ReadAll(result)
	require.NoError(t, err)
	assert.Equal(t, content, string(output))
}

func TestStripBOM_OnlyBOMBytes(t *testing.T) {
	t.Parallel()

	bom := []byte{0xEF, 0xBB, 0xBF}

	result, err := StripBOM(bytes.NewReader(bom))
	require.NoError(t, err)

	output, err := io.ReadAll(result)
	require.NoError(t, err)
	assert.Empty(t, output)
}

func TestStripBOM_PartialBOMMatch(t *testing.T) {
	t.Parallel()

	// First two bytes match BOM but third does not
	content := []byte{0xEF, 0xBB, 0x00, 0x41, 0x42}

	result, err := StripBOM(bytes.NewReader(content))
	require.NoError(t, err)

	output, err := io.ReadAll(result)
	require.NoError(t, err)
	assert.Equal(t, content, output)
}

func TestCSVParser_Parse_WithBOM(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "test.csv", 1000)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	// Simulate a CSV file with UTF-8 BOM prefix (common from Excel on Windows)
	bom := "\xEF\xBB\xBF"
	csvData := bom + "id,amount,currency,date,desc\n1,100.50,USD,2024-01-15,Payment received\n"

	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)

	tx := result.Transactions[0]
	assert.Equal(t, "1", tx.ExternalID)
	assert.Equal(t, "USD", tx.Currency)
	assert.Equal(t, "Payment received", tx.Description)
}

func TestCSVParser_Parse_WithoutBOM(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "test.csv", 1000)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	csvData := "id,amount,currency,date,desc\n1,100.50,USD,2024-01-15,Payment received\n"

	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)

	tx := result.Transactions[0]
	assert.Equal(t, "1", tx.ExternalID)
	assert.Equal(t, "USD", tx.Currency)
	assert.Equal(t, "Payment received", tx.Description)
}

func TestCSVParser_Parse_BOMWithMultipleRows(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "test.csv", 1000)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	bom := "\xEF\xBB\xBF"
	csvData := bom + "id,amount,currency,date,desc\n1,10.00,USD,2024-01-01,First\n2,20.00,EUR,2024-01-02,Second\n3,30.00,GBP,2024-01-03,Third\n"

	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 3)

	assert.Equal(t, "1", result.Transactions[0].ExternalID)
	assert.Equal(t, "2", result.Transactions[1].ExternalID)
	assert.Equal(t, "3", result.Transactions[2].ExternalID)
}
