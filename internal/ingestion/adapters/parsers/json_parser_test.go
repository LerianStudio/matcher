// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package parsers

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

var errTestCallbackFailed = errors.New("callback failed")

func TestNewJSONParser(t *testing.T) {
	t.Parallel()

	parser := NewJSONParser()
	require.NotNil(t, parser)
}

func TestJSONParser_SupportedFormat(t *testing.T) {
	t.Parallel()

	parser := NewJSONParser()
	assert.Equal(t, "json", parser.SupportedFormat())
}

func TestJSONParser_Parse(t *testing.T) {
	t.Parallel()

	validFieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	tests := []struct {
		name             string
		jsonData         string
		fieldMap         *shared.FieldMap
		setupJob         bool
		nilReader        bool
		nilJob           bool
		wantErr          bool
		wantErrContains  string
		wantTransactions int
		wantParseErrors  int
		validateFn       func(t *testing.T, result *ports.ParseResult)
	}{
		{
			name:             "valid JSON array of objects",
			jsonData:         `[{"id":"tx1","amount":"100.50","currency":"USD","date":"2024-01-15","desc":"Payment received"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 1,
			validateFn: func(t *testing.T, result *ports.ParseResult) {
				t.Helper()

				tx := result.Transactions[0]
				assert.Equal(t, "tx1", tx.ExternalID)
				assert.True(t, tx.Amount.Equal(decimal.NewFromFloat(100.50)))
				assert.Equal(t, "USD", tx.Currency)
				assert.Equal(t, "Payment received", tx.Description)
			},
		},
		{
			name:             "valid single JSON object (not array)",
			jsonData:         `{"id":"single-tx","amount":"50.00","currency":"EUR","date":"2024-02-20","desc":"Single object test"}`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 1,
			validateFn: func(t *testing.T, result *ports.ParseResult) {
				t.Helper()

				tx := result.Transactions[0]
				assert.Equal(t, "single-tx", tx.ExternalID)
				assert.True(t, tx.Amount.Equal(decimal.NewFromFloat(50.00)))
				assert.Equal(t, "EUR", tx.Currency)
				assert.Equal(t, "Single object test", tx.Description)
			},
		},
		{
			name:             "multiple transactions",
			jsonData:         `[{"id":"tx1","amount":"10.00","currency":"USD","date":"2024-01-01","desc":"First"},{"id":"tx2","amount":"20.00","currency":"USD","date":"2024-01-02","desc":"Second"},{"id":"tx3","amount":"30.00","currency":"USD","date":"2024-01-03","desc":"Third"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 3,
		},
		{
			name:            "nil reader returns error",
			nilReader:       true,
			fieldMap:        validFieldMap,
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "reader is required",
		},
		{
			name:            "nil job returns error",
			jsonData:        `[{"id":"tx1","amount":"10.00","currency":"USD","date":"2024-01-01","desc":"Test"}]`,
			fieldMap:        validFieldMap,
			nilJob:          true,
			wantErr:         true,
			wantErrContains: "ingestion job is required",
		},
		{
			name:            "nil fieldMap returns error",
			jsonData:        `[{"id":"tx1","amount":"10.00","currency":"USD","date":"2024-01-01","desc":"Test"}]`,
			fieldMap:        nil,
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "field map is required",
		},
		{
			name:     "invalid fieldMap missing required keys",
			jsonData: `[{"id":"tx1","amount":"10.00","currency":"USD","date":"2024-01-01","desc":"Test"}]`,
			fieldMap: &shared.FieldMap{Mapping: map[string]any{
				"external_id": "id",
			}},
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "missing required mapping key",
		},
		{
			name:     "empty fieldMap mapping",
			jsonData: `[{"id":"tx1","amount":"10.00","currency":"USD","date":"2024-01-01","desc":"Test"}]`,
			fieldMap: &shared.FieldMap{Mapping: map[string]any{}},
			setupJob: true,
			wantErr:  true,
		},
		{
			name:             "empty JSON array returns empty result",
			jsonData:         `[]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 0,
			wantParseErrors:  0,
		},
		{
			name:            "invalid JSON syntax",
			jsonData:        `{"id": "broken",`,
			fieldMap:        validFieldMap,
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "failed to decode json",
		},
		{
			name:            "JSON with trailing comma (invalid syntax)",
			jsonData:        `[{"id":"tx1","amount":"10.00","currency":"USD","date":"2024-01-01",}]`,
			fieldMap:        validFieldMap,
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "failed to decode json",
		},
		{
			name:            "JSON array with non-object elements",
			jsonData:        `["string1", "string2", "string3"]`,
			fieldMap:        validFieldMap,
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "json array must contain objects",
		},
		{
			name:            "JSON array with mixed elements",
			jsonData:        `[{"id":"tx1"}, 123, "string"]`,
			fieldMap:        validFieldMap,
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "json array must contain objects",
		},
		{
			name:            "JSON with non-array non-object root (string)",
			jsonData:        `"just a string"`,
			fieldMap:        validFieldMap,
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "json payload must be an object or array of objects",
		},
		{
			name:            "JSON with non-array non-object root (number)",
			jsonData:        `12345`,
			fieldMap:        validFieldMap,
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "json payload must be an object or array of objects",
		},
		{
			name:            "JSON with non-array non-object root (boolean)",
			jsonData:        `true`,
			fieldMap:        validFieldMap,
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "json payload must be an object or array of objects",
		},
		{
			name:            "JSON with non-array non-object root (null)",
			jsonData:        `null`,
			fieldMap:        validFieldMap,
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "json payload must be an object or array of objects",
		},
		{
			name:             "JSON with numeric values (json.Number handling)",
			jsonData:         `[{"id":"tx-numeric","amount":99.99,"currency":"GBP","date":"2024-03-10","desc":"Numeric amount"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 1,
			validateFn: func(t *testing.T, result *ports.ParseResult) {
				t.Helper()

				tx := result.Transactions[0]
				assert.Equal(t, "tx-numeric", tx.ExternalID)
				assert.True(t, tx.Amount.Equal(decimal.NewFromFloat(99.99)))
			},
		},
		{
			name:             "JSON with integer numeric values",
			jsonData:         `[{"id":12345,"amount":100,"currency":"USD","date":"2024-03-10","desc":"Integer values"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 1,
			validateFn: func(t *testing.T, result *ports.ParseResult) {
				t.Helper()

				tx := result.Transactions[0]
				assert.Equal(t, "12345", tx.ExternalID)
				assert.True(t, tx.Amount.Equal(decimal.NewFromInt(100)))
			},
		},
		{
			name:             "row missing required field records parse error",
			jsonData:         `[{"id":"tx1","currency":"USD","date":"2024-01-01","desc":"Missing amount"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 0,
			wantParseErrors:  1,
		},
		{
			name:             "row with invalid amount records parse error",
			jsonData:         `[{"id":"tx1","amount":"invalid","currency":"USD","date":"2024-01-01","desc":"Bad amount"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 0,
			wantParseErrors:  1,
		},
		{
			name:             "row with invalid date records parse error",
			jsonData:         `[{"id":"tx1","amount":"10.00","currency":"USD","date":"not-a-date","desc":"Bad date"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 0,
			wantParseErrors:  1,
		},
		{
			name:             "multiple parse errors in result",
			jsonData:         `[{"id":"tx1","currency":"USD","date":"2024-01-01"},{"id":"tx2","amount":"bad","currency":"USD","date":"2024-01-01"},{"id":"tx3","amount":"10.00","currency":"USD","date":"invalid-date"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 0,
			wantParseErrors:  3,
		},
		{
			name:             "mixed valid and invalid rows",
			jsonData:         `[{"id":"valid","amount":"50.00","currency":"USD","date":"2024-01-15","desc":"Good"},{"id":"invalid","currency":"USD","date":"2024-01-01"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 1,
			wantParseErrors:  1,
		},
		{
			name:             "JSON with nested objects (top-level access only)",
			jsonData:         `[{"id":"nested-tx","amount":"75.00","currency":"JPY","date":"2024-04-01","desc":"Nested test","metadata":{"key":"value"}}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 1,
			validateFn: func(t *testing.T, result *ports.ParseResult) {
				t.Helper()

				tx := result.Transactions[0]
				assert.Equal(t, "nested-tx", tx.ExternalID)
				assert.Equal(t, "JPY", tx.Currency)
			},
		},
		{
			name:             "date range tracking with multiple dates",
			jsonData:         `[{"id":"tx1","amount":"10.00","currency":"USD","date":"2024-01-15","desc":"First"},{"id":"tx2","amount":"20.00","currency":"USD","date":"2024-01-05","desc":"Earliest"},{"id":"tx3","amount":"30.00","currency":"USD","date":"2024-01-25","desc":"Latest"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 3,
			validateFn: func(t *testing.T, result *ports.ParseResult) {
				t.Helper()

				require.NotNil(t, result.DateRange)

				expectedStart := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)
				expectedEnd := time.Date(2024, 1, 25, 0, 0, 0, 0, time.UTC)

				assert.Equal(t, expectedStart, result.DateRange.Start)
				assert.Equal(t, expectedEnd, result.DateRange.End)
			},
		},
		{
			name:             "date formats RFC3339",
			jsonData:         `[{"id":"tx-rfc3339","amount":"10.00","currency":"USD","date":"2024-06-15T10:30:00Z","desc":"RFC3339 date"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 1,
		},
		{
			name:             "date formats datetime",
			jsonData:         `[{"id":"tx-datetime","amount":"10.00","currency":"USD","date":"2024-06-15 10:30:00","desc":"Datetime format"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 1,
		},
		{
			name:             "currency normalization to uppercase",
			jsonData:         `[{"id":"tx-lower","amount":"10.00","currency":"eur","date":"2024-01-01","desc":"Lowercase currency"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 1,
			validateFn: func(t *testing.T, result *ports.ParseResult) {
				t.Helper()

				assert.Equal(t, "EUR", result.Transactions[0].Currency)
			},
		},
		{
			name:             "whitespace in values is trimmed",
			jsonData:         `[{"id":"  tx-whitespace  ","amount":"  10.00  ","currency":"  USD  ","date":"  2024-01-01  ","desc":"  Trimmed  "}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 1,
			validateFn: func(t *testing.T, result *ports.ParseResult) {
				t.Helper()

				tx := result.Transactions[0]
				assert.Equal(t, "tx-whitespace", tx.ExternalID)
				assert.Equal(t, "Trimmed", tx.Description)
			},
		},
		{
			name:             "empty description is allowed",
			jsonData:         `[{"id":"tx-no-desc","amount":"10.00","currency":"USD","date":"2024-01-01"}]`,
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantTransactions: 1,
			validateFn: func(t *testing.T, result *ports.ParseResult) {
				t.Helper()

				assert.Empty(t, result.Transactions[0].Description)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parser := NewJSONParser()
			ctx := context.Background()

			var job *entities.IngestionJob

			if tt.setupJob && !tt.nilJob {
				var err error

				job, err = entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "test.json", 1000)
				require.NoError(t, err)
			}

			var reader io.Reader

			if !tt.nilReader {
				reader = strings.NewReader(tt.jsonData)
			}

			result, err := parser.Parse(ctx, reader, job, tt.fieldMap)
			if tt.wantErr {
				require.Error(t, err)

				if tt.wantErrContains != "" {
					assert.Contains(t, err.Error(), tt.wantErrContains)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Len(t, result.Transactions, tt.wantTransactions)
			assert.Len(t, result.Errors, tt.wantParseErrors)

			if tt.validateFn != nil {
				tt.validateFn(t, result)
			}
		})
	}
}

func TestJSONParser_Parse_ContextCancellation(t *testing.T) {
	t.Parallel()

	parser := NewJSONParser()
	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	job, err := entities.NewIngestionJob(
		context.Background(),
		uuid.New(),
		uuid.New(),
		"test.json",
		1000,
	)
	require.NoError(t, err)

	jsonData := `[{"id":"tx1","amount":"10.00","currency":"USD","date":"2024-01-01","desc":"Test"}]`

	result, err := parser.Parse(ctx, strings.NewReader(jsonData), job, fieldMap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cancelled")
	assert.Nil(t, result)
}

func TestJSONParser_Parse_LargeJSONArray(t *testing.T) {
	t.Parallel()

	parser := NewJSONParser()
	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	ctx := context.Background()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "large.json", 100000)
	require.NoError(t, err)

	var sb strings.Builder

	sb.WriteString("[")

	numTransactions := 1000

	for i := 0; i < numTransactions; i++ {
		if i > 0 {
			sb.WriteString(",")
		}

		sb.WriteString(`{"id":"tx-`)
		sb.WriteString(string(rune('0' + (i % 10))))
		sb.WriteString(`-`)
		sb.WriteString(string(rune('0' + (i / 10 % 10))))
		sb.WriteString(`-`)
		sb.WriteString(string(rune('0' + (i / 100 % 10))))
		sb.WriteString(`-`)
		sb.WriteString(string(rune('0' + (i / 1000 % 10))))
		sb.WriteString(
			`","amount":"10.00","currency":"USD","date":"2024-01-01","desc":"Transaction"}`,
		)
	}

	sb.WriteString("]")

	result, err := parser.Parse(ctx, strings.NewReader(sb.String()), job, fieldMap)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.Transactions, numTransactions)
	assert.Empty(t, result.Errors)
}

func TestJSONParser_ParseStreaming(t *testing.T) {
	t.Parallel()

	validFieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	t.Run("streams large JSON array in chunks", func(t *testing.T) {
		t.Parallel()

		var jsonBuilder strings.Builder
		jsonBuilder.WriteString("[")

		for i := range 100 {
			if i > 0 {
				jsonBuilder.WriteString(",")
			}

			jsonBuilder.WriteString(`{"id":"tx-`)
			jsonBuilder.WriteString(strconv.Itoa(i))
			jsonBuilder.WriteString(
				`","amount":"10.00","currency":"USD","date":"2024-01-01","desc":"Test"}`,
			)
		}

		jsonBuilder.WriteString("]")

		parser := NewJSONParser()
		job, err := entities.NewIngestionJob(
			context.Background(),
			uuid.New(),
			uuid.New(),
			"stream.json",
			10000,
		)
		require.NoError(t, err)

		reader := strings.NewReader(jsonBuilder.String())

		var chunkCount int

		var totalTransactions int

		result, err := parser.ParseStreaming(
			context.Background(),
			reader,
			job,
			validFieldMap,
			25,
			func(chunk []*shared.Transaction, _ []ports.ParseError) error {
				chunkCount++
				totalTransactions += len(chunk)

				return nil
			},
		)

		require.NoError(t, err)
		assert.Equal(t, 4, chunkCount)
		assert.Equal(t, 100, totalTransactions)
		assert.Equal(t, 100, result.TotalRecords)
	})

	t.Run("callback error stops processing", func(t *testing.T) {
		t.Parallel()

		jsonData := `[{"id":"tx1","amount":"10.00","currency":"USD","date":"2024-01-01"},{"id":"tx2","amount":"20.00","currency":"USD","date":"2024-01-02"}]`
		parser := NewJSONParser()
		job, err := entities.NewIngestionJob(
			context.Background(),
			uuid.New(),
			uuid.New(),
			"callback.json",
			1000,
		)
		require.NoError(t, err)

		reader := strings.NewReader(jsonData)

		_, parseErr := parser.ParseStreaming(
			context.Background(),
			reader,
			job,
			validFieldMap,
			1,
			func(_ []*shared.Transaction, _ []ports.ParseError) error {
				return errTestCallbackFailed
			},
		)

		require.Error(t, parseErr)
		assert.Contains(t, parseErr.Error(), "chunk callback failed")
	})

	t.Run("nil reader returns error", func(t *testing.T) {
		t.Parallel()

		parser := NewJSONParser()
		job, err := entities.NewIngestionJob(
			context.Background(),
			uuid.New(),
			uuid.New(),
			"nil-reader.json",
			1000,
		)
		require.NoError(t, err)

		_, err = parser.ParseStreaming(
			context.Background(),
			nil,
			job,
			validFieldMap,
			10,
			func(_ []*shared.Transaction, _ []ports.ParseError) error {
				return nil
			},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, errReaderRequired)
	})

	t.Run("nil job returns error", func(t *testing.T) {
		t.Parallel()

		parser := NewJSONParser()
		reader := strings.NewReader(`[]`)

		_, err := parser.ParseStreaming(
			context.Background(),
			reader,
			nil,
			validFieldMap,
			10,
			func(_ []*shared.Transaction, _ []ports.ParseError) error {
				return nil
			},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, errMissingIngestionJob)
	})

	t.Run("default chunk size when zero provided", func(t *testing.T) {
		t.Parallel()

		jsonData := `[{"id":"tx1","amount":"10.00","currency":"USD","date":"2024-01-01"}]`
		parser := NewJSONParser()
		job, err := entities.NewIngestionJob(
			context.Background(),
			uuid.New(),
			uuid.New(),
			"default-chunk.json",
			1000,
		)
		require.NoError(t, err)

		reader := strings.NewReader(jsonData)

		result, err := parser.ParseStreaming(
			context.Background(),
			reader,
			job,
			validFieldMap,
			0,
			func(_ []*shared.Transaction, _ []ports.ParseError) error {
				return nil
			},
		)

		require.NoError(t, err)
		assert.Equal(t, 1, result.TotalRecords)
	})

	t.Run("tracks date range across chunks", func(t *testing.T) {
		t.Parallel()

		jsonData := `[
			{"id":"tx1","amount":"10.00","currency":"USD","date":"2024-06-15"},
			{"id":"tx2","amount":"20.00","currency":"USD","date":"2024-01-01"},
			{"id":"tx3","amount":"30.00","currency":"USD","date":"2024-12-31"}
		]`
		parser := NewJSONParser()
		job, err := entities.NewIngestionJob(
			context.Background(),
			uuid.New(),
			uuid.New(),
			"date-range.json",
			1000,
		)
		require.NoError(t, err)

		reader := strings.NewReader(jsonData)

		result, err := parser.ParseStreaming(
			context.Background(),
			reader,
			job,
			validFieldMap,
			1,
			func(_ []*shared.Transaction, _ []ports.ParseError) error {
				return nil
			},
		)

		require.NoError(t, err)
		require.NotNil(t, result.DateRange)
		assert.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), result.DateRange.Start)
		assert.Equal(t, time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC), result.DateRange.End)
	})

	t.Run("collects errors from multiple chunks", func(t *testing.T) {
		t.Parallel()

		jsonData := `[
			{"id":"tx1","currency":"USD","date":"2024-01-01"},
			{"id":"tx2","amount":"10.00","currency":"USD","date":"2024-01-02"},
			{"id":"tx3","currency":"USD","date":"2024-01-03"}
		]`
		parser := NewJSONParser()
		job, err := entities.NewIngestionJob(
			context.Background(),
			uuid.New(),
			uuid.New(),
			"multi-errors.json",
			1000,
		)
		require.NoError(t, err)

		reader := strings.NewReader(jsonData)

		result, err := parser.ParseStreaming(
			context.Background(),
			reader,
			job,
			validFieldMap,
			1,
			func(_ []*shared.Transaction, _ []ports.ParseError) error {
				return nil
			},
		)

		require.NoError(t, err)
		assert.Equal(t, 1, result.TotalRecords)
		assert.Equal(t, 2, result.TotalErrors)
	})

	t.Run("single object streaming", func(t *testing.T) {
		t.Parallel()

		jsonData := `{"id":"single","amount":"99.99","currency":"EUR","date":"2024-07-04","desc":"Single object"}`
		parser := NewJSONParser()
		job, err := entities.NewIngestionJob(
			context.Background(),
			uuid.New(),
			uuid.New(),
			"single-object.json",
			1000,
		)
		require.NoError(t, err)

		reader := strings.NewReader(jsonData)

		var receivedChunks int

		result, err := parser.ParseStreaming(
			context.Background(),
			reader,
			job,
			validFieldMap,
			10,
			func(chunk []*shared.Transaction, _ []ports.ParseError) error {
				receivedChunks++

				assert.Len(t, chunk, 1)
				assert.Equal(t, "single", chunk[0].ExternalID)

				return nil
			},
		)

		require.NoError(t, err)
		assert.Equal(t, 1, receivedChunks)
		assert.Equal(t, 1, result.TotalRecords)
	})
}

var _ = ports.ParseResult{}
