// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package query

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreviewFile_Formats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		input            string
		format           string
		maxRows          int
		expectedCols     []string // nil to skip column assertions
		checkContains    bool     // true = Contains per column, false = Equal on slice
		expectedRows     int      // asserted via Len(SampleRows)
		checkRowCount    bool     // also assert result.RowCount == expectedRows
		expectedFirstRow []string // nil to skip first-row value assertion
	}{
		{
			name:             "CSV format",
			input:            "id,amount,currency,date\n1,100.50,USD,2025-01-01\n2,200.00,EUR,2025-01-02\n3,300.75,GBP,2025-01-03\n",
			format:           "csv",
			maxRows:          5,
			expectedCols:     []string{"id", "amount", "currency", "date"},
			expectedRows:     3,
			checkRowCount:    true,
			expectedFirstRow: []string{"1", "100.50", "USD", "2025-01-01"},
		},
		{
			name:          "CSV max rows",
			input:         "col1,col2\na,b\nc,d\ne,f\ng,h\ni,j\n",
			format:        "csv",
			maxRows:       2,
			expectedRows:  2,
			checkRowCount: true,
		},
		{
			name:             "JSON array sorted columns",
			input:            `[{"id":"1","amount":"100.50","currency":"USD"},{"id":"2","amount":"200","currency":"EUR"}]`,
			format:           "json",
			maxRows:          5,
			expectedCols:     []string{"amount", "currency", "id"},
			expectedRows:     2,
			checkRowCount:    true,
			expectedFirstRow: []string{"100.50", "USD", "1"},
		},
		{
			name:             "JSON single object sorted columns",
			input:            `{"id":"1","amount":"100.50","currency":"USD"}`,
			format:           "json",
			maxRows:          5,
			expectedCols:     []string{"amount", "currency", "id"},
			expectedRows:     1,
			checkRowCount:    true,
			expectedFirstRow: []string{"100.50", "USD", "1"},
		},
		{
			name:          "XML format",
			input:         `<transactions><transaction><id>1</id><amount>100.50</amount><currency>USD</currency></transaction><transaction><id>2</id><amount>200</amount><currency>EUR</currency></transaction></transactions>`,
			format:        "xml",
			maxRows:       5,
			expectedCols:  []string{"id", "amount", "currency"},
			checkContains: true,
			expectedRows:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uc := &UseCase{}
			reader := strings.NewReader(tc.input)

			result, err := uc.PreviewFile(context.Background(), reader, tc.format, tc.maxRows)
			require.NoError(t, err)
			require.NotNil(t, result)

			assert.Equal(t, tc.format, result.Format)
			assert.Len(t, result.SampleRows, tc.expectedRows)

			if tc.checkRowCount {
				assert.Equal(t, tc.expectedRows, result.RowCount)
			}

			if tc.expectedCols != nil {
				if tc.checkContains {
					for _, col := range tc.expectedCols {
						assert.Contains(t, result.Columns, col)
					}
				} else {
					assert.Equal(t, tc.expectedCols, result.Columns)
				}
			}

			if tc.expectedFirstRow != nil {
				require.NotEmpty(t, result.SampleRows)
				assert.Equal(t, tc.expectedFirstRow, result.SampleRows[0])
			}
		})
	}
}

func TestPreviewFile_CSVWithBOM(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	// UTF-8 BOM prefix followed by normal CSV content.
	bom := "\xEF\xBB\xBF"
	input := bom + "id,amount,currency\n1,100.50,USD\n"

	result, err := uc.PreviewFile(context.Background(), strings.NewReader(input), "csv", 5)
	require.NoError(t, err)
	require.NotNil(t, result)

	// The BOM must be stripped so the first column is "id", not "\xEF\xBB\xBFid".
	assert.Equal(t, []string{"id", "amount", "currency"}, result.Columns)
	require.Len(t, result.SampleRows, 1)
	assert.Equal(t, []string{"1", "100.50", "USD"}, result.SampleRows[0])
}

func TestPreviewFile_EmptyFile(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	reader := strings.NewReader("")

	_, err := uc.PreviewFile(context.Background(), reader, "csv", 5)
	assert.ErrorIs(t, err, ErrPreviewEmptyFile)
}

func TestPreviewFile_NilReader(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	_, err := uc.PreviewFile(context.Background(), nil, "csv", 5)
	assert.ErrorIs(t, err, ErrPreviewReaderRequired)
}

func TestPreviewFile_InvalidFormat(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	reader := strings.NewReader("data")

	_, err := uc.PreviewFile(context.Background(), reader, "xlsx", 5)
	assert.ErrorIs(t, err, ErrPreviewInvalidFormat)
}

func TestPreviewFile_EmptyFormat(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	reader := strings.NewReader("data")

	_, err := uc.PreviewFile(context.Background(), reader, "", 5)
	assert.ErrorIs(t, err, ErrPreviewFormatRequired)
}

func TestPreviewFile_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	reader := strings.NewReader("data")

	_, err := uc.PreviewFile(context.Background(), reader, "csv", 5)
	assert.ErrorIs(t, err, ErrNilUseCase)
}

func TestPreviewFile_MaxRowsClamped(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	// 25 rows of data
	var b strings.Builder
	b.WriteString("col\n")

	for i := range 25 {
		b.WriteString("row")
		b.WriteString(strings.Repeat("x", i))
		b.WriteString("\n")
	}

	reader := strings.NewReader(b.String())

	result, err := uc.PreviewFile(context.Background(), reader, "csv", 100)
	require.NoError(t, err)

	// Should be clamped to maxPreviewMaxRows (20)
	assert.LessOrEqual(t, result.RowCount, 20)
}

func TestPreviewFile_ExactlyMaxPreviewRows(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	// Build CSV with exactly 20 data rows
	var sb strings.Builder

	sb.WriteString("id,amount\n")

	for i := range 20 {
		sb.WriteString(fmt.Sprintf("%d,100.00\n", i+1))
	}

	result, err := uc.PreviewFile(context.Background(), strings.NewReader(sb.String()), "csv", 100)
	assert.NoError(t, err)
	assert.Equal(t, 20, result.RowCount)
}
