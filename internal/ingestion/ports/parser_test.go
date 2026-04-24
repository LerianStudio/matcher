// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestParseResultStruct(t *testing.T) {
	t.Parallel()

	now := time.Now()

	result := ParseResult{
		Transactions: []*shared.Transaction{},
		Errors:       []ParseError{{Row: 1, Field: "amount", Message: "invalid"}},
		DateRange:    &DateRange{Start: now, End: now.Add(time.Hour)},
	}

	assert.NotNil(t, result.Transactions)
	assert.Len(t, result.Errors, 1)
	assert.Equal(t, 1, result.Errors[0].Row)
	assert.Equal(t, "amount", result.Errors[0].Field)
	assert.Equal(t, "invalid", result.Errors[0].Message)
	assert.NotNil(t, result.DateRange)
}

func TestDateRangeStruct(t *testing.T) {
	t.Parallel()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)
	dr := DateRange{Start: start, End: end}

	assert.Equal(t, start, dr.Start)
	assert.Equal(t, end, dr.End)
}

func TestParseErrorStruct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     ParseError
		wantRow int
		wantFld string
		wantMsg string
	}{
		{
			name:    "complete error",
			err:     ParseError{Row: 5, Field: "date", Message: "invalid format"},
			wantRow: 5,
			wantFld: "date",
			wantMsg: "invalid format",
		},
		{
			name:    "error without field",
			err:     ParseError{Row: 10, Message: "row corrupted"},
			wantRow: 10,
			wantFld: "",
			wantMsg: "row corrupted",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, testCase.wantRow, testCase.err.Row)
			assert.Equal(t, testCase.wantFld, testCase.err.Field)
			assert.Equal(t, testCase.wantMsg, testCase.err.Message)
		})
	}
}

func TestParserInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ Parser = (*mockParser)(nil)
}

type mockParser struct {
	format string
}

func (m *mockParser) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ParseResult, error) {
	return &ParseResult{}, nil
}

func (m *mockParser) SupportedFormat() string {
	return m.format
}

func TestMockParserImplementation(t *testing.T) {
	t.Parallel()

	t.Run("Parse returns empty result", func(t *testing.T) {
		t.Parallel()

		parser := &mockParser{format: "csv"}
		ctx := t.Context()

		result, err := parser.Parse(ctx, nil, nil, nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("SupportedFormat returns format", func(t *testing.T) {
		t.Parallel()

		parser := &mockParser{format: "csv"}
		assert.Equal(t, "csv", parser.SupportedFormat())
	})
}

func TestParserRegistryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ ParserRegistry = (*mockParserRegistry)(nil)
}

type mockParserRegistry struct {
	parsers map[string]Parser
}

func (m *mockParserRegistry) GetParser(format string) (Parser, error) {
	p, ok := m.parsers[format]
	if !ok {
		return nil, assert.AnError
	}

	return p, nil
}

func (m *mockParserRegistry) Register(parser Parser) {
	if m.parsers == nil {
		m.parsers = make(map[string]Parser)
	}

	m.parsers[parser.SupportedFormat()] = parser
}

func TestMockParserRegistryImplementation(t *testing.T) {
	t.Parallel()

	t.Run("Register adds parser", func(t *testing.T) {
		t.Parallel()

		registry := &mockParserRegistry{parsers: make(map[string]Parser)}
		csvParser := &mockParser{format: "csv"}

		registry.Register(csvParser)
		assert.Len(t, registry.parsers, 1)
	})

	t.Run("GetParser returns registered parser", func(t *testing.T) {
		t.Parallel()

		registry := &mockParserRegistry{parsers: make(map[string]Parser)}
		csvParser := &mockParser{format: "csv"}
		registry.Register(csvParser)

		p, err := registry.GetParser("csv")
		require.NoError(t, err)
		assert.Equal(t, "csv", p.SupportedFormat())
	})

	t.Run("GetParser returns error for unknown format", func(t *testing.T) {
		t.Parallel()

		registry := &mockParserRegistry{parsers: make(map[string]Parser)}

		_, err := registry.GetParser("xml")
		require.Error(t, err)
	})
}
