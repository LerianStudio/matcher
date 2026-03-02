//go:build unit

package parsers

import (
	"encoding/json"
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

func TestNormalizeTransactionRequiresMappings(t *testing.T) {
	t.Parallel()

	_, err := mappingFromFieldMap(nil)
	require.Error(t, err)

	_, err = mappingFromFieldMap(&shared.FieldMap{Mapping: map[string]any{}})
	require.Error(t, err)

	_, err = mappingFromFieldMap(&shared.FieldMap{Mapping: map[string]any{"external_id": "id"}})
	require.Error(t, err)
}

func TestNormalizeTransactionSuccess(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	csvData := "id,amount,currency,date,desc\n1,10.00,USD,2024-01-01,payment\n"

	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.Empty(t, result.Errors)

	transaction := result.Transactions[0]
	require.Equal(t, "1", transaction.ExternalID)
	require.Equal(t, "USD", transaction.Currency)
	require.True(t, transaction.Amount.Equal(decimal.NewFromFloat(10.00)))
	require.Equal(t, "payment", transaction.Description)
}

func TestNormalizeTransactionRejectsInvalidCurrency(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
	}}

	csvData := "id,amount,currency,date\n1,10.00,INVALID_CURRENCY,2024-01-01\n"

	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Empty(t, result.Transactions)
	require.Len(t, result.Errors, 1)
	require.Equal(t, "currency", result.Errors[0].Field)
	require.Contains(t, result.Errors[0].Message, "ISO 4217")
}

func TestNormalizeTransactionCurrencyValidation(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
	}}

	t.Run("valid ISO 4217 code accepted", func(t *testing.T) {
		t.Parallel()

		validCSV := "id,amount,currency,date\n1,10.00,USD,2024-01-01\n"
		result, err := parser.Parse(ctx, strings.NewReader(validCSV), job, fieldMap)
		require.NoError(t, err)
		require.Len(t, result.Transactions, 1)
		require.Empty(t, result.Errors)
	})

	t.Run("lowercase converted to uppercase", func(t *testing.T) {
		t.Parallel()

		lowerCSV := "id,amount,currency,date\n1,10.00,usd,2024-01-01\n"
		result, err := parser.Parse(ctx, strings.NewReader(lowerCSV), job, fieldMap)
		require.NoError(t, err)
		require.Len(t, result.Transactions, 1)
		require.Empty(t, result.Errors)
		require.Equal(t, "USD", result.Transactions[0].Currency)
	})

	t.Run("too long currency rejected", func(t *testing.T) {
		t.Parallel()

		invalidCSV := "id,amount,currency,date\n1,10.00,USDD,2024-01-01\n"
		result, err := parser.Parse(ctx, strings.NewReader(invalidCSV), job, fieldMap)
		require.NoError(t, err)
		require.Empty(t, result.Transactions)
		require.Len(t, result.Errors, 1)
		require.Equal(t, "currency", result.Errors[0].Field)
	})

	t.Run("non-ISO code rejected", func(t *testing.T) {
		t.Parallel()

		invalidCSV := "id,amount,currency,date\n1,10.00,ABC,2024-01-01\n"
		result, err := parser.Parse(ctx, strings.NewReader(invalidCSV), job, fieldMap)
		require.NoError(t, err)
		require.Empty(t, result.Transactions)
		require.Len(t, result.Errors, 1)
		require.Equal(t, "currency", result.Errors[0].Field)
		require.Contains(t, result.Errors[0].Message, "ISO 4217")
	})
}

func TestJSONParser(t *testing.T) {
	t.Parallel()

	parser := NewJSONParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.json", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	jsonData := `[{"id":"1","amount":"5.00","currency":"USD","date":"2024-01-02","desc":"fee"}]`

	result, err := parser.Parse(ctx, strings.NewReader(jsonData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.Empty(t, result.Errors)

	transaction := result.Transactions[0]
	require.Equal(t, "1", transaction.ExternalID)
	require.Equal(t, "USD", transaction.Currency)
	require.True(t, transaction.Amount.Equal(decimal.NewFromFloat(5.00)))
	require.Equal(t, "fee", transaction.Description)

	emptyResult, err := parser.Parse(ctx, strings.NewReader(`[]`), job, fieldMap)
	require.NoError(t, err)
	require.Empty(t, emptyResult.Transactions)
	require.Empty(t, emptyResult.Errors)

	_, err = parser.Parse(ctx, strings.NewReader(`invalid`), job, fieldMap)
	require.Error(t, err)

	_, err = parser.Parse(
		ctx,
		strings.NewReader(`[{"id":"1","amount":"5.00","currency":"USD","date":"2024-01-02",}]`),
		job,
		fieldMap,
	)
	require.Error(t, err)

	partialData := `[{"id":"1","currency":"USD","date":"2024-01-02"}]`

	partialResult, err := parser.Parse(ctx, strings.NewReader(partialData), job, fieldMap)
	require.NoError(t, err)
	require.Empty(t, partialResult.Transactions)
	require.Len(t, partialResult.Errors, 1)
}

func TestXMLParser(t *testing.T) {
	t.Parallel()

	parser := NewXMLParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.xml", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "external_id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}}

	xmlData := `<transactions><transaction><external_id>1</external_id><amount>9.99</amount><currency>USD</currency><date>2024-01-03</date><description>charge</description></transaction></transactions>`

	result, err := parser.Parse(ctx, strings.NewReader(xmlData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.Empty(t, result.Errors)

	transaction := result.Transactions[0]
	require.Equal(t, "1", transaction.ExternalID)
	require.Equal(t, "USD", transaction.Currency)
	require.True(t, transaction.Amount.Equal(decimal.NewFromFloat(9.99)))
	require.Equal(t, "charge", transaction.Description)

	emptyResult, err := parser.Parse(
		ctx,
		strings.NewReader(`<transactions></transactions>`),
		job,
		fieldMap,
	)
	require.NoError(t, err)
	require.Empty(t, emptyResult.Transactions)
	require.Empty(t, emptyResult.Errors)

	_, err = parser.Parse(
		ctx,
		strings.NewReader(`<transactions><transaction><external_id>1</external_id></transactions>`),
		job,
		fieldMap,
	)
	require.Error(t, err)

	_, err = parser.Parse(
		ctx,
		strings.NewReader(
			`<transactions><transaction><external_id>1</external_id><amount>9.99</amount></transaction>`,
		),
		job,
		fieldMap,
	)
	require.Error(t, err)

	partialData := `<transactions><transaction><external_id>1</external_id><currency>USD</currency><date>2024-01-03</date></transaction></transactions>`

	partialResult, err := parser.Parse(ctx, strings.NewReader(partialData), job, fieldMap)
	require.NoError(t, err)
	require.Empty(t, partialResult.Transactions)
	require.Len(t, partialResult.Errors, 1)
}

func TestParseTimeFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected time.Time
		wantErr  bool
	}{
		{
			name:     "RFC3339",
			input:    "2024-01-15T10:30:00Z",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "RFC3339 with timezone",
			input:    "2024-01-15T10:30:00+05:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.FixedZone("", 5*3600)),
		},
		{
			name:     "RFC3339Nano",
			input:    "2024-01-15T10:30:00.123456789Z",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC),
		},
		{
			name:     "ISO date with T separator",
			input:    "2024-01-15T10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "ISO datetime with space",
			input:    "2024-01-15 10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "ISO date only",
			input:    "2024-01-15",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "slash datetime",
			input:    "2024/01/15 10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "slash date only",
			input:    "2024/01/15",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "compact datetime",
			input:    "20240115103000",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "compact date only",
			input:    "20240115",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "DD-Mon-YYYY datetime",
			input:    "15-Jan-2024 10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "DD-Mon-YYYY date only",
			input:    "15-Jan-2024",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Mon D, YYYY datetime",
			input:    "Jan 15, 2024 10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "Mon D, YYYY date only",
			input:    "Jan 15, 2024",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "full month name datetime",
			input:    "January 15, 2024 10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "full month name date only",
			input:    "January 15, 2024",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "D Mon YYYY datetime",
			input:    "15 Jan 2024 10:30:00",
			expected: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name:     "D Mon YYYY date only",
			input:    "15 Jan 2024",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Unix timestamp seconds",
			input:    "1705312200",
			expected: time.Date(2024, 1, 15, 9, 50, 0, 0, time.UTC),
		},
		{
			name:     "Unix timestamp milliseconds",
			input:    "1705312200000",
			expected: time.Date(2024, 1, 15, 9, 50, 0, 0, time.UTC),
		},
		{
			name:     "whitespace trimmed",
			input:    "  2024-01-15  ",
			expected: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid format",
			input:   "not-a-date",
			wantErr: true,
		},
		{
			name:    "ambiguous US format rejected",
			input:   "01/15/2024",
			wantErr: true,
		},
		{
			name:    "ambiguous EU format rejected",
			input:   "15/01/2024",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := parseTime(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.True(t, result.Equal(tt.expected), "expected %v, got %v", tt.expected, result)
		})
	}
}

func TestParseUnixTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected time.Time
		ok       bool
	}{
		{
			name:     "valid seconds",
			input:    "1705312200",
			expected: time.Date(2024, 1, 15, 9, 50, 0, 0, time.UTC),
			ok:       true,
		},
		{
			name:     "valid milliseconds",
			input:    "1705312200000",
			expected: time.Date(2024, 1, 15, 9, 50, 0, 0, time.UTC),
			ok:       true,
		},
		{
			name:  "too short",
			input: "123456789",
			ok:    false,
		},
		{
			name:  "too long",
			input: "12345678901234",
			ok:    false,
		},
		{
			name:  "contains non-digits",
			input: "170531220a",
			ok:    false,
		},
		{
			name:  "11 digits rejected",
			input: "17053122000",
			ok:    false,
		},
		{
			name:  "12 digits rejected",
			input: "170531220000",
			ok:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, ok := parseUnixTimestamp(tt.input)
			require.Equal(t, tt.ok, ok)

			if tt.ok {
				require.True(
					t,
					result.Equal(tt.expected),
					"expected %v, got %v",
					tt.expected,
					result,
				)
			}
		})
	}
}

func TestUpdateDateRange(t *testing.T) {
	t.Parallel()

	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)
	dateRange := &ports.DateRange{Start: start, End: end}

	newStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dateRange = updateDateRange(dateRange, newStart)
	require.Equal(t, newStart, dateRange.Start)
	require.Equal(t, end, dateRange.End)

	newEnd := time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC)
	dateRange = updateDateRange(dateRange, newEnd)
	require.Equal(t, newStart, dateRange.Start)
	require.Equal(t, newEnd, dateRange.End)

	initial := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	rangeFromNil := updateDateRange(nil, initial)
	require.Equal(t, initial, rangeFromNil.Start)
	require.Equal(t, initial, rangeFromNil.End)
}

func TestBuildMappedFieldSet(t *testing.T) {
	t.Parallel()

	t.Run("includes all mapped raw field names", func(t *testing.T) {
		t.Parallel()

		mapping := map[string]string{
			"external_id": "txn_id",
			"amount":      "txn_amount",
			"currency":    "ccy",
			"date":        "txn_date",
			"description": "memo",
		}

		result := buildMappedFieldSet(mapping)

		require.True(t, result["txn_id"])
		require.True(t, result["txn_amount"])
		require.True(t, result["ccy"])
		require.True(t, result["txn_date"])
		require.True(t, result["memo"])
		require.Len(t, result, 5)
	})

	t.Run("skips empty raw field names", func(t *testing.T) {
		t.Parallel()

		mapping := map[string]string{
			"external_id": "txn_id",
			"amount":      "",
		}

		result := buildMappedFieldSet(mapping)

		require.True(t, result["txn_id"])
		require.False(t, result[""])
		require.Len(t, result, 1)
	})

	t.Run("empty mapping returns empty set", func(t *testing.T) {
		t.Parallel()

		result := buildMappedFieldSet(map[string]string{})
		require.Empty(t, result)
	})
}

func TestBuildMetadataExcludesMappedFields(t *testing.T) {
	t.Parallel()

	t.Run("excludes mapped fields from metadata", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"id":           "TXN-001",
			"amount":       "100.50",
			"currency":     "USD",
			"date":         "2024-01-15",
			"description":  "Wire transfer",
			"extra_field":  "should be kept",
			"another_note": "also kept",
		}
		mappedFields := map[string]bool{
			"id":          true,
			"amount":      true,
			"currency":    true,
			"date":        true,
			"description": true,
		}

		metadata := buildMetadata(row, mappedFields)

		require.Len(t, metadata, 2)
		require.Equal(t, "should be kept", metadata["extra_field"])
		require.Equal(t, "also kept", metadata["another_note"])
		require.NotContains(t, metadata, "id")
		require.NotContains(t, metadata, "amount")
		require.NotContains(t, metadata, "currency")
		require.NotContains(t, metadata, "date")
		require.NotContains(t, metadata, "description")
	})

	t.Run("nil row returns empty map", func(t *testing.T) {
		t.Parallel()

		metadata := buildMetadata(nil, map[string]bool{"id": true})
		require.Empty(t, metadata)
		require.NotNil(t, metadata)
	})

	t.Run("nil mapped fields keeps all row data", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"id":     "TXN-001",
			"amount": "50.00",
			"extra":  "value",
		}

		metadata := buildMetadata(row, nil)

		require.Len(t, metadata, 3)
		require.Equal(t, "TXN-001", metadata["id"])
		require.Equal(t, "50.00", metadata["amount"])
		require.Equal(t, "value", metadata["extra"])
	})

	t.Run("empty mapped fields keeps all row data", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"id":     "TXN-001",
			"amount": "50.00",
		}

		metadata := buildMetadata(row, map[string]bool{})

		require.Len(t, metadata, 2)
		require.Equal(t, "TXN-001", metadata["id"])
		require.Equal(t, "50.00", metadata["amount"])
	})

	t.Run("all fields mapped results in empty metadata", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"id":       "TXN-001",
			"amount":   "100.00",
			"currency": "EUR",
		}
		mappedFields := map[string]bool{
			"id":       true,
			"amount":   true,
			"currency": true,
		}

		metadata := buildMetadata(row, mappedFields)

		require.Empty(t, metadata)
		require.NotNil(t, metadata)
	})
}

func TestNormalizeTransactionMetadataExcludesMappedFields(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	csvData := "id,amount,currency,date,desc,extra_info,notes\n1,10.00,USD,2024-01-01,payment,some_extra,some_notes\n"

	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.Empty(t, result.Errors)

	transaction := result.Transactions[0]

	// Mapped fields must NOT appear in metadata (they are in dedicated columns)
	require.NotContains(t, transaction.Metadata, "id")
	require.NotContains(t, transaction.Metadata, "amount")
	require.NotContains(t, transaction.Metadata, "currency")
	require.NotContains(t, transaction.Metadata, "date")
	require.NotContains(t, transaction.Metadata, "desc")

	// Unmapped fields MUST appear in metadata
	require.Equal(t, "some_extra", transaction.Metadata["extra_info"])
	require.Equal(t, "some_notes", transaction.Metadata["notes"])
}

func TestNormalizeTransactionMetadataNoExtraFields(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
	}}

	// CSV with only mapped columns and no extras
	csvData := "id,amount,currency,date\n1,10.00,USD,2024-01-01\n"

	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)

	transaction := result.Transactions[0]

	// All fields are mapped, so metadata should be empty
	require.Empty(t, transaction.Metadata)
}

func TestSanitizeFormulaInjection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"equals prefix sanitized", "=HYPERLINK(\"http://evil.com\",\"Click\")", "'=HYPERLINK(\"http://evil.com\",\"Click\")"},
		{"plus formula sanitized", "+cmd|'/c calc'!A0", "'+cmd|'/c calc'!A0"},
		{"minus formula sanitized", "-cmd|'/c calc'!A0", "'-cmd|'/c calc'!A0"},
		{"at symbol sanitized", "@SUM(1+1)", "'@SUM(1+1)"},
		{"tab prefix sanitized", "\tdata", "'\tdata"},
		{"carriage return sanitized", "\rdata", "'\rdata"},
		{"positive numeric preserved", "+100.50", "+100.50"},
		{"negative numeric preserved", "-200.00", "-200.00"},
		{"positive integer preserved", "+42", "+42"},
		{"negative integer preserved", "-42", "-42"},
		{"scientific notation preserved", "+1.5e3", "+1.5e3"},
		{"normal text unchanged", "Normal text", "Normal text"},
		{"empty string unchanged", "", ""},
		{"plain number unchanged", "12345", "12345"},
		{"embedded equals unchanged", "a=b", "a=b"},
		{"embedded plus unchanged", "a+b", "a+b"},
		{"single plus sanitized", "+", "'+"},
		{"single minus sanitized", "-", "'-"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result := sanitizeFormulaInjection(testCase.input)
			assert.Equal(t, testCase.expected, result)
		})
	}
}

func TestGetStringValue_SanitizesFormulas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		row      map[string]any
		field    string
		expected string
		ok       bool
	}{
		{
			name:     "equals formula sanitized",
			row:      map[string]any{"desc": "=HYPERLINK(\"http://evil.com\")"},
			field:    "desc",
			expected: "'=HYPERLINK(\"http://evil.com\")",
			ok:       true,
		},
		{
			name:     "at formula sanitized",
			row:      map[string]any{"note": "@SUM(A1)"},
			field:    "note",
			expected: "'@SUM(A1)",
			ok:       true,
		},
		{
			name:     "plus formula sanitized",
			row:      map[string]any{"val": "+cmd|calc"},
			field:    "val",
			expected: "'+cmd|calc",
			ok:       true,
		},
		{
			name:     "negative number preserved",
			row:      map[string]any{"amount": "-100.50"},
			field:    "amount",
			expected: "-100.50",
			ok:       true,
		},
		{
			name:     "positive number preserved",
			row:      map[string]any{"amount": "+200.00"},
			field:    "amount",
			expected: "+200.00",
			ok:       true,
		},
		{
			name:     "normal text preserved",
			row:      map[string]any{"name": "Alice"},
			field:    "name",
			expected: "Alice",
			ok:       true,
		},
		{
			name:     "missing field returns false",
			row:      map[string]any{"name": "Alice"},
			field:    "missing",
			expected: "",
			ok:       false,
		},
		{
			name:     "nil value returns false",
			row:      map[string]any{"name": nil},
			field:    "name",
			expected: "",
			ok:       false,
		},
		{
			name:     "tab prefix stripped by TrimSpace",
			row:      map[string]any{"val": "\tmalicious"},
			field:    "val",
			expected: "malicious",
			ok:       true,
		},
		{
			name:     "carriage return prefix stripped by TrimSpace",
			row:      map[string]any{"val": "\revil"},
			field:    "val",
			expected: "evil",
			ok:       true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result, ok := getStringValue(testCase.row, testCase.field)
			assert.Equal(t, testCase.ok, ok)
			assert.Equal(t, testCase.expected, result)
		})
	}
}

func TestBuildMetadata_SanitizesStringValues(t *testing.T) {
	t.Parallel()

	t.Run("formula strings in metadata are sanitized", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"safe_field":    "normal text",
			"formula_field": "=HYPERLINK(\"http://evil.com\")",
			"at_field":      "@SUM(1+1)",
			"plus_field":    "+cmd|calc",
			"minus_field":   "-cmd|calc",
			"tab_field":     "\tdata",
			"cr_field":      "\rdata",
			"numeric_field": "-100.50",
			"int_field":     42,
		}
		mappedFields := map[string]bool{}

		metadata := buildMetadata(row, mappedFields)

		assert.Equal(t, "normal text", metadata["safe_field"])
		assert.Equal(t, "'=HYPERLINK(\"http://evil.com\")", metadata["formula_field"])
		assert.Equal(t, "'@SUM(1+1)", metadata["at_field"])
		assert.Equal(t, "'+cmd|calc", metadata["plus_field"])
		assert.Equal(t, "'-cmd|calc", metadata["minus_field"])
		// Tab and CR are stripped by TrimSpace, leaving just "data"
		assert.Equal(t, "data", metadata["tab_field"])
		assert.Equal(t, "data", metadata["cr_field"])
		// Numeric strings that are valid numbers should NOT be sanitized
		assert.Equal(t, "-100.50", metadata["numeric_field"])
		// Non-string values pass through unchanged
		assert.Equal(t, 42, metadata["int_field"])
	})

	t.Run("non-string values not affected by sanitization", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"bool_val":  true,
			"int_val":   123,
			"float_val": 3.14,
		}
		mappedFields := map[string]bool{}

		metadata := buildMetadata(row, mappedFields)

		assert.Equal(t, true, metadata["bool_val"])
		assert.Equal(t, 123, metadata["int_val"])
		assert.Equal(t, 3.14, metadata["float_val"])
	})

	t.Run("leading whitespace before formula chars is trimmed then sanitized", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"formula_with_space": " =HYPERLINK(\"http://evil.com\")",
			"at_with_space":     " @evil",
			"plus_with_space":   " +cmd",
			"minus_with_space":  " -cmd",
		}
		mappedFields := map[string]bool{}

		metadata := buildMetadata(row, mappedFields)

		// Leading space is trimmed, then the formula char triggers sanitization
		assert.Equal(t, "'=HYPERLINK(\"http://evil.com\")", metadata["formula_with_space"])
		assert.Equal(t, "'@evil", metadata["at_with_space"])
		assert.Equal(t, "'+cmd", metadata["plus_with_space"])
		assert.Equal(t, "'-cmd", metadata["minus_with_space"])
	})

	t.Run("json.Number values are sanitized", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"json_num_safe":    json.Number("12345"),
			"json_num_formula": json.Number("=MALICIOUS"),
		}
		mappedFields := map[string]bool{}

		metadata := buildMetadata(row, mappedFields)

		// Safe numeric json.Number passes through unchanged
		assert.Equal(t, "12345", metadata["json_num_safe"])
		// Formula-prefixed json.Number gets sanitized
		assert.Equal(t, "'=MALICIOUS", metadata["json_num_formula"])
	})
}

func TestJSONParser_FormulaInjectionSanitized(t *testing.T) {
	t.Parallel()

	parser := NewJSONParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.json", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	jsonData := `[{"id":"=MALICIOUS_ID","amount":"100.00","currency":"USD","date":"2024-01-15","desc":"=HYPERLINK(\"http://evil.com\",\"Click\")","extra":"@SUM(1+1)"}]`

	result, err := parser.Parse(ctx, strings.NewReader(jsonData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.Empty(t, result.Errors)

	tx := result.Transactions[0]
	// external_id with formula prefix should be sanitized via getStringValue.
	// This is a behavioral change for JSON/XML (CSV was already sanitized at
	// parse time). Formula-prefixed external IDs get a ' prefix, which affects
	// dedup hash calculation — intentional security hardening.
	assert.Equal(t, "'=MALICIOUS_ID", tx.ExternalID)
	// Description should be sanitized via getStringValue
	assert.Equal(t, "'=HYPERLINK(\"http://evil.com\",\"Click\")", tx.Description)
	// Metadata string values should be sanitized via buildMetadata
	assert.Equal(t, "'@SUM(1+1)", tx.Metadata["extra"])
}

func TestXMLParser_FormulaInjectionSanitized(t *testing.T) {
	t.Parallel()

	parser := NewXMLParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.xml", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "external_id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}}

	xmlData := `<transactions><transaction>` +
		`<external_id>=MALICIOUS_ID</external_id>` +
		`<amount>100.00</amount>` +
		`<currency>USD</currency>` +
		`<date>2024-01-15</date>` +
		`<description>=HYPERLINK("http://evil.com","Click")</description>` +
		`<extra>@SUM(1+1)</extra>` +
		`</transaction></transactions>`

	result, err := parser.Parse(ctx, strings.NewReader(xmlData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.Empty(t, result.Errors)

	tx := result.Transactions[0]
	// external_id with formula prefix should be sanitized via getStringValue.
	// Same behavioral change as JSON — see comment in normalizeTransaction().
	assert.Equal(t, "'=MALICIOUS_ID", tx.ExternalID)
	// Description should be sanitized via getStringValue
	assert.Equal(t, "'=HYPERLINK(\"http://evil.com\",\"Click\")", tx.Description)
	// Metadata string values should be sanitized via buildMetadata
	assert.Equal(t, "'@SUM(1+1)", tx.Metadata["extra"])
}
