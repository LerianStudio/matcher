//go:build unit

package parsers

import (
	"context"
	"io"
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

// ParseResultForTest is a test helper struct for validating parse results.
type ParseResultForTest struct {
	Transactions []*shared.Transaction
	Errors       []ports.ParseError
	DateRange    *ports.DateRange
}

func TestNewCSVParser(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	require.NotNil(t, parser)
}

func TestCSVParser_SupportedFormat(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	assert.Equal(t, "csv", parser.SupportedFormat())
}

func TestCSVParser_Parse(t *testing.T) {
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
		csvData          string
		fieldMap         *shared.FieldMap
		setupJob         bool
		nilReader        bool
		nilJob           bool
		wantErr          bool
		wantErrContains  string
		wantTransactions int
		wantParseErrors  int
		validateResult   func(t *testing.T, result *ParseResultForTest)
	}{
		{
			name:            "nil reader returns errReaderRequired",
			csvData:         "",
			fieldMap:        validFieldMap,
			setupJob:        true,
			nilReader:       true,
			wantErr:         true,
			wantErrContains: "reader is required",
		},
		{
			name:            "nil job returns errMissingIngestionJob",
			csvData:         "id,amount,currency,date,desc\n",
			fieldMap:        validFieldMap,
			setupJob:        false,
			nilJob:          true,
			wantErr:         true,
			wantErrContains: "ingestion job is required",
		},
		{
			name:            "nil fieldMap returns error",
			csvData:         "id,amount,currency,date,desc\n",
			fieldMap:        nil,
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "field map is required",
		},
		{
			name:            "empty fieldMap mapping returns error",
			csvData:         "id,amount,currency,date,desc\n",
			fieldMap:        &shared.FieldMap{Mapping: map[string]any{}},
			setupJob:        true,
			wantErr:         true,
			wantErrContains: "field map mapping is required",
		},
		{
			name:     "missing required mapping key returns error",
			csvData:  "id,amount,currency,date,desc\n",
			fieldMap: &shared.FieldMap{Mapping: map[string]any{"external_id": "id"}},
			setupJob: true,
			wantErr:  true,
		},
		{
			name:             "empty CSV with headers only returns empty result",
			csvData:          "id,amount,currency,date,desc\n",
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 0,
			wantParseErrors:  0,
		},
		{
			name:             "valid CSV with all required fields",
			csvData:          "id,amount,currency,date,desc\n1,100.50,USD,2024-01-15,Payment received\n",
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 1,
			wantParseErrors:  0,
			validateResult: func(t *testing.T, result *ParseResultForTest) {
				t.Helper()

				tx := result.Transactions[0]
				assert.Equal(t, "1", tx.ExternalID)
				assert.True(t, tx.Amount.Equal(decimal.NewFromFloat(100.50)))
				assert.Equal(t, "USD", tx.Currency)
				assert.Equal(t, "Payment received", tx.Description)
			},
		},
		{
			name:             "CSV with multiple rows",
			csvData:          "id,amount,currency,date,desc\n1,10.00,USD,2024-01-01,First\n2,20.00,EUR,2024-01-02,Second\n3,30.00,GBP,2024-01-03,Third\n",
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 3,
			wantParseErrors:  0,
		},
		{
			name:             "CSV with extra columns triggers field count error",
			csvData:          "id,amount,currency,date,desc\n1,100.00,USD,2024-01-15,Payment,extra1,extra2\n",
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 0,
			wantParseErrors:  1,
			validateResult: func(t *testing.T, result *ParseResultForTest) {
				t.Helper()
				assert.Contains(t, result.Errors[0].Message, "failed to read csv row")
			},
		},
		{
			name:             "CSV with fewer columns than headers triggers field count error",
			csvData:          "id,amount,currency,date,desc\n1,100.00,USD\n",
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 0,
			wantParseErrors:  1,
			validateResult: func(t *testing.T, result *ParseResultForTest) {
				t.Helper()
				assert.Contains(t, result.Errors[0].Message, "failed to read csv row")
			},
		},
		{
			name:             "CSV with whitespace in headers and values",
			csvData:          "  id  ,  amount  ,  currency  ,  date  ,  desc  \n  1  ,  100.00  ,  usd  ,  2024-01-15  ,  Payment  \n",
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 1,
			wantParseErrors:  0,
			validateResult: func(t *testing.T, result *ParseResultForTest) {
				t.Helper()

				tx := result.Transactions[0]
				assert.Equal(t, "1", tx.ExternalID)
				assert.Equal(t, "USD", tx.Currency)
				assert.Equal(t, "Payment", tx.Description)
			},
		},
		{
			name:             "CSV with special characters in values",
			csvData:          "id,amount,currency,date,desc\n1,100.00,USD,2024-01-15,\"Payment with, comma and \"\"quotes\"\"\"\n",
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 1,
			wantParseErrors:  0,
			validateResult: func(t *testing.T, result *ParseResultForTest) {
				t.Helper()

				tx := result.Transactions[0]
				assert.Equal(t, "Payment with, comma and \"quotes\"", tx.Description)
			},
		},
		{
			name:             "CSV with invalid amount continues to next row",
			csvData:          "id,amount,currency,date,desc\n1,invalid,USD,2024-01-15,Bad\n2,100.00,USD,2024-01-15,Good\n",
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 1,
			wantParseErrors:  1,
			validateResult: func(t *testing.T, result *ParseResultForTest) {
				t.Helper()
				assert.Equal(t, "2", result.Transactions[0].ExternalID)
				assert.Contains(t, result.Errors[0].Message, "invalid decimal amount")
			},
		},
		{
			name:             "CSV with invalid date continues to next row",
			csvData:          "id,amount,currency,date,desc\n1,100.00,USD,not-a-date,Bad\n2,100.00,USD,2024-01-15,Good\n",
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 1,
			wantParseErrors:  1,
			validateResult: func(t *testing.T, result *ParseResultForTest) {
				t.Helper()
				assert.Equal(t, "2", result.Transactions[0].ExternalID)
				assert.Contains(t, result.Errors[0].Message, "invalid date format")
			},
		},
		{
			name:             "CSV with missing external_id continues to next row",
			csvData:          "id,amount,currency,date,desc\n,100.00,USD,2024-01-15,Bad\n2,100.00,USD,2024-01-15,Good\n",
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 1,
			wantParseErrors:  1,
		},
		{
			name:             "CSV with lowercase currency is uppercased",
			csvData:          "id,amount,currency,date,desc\n1,100.00,usd,2024-01-15,Test\n",
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 1,
			wantParseErrors:  0,
			validateResult: func(t *testing.T, result *ParseResultForTest) {
				t.Helper()
				assert.Equal(t, "USD", result.Transactions[0].Currency)
			},
		},
		{
			name:    "CSV without description field",
			csvData: "id,amount,currency,date\n1,100.00,USD,2024-01-15\n",
			fieldMap: &shared.FieldMap{
				Mapping: map[string]any{
					"external_id": "id",
					"amount":      "amount",
					"currency":    "currency",
					"date":        "date",
				},
			},
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 1,
			wantParseErrors:  0,
			validateResult: func(t *testing.T, result *ParseResultForTest) {
				t.Helper()
				assert.Empty(t, result.Transactions[0].Description)
			},
		},
		{
			name:             "multiple parse errors accumulated in result",
			csvData:          "id,amount,currency,date,desc\n,100.00,USD,2024-01-15,Missing ID\n2,invalid,USD,2024-01-15,Invalid amount\n3,100.00,USD,bad-date,Invalid date\n4,100.00,USD,2024-01-15,Valid\n",
			fieldMap:         validFieldMap,
			setupJob:         true,
			wantErr:          false,
			wantTransactions: 1,
			wantParseErrors:  3,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			parser := NewCSVParser()
			ctx := t.Context()

			var job *entities.IngestionJob

			if testCase.setupJob && !testCase.nilJob {
				var err error

				job, err = entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "test.csv", 1000)
				require.NoError(t, err)
			}

			var reader io.Reader

			if !testCase.nilReader {
				reader = strings.NewReader(testCase.csvData)
			}

			result, err := parser.Parse(ctx, reader, job, testCase.fieldMap)

			if testCase.wantErr {
				require.Error(t, err)

				if testCase.wantErrContains != "" {
					assert.Contains(t, err.Error(), testCase.wantErrContains)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Len(t, result.Transactions, testCase.wantTransactions)
			assert.Len(t, result.Errors, testCase.wantParseErrors)

			if testCase.validateResult != nil {
				testCase.validateResult(t, &ParseResultForTest{
					Transactions: result.Transactions,
					Errors:       result.Errors,
					DateRange:    result.DateRange,
				})
			}
		})
	}
}

func TestCSVParser_Parse_ContextCancellation(t *testing.T) {
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

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	csvData := "id,amount,currency,date,desc\n1,100.00,USD,2024-01-15,Test\n"
	result, err := parser.Parse(cancelCtx, strings.NewReader(csvData), job, fieldMap)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "csv parsing cancelled")
}

func TestCSVParser_Parse_DateRangeTracking(t *testing.T) {
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

	csvData := `id,amount,currency,date,desc
1,100.00,USD,2024-01-15,First
2,100.00,USD,2024-01-01,Earliest
3,100.00,USD,2024-01-31,Latest
4,100.00,USD,2024-01-20,Middle
`
	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.NotNil(t, result.DateRange)

	expectedStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	expectedEnd := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

	assert.Equal(t, expectedStart, result.DateRange.Start)
	assert.Equal(t, expectedEnd, result.DateRange.End)
}

func TestCSVParser_Parse_MalformedCSVRow(t *testing.T) {
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

	csvData := `id,amount,currency,date,desc
1,100.00,USD,2024-01-15,Valid
"unclosed quote
3,100.00,USD,2024-01-15,Also valid
`
	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(result.Transactions), 1)
	assert.GreaterOrEqual(t, len(result.Errors), 1)

	hasCSVError := false

	for _, parseErr := range result.Errors {
		if strings.Contains(parseErr.Message, "failed to read csv row") {
			hasCSVError = true

			break
		}
	}

	assert.True(t, hasCSVError, "expected a CSV read error in parse errors")
}

func TestCSVParser_Parse_VariousDateFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dateValue  string
		shouldPass bool
	}{
		{"RFC3339", "2024-01-15T10:30:00Z", true},
		{"RFC3339Nano", "2024-01-15T10:30:00.123456789Z", true},
		{"date only", "2024-01-15", true},
		{"datetime", "2024-01-15 10:30:00", true},
		{"invalid", "15/01/2024", false},
		{"empty", "", false},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
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

			csvData := "id,amount,currency,date,desc\n1,100.00,USD," + testCase.dateValue + ",Test\n"
			result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
			require.NoError(t, err)

			if testCase.shouldPass {
				assert.Len(t, result.Transactions, 1)
				assert.Empty(t, result.Errors)
			} else {
				assert.Empty(t, result.Transactions)
				assert.Len(t, result.Errors, 1)
			}
		})
	}
}

func TestCSVParser_Parse_EmptyCSVFile(t *testing.T) {
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
	}}

	_, err = parser.Parse(ctx, strings.NewReader(""), job, fieldMap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read csv headers")
}

func TestCSVParser_Parse_MetadataPreserved(t *testing.T) {
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

	csvData := "id,amount,currency,date,desc,extra_field,another_field\n1,100.00,USD,2024-01-15,Test,extra_value,another_value\n"
	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)

	tx := result.Transactions[0]
	require.NotNil(t, tx.Metadata)
	assert.Equal(t, "extra_value", tx.Metadata["extra_field"])
	assert.Equal(t, "another_value", tx.Metadata["another_field"])
}

func TestSanitizeCSVValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty string", "", ""},
		{"normal value", "hello", "hello"},
		{"numeric value", "12345", "12345"},
		{"formula equals", "=SUM(A1:A10)", "'=SUM(A1:A10)"},
		{"formula plus", "+1+2", "'+1+2"},
		{"formula minus", "-1-2", "'-1-2"}, // not a valid number, so sanitized
		{"formula at", "@SUM(A1)", "'@SUM(A1)"},
		{"formula tab", "\tcommand", "'\tcommand"},
		{"formula carriage return", "\rcommand", "'\rcommand"},
		{"leading space preserved", " hello", " hello"},
		{"embedded equals", "a=b", "a=b"},
		{"embedded plus", "a+b", "a+b"},
		{"embedded minus", "a-b", "a-b"},
		{"negative number", "-100.50", "-100.50"},           // valid number, not sanitized
		{"positive number with plus", "+100.50", "+100.50"}, // valid number, not sanitized
		{"formula plus expression", "+cmd|calc", "'+cmd|calc"},
		{"formula minus expression", "-cmd|calc", "'-cmd|calc"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result := sanitizeCSVValue(testCase.input)
			assert.Equal(t, testCase.expected, result)
		})
	}
}

func TestIsNumericString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected bool
	}{
		{"", false},
		{"123", true},
		{"-123", true},
		{"+123", true},
		{"12.34", true},
		{"-12.34", true},
		{"+12.34", true},
		{"0", true},
		{"0.0", true},
		{"-", false},
		{"+", false},
		{".", false},
		{"12.", true},
		{".12", true},
		{"12.34.56", false},
		{"abc", false},
		{"-abc", false},
		{"+abc", false},
		{"12abc", false},
		{"-12abc", false},
		{"1+2", false},
		{"1-2", false},
		// Scientific notation.
		{"1e10", true},
		{"1E10", true},
		{"-1e10", true},
		{"+1e10", true},
		{"1.5e3", true},
		{"-1.5E3", true},
		{"+1.5e3", true},
		{"1e+10", true},
		{"1e-10", true},
		{"1.5E+3", true},
		{"1.5E-3", true},
		{".5e2", true},
		{"1.e2", true},
		{"1e0", true},
		// Malformed scientific notation.
		{"e10", false},
		{"E10", false},
		{".e10", false},
		{"1e", false},
		{"1E", false},
		{"1e+", false},
		{"1e-", false},
		{"-e5", false},
		{"+e5", false},
		{"1e2e3", false},
		{"1e2.3", false},
		{"1e2+3", false},
	}

	for _, testCase := range tests {
		t.Run(testCase.input, func(t *testing.T) {
			t.Parallel()

			result := isNumericString(testCase.input)
			assert.Equal(t, testCase.expected, result, "for input: %q", testCase.input)
		})
	}
}

func TestCSVParser_Parse_ZeroAmount(t *testing.T) {
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

	csvData := "id,amount,currency,date,desc\n1,0,USD,2024-01-15,Zero amount\n2,0.00,USD,2024-01-15,Zero decimal\n"
	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 2)

	assert.True(t, result.Transactions[0].Amount.IsZero())
	assert.True(t, result.Transactions[1].Amount.IsZero())
}

func TestCSVParser_Parse_NegativeAmount(t *testing.T) {
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

	csvData := "id,amount,currency,date,desc\n1,-100.50,USD,2024-01-15,Refund\n2,+50.25,USD,2024-01-16,Positive\n"
	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 2)

	expectedNegative := decimal.NewFromFloat(-100.50)
	expectedPositive := decimal.NewFromFloat(50.25)

	assert.True(t, result.Transactions[0].Amount.Equal(expectedNegative))
	assert.True(t, result.Transactions[1].Amount.Equal(expectedPositive))
}

func TestCSVParser_Parse_FormulaInjectionSanitized(t *testing.T) {
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

	csvData := "id,amount,currency,date,desc\n1,100.00,USD,2024-01-15,=SUM(A1:A10)\n2,100.00,USD,2024-01-15,+cmd|' /C calc'!A0\n3,100.00,USD,2024-01-15,@SUM(A1)\n"
	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 3)

	assert.Equal(t, "'=SUM(A1:A10)", result.Transactions[0].Description)
	assert.Equal(t, "'+cmd|' /C calc'!A0", result.Transactions[1].Description)
	assert.Equal(t, "'@SUM(A1)", result.Transactions[2].Description)
}
