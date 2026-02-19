//go:build unit

package parsers

import (
	"context"
	"fmt"
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

// errTestCallbackFailed is declared in json_parser_test.go.

func TestNewXMLParser(t *testing.T) {
	t.Parallel()

	parser := NewXMLParser()
	require.NotNil(t, parser)
	assert.IsType(t, &XMLParser{}, parser)
}

func TestXMLParser_SupportedFormat(t *testing.T) {
	t.Parallel()

	parser := NewXMLParser()
	assert.Equal(t, "xml", parser.SupportedFormat())
}

func TestXMLParser_Parse(t *testing.T) {
	t.Parallel()

	validFieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "external_id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}}

	createJob := func(t *testing.T) *entities.IngestionJob {
		t.Helper()

		job, err := entities.NewIngestionJob(
			context.Background(),
			uuid.New(),
			uuid.New(),
			"file.xml",
			100,
		)
		require.NoError(t, err)

		return job
	}

	tests := []struct {
		name              string
		xmlData           string
		fieldMap          *shared.FieldMap
		createJob         bool
		nilReader         bool
		wantErr           bool
		errContains       string
		wantTransactions  int
		wantErrors        int
		validate          func(t *testing.T, result any)
		validateDateRange func(t *testing.T, result *ports.DateRange)
	}{
		{
			name:             "valid XML with transaction elements",
			xmlData:          `<transactions><transaction><external_id>tx1</external_id><amount>100.50</amount><currency>USD</currency><date>2024-01-15</date><description>Payment</description></transaction></transactions>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 1,
			validate: func(t *testing.T, result any) {
				t.Helper()

				tx, ok := result.(*shared.Transaction)
				require.True(t, ok, "result should be *shared.Transaction")
				assert.Equal(t, "tx1", tx.ExternalID)
				assert.True(t, tx.Amount.Equal(decimal.NewFromFloat(100.50)))
				assert.Equal(t, "USD", tx.Currency)
				assert.Equal(t, "Payment", tx.Description)
			},
		},
		{
			name:             "valid XML with row elements",
			xmlData:          `<data><row><external_id>row1</external_id><amount>50.00</amount><currency>EUR</currency><date>2024-02-20</date><description>Fee</description></row></data>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 1,
			validate: func(t *testing.T, result any) {
				t.Helper()

				tx, ok := result.(*shared.Transaction)
				require.True(t, ok, "result should be *shared.Transaction")
				assert.Equal(t, "row1", tx.ExternalID)
				assert.Equal(t, "EUR", tx.Currency)
			},
		},
		{
			name:      "nil reader returns error",
			nilReader: true,
			fieldMap:  validFieldMap,
			createJob: true,
			wantErr:   true,
		},
		{
			name:      "nil job returns error",
			xmlData:   `<transactions><transaction><external_id>1</external_id></transaction></transactions>`,
			fieldMap:  validFieldMap,
			createJob: false,
			wantErr:   true,
		},
		{
			name:      "nil fieldMap returns error",
			xmlData:   `<transactions><transaction><external_id>1</external_id></transaction></transactions>`,
			fieldMap:  nil,
			createJob: true,
			wantErr:   true,
		},
		{
			name:      "empty fieldMap mapping returns error",
			xmlData:   `<transactions><transaction><external_id>1</external_id></transaction></transactions>`,
			fieldMap:  &shared.FieldMap{Mapping: map[string]any{}},
			createJob: true,
			wantErr:   true,
		},
		{
			name:             "empty XML no transactions",
			xmlData:          `<transactions></transactions>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 0,
			wantErrors:       0,
		},
		{
			name:        "malformed XML returns error",
			xmlData:     `<transactions><transaction>not closed`,
			fieldMap:    validFieldMap,
			createJob:   true,
			wantErr:     true,
			errContains: "failed to decode xml",
		},
		{
			name:        "XML with unclosed tags returns error",
			xmlData:     `<transactions><transaction><external_id>1</external_id></transactions>`,
			fieldMap:    validFieldMap,
			createJob:   true,
			wantErr:     true,
			errContains: "failed to decode xml",
		},
		{
			name:             "case-insensitive Transaction element",
			xmlData:          `<data><Transaction><external_id>tx2</external_id><amount>25.00</amount><currency>GBP</currency><date>2024-03-10</date><description>Test</description></Transaction></data>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 1,
			validate: func(t *testing.T, result any) {
				t.Helper()

				tx, ok := result.(*shared.Transaction)
				require.True(t, ok, "result should be *shared.Transaction")
				assert.Equal(t, "tx2", tx.ExternalID)
			},
		},
		{
			name:             "case-insensitive TRANSACTION element",
			xmlData:          `<data><TRANSACTION><external_id>tx3</external_id><amount>30.00</amount><currency>JPY</currency><date>2024-04-05</date><description>Upper</description></TRANSACTION></data>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 1,
		},
		{
			name:             "case-insensitive Row element",
			xmlData:          `<data><Row><external_id>row2</external_id><amount>15.00</amount><currency>CAD</currency><date>2024-05-15</date><description>Mixed</description></Row></data>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 1,
		},
		{
			name:             "multiple transactions",
			xmlData:          `<transactions><transaction><external_id>tx1</external_id><amount>10.00</amount><currency>USD</currency><date>2024-01-01</date><description>First</description></transaction><transaction><external_id>tx2</external_id><amount>20.00</amount><currency>USD</currency><date>2024-01-02</date><description>Second</description></transaction><transaction><external_id>tx3</external_id><amount>30.00</amount><currency>USD</currency><date>2024-01-03</date><description>Third</description></transaction></transactions>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 3,
		},
		{
			name:             "whitespace in element content is trimmed",
			xmlData:          `<transactions><transaction><external_id>  tx_ws  </external_id><amount>  99.99  </amount><currency>  USD  </currency><date>  2024-06-01  </date><description>  Whitespace Test  </description></transaction></transactions>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 1,
			validate: func(t *testing.T, result any) {
				t.Helper()

				tx, ok := result.(*shared.Transaction)
				require.True(t, ok, "result should be *shared.Transaction")
				assert.Equal(t, "tx_ws", tx.ExternalID)
				assert.Equal(t, "Whitespace Test", tx.Description)
			},
		},
		{
			name:             "XML attributes are ignored",
			xmlData:          `<transactions><transaction id="attr123" type="payment"><external_id>tx_attr</external_id><amount>50.00</amount><currency>USD</currency><date>2024-07-01</date><description>Attrs</description></transaction></transactions>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 1,
			validate: func(t *testing.T, result any) {
				t.Helper()

				tx, ok := result.(*shared.Transaction)
				require.True(t, ok, "result should be *shared.Transaction")
				assert.Equal(t, "tx_attr", tx.ExternalID)
			},
		},
		{
			name:             "nested elements inside transaction",
			xmlData:          `<transactions><transaction><external_id>tx_nested</external_id><amount>75.00</amount><currency>USD</currency><date>2024-08-01</date><description>Nested</description><extra><nested>value</nested></extra></transaction></transactions>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 1,
			validate: func(t *testing.T, result any) {
				t.Helper()

				tx, ok := result.(*shared.Transaction)
				require.True(t, ok, "result should be *shared.Transaction")
				assert.Equal(t, "tx_nested", tx.ExternalID)
			},
		},
		{
			name:       "missing required field generates parse error",
			xmlData:    `<transactions><transaction><external_id>tx_missing</external_id><currency>USD</currency><date>2024-09-01</date></transaction></transactions>`,
			fieldMap:   validFieldMap,
			createJob:  true,
			wantErrors: 1,
		},
		{
			name:             "multiple parse errors in result",
			xmlData:          `<transactions><transaction><external_id>tx_bad1</external_id><currency>USD</currency><date>2024-09-01</date></transaction><transaction><external_id>tx_bad2</external_id><amount>invalid</amount><currency>USD</currency><date>2024-09-02</date></transaction></transactions>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 0,
			wantErrors:       2,
		},
		{
			name:             "CDATA sections",
			xmlData:          `<transactions><transaction><external_id><![CDATA[tx_cdata]]></external_id><amount>88.88</amount><currency>USD</currency><date>2024-10-01</date><description><![CDATA[CDATA Description]]></description></transaction></transactions>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 1,
			validate: func(t *testing.T, result any) {
				t.Helper()

				tx, ok := result.(*shared.Transaction)
				require.True(t, ok, "result should be *shared.Transaction")
				assert.Equal(t, "tx_cdata", tx.ExternalID)
				assert.Equal(t, "CDATA Description", tx.Description)
			},
		},
		{
			name:             "date range tracking",
			xmlData:          `<transactions><transaction><external_id>tx1</external_id><amount>10.00</amount><currency>USD</currency><date>2024-03-15</date><description>Mid</description></transaction><transaction><external_id>tx2</external_id><amount>20.00</amount><currency>USD</currency><date>2024-01-01</date><description>Start</description></transaction><transaction><external_id>tx3</external_id><amount>30.00</amount><currency>USD</currency><date>2024-12-31</date><description>End</description></transaction></transactions>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 3,
			validateDateRange: func(t *testing.T, result *ports.DateRange) {
				t.Helper()

				require.NotNil(t, result)
				assert.Equal(t, 2024, result.Start.Year())
				assert.Equal(t, time.January, result.Start.Month())
				assert.Equal(t, 1, result.Start.Day())
				assert.Equal(t, 2024, result.End.Year())
				assert.Equal(t, time.December, result.End.Month())
				assert.Equal(t, 31, result.End.Day())
			},
		},
		{
			name:       "invalid date format generates parse error",
			xmlData:    `<transactions><transaction><external_id>tx_date</external_id><amount>10.00</amount><currency>USD</currency><date>not-a-date</date><description>Bad date</description></transaction></transactions>`,
			fieldMap:   validFieldMap,
			createJob:  true,
			wantErrors: 1,
		},
		{
			name:       "invalid amount format generates parse error",
			xmlData:    `<transactions><transaction><external_id>tx_amt</external_id><amount>not-a-number</amount><currency>USD</currency><date>2024-01-01</date><description>Bad amount</description></transaction></transactions>`,
			fieldMap:   validFieldMap,
			createJob:  true,
			wantErrors: 1,
		},
		{
			name:             "currency is uppercased",
			xmlData:          `<transactions><transaction><external_id>tx_curr</external_id><amount>10.00</amount><currency>usd</currency><date>2024-01-01</date><description>Lower currency</description></transaction></transactions>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 1,
			validate: func(t *testing.T, result any) {
				t.Helper()

				tx, ok := result.(*shared.Transaction)
				require.True(t, ok, "result should be *shared.Transaction")
				assert.Equal(t, "USD", tx.Currency)
			},
		},
		{
			name:             "empty elements are skipped",
			xmlData:          `<transactions><transaction><external_id>tx_empty</external_id><amount>10.00</amount><currency>USD</currency><date>2024-01-01</date><description></description></transaction></transactions>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 1,
			validate: func(t *testing.T, result any) {
				t.Helper()

				tx, ok := result.(*shared.Transaction)
				require.True(t, ok, "result should be *shared.Transaction")
				assert.Empty(t, tx.Description)
			},
		},
		{
			name:             "mixed transaction and row elements",
			xmlData:          `<data><transaction><external_id>tx1</external_id><amount>10.00</amount><currency>USD</currency><date>2024-01-01</date><description>Transaction</description></transaction><row><external_id>row1</external_id><amount>20.00</amount><currency>EUR</currency><date>2024-01-02</date><description>Row</description></row></data>`,
			fieldMap:         validFieldMap,
			createJob:        true,
			wantTransactions: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parser := NewXMLParser()

			var job *entities.IngestionJob

			if tt.createJob {
				job = createJob(t)
			}

			ctx := context.Background()

			var result *ports.ParseResult

			var err error

			if tt.nilReader {
				result, err = parser.Parse(ctx, nil, job, tt.fieldMap)
			} else {
				result, err = parser.Parse(ctx, strings.NewReader(tt.xmlData), job, tt.fieldMap)
			}

			if tt.wantErr {
				require.Error(t, err)

				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)

			assert.Len(t, result.Transactions, tt.wantTransactions)
			assert.Len(t, result.Errors, tt.wantErrors)

			if tt.validateDateRange != nil {
				require.NotNil(t, result.DateRange)
				tt.validateDateRange(t, result.DateRange)
			}

			if tt.validate != nil && len(result.Transactions) > 0 {
				tt.validate(t, result.Transactions[0])
			}
		})
	}
}

func TestXMLParser_Parse_ContextCancellation(t *testing.T) {
	t.Parallel()

	parser := NewXMLParser()

	job, err := entities.NewIngestionJob(
		context.Background(),
		uuid.New(),
		uuid.New(),
		"file.xml",
		100,
	)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "external_id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	xmlData := `<transactions><transaction><external_id>1</external_id><amount>10.00</amount><currency>USD</currency><date>2024-01-01</date><description>Test</description></transaction></transactions>`

	_, err = parser.Parse(ctx, strings.NewReader(xmlData), job, fieldMap)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cancelled")
}

func TestXMLParser_Parse_FieldMapValidation(t *testing.T) {
	t.Parallel()

	createJob := func(t *testing.T) *entities.IngestionJob {
		t.Helper()

		job, err := entities.NewIngestionJob(
			context.Background(),
			uuid.New(),
			uuid.New(),
			"file.xml",
			100,
		)
		require.NoError(t, err)

		return job
	}

	tests := []struct {
		name     string
		fieldMap *shared.FieldMap
		wantErr  bool
	}{
		{
			name:     "nil fieldMap",
			fieldMap: nil,
			wantErr:  true,
		},
		{
			name:     "empty mapping",
			fieldMap: &shared.FieldMap{Mapping: map[string]any{}},
			wantErr:  true,
		},
		{
			name: "missing external_id",
			fieldMap: &shared.FieldMap{Mapping: map[string]any{
				"amount":   "amount",
				"currency": "currency",
				"date":     "date",
			}},
			wantErr: true,
		},
		{
			name: "missing amount",
			fieldMap: &shared.FieldMap{Mapping: map[string]any{
				"external_id": "external_id",
				"currency":    "currency",
				"date":        "date",
			}},
			wantErr: true,
		},
		{
			name: "missing currency",
			fieldMap: &shared.FieldMap{Mapping: map[string]any{
				"external_id": "external_id",
				"amount":      "amount",
				"date":        "date",
			}},
			wantErr: true,
		},
		{
			name: "missing date",
			fieldMap: &shared.FieldMap{Mapping: map[string]any{
				"external_id": "external_id",
				"amount":      "amount",
				"currency":    "currency",
			}},
			wantErr: true,
		},
		{
			name: "non-string mapping value",
			fieldMap: &shared.FieldMap{Mapping: map[string]any{
				"external_id": 123,
				"amount":      "amount",
				"currency":    "currency",
				"date":        "date",
			}},
			wantErr: true,
		},
		{
			name: "empty string mapping value",
			fieldMap: &shared.FieldMap{Mapping: map[string]any{
				"external_id": "",
				"amount":      "amount",
				"currency":    "currency",
				"date":        "date",
			}},
			wantErr: true,
		},
		{
			name: "whitespace-only mapping value",
			fieldMap: &shared.FieldMap{Mapping: map[string]any{
				"external_id": "   ",
				"amount":      "amount",
				"currency":    "currency",
				"date":        "date",
			}},
			wantErr: true,
		},
		{
			name: "valid mapping without description",
			fieldMap: &shared.FieldMap{Mapping: map[string]any{
				"external_id": "external_id",
				"amount":      "amount",
				"currency":    "currency",
				"date":        "date",
			}},
			wantErr: false,
		},
		{
			name: "valid mapping with description",
			fieldMap: &shared.FieldMap{Mapping: map[string]any{
				"external_id": "external_id",
				"amount":      "amount",
				"currency":    "currency",
				"date":        "date",
				"description": "description",
			}},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parser := NewXMLParser()
			job := createJob(t)

			xmlData := `<transactions><transaction><external_id>1</external_id><amount>10.00</amount><currency>USD</currency><date>2024-01-01</date><description>Test</description></transaction></transactions>`

			_, err := parser.Parse(
				context.Background(),
				strings.NewReader(xmlData),
				job,
				tt.fieldMap,
			)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestXMLParser_Parse_MetadataPreservation(t *testing.T) {
	t.Parallel()

	parser := NewXMLParser()

	job, err := entities.NewIngestionJob(
		context.Background(),
		uuid.New(),
		uuid.New(),
		"file.xml",
		100,
	)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "external_id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}}

	xmlData := `<transactions><transaction><external_id>tx_meta</external_id><amount>100.00</amount><currency>USD</currency><date>2024-01-01</date><description>Metadata test</description><custom_field>custom_value</custom_field><reference>REF123</reference></transaction></transactions>`

	result, err := parser.Parse(context.Background(), strings.NewReader(xmlData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)

	tx := result.Transactions[0]
	require.NotNil(t, tx.Metadata)
	assert.Equal(t, "custom_value", tx.Metadata["custom_field"])
	assert.Equal(t, "REF123", tx.Metadata["reference"])
}

func TestXMLParser_ParseStreaming(t *testing.T) {
	t.Parallel()

	validFieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "external_id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}}

	createJob := func(t *testing.T) *entities.IngestionJob {
		t.Helper()

		job, err := entities.NewIngestionJob(
			context.Background(),
			uuid.New(),
			uuid.New(),
			"file.xml",
			100,
		)
		require.NoError(t, err)

		return job
	}

	t.Run("streams large XML in chunks", func(t *testing.T) {
		t.Parallel()

		var xmlBuilder strings.Builder
		xmlBuilder.WriteString("<transactions>")

		for i := range 100 {
			xmlBuilder.WriteString(
				fmt.Sprintf(
					"<transaction><external_id>tx-%d</external_id><amount>10.00</amount><currency>USD</currency><date>2024-01-01</date><description>Test</description></transaction>",
					i,
				),
			)
		}

		xmlBuilder.WriteString("</transactions>")

		parser := NewXMLParser()
		job := createJob(t)
		reader := strings.NewReader(xmlBuilder.String())

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

		xmlData := `<transactions><transaction><external_id>tx1</external_id><amount>10.00</amount><currency>USD</currency><date>2024-01-01</date></transaction><transaction><external_id>tx2</external_id><amount>20.00</amount><currency>USD</currency><date>2024-01-02</date></transaction></transactions>`
		parser := NewXMLParser()
		job := createJob(t)
		reader := strings.NewReader(xmlData)

		_, err := parser.ParseStreaming(
			context.Background(),
			reader,
			job,
			validFieldMap,
			1,
			func(_ []*shared.Transaction, _ []ports.ParseError) error {
				return errTestCallbackFailed
			},
		)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "chunk callback failed")
	})

	t.Run("nil reader returns error", func(t *testing.T) {
		t.Parallel()

		parser := NewXMLParser()
		job := createJob(t)

		_, err := parser.ParseStreaming(
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

		parser := NewXMLParser()
		reader := strings.NewReader(`<transactions></transactions>`)

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

		xmlData := `<transactions><transaction><external_id>tx1</external_id><amount>10.00</amount><currency>USD</currency><date>2024-01-01</date></transaction></transactions>`
		parser := NewXMLParser()
		job := createJob(t)
		reader := strings.NewReader(xmlData)

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

		xmlData := `<transactions>
			<transaction><external_id>tx1</external_id><amount>10.00</amount><currency>USD</currency><date>2024-06-15</date></transaction>
			<transaction><external_id>tx2</external_id><amount>20.00</amount><currency>USD</currency><date>2024-01-01</date></transaction>
			<transaction><external_id>tx3</external_id><amount>30.00</amount><currency>USD</currency><date>2024-12-31</date></transaction>
		</transactions>`
		parser := NewXMLParser()
		job := createJob(t)
		reader := strings.NewReader(xmlData)

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

		xmlData := `<transactions>
			<transaction><external_id>tx1</external_id><currency>USD</currency><date>2024-01-01</date></transaction>
			<transaction><external_id>tx2</external_id><amount>10.00</amount><currency>USD</currency><date>2024-01-02</date></transaction>
			<transaction><external_id>tx3</external_id><currency>USD</currency><date>2024-01-03</date></transaction>
		</transactions>`
		parser := NewXMLParser()
		job := createJob(t)
		reader := strings.NewReader(xmlData)

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

	t.Run("row element also recognized", func(t *testing.T) {
		t.Parallel()

		xmlData := `<data><row><external_id>tx1</external_id><amount>50.00</amount><currency>EUR</currency><date>2024-03-15</date></row></data>`
		parser := NewXMLParser()
		job := createJob(t)
		reader := strings.NewReader(xmlData)

		var received []*shared.Transaction

		result, err := parser.ParseStreaming(
			context.Background(),
			reader,
			job,
			validFieldMap,
			10,
			func(chunk []*shared.Transaction, _ []ports.ParseError) error {
				received = append(received, chunk...)

				return nil
			},
		)

		require.NoError(t, err)
		assert.Equal(t, 1, result.TotalRecords)
		require.Len(t, received, 1)
		assert.Equal(t, "tx1", received[0].ExternalID)
	})
}
