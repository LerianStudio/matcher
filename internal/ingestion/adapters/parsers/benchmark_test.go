//go:build unit

package parsers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/sanitize"
)

// Package-level sinks to prevent compiler optimizations from eliminating benchmarked code.
var (
	sinkParseResult *ports.ParseResult
	sinkString      string
	sinkBool        bool
	sinkDecimal     decimal.Decimal
)

// Package-level error sink for benchmark error checking.
var sinkError error

// --- Test Data Generators ---

func generateCSVData(rows int) string {
	var builder strings.Builder

	builder.WriteString("external_id,amount,currency,date,description\n")

	for i := 0; i < rows; i++ {
		builder.WriteString(fmt.Sprintf("TX-%d,%.2f,USD,2024-01-15,Payment %d\n",
			i+1, float64(100+i%1000)/100, i+1))
	}

	return builder.String()
}

func generateJSONData(rows int) []byte {
	type transaction struct {
		ExternalID  string `json:"external_id"`
		Amount      string `json:"amount"`
		Currency    string `json:"currency"`
		Date        string `json:"date"`
		Description string `json:"description"`
	}

	transactions := make([]transaction, rows)

	for i := 0; i < rows; i++ {
		transactions[i] = transaction{
			ExternalID:  fmt.Sprintf("TX-%d", i+1),
			Amount:      fmt.Sprintf("%.2f", float64(100+i%1000)/100),
			Currency:    "USD",
			Date:        "2024-01-15",
			Description: fmt.Sprintf("Payment %d", i+1),
		}
	}

	data, _ := json.Marshal(transactions)

	return data
}

func generateXMLData(rows int) string {
	var builder strings.Builder

	builder.WriteString("<?xml version=\"1.0\"?>\n<transactions>\n")

	for i := 0; i < rows; i++ {
		builder.WriteString(fmt.Sprintf(`  <transaction>
    <external_id>TX-%d</external_id>
    <amount>%.2f</amount>
    <currency>USD</currency>
    <date>2024-01-15</date>
    <description>Payment %d</description>
  </transaction>
`, i+1, float64(100+i%1000)/100, i+1))
	}

	builder.WriteString("</transactions>")

	return builder.String()
}

func createBenchmarkJob() *entities.IngestionJob {
	return &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Status:    value_objects.JobStatusProcessing,
		Metadata:  entities.JobMetadata{FileName: "benchmark.csv"},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		StartedAt: time.Now().UTC(),
	}
}

func createBenchmarkFieldMap() *shared.FieldMap {
	return &shared.FieldMap{
		ID:       uuid.New(),
		SourceID: uuid.New(),
		Mapping: map[string]any{
			"external_id": "external_id",
			"amount":      "amount",
			"currency":    "currency",
			"date":        "date",
			"description": "description",
		},
	}
}

// --- CSV Parser Benchmarks ---

func BenchmarkCSVParser_Parse_100Rows(b *testing.B) {
	parser := NewCSVParser()
	csvData := generateCSVData(100)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader := strings.NewReader(csvData)

		result, err := parser.Parse(ctx, reader, job, fieldMap)
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}

		sinkParseResult = result
	}
}

func BenchmarkCSVParser_Parse_1000Rows(b *testing.B) {
	parser := NewCSVParser()
	csvData := generateCSVData(1000)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader := strings.NewReader(csvData)

		result, err := parser.Parse(ctx, reader, job, fieldMap)
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}

		sinkParseResult = result
	}
}

func BenchmarkCSVParser_Parse_10000Rows(b *testing.B) {
	parser := NewCSVParser()
	csvData := generateCSVData(10000)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader := strings.NewReader(csvData)

		result, err := parser.Parse(ctx, reader, job, fieldMap)
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}

		sinkParseResult = result
	}
}

func BenchmarkCSVParser_ParseStreaming_1000Rows(b *testing.B) {
	parser := NewCSVParser()
	csvData := generateCSVData(1000)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := strings.NewReader(csvData)

		_, err := parser.ParseStreaming(ctx, reader, job, fieldMap, ports.DefaultChunkSize,
			func(chunk []*shared.Transaction, errs []ports.ParseError) error {
				return nil
			})
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}
	}
}

func BenchmarkCSVParser_ParseStreaming_10000Rows_ChunkSize100(b *testing.B) {
	parser := NewCSVParser()
	csvData := generateCSVData(10000)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := strings.NewReader(csvData)

		_, err := parser.ParseStreaming(ctx, reader, job, fieldMap, 100,
			func(chunk []*shared.Transaction, errs []ports.ParseError) error {
				return nil
			})
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}
	}
}

func BenchmarkCSVParser_ParseStreaming_10000Rows_ChunkSize1000(b *testing.B) {
	parser := NewCSVParser()
	csvData := generateCSVData(10000)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := strings.NewReader(csvData)

		_, err := parser.ParseStreaming(ctx, reader, job, fieldMap, 1000,
			func(chunk []*shared.Transaction, errs []ports.ParseError) error {
				return nil
			})
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}
	}
}

// --- JSON Parser Benchmarks ---

func BenchmarkJSONParser_Parse_100Rows(b *testing.B) {
	parser := NewJSONParser()
	jsonData := generateJSONData(100)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(jsonData)

		_, err := parser.Parse(ctx, reader, job, fieldMap)
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}
	}
}

func BenchmarkJSONParser_Parse_1000Rows(b *testing.B) {
	parser := NewJSONParser()
	jsonData := generateJSONData(1000)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(jsonData)

		_, err := parser.Parse(ctx, reader, job, fieldMap)
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}
	}
}

func BenchmarkJSONParser_Parse_10000Rows(b *testing.B) {
	parser := NewJSONParser()
	jsonData := generateJSONData(10000)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(jsonData)

		_, err := parser.Parse(ctx, reader, job, fieldMap)
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}
	}
}

func BenchmarkJSONParser_ParseStreaming_1000Rows(b *testing.B) {
	parser := NewJSONParser()
	jsonData := generateJSONData(1000)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(jsonData)

		_, err := parser.ParseStreaming(ctx, reader, job, fieldMap, ports.DefaultChunkSize,
			func(chunk []*shared.Transaction, errs []ports.ParseError) error {
				return nil
			})
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}
	}
}

// --- XML Parser Benchmarks ---

func BenchmarkXMLParser_Parse_100Rows(b *testing.B) {
	parser := NewXMLParser()
	xmlData := generateXMLData(100)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := strings.NewReader(xmlData)

		_, err := parser.Parse(ctx, reader, job, fieldMap)
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}
	}
}

func BenchmarkXMLParser_Parse_1000Rows(b *testing.B) {
	parser := NewXMLParser()
	xmlData := generateXMLData(1000)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := strings.NewReader(xmlData)

		_, err := parser.Parse(ctx, reader, job, fieldMap)
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}
	}
}

// --- Normalizer Benchmarks ---

func BenchmarkNormalizeTransaction_SingleRow(b *testing.B) {
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	mapping, _ := mappingFromFieldMap(fieldMap)

	row := map[string]any{
		"external_id": "TX-12345",
		"amount":      "1234.56",
		"currency":    "USD",
		"date":        "2024-01-15",
		"description": "Test payment",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := normalizeTransaction(job, mapping, row, 1)
		if err != nil {
			b.Fatalf("normalize failed: %v", err.Message)
		}
	}
}

func BenchmarkSanitizeFormulaInjection_SafeValue(b *testing.B) {
	value := "safe value 12345"

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sinkString = sanitizeFormulaInjection(value)
	}
}

func BenchmarkSanitizeFormulaInjection_NumericValue(b *testing.B) {
	value := "-1234.56"

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sinkString = sanitizeFormulaInjection(value)
	}
}

func BenchmarkSanitizeFormulaInjection_FormulaInjection(b *testing.B) {
	value := "=SUM(A1:A10)"

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sinkString = sanitizeFormulaInjection(value)
	}
}

func BenchmarkIsNumericString_ValidNumber(b *testing.B) {
	value := "-1234.567890"

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sinkBool = sanitize.IsNumericString(value)
	}
}

func BenchmarkIsNumericString_NonNumeric(b *testing.B) {
	value := "not a number"

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sinkBool = sanitize.IsNumericString(value)
	}
}

// --- Currency Validation Benchmarks ---

func BenchmarkIsValidCurrencyCode_ValidCode(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sinkBool = isValidCurrencyCode("USD")
	}
}

func BenchmarkIsValidCurrencyCode_InvalidCode(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sinkBool = isValidCurrencyCode("XXX")
	}
}

// --- Memory Allocation Benchmarks ---

func BenchmarkCSVParser_MemoryProfile_LargeFile(b *testing.B) {
	parser := NewCSVParser()
	csvData := generateCSVData(50000)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := strings.NewReader(csvData)
		var totalTransactions int

		_, err := parser.ParseStreaming(ctx, reader, job, fieldMap, 1000,
			func(chunk []*shared.Transaction, errs []ports.ParseError) error {
				totalTransactions += len(chunk)
				return nil
			})
		if err != nil {
			b.Fatalf("parse failed: %v", err)
		}

		if totalTransactions != 50000 {
			b.Fatalf("expected 50000 transactions, got %d", totalTransactions)
		}
	}
}

// --- Comparative Benchmarks ---

func BenchmarkParseVsStreaming_1000Rows(b *testing.B) {
	parser := NewCSVParser()
	csvData := generateCSVData(1000)
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	b.Run("Parse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader := strings.NewReader(csvData)
			_, _ = parser.Parse(ctx, reader, job, fieldMap)
		}
	})

	b.Run("ParseStreaming", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader := strings.NewReader(csvData)
			_, _ = parser.ParseStreaming(ctx, reader, job, fieldMap, ports.DefaultChunkSize,
				func(chunk []*shared.Transaction, errs []ports.ParseError) error {
					return nil
				})
		}
	})
}

func BenchmarkFormatComparison_1000Rows(b *testing.B) {
	job := createBenchmarkJob()
	fieldMap := createBenchmarkFieldMap()
	ctx := context.Background()

	csvData := generateCSVData(1000)
	jsonData := generateJSONData(1000)
	xmlData := generateXMLData(1000)

	b.Run("CSV", func(b *testing.B) {
		parser := NewCSVParser()
		for i := 0; i < b.N; i++ {
			reader := strings.NewReader(csvData)
			_, _ = parser.Parse(ctx, reader, job, fieldMap)
		}
	})

	b.Run("JSON", func(b *testing.B) {
		parser := NewJSONParser()
		for i := 0; i < b.N; i++ {
			reader := bytes.NewReader(jsonData)
			_, _ = parser.Parse(ctx, reader, job, fieldMap)
		}
	})

	b.Run("XML", func(b *testing.B) {
		parser := NewXMLParser()
		for i := 0; i < b.N; i++ {
			reader := strings.NewReader(xmlData)
			_, _ = parser.Parse(ctx, reader, job, fieldMap)
		}
	})
}

// --- Registry Benchmarks ---

func BenchmarkRegistry_GetParser(b *testing.B) {
	registry := NewParserRegistry()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, sinkError = registry.GetParser("csv")
	}
}

func BenchmarkRegistry_GetParserNotFound(b *testing.B) {
	registry := NewParserRegistry()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, sinkError = registry.GetParser("unknown")
	}
}

// --- Decimal Parsing Benchmarks ---

func BenchmarkDecimalParsing(b *testing.B) {
	amounts := []string{
		"1234.56",
		"0.001",
		"999999999.99",
		"-1234.56",
		"+1234.56",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		for _, amount := range amounts {
			sinkDecimal, sinkError = decimal.NewFromString(amount)
		}
	}
}
