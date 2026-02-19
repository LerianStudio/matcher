// Package parsers provides file format parsers for ingestion.
package parsers

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// CSVParser implements Parser for CSV format files.
type CSVParser struct{}

// NewCSVParser creates a new CSV parser instance.
func NewCSVParser() *CSVParser {
	return &CSVParser{}
}

// SupportedFormat returns the format this parser supports.
func (p *CSVParser) SupportedFormat() string {
	return "csv"
}

// Parse reads CSV data and converts it to transactions.
//
//nolint:varnamelen // receiver name matches consistent pattern
func (p *CSVParser) Parse(
	ctx context.Context,
	reader io.Reader,
	job *entities.IngestionJob,
	fieldMap *shared.FieldMap,
) (*ports.ParseResult, error) {
	result := &ports.ParseResult{
		Transactions: make([]*shared.Transaction, 0),
		Errors:       make([]ports.ParseError, 0),
	}

	streamResult, err := p.ParseStreaming(
		ctx,
		reader,
		job,
		fieldMap,
		ports.DefaultChunkSize,
		func(chunk []*shared.Transaction, chunkErrors []ports.ParseError) error {
			result.Transactions = append(result.Transactions, chunk...)
			result.Errors = append(result.Errors, chunkErrors...)

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	result.DateRange = streamResult.DateRange

	return result, nil
}

// ParseStreaming reads CSV data in chunks to minimize memory usage.
//
//nolint:gocognit,gocyclo,cyclop // parsing logic requires multiple validation branches
func (p *CSVParser) ParseStreaming(
	ctx context.Context,
	reader io.Reader,
	job *entities.IngestionJob,
	fieldMap *shared.FieldMap,
	chunkSize int,
	callback ports.ChunkCallback,
) (*ports.StreamingParseResult, error) {
	if reader == nil {
		return nil, errReaderRequired
	}

	if job == nil {
		return nil, errMissingIngestionJob
	}

	if chunkSize <= 0 {
		chunkSize = ports.DefaultChunkSize
	}

	mapping, err := mappingFromFieldMap(fieldMap)
	if err != nil {
		return nil, err
	}

	bomFreeReader, err := StripBOM(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to strip BOM from csv input: %w", err)
	}

	csvReader := csv.NewReader(bomFreeReader)
	csvReader.TrimLeadingSpace = true

	headers, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read csv headers: %w", err)
	}

	for i, header := range headers {
		headers[i] = strings.TrimSpace(header)
	}

	result := &ports.StreamingParseResult{}

	var dateRange *ports.DateRange

	rowNumber := 1
	chunk := make([]*shared.Transaction, 0, chunkSize)
	chunkErrors := make([]ports.ParseError, 0)

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("csv parsing cancelled: %w", ctx.Err())
		default:
		}

		record, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			chunkErrors = append(chunkErrors, ports.ParseError{
				Row:     rowNumber + 1,
				Message: "failed to read csv row",
			})
			rowNumber++

			continue
		}

		rowNumber++

		row := make(map[string]any, len(headers))
		for i, header := range headers {
			if i < len(record) {
				row[header] = sanitizeCSVValue(strings.TrimSpace(record[i]))
			} else {
				row[header] = ""
			}
		}

		if len(record) > len(headers) {
			chunkErrors = append(chunkErrors, ports.ParseError{
				Row:     rowNumber,
				Message: "extra columns ignored",
			})
		}

		transaction, parseErr := normalizeTransaction(job, mapping, row, rowNumber)
		if parseErr != nil {
			chunkErrors = append(chunkErrors, *parseErr)

			continue
		}

		chunk = append(chunk, transaction)
		dateRange = updateDateRange(dateRange, transaction.Date)
		result.TotalRecords++

		if len(chunk) >= chunkSize {
			if err := callback(chunk, chunkErrors); err != nil {
				return nil, fmt.Errorf("chunk callback failed: %w", err)
			}

			if result.FirstBatchErrs == nil && len(chunkErrors) > 0 {
				result.FirstBatchErrs = chunkErrors
			}

			result.TotalErrors += len(chunkErrors)
			chunk = make([]*shared.Transaction, 0, chunkSize)
			chunkErrors = make([]ports.ParseError, 0)
		}
	}

	if len(chunk) > 0 || len(chunkErrors) > 0 {
		if err := callback(chunk, chunkErrors); err != nil {
			return nil, fmt.Errorf("chunk callback failed: %w", err)
		}

		if result.FirstBatchErrs == nil && len(chunkErrors) > 0 {
			result.FirstBatchErrs = chunkErrors
		}

		result.TotalErrors += len(chunkErrors)
	}

	result.DateRange = dateRange

	return result, nil
}

// sanitizeCSVValue removes formula injection characters from CSV values.
// Spreadsheet applications (Excel, Google Sheets) treat cells starting with
// =, +, -, @, tab, or carriage return as formulas, which can be exploited.
// Numeric values (including negative/positive numbers) are preserved.
func sanitizeCSVValue(value string) string {
	if len(value) == 0 {
		return value
	}

	switch value[0] {
	case '=', '@', '\t', '\r':
		return "'" + value
	case '+', '-':
		if isNumericString(value) {
			return value
		}

		return "'" + value
	default:
		return value
	}
}

// isNumericString checks if a string represents a valid numeric value.
// Accepts formats like: 123, -123, +123, 12.34, .12, 12., 1e10, 1.5E-3, etc.
func isNumericString(value string) bool {
	if len(value) == 0 {
		return false
	}

	start := skipSign(value)
	if start >= len(value) {
		return false
	}

	return scanMantissa(value, start)
}

// skipSign returns the index after an optional leading '+' or '-'.
func skipSign(value string) int {
	if len(value) == 0 {
		return 0
	}

	if value[0] == '+' || value[0] == '-' {
		return 1
	}

	return 0
}

// scanMantissa scans digit and dot characters starting at pos,
// delegating to consumeExponent when 'e'/'E' is encountered.
func scanMantissa(value string, pos int) bool {
	hasDigit := false
	hasDot := false

	for i := pos; i < len(value); i++ {
		char := value[i]

		switch {
		case char >= '0' && char <= '9':
			hasDigit = true
		case char == '.' && !hasDot:
			hasDot = true
		case (char == 'e' || char == 'E') && hasDigit:
			return consumeExponent(value, i+1)
		default:
			return false
		}
	}

	return hasDigit
}

// consumeExponent validates the exponent part of a numeric string starting at
// position pos (immediately after the 'e'/'E'). It expects an optional sign
// followed by one or more digits.
func consumeExponent(value string, pos int) bool {
	if pos < len(value) && (value[pos] == '+' || value[pos] == '-') {
		pos++
	}

	hasDigit := false

	for pos < len(value) {
		if value[pos] < '0' || value[pos] > '9' {
			return false
		}

		hasDigit = true
		pos++
	}

	return hasDigit
}
