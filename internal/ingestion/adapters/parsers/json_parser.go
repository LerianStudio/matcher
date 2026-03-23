// Package parsers provides file format parsers for ingestion.
package parsers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// JSONParser implements Parser for JSON format files.
type JSONParser struct{}

// NewJSONParser creates a new JSON parser instance.
func NewJSONParser() *JSONParser {
	return &JSONParser{}
}

// SupportedFormat returns the format this parser supports.
func (parser *JSONParser) SupportedFormat() string {
	return "json"
}

// Parse reads JSON data and converts it to transactions.
// It delegates to ParseStreaming for memory-efficient processing.
func (parser *JSONParser) Parse(
	ctx context.Context,
	reader io.Reader,
	job *entities.IngestionJob,
	fieldMap *shared.FieldMap,
) (*ports.ParseResult, error) {
	result := &ports.ParseResult{
		Transactions: make([]*shared.Transaction, 0),
		Errors:       make([]ports.ParseError, 0),
	}

	streamResult, err := parser.ParseStreaming(
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

// ParseStreaming reads JSON data in chunks to minimize memory usage.
// Supports JSON arrays of objects for streaming. Single objects are processed as one chunk.
func (parser *JSONParser) ParseStreaming(
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

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant: %w", err)
	}

	mapping, err := mappingFromFieldMap(fieldMap)
	if err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(reader)
	decoder.UseNumber()

	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to read json token: %w", err)
	}

	delim, isDelim := token.(json.Delim)
	if isDelim && delim == '[' {
		return parser.streamJSONArray(ctx, tenantID, decoder, job, mapping, chunkSize, callback)
	}

	if isDelim && delim == '{' {
		return parser.processSingleObject(ctx, tenantID, decoder, job, mapping, callback)
	}

	return nil, errJSONPayloadInvalid
}

// jsonStreamState tracks state during JSON array streaming.
type jsonStreamState struct {
	result      *ports.StreamingParseResult
	dateRange   *ports.DateRange
	chunk       []*shared.Transaction
	chunkErrors []ports.ParseError
	rowNumber   int
	chunkSize   int
}

// streamJSONArray processes a JSON array by streaming objects one at a time.
func (parser *JSONParser) streamJSONArray(
	ctx context.Context,
	tenantID uuid.UUID,
	decoder *json.Decoder,
	job *entities.IngestionJob,
	mapping map[string]string,
	chunkSize int,
	callback ports.ChunkCallback,
) (*ports.StreamingParseResult, error) {
	state := &jsonStreamState{
		result:      &ports.StreamingParseResult{},
		chunk:       make([]*shared.Transaction, 0, chunkSize),
		chunkErrors: make([]ports.ParseError, 0),
		chunkSize:   chunkSize,
	}

	for decoder.More() {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("json parsing cancelled: %w", err)
		}

		if err := parser.processNextArrayElement(ctx, tenantID, decoder, job, mapping, state); err != nil {
			return nil, err
		}

		if len(state.chunk) >= state.chunkSize {
			if err := flushJSONChunk(state, callback); err != nil {
				return nil, err
			}
		}
	}

	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("failed to read closing bracket: %w", err)
	}

	if len(state.chunk) > 0 || len(state.chunkErrors) > 0 {
		if err := flushJSONChunk(state, callback); err != nil {
			return nil, err
		}
	}

	state.result.DateRange = state.dateRange

	return state.result, nil
}

// processNextArrayElement decodes one JSON object from the array.
func (parser *JSONParser) processNextArrayElement(
	ctx context.Context,
	tenantID uuid.UUID,
	decoder *json.Decoder,
	job *entities.IngestionJob,
	mapping map[string]string,
	state *jsonStreamState,
) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to decode json: %w", err)
	}

	delim, isDelim := token.(json.Delim)
	if !isDelim || delim != '{' {
		return errJSONArrayNotObjects
	}

	row, err := parser.decodeObjectFields(decoder)
	if err != nil {
		return fmt.Errorf("failed to decode json: %w", err)
	}

	state.rowNumber++

	transaction, parseErr := normalizeTransaction(ctx, tenantID, job, mapping, row, state.rowNumber)
	if parseErr != nil {
		state.chunkErrors = append(state.chunkErrors, *parseErr)

		return nil
	}

	state.chunk = append(state.chunk, transaction)
	state.dateRange = updateDateRange(state.dateRange, transaction.Date)
	state.result.TotalRecords++

	return nil
}

// flushJSONChunk sends the current chunk via callback and resets state.
func flushJSONChunk(state *jsonStreamState, callback ports.ChunkCallback) error {
	if err := callback(state.chunk, state.chunkErrors); err != nil {
		return fmt.Errorf("chunk callback failed: %w", err)
	}

	if state.result.FirstBatchErrs == nil && len(state.chunkErrors) > 0 {
		state.result.FirstBatchErrs = state.chunkErrors
	}

	state.result.TotalErrors += len(state.chunkErrors)
	state.chunk = make([]*shared.Transaction, 0, state.chunkSize)
	state.chunkErrors = make([]ports.ParseError, 0)

	return nil
}

// decodeObjectFields reads key-value pairs from a JSON object (after '{' token was consumed).
func (parser *JSONParser) decodeObjectFields(decoder *json.Decoder) (map[string]any, error) {
	row := make(map[string]any)

	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("reading object key: %w", err)
		}

		key, ok := keyToken.(string)
		if !ok {
			return nil, errJSONUnexpectedKeyType
		}

		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("decoding object value: %w", err)
		}

		row[key] = value
	}

	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("reading object closing: %w", err)
	}

	return row, nil
}

// processSingleObject handles a JSON object (not array) by decoding the remaining fields.
func (parser *JSONParser) processSingleObject(
	ctx context.Context,
	tenantID uuid.UUID,
	decoder *json.Decoder,
	job *entities.IngestionJob,
	mapping map[string]string,
	callback ports.ChunkCallback,
) (*ports.StreamingParseResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("json parsing cancelled: %w", err)
	}

	row, err := parser.decodeObjectFields(decoder)
	if err != nil {
		return nil, fmt.Errorf("failed to decode json: %w", err)
	}

	result := &ports.StreamingParseResult{TotalRecords: 1}

	transaction, parseErr := normalizeTransaction(ctx, tenantID, job, mapping, row, 1)
	if parseErr != nil {
		result.TotalRecords = 0
		result.TotalErrors = 1
		result.FirstBatchErrs = []ports.ParseError{*parseErr}

		if err := callback(nil, []ports.ParseError{*parseErr}); err != nil {
			return nil, fmt.Errorf("chunk callback failed: %w", err)
		}

		return result, nil
	}

	result.DateRange = &ports.DateRange{Start: transaction.Date, End: transaction.Date}

	if err := callback([]*shared.Transaction{transaction}, nil); err != nil {
		return nil, fmt.Errorf("chunk callback failed: %w", err)
	}

	return result, nil
}
