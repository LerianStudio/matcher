package parsers

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// XMLParser implements Parser for XML format files.
type XMLParser struct{}

// NewXMLParser creates a new XML parser instance.
func NewXMLParser() *XMLParser {
	return &XMLParser{}
}

// SupportedFormat returns the format this parser supports.
func (p *XMLParser) SupportedFormat() string {
	return "xml"
}

// Parse reads XML data and converts it to transactions.
// It delegates to ParseStreaming for memory-efficient processing.
//
//nolint:varnamelen // receiver name matches consistent pattern
func (p *XMLParser) Parse(
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

// xmlStreamState tracks state during XML streaming.
type xmlStreamState struct {
	result         *ports.StreamingParseResult
	dateRange      *ports.DateRange
	chunk          []*shared.Transaction
	chunkErrors    []ports.ParseError
	current        map[string]any
	currentElement string
	rowNumber      int
	chunkSize      int
}

// validateStreamingInputs validates inputs for ParseStreaming and returns the mapping.
func validateStreamingInputs(
	reader io.Reader,
	job *entities.IngestionJob,
	callback ports.ChunkCallback,
	fieldMap *shared.FieldMap,
) (map[string]string, error) {
	if reader == nil {
		return nil, errReaderRequired
	}

	if job == nil {
		return nil, errMissingIngestionJob
	}

	if callback == nil {
		return nil, errCallbackRequired
	}

	return mappingFromFieldMap(fieldMap)
}

// ParseStreaming reads XML data in chunks to minimize memory usage.
// Processes transaction/row elements incrementally without loading entire file.
//
//nolint:varnamelen // receiver name matches consistent pattern
func (p *XMLParser) ParseStreaming(
	ctx context.Context,
	reader io.Reader,
	job *entities.IngestionJob,
	fieldMap *shared.FieldMap,
	chunkSize int,
	callback ports.ChunkCallback,
) (*ports.StreamingParseResult, error) {
	mapping, err := validateStreamingInputs(reader, job, callback, fieldMap)
	if err != nil {
		return nil, err
	}

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant: %w", err)
	}

	if chunkSize <= 0 {
		chunkSize = ports.DefaultChunkSize
	}

	state := &xmlStreamState{
		result:      &ports.StreamingParseResult{},
		chunk:       make([]*shared.Transaction, 0, chunkSize),
		chunkErrors: make([]ports.ParseError, 0),
		chunkSize:   chunkSize,
	}

	decoder := xml.NewDecoder(reader)

	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("xml parsing cancelled: %w", err)
		}

		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("failed to decode xml: %w", err)
		}

		if err := p.handleXMLToken(ctx, tenantID, token, job, mapping, state, callback); err != nil {
			return nil, err
		}
	}

	if len(state.chunk) > 0 || len(state.chunkErrors) > 0 {
		if err := flushXMLChunk(state, callback); err != nil {
			return nil, err
		}
	}

	state.result.DateRange = state.dateRange

	return state.result, nil
}

// handleXMLToken processes a single XML token and updates state.
//
//nolint:varnamelen // receiver name matches consistent pattern
func (p *XMLParser) handleXMLToken(
	ctx context.Context,
	tenantID uuid.UUID,
	token xml.Token,
	job *entities.IngestionJob,
	mapping map[string]string,
	state *xmlStreamState,
	callback ports.ChunkCallback,
) error {
	switch typed := token.(type) {
	case xml.StartElement:
		state.currentElement = typed.Name.Local
		if IsXMLRecordElement(state.currentElement) {
			state.current = make(map[string]any)
		}
	case xml.EndElement:
		if err := p.handleEndElement(ctx, tenantID, typed, job, mapping, state, callback); err != nil {
			return err
		}
	case xml.CharData:
		handleCharData(typed, state)
	}

	return nil
}

// handleEndElement processes an XML end element token.
func (p *XMLParser) handleEndElement(
	ctx context.Context,
	tenantID uuid.UUID,
	elem xml.EndElement,
	job *entities.IngestionJob,
	mapping map[string]string,
	state *xmlStreamState,
	callback ports.ChunkCallback,
) error {
	defer func() { state.currentElement = "" }()

	if state.current == nil || !IsXMLRecordElement(elem.Name.Local) {
		return nil
	}

	state.rowNumber++

	transaction, parseErr := normalizeTransaction(ctx, tenantID, job, mapping, state.current, state.rowNumber)
	if parseErr != nil {
		state.chunkErrors = append(state.chunkErrors, *parseErr)
	} else {
		state.chunk = append(state.chunk, transaction)
		state.dateRange = updateDateRange(state.dateRange, transaction.Date)
		state.result.TotalRecords++
	}

	state.current = nil

	if len(state.chunk) >= state.chunkSize {
		return flushXMLChunk(state, callback)
	}

	return nil
}

// handleCharData processes XML character data.
func handleCharData(data xml.CharData, state *xmlStreamState) {
	if state.current == nil || state.currentElement == "" {
		return
	}

	value := strings.TrimSpace(string(data))
	if value == "" {
		return
	}

	state.current[state.currentElement] = value
}

// flushXMLChunk sends the current chunk via callback and resets state.
func flushXMLChunk(state *xmlStreamState, callback ports.ChunkCallback) error {
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
