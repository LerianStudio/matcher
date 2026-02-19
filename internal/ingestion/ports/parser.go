package ports

import (
	"context"
	"io"
	"time"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// DefaultChunkSize is the default number of records to process per chunk.
const DefaultChunkSize = 1000

// ParseResult contains parsed transactions and any row-level errors.
type ParseResult struct {
	Transactions []*shared.Transaction
	Errors       []ParseError
	DateRange    *DateRange
}

// ChunkCallback is called for each chunk of parsed transactions.
// Returns an error to stop processing.
type ChunkCallback func(chunk []*shared.Transaction, errors []ParseError) error

// StreamingParseResult contains aggregated stats from streaming parse.
type StreamingParseResult struct {
	TotalRecords   int
	TotalErrors    int
	DateRange      *DateRange
	FirstBatchErrs []ParseError
}

// DateRange captures earliest and latest transaction dates.
type DateRange struct {
	Start time.Time
	End   time.Time
}

// ParseError represents a parsing error for a specific row.
type ParseError struct {
	Row     int    `json:"row"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

// Parser defines the interface for file format parsers.
type Parser interface {
	// Parse reads from the reader and returns normalized transactions.
	// Uses FieldMap.Mapping to map source fields to canonical Transaction fields.
	// Expected mapping structure:
	//   {
	//     "external_id": "source_field_for_id",
	//     "amount": "source_field_for_amount",
	//     "currency": "source_field_for_currency",
	//     "date": "source_field_for_date",
	//     "description": "source_field_for_description"
	//   }
	Parse(
		ctx context.Context,
		reader io.Reader,
		job *entities.IngestionJob,
		fieldMap *shared.FieldMap,
	) (*ParseResult, error)

	// SupportedFormat returns the file format this parser handles (e.g., "csv", "json", "xml")
	SupportedFormat() string
}

// StreamingParser extends Parser with chunk-based processing for memory efficiency.
type StreamingParser interface {
	Parser

	// ParseStreaming reads from the reader and calls the callback for each chunk.
	// This avoids loading the entire file into memory.
	ParseStreaming(
		ctx context.Context,
		reader io.Reader,
		job *entities.IngestionJob,
		fieldMap *shared.FieldMap,
		chunkSize int,
		callback ChunkCallback,
	) (*StreamingParseResult, error)
}

// ParserRegistry allows selecting parser by format.
type ParserRegistry interface {
	GetParser(format string) (Parser, error)
	Register(parser Parser)
}
