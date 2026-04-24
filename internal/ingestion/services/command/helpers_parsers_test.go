// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

type errorParser struct{}

func (errorParser) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return nil, errParse
}

func (errorParser) SupportedFormat() string { return "csv" }

type parserWithRange struct{}

func (parserWithRange) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return &ports.ParseResult{
		Transactions: []*shared.Transaction{
			{
				ID:               uuid.New(),
				IngestionJobID:   uuid.New(),
				SourceID:         uuid.New(),
				ExternalID:       "ext",
				Amount:           decimal.RequireFromString("10"),
				Currency:         "USD",
				Date:             time.Now().UTC(),
				ExtractionStatus: shared.ExtractionStatusComplete,
				Status:           shared.TransactionStatusMatched,
			},
		},
		DateRange: &ports.DateRange{Start: time.Now().UTC(), End: time.Now().UTC()},
	}, nil
}

func (parserWithRange) SupportedFormat() string { return "csv" }

// failingRegistry returns an error when getting a parser.
type failingRegistry struct{}

func (failingRegistry) GetParser(_ string) (ports.Parser, error) { return nil, errGetParser }
func (failingRegistry) Register(_ ports.Parser)                  {}

type fakeStreamingParser struct {
	result     *ports.StreamingParseResult
	parseErr   error
	callbackFn func(chunk []*shared.Transaction, errors []ports.ParseError) error
}

func (f fakeStreamingParser) Parse(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
) (*ports.ParseResult, error) {
	return &ports.ParseResult{}, nil
}

func (f fakeStreamingParser) SupportedFormat() string { return "csv" }

func (f fakeStreamingParser) ParseStreaming(
	_ context.Context,
	_ io.Reader,
	_ *entities.IngestionJob,
	_ *shared.FieldMap,
	_ int,
	callback ports.ChunkCallback,
) (*ports.StreamingParseResult, error) {
	if f.parseErr != nil {
		return nil, f.parseErr
	}

	if f.callbackFn != nil {
		if err := f.callbackFn(nil, nil); err != nil {
			return nil, err
		}
	}

	return f.result, nil
}
