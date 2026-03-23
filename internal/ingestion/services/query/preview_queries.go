package query

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v4/commons/security"

	valueObjects "github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/sanitize"
)

// TODO(telemetry): ingestion/adapters/http/handlers.go — logSpanError uses HandleSpanError for
// business outcomes (badRequest, notFound, writeNotFound). Add logSpanBusinessEvent using
// HandleSpanBusinessErrorEvent and create business-aware variants for 400/404 responses.
// See reporting/adapters/http/handlers_export_job.go for the reference implementation.

const (
	defaultPreviewMaxRows = 5
	maxPreviewMaxRows     = 20
)

// Preview errors.
var (
	ErrPreviewReaderRequired = errors.New("reader is required for preview")
	ErrPreviewFormatRequired = errors.New("format is required for preview")
	ErrPreviewInvalidFormat  = errors.New("invalid format for preview: must be csv, json, or xml")
	ErrPreviewEmptyFile      = errors.New("file is empty or has no content")
	ErrPreviewInvalidJSON    = errors.New("json payload must be an object or array of objects")
)

// FilePreviewResult contains extracted column names and sample rows from a file.
type FilePreviewResult struct {
	Columns     []string   `json:"columns"`
	SampleRows  [][]string `json:"sampleRows"`
	RowCount    int        `json:"rowCount"`
	Format      string     `json:"format"`
	ParseErrors int        `json:"parse_errors,omitempty"`
}

// PreviewFile parses a file and returns column headers and sample rows without persisting anything.
//
//nolint:cyclop // format dispatch with validation
func (uc *UseCase) PreviewFile(
	ctx context.Context,
	reader io.Reader,
	format string,
	maxRows int,
) (*FilePreviewResult, error) {
	if uc == nil {
		return nil, ErrNilUseCase
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "query.ingestion.preview_file")
	defer span.End()

	if reader == nil {
		return nil, ErrPreviewReaderRequired
	}

	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		return nil, ErrPreviewFormatRequired
	}

	if format != "csv" && format != "json" && format != "xml" {
		return nil, ErrPreviewInvalidFormat
	}

	if maxRows <= 0 {
		maxRows = defaultPreviewMaxRows
	}

	if maxRows > maxPreviewMaxRows {
		maxRows = maxPreviewMaxRows
	}

	var result *FilePreviewResult

	var err error

	switch format {
	case "csv":
		result, err = previewCSV(ctx, reader, maxRows)
	case "json":
		result, err = previewJSON(ctx, reader, maxRows)
	case "xml":
		result, err = previewXML(ctx, reader, maxRows)
	}

	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to preview file", err)

		return nil, fmt.Errorf("preview file: %w", err)
	}

	if result.ParseErrors > 0 {
		logger.With(libLog.Any("skipped_rows", result.ParseErrors)).Log(ctx, libLog.LevelDebug, "preview file: skipped malformed rows")
	}

	result.Format = format
	redactSensitivePreviewColumns(result)

	return result, nil
}

// previewCSV reads CSV headers and up to maxRows of sample data.
func previewCSV(ctx context.Context, reader io.Reader, maxRows int) (*FilePreviewResult, error) {
	bomFreeReader, err := valueObjects.StripUTF8BOM(reader)
	if err != nil {
		return nil, fmt.Errorf("strip bom from csv preview: %w", err)
	}

	csvReader := csv.NewReader(bomFreeReader)
	csvReader.TrimLeadingSpace = true

	headers, err := csvReader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ErrPreviewEmptyFile
		}

		return nil, fmt.Errorf("read csv headers: %w", err)
	}

	for i, h := range headers {
		headers[i] = strings.TrimSpace(h)
	}

	rows := make([][]string, 0, maxRows)

	var parseErrors int

	for range maxRows {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		record, readErr := csvReader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}

			parseErrors++

			continue
		}

		row := make([]string, len(headers))
		for i := range headers {
			if i < len(record) {
				row[i] = sanitize.SanitizeFormulaInjection(strings.TrimSpace(record[i]))
			}
		}

		rows = append(rows, row)
	}

	return &FilePreviewResult{
		Columns:     headers,
		SampleRows:  rows,
		RowCount:    len(rows),
		ParseErrors: parseErrors,
	}, nil
}

// previewJSON reads JSON array or object and extracts column names and sample rows.
func previewJSON(ctx context.Context, reader io.Reader, maxRows int) (*FilePreviewResult, error) {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()

	token, err := decoder.Token()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ErrPreviewEmptyFile
		}

		return nil, fmt.Errorf("read json token: %w", err)
	}

	delim, isDelim := token.(json.Delim)

	if isDelim && delim == '[' {
		return previewJSONArray(ctx, decoder, maxRows)
	}

	if isDelim && delim == '{' {
		return previewJSONObject(ctx, decoder)
	}

	return nil, ErrPreviewInvalidJSON
}

func previewJSONArray(ctx context.Context, decoder *json.Decoder, maxRows int) (*FilePreviewResult, error) {
	var allColumns []string

	columnSet := make(map[string]bool)

	// Collect raw objects so we can rebuild rows after sorting columns.
	objects := make([]map[string]any, 0, maxRows)

	var parseErrors int

	for decoder.More() && len(objects) < maxRows {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		var obj map[string]any
		if err := decoder.Decode(&obj); err != nil {
			parseErrors++

			continue
		}

		for key := range obj {
			if !columnSet[key] {
				columnSet[key] = true
				allColumns = append(allColumns, key)
			}
		}

		objects = append(objects, obj)
	}

	if len(allColumns) == 0 {
		return nil, ErrPreviewEmptyFile
	}

	sort.Strings(allColumns)

	// Build rows with deterministic column order, sanitizing against formula injection.
	rows := make([][]string, 0, len(objects))

	for _, obj := range objects {
		row := make([]string, len(allColumns))
		for i, col := range allColumns {
			row[i] = sanitize.SanitizeFormulaInjection(formatJSONValue(obj[col]))
		}

		rows = append(rows, row)
	}

	return &FilePreviewResult{
		Columns:     allColumns,
		SampleRows:  rows,
		RowCount:    len(rows),
		ParseErrors: parseErrors,
	}, nil
}

func previewJSONObject(ctx context.Context, decoder *json.Decoder) (*FilePreviewResult, error) {
	var obj map[string]any

	fields := make(map[string]any)

	for decoder.More() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		keyToken, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("reading json key: %w", err)
		}

		key, ok := keyToken.(string)
		if !ok {
			continue
		}

		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("reading json value: %w", err)
		}

		fields[key] = value
	}

	obj = fields

	columns := make([]string, 0, len(obj))
	for key := range obj {
		columns = append(columns, key)
	}

	sort.Strings(columns)

	row := make([]string, 0, len(obj))
	for _, key := range columns {
		row = append(row, sanitize.SanitizeFormulaInjection(formatJSONValue(obj[key])))
	}

	if len(columns) == 0 {
		return nil, ErrPreviewEmptyFile
	}

	return &FilePreviewResult{
		Columns:    columns,
		SampleRows: [][]string{row},
		RowCount:   1,
	}, nil
}

func formatJSONValue(v any) string {
	if v == nil {
		return ""
	}

	switch typed := v.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return fmt.Sprintf("%v", typed)
	case bool:
		return strconv.FormatBool(typed)
	default:
		return fmt.Sprintf("%v", typed)
	}
}

// previewXML reads XML elements and extracts field names from the first N record elements.
//
//nolint:gocognit,gocyclo,cyclop // XML streaming parser is inherently complex
func previewXML(ctx context.Context, reader io.Reader, maxRows int) (*FilePreviewResult, error) {
	decoder := xml.NewDecoder(reader)

	var allColumns []string

	columnSet := make(map[string]bool)
	rows := make([][]string, 0, maxRows)

	var current map[string]string

	var currentElement string

parseLoop:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("decode xml: %w", err)
		}

		switch typed := token.(type) {
		case xml.StartElement:
			name := typed.Name.Local
			if valueObjects.IsXMLRecordElement(name) {
				current = make(map[string]string)
			} else if current != nil {
				currentElement = name
			}
		case xml.EndElement:
			name := typed.Name.Local
			if valueObjects.IsXMLRecordElement(name) && current != nil {
				for key := range current {
					if !columnSet[key] {
						columnSet[key] = true
						allColumns = append(allColumns, key)
					}
				}

				row := make([]string, len(allColumns))
				for i, col := range allColumns {
					row[i] = current[col]
				}

				rows = append(rows, row)
				current = nil

				if len(rows) >= maxRows {
					break parseLoop
				}
			}

			currentElement = ""
		case xml.CharData:
			if current != nil && currentElement != "" {
				value := strings.TrimSpace(string(typed))
				if value != "" {
					current[currentElement] = sanitize.SanitizeFormulaInjection(value)
				}
			}
		}
	}

	// Pad earlier rows
	for i := range rows {
		for len(rows[i]) < len(allColumns) {
			rows[i] = append(rows[i], "")
		}
	}

	if len(allColumns) == 0 {
		return nil, ErrPreviewEmptyFile
	}

	return &FilePreviewResult{
		Columns:     allColumns,
		SampleRows:  rows,
		RowCount:    len(rows),
		ParseErrors: 0,
	}, nil
}

// sensitivePreviewRedaction is the placeholder value shown for sensitive
// column values in file preview responses.
const sensitivePreviewRedaction = "***REDACTED***"

// redactSensitivePreviewColumns replaces sample row values with a redaction
// placeholder for every column whose name is classified as sensitive by
// security.IsSensitiveField. Column names are kept intact so the user can
// still see which fields exist, but their values are never exposed.
func redactSensitivePreviewColumns(result *FilePreviewResult) {
	if result == nil || len(result.Columns) == 0 {
		return
	}

	// Build a set of column indices that need redaction.
	sensitiveIdx := make([]int, 0, len(result.Columns))
	for i, col := range result.Columns {
		if security.IsSensitiveField(col) {
			sensitiveIdx = append(sensitiveIdx, i)
		}
	}

	if len(sensitiveIdx) == 0 {
		return
	}

	for _, row := range result.SampleRows {
		for _, idx := range sensitiveIdx {
			if idx < len(row) {
				row[idx] = sensitivePreviewRedaction
			}
		}
	}
}
