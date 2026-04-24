// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package exports provides report export builders for CSV and PDF formats.
package exports

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"time"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

// StreamingCSVWriter writes CSV data incrementally to avoid memory pressure.
type StreamingCSVWriter struct {
	writer        *csv.Writer
	headerWritten bool
	recordCount   int64
}

// NewStreamingCSVWriter creates a new streaming CSV writer.
func NewStreamingCSVWriter(w io.Writer) *StreamingCSVWriter {
	return &StreamingCSVWriter{
		writer: csv.NewWriter(w),
	}
}

// WriteMatchedHeader writes the CSV header for matched items.
func (writer *StreamingCSVWriter) WriteMatchedHeader() error {
	if writer.headerWritten {
		return nil
	}

	if err := writer.writer.Write([]string{"transaction_id", "match_group_id", "source_id", "amount", "currency", "date"}); err != nil {
		return errWriteCSVHeader(err)
	}

	writer.headerWritten = true

	return nil
}

// WriteMatchedRow writes a single matched item row.
func (writer *StreamingCSVWriter) WriteMatchedRow(item *entities.MatchedItem) error {
	if item == nil {
		return nil
	}

	if err := writer.writer.Write([]string{
		sanitizeCSVValue(item.TransactionID.String()),
		sanitizeCSVValue(item.MatchGroupID.String()),
		sanitizeCSVValue(item.SourceID.String()),
		sanitizeCSVValue(item.Amount.String()),
		sanitizeCSVValue(item.Currency),
		sanitizeCSVValue(item.Date.UTC().Format(time.RFC3339)),
	}); err != nil {
		return errWriteCSVRow(err)
	}

	writer.recordCount++

	return nil
}

// WriteUnmatchedHeader writes the CSV header for unmatched items.
func (writer *StreamingCSVWriter) WriteUnmatchedHeader() error {
	if writer.headerWritten {
		return nil
	}

	if err := writer.writer.Write([]string{"transaction_id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}); err != nil {
		return errWriteCSVHeader(err)
	}

	writer.headerWritten = true

	return nil
}

// WriteUnmatchedRow writes a single unmatched item row.
func (writer *StreamingCSVWriter) WriteUnmatchedRow(item *entities.UnmatchedItem) error {
	if item == nil {
		return nil
	}

	exceptionID := ""
	if item.ExceptionID != nil {
		exceptionID = item.ExceptionID.String()
	}

	dueAt := ""
	if item.DueAt != nil {
		dueAt = item.DueAt.UTC().Format(time.RFC3339)
	}

	if err := writer.writer.Write([]string{
		sanitizeCSVValue(item.TransactionID.String()),
		sanitizeCSVValue(item.SourceID.String()),
		sanitizeCSVValue(item.Amount.String()),
		sanitizeCSVValue(item.Currency),
		sanitizeCSVValue(item.Status),
		sanitizeCSVValue(item.Date.UTC().Format(time.RFC3339)),
		sanitizeCSVValue(exceptionID),
		sanitizeCSVValue(dueAt),
	}); err != nil {
		return errWriteCSVRow(err)
	}

	writer.recordCount++

	return nil
}

// WriteVarianceHeader writes the CSV header for variance items.
func (writer *StreamingCSVWriter) WriteVarianceHeader() error {
	if writer.headerWritten {
		return nil
	}

	if err := writer.writer.Write([]string{"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance", "variance_pct"}); err != nil {
		return errWriteCSVHeader(err)
	}

	writer.headerWritten = true

	return nil
}

// WriteVarianceRow writes a single variance row.
func (writer *StreamingCSVWriter) WriteVarianceRow(row *entities.VarianceReportRow) error {
	if row == nil {
		return nil
	}

	variancePct := ""
	if row.VariancePct != nil {
		variancePct = row.VariancePct.StringFixed(variancePctDecimalPlaces)
	}

	if err := writer.writer.Write([]string{
		sanitizeCSVValue(row.SourceID.String()),
		sanitizeCSVValue(row.Currency),
		sanitizeCSVValue(row.FeeScheduleID.String()),
		sanitizeCSVValue(row.FeeScheduleName),
		sanitizeCSVValue(row.TotalExpected.String()),
		sanitizeCSVValue(row.TotalActual.String()),
		sanitizeCSVValue(row.NetVariance.String()),
		sanitizeCSVValue(variancePct),
	}); err != nil {
		return errWriteCSVRow(err)
	}

	writer.recordCount++

	return nil
}

// Flush flushes any buffered data to the underlying writer.
func (writer *StreamingCSVWriter) Flush() error {
	writer.writer.Flush()

	if err := writer.writer.Error(); err != nil {
		return errFlushCSV(err)
	}

	return nil
}

// RecordCount returns the number of records written.
func (writer *StreamingCSVWriter) RecordCount() int64 {
	return writer.recordCount
}

// StreamingJSONWriter writes JSON array data incrementally.
type StreamingJSONWriter struct {
	writer      io.Writer
	encoder     *json.Encoder
	firstRow    bool
	recordCount int64
}

// NewStreamingJSONWriter creates a new streaming JSON writer.
func NewStreamingJSONWriter(w io.Writer) *StreamingJSONWriter {
	return &StreamingJSONWriter{
		writer:   w,
		encoder:  json.NewEncoder(w),
		firstRow: true,
	}
}

// WriteArrayStart writes the opening bracket of a JSON array.
func (jsonWriter *StreamingJSONWriter) WriteArrayStart() error {
	if _, err := jsonWriter.writer.Write([]byte("[")); err != nil {
		return fmt.Errorf("write array start: %w", err)
	}

	return nil
}

// WriteArrayEnd writes the closing bracket of a JSON array.
func (jsonWriter *StreamingJSONWriter) WriteArrayEnd() error {
	if _, err := jsonWriter.writer.Write([]byte("]")); err != nil {
		return fmt.Errorf("write array end: %w", err)
	}

	return nil
}

// WriteRow writes a single row as a JSON object.
func (jsonWriter *StreamingJSONWriter) WriteRow(row any) error {
	if !jsonWriter.firstRow {
		if _, err := jsonWriter.writer.Write([]byte(",")); err != nil {
			return fmt.Errorf("write comma: %w", err)
		}
	}

	jsonWriter.firstRow = false

	data, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("marshal row: %w", err)
	}

	if _, err := jsonWriter.writer.Write(data); err != nil {
		return fmt.Errorf("write row: %w", err)
	}

	jsonWriter.recordCount++

	return nil
}

// RecordCount returns the number of records written.
func (jsonWriter *StreamingJSONWriter) RecordCount() int64 {
	return jsonWriter.recordCount
}

// StreamingXMLWriter writes XML data incrementally.
type StreamingXMLWriter struct {
	writer      io.Writer
	encoder     *xml.Encoder
	recordCount int64
}

// NewStreamingXMLWriter creates a new streaming XML writer.
func NewStreamingXMLWriter(w io.Writer) *StreamingXMLWriter {
	return &StreamingXMLWriter{
		writer:  w,
		encoder: xml.NewEncoder(w),
	}
}

// WriteHeader writes the XML declaration and root element start.
func (xmlWriter *StreamingXMLWriter) WriteHeader(rootElement string) error {
	if _, err := xmlWriter.writer.Write([]byte(xml.Header)); err != nil {
		return fmt.Errorf("write xml header: %w", err)
	}

	if _, err := fmt.Fprintf(xmlWriter.writer, "<%s>", rootElement); err != nil {
		return fmt.Errorf("write root start: %w", err)
	}

	return nil
}

// WriteFooter writes the root element end.
func (xmlWriter *StreamingXMLWriter) WriteFooter(rootElement string) error {
	if _, err := fmt.Fprintf(xmlWriter.writer, "</%s>", rootElement); err != nil {
		return fmt.Errorf("write root end: %w", err)
	}

	return nil
}

// WriteRow writes a single row as an XML element.
func (xmlWriter *StreamingXMLWriter) WriteRow(elementName string, row any) error {
	if err := xmlWriter.encoder.EncodeElement(row, xml.StartElement{Name: xml.Name{Local: elementName}}); err != nil {
		return fmt.Errorf("encode element: %w", err)
	}

	xmlWriter.recordCount++

	return nil
}

// Flush flushes any buffered data.
func (xmlWriter *StreamingXMLWriter) Flush() error {
	if err := xmlWriter.encoder.Flush(); err != nil {
		return fmt.Errorf("flush xml: %w", err)
	}

	return nil
}

// RecordCount returns the number of records written.
func (xmlWriter *StreamingXMLWriter) RecordCount() int64 {
	return xmlWriter.recordCount
}
