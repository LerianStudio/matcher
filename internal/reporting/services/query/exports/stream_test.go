//go:build unit

package exports

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

var (
	errTestWriteFailed           = errors.New("write failed")
	errTestConditionalWriteError = errors.New("conditional write error")
)

func TestNewStreamingCSVWriter(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	writer := NewStreamingCSVWriter(buf)

	assert.NotNil(t, writer)
	assert.NotNil(t, writer.writer)
	assert.False(t, writer.headerWritten)
	assert.Equal(t, int64(0), writer.recordCount)
}

func TestStreamingCSVWriter_WriteMatchedHeader(t *testing.T) {
	t.Parallel()

	t.Run("writes header once", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingCSVWriter(buf)

		err := writer.WriteMatchedHeader()
		require.NoError(t, err)
		assert.True(t, writer.headerWritten)

		err = writer.WriteMatchedHeader()
		require.NoError(t, err)

		require.NoError(t, writer.Flush())

		reader := csv.NewReader(strings.NewReader(buf.String()))
		rows, err := reader.ReadAll()
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(
			t,
			[]string{"transaction_id", "match_group_id", "source_id", "amount", "currency", "date"},
			rows[0],
		)
	})
}

func TestStreamingCSVWriter_WriteMatchedRow(t *testing.T) {
	t.Parallel()

	t.Run("writes valid row", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingCSVWriter(buf)

		txID := uuid.New()
		matchGroupID := uuid.New()
		sourceID := uuid.New()
		date := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

		item := &entities.MatchedItem{
			TransactionID: txID,
			MatchGroupID:  matchGroupID,
			SourceID:      sourceID,
			Amount:        decimal.NewFromFloat(100.50),
			Currency:      "USD",
			Date:          date,
		}

		require.NoError(t, writer.WriteMatchedHeader())
		err := writer.WriteMatchedRow(item)
		require.NoError(t, err)
		require.NoError(t, writer.Flush())

		assert.Equal(t, int64(1), writer.RecordCount())

		reader := csv.NewReader(strings.NewReader(buf.String()))
		rows, err := reader.ReadAll()
		require.NoError(t, err)
		require.Len(t, rows, 2)
		assert.Equal(t, txID.String(), rows[1][0])
		assert.Equal(t, "100.5", rows[1][3])
		assert.Equal(t, "USD", rows[1][4])
	})

	t.Run("skips nil item", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingCSVWriter(buf)

		err := writer.WriteMatchedRow(nil)
		require.NoError(t, err)
		assert.Equal(t, int64(0), writer.RecordCount())
	})

	t.Run("writes multiple rows", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingCSVWriter(buf)

		require.NoError(t, writer.WriteMatchedHeader())

		for i := 0; i < 100; i++ {
			item := &entities.MatchedItem{
				TransactionID: uuid.New(),
				MatchGroupID:  uuid.New(),
				SourceID:      uuid.New(),
				Amount:        decimal.NewFromInt(int64(i)),
				Currency:      "USD",
				Date:          time.Now().UTC(),
			}
			require.NoError(t, writer.WriteMatchedRow(item))
		}

		require.NoError(t, writer.Flush())
		assert.Equal(t, int64(100), writer.RecordCount())

		reader := csv.NewReader(strings.NewReader(buf.String()))
		rows, err := reader.ReadAll()
		require.NoError(t, err)
		assert.Len(t, rows, 101)
	})
}

func TestStreamingCSVWriter_WriteUnmatchedHeader(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	writer := NewStreamingCSVWriter(buf)

	err := writer.WriteUnmatchedHeader()
	require.NoError(t, err)
	assert.True(t, writer.headerWritten)

	require.NoError(t, writer.Flush())

	reader := csv.NewReader(strings.NewReader(buf.String()))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(
		t,
		[]string{
			"transaction_id",
			"source_id",
			"amount",
			"currency",
			"status",
			"date",
			"exception_id",
			"due_at",
		},
		rows[0],
	)
}

func TestStreamingCSVWriter_WriteUnmatchedRow(t *testing.T) {
	t.Parallel()

	t.Run("writes row with all fields", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingCSVWriter(buf)

		exceptionID := uuid.New()
		dueAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

		item := &entities.UnmatchedItem{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromFloat(250.00),
			Currency:      "EUR",
			Status:        "PENDING",
			Date:          time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
			ExceptionID:   &exceptionID,
			DueAt:         &dueAt,
		}

		require.NoError(t, writer.WriteUnmatchedHeader())
		err := writer.WriteUnmatchedRow(item)
		require.NoError(t, err)
		require.NoError(t, writer.Flush())

		reader := csv.NewReader(strings.NewReader(buf.String()))
		rows, err := reader.ReadAll()
		require.NoError(t, err)
		require.Len(t, rows, 2)
		assert.Equal(t, exceptionID.String(), rows[1][6])
		assert.Equal(t, "2026-02-01T00:00:00Z", rows[1][7])
	})

	t.Run("writes row with nil optional fields", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingCSVWriter(buf)

		item := &entities.UnmatchedItem{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromFloat(100.00),
			Currency:      "USD",
			Status:        "UNMATCHED",
			Date:          time.Now().UTC(),
			ExceptionID:   nil,
			DueAt:         nil,
		}

		require.NoError(t, writer.WriteUnmatchedHeader())
		err := writer.WriteUnmatchedRow(item)
		require.NoError(t, err)
		require.NoError(t, writer.Flush())

		reader := csv.NewReader(strings.NewReader(buf.String()))
		rows, err := reader.ReadAll()
		require.NoError(t, err)
		require.Len(t, rows, 2)
		assert.Empty(t, rows[1][6])
		assert.Empty(t, rows[1][7])
	})

	t.Run("skips nil item", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingCSVWriter(buf)

		err := writer.WriteUnmatchedRow(nil)
		require.NoError(t, err)
		assert.Equal(t, int64(0), writer.RecordCount())
	})
}

func TestStreamingCSVWriter_WriteVarianceHeader(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	writer := NewStreamingCSVWriter(buf)

	err := writer.WriteVarianceHeader()
	require.NoError(t, err)
	assert.True(t, writer.headerWritten)

	require.NoError(t, writer.Flush())

	reader := csv.NewReader(strings.NewReader(buf.String()))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(
		t,
		[]string{
			"source_id",
			"currency",
			"fee_type",
			"total_expected",
			"total_actual",
			"net_variance",
			"variance_pct",
		},
		rows[0],
	)
}

func TestStreamingCSVWriter_WriteVarianceRow(t *testing.T) {
	t.Parallel()

	t.Run("writes row with variance percentage", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingCSVWriter(buf)

		variancePct := decimal.NewFromFloat(5.25)
		row := &entities.VarianceReportRow{
			SourceID:      uuid.New(),
			Currency:      "USD",
			FeeType:       "PERCENTAGE",
			TotalExpected: decimal.NewFromInt(1000),
			TotalActual:   decimal.NewFromInt(1050),
			NetVariance:   decimal.NewFromInt(50),
			VariancePct:   &variancePct,
		}

		require.NoError(t, writer.WriteVarianceHeader())
		err := writer.WriteVarianceRow(row)
		require.NoError(t, err)
		require.NoError(t, writer.Flush())

		reader := csv.NewReader(strings.NewReader(buf.String()))
		rows, err := reader.ReadAll()
		require.NoError(t, err)
		require.Len(t, rows, 2)
		assert.Equal(t, "5.25", rows[1][6])
	})

	t.Run("writes row with nil variance percentage", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingCSVWriter(buf)

		row := &entities.VarianceReportRow{
			SourceID:      uuid.New(),
			Currency:      "EUR",
			FeeType:       "FLAT",
			TotalExpected: decimal.Zero,
			TotalActual:   decimal.NewFromInt(100),
			NetVariance:   decimal.NewFromInt(100),
			VariancePct:   nil,
		}

		require.NoError(t, writer.WriteVarianceHeader())
		err := writer.WriteVarianceRow(row)
		require.NoError(t, err)
		require.NoError(t, writer.Flush())

		reader := csv.NewReader(strings.NewReader(buf.String()))
		rows, err := reader.ReadAll()
		require.NoError(t, err)
		require.Len(t, rows, 2)
		assert.Empty(t, rows[1][6])
	})

	t.Run("skips nil row", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingCSVWriter(buf)

		err := writer.WriteVarianceRow(nil)
		require.NoError(t, err)
		assert.Equal(t, int64(0), writer.RecordCount())
	})
}

func TestStreamingCSVWriter_Flush(t *testing.T) {
	t.Parallel()

	t.Run("flushes successfully", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingCSVWriter(buf)

		require.NoError(t, writer.WriteMatchedHeader())
		err := writer.Flush()
		require.NoError(t, err)
		assert.Contains(t, buf.String(), "transaction_id")
	})

	t.Run("returns error on write failure", func(t *testing.T) {
		t.Parallel()

		ew := &errorWriter{err: errTestWriteFailed}
		writer := NewStreamingCSVWriter(ew)

		_ = writer.WriteMatchedHeader()
		err := writer.Flush()
		require.Error(t, err)
	})
}

func TestStreamingCSVWriter_RecordCount(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	writer := NewStreamingCSVWriter(buf)

	assert.Equal(t, int64(0), writer.RecordCount())

	item := &entities.MatchedItem{
		TransactionID: uuid.New(),
		MatchGroupID:  uuid.New(),
		SourceID:      uuid.New(),
		Amount:        decimal.NewFromInt(100),
		Currency:      "USD",
		Date:          time.Now().UTC(),
	}

	_ = writer.WriteMatchedRow(item)
	assert.Equal(t, int64(1), writer.RecordCount())

	_ = writer.WriteMatchedRow(item)
	assert.Equal(t, int64(2), writer.RecordCount())
}

func TestNewStreamingJSONWriter(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	writer := NewStreamingJSONWriter(buf)

	assert.NotNil(t, writer)
	assert.NotNil(t, writer.writer)
	assert.NotNil(t, writer.encoder)
	assert.True(t, writer.firstRow)
	assert.Equal(t, int64(0), writer.recordCount)
}

func TestStreamingJSONWriter_WriteArrayStartEnd(t *testing.T) {
	t.Parallel()

	t.Run("writes empty array", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingJSONWriter(buf)

		err := writer.WriteArrayStart()
		require.NoError(t, err)

		err = writer.WriteArrayEnd()
		require.NoError(t, err)

		assert.Equal(t, "[]", buf.String())
	})

	t.Run("returns error on write array start failure", func(t *testing.T) {
		t.Parallel()

		ew := &errorWriter{err: errTestWriteFailed}
		writer := NewStreamingJSONWriter(ew)

		err := writer.WriteArrayStart()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "write array start")
	})

	t.Run("returns error on write array end failure", func(t *testing.T) {
		t.Parallel()

		ew := &errorWriter{err: errTestWriteFailed}
		writer := NewStreamingJSONWriter(ew)

		err := writer.WriteArrayEnd()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "write array end")
	})
}

func TestStreamingJSONWriter_WriteRow(t *testing.T) {
	t.Parallel()

	t.Run("writes single row", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingJSONWriter(buf)

		require.NoError(t, writer.WriteArrayStart())

		row := map[string]string{"name": "test", "value": "123"}
		err := writer.WriteRow(row)
		require.NoError(t, err)

		require.NoError(t, writer.WriteArrayEnd())

		var result []map[string]string
		require.NoError(t, json.Unmarshal(buf.Bytes(), &result))
		require.Len(t, result, 1)
		assert.Equal(t, "test", result[0]["name"])
		assert.Equal(t, int64(1), writer.RecordCount())
	})

	t.Run("writes multiple rows with commas", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingJSONWriter(buf)

		require.NoError(t, writer.WriteArrayStart())

		require.NoError(t, writer.WriteRow(map[string]int{"id": 1}))
		require.NoError(t, writer.WriteRow(map[string]int{"id": 2}))
		require.NoError(t, writer.WriteRow(map[string]int{"id": 3}))

		require.NoError(t, writer.WriteArrayEnd())

		var result []map[string]int
		require.NoError(t, json.Unmarshal(buf.Bytes(), &result))
		require.Len(t, result, 3)
		assert.Equal(t, int64(3), writer.RecordCount())
	})

	t.Run("returns error on comma write failure", func(t *testing.T) {
		t.Parallel()

		ew := &conditionalErrorWriter{failAfter: 1}
		writer := NewStreamingJSONWriter(ew)

		require.NoError(t, writer.WriteRow(map[string]int{"id": 1}))

		err := writer.WriteRow(map[string]int{"id": 2})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "write comma")
	})

	t.Run("returns error on marshal failure", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingJSONWriter(buf)

		err := writer.WriteRow(make(chan int))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "marshal row")
	})
}

func TestStreamingJSONWriter_RecordCount(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	writer := NewStreamingJSONWriter(buf)

	assert.Equal(t, int64(0), writer.RecordCount())

	_ = writer.WriteRow(map[string]int{"id": 1})
	assert.Equal(t, int64(1), writer.RecordCount())
}

func TestNewStreamingXMLWriter(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	writer := NewStreamingXMLWriter(buf)

	assert.NotNil(t, writer)
	assert.NotNil(t, writer.writer)
	assert.NotNil(t, writer.encoder)
	assert.Equal(t, int64(0), writer.recordCount)
}

func TestStreamingXMLWriter_WriteHeaderFooter(t *testing.T) {
	t.Parallel()

	t.Run("writes XML declaration and root element", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingXMLWriter(buf)

		err := writer.WriteHeader("items")
		require.NoError(t, err)

		err = writer.WriteFooter("items")
		require.NoError(t, err)

		output := buf.String()
		assert.Contains(t, output, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
		assert.Contains(t, output, "<items>")
		assert.Contains(t, output, "</items>")
	})

	t.Run("returns error on header write failure", func(t *testing.T) {
		t.Parallel()

		ew := &errorWriter{err: errTestWriteFailed}
		writer := NewStreamingXMLWriter(ew)

		err := writer.WriteHeader("root")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "write xml header")
	})

	t.Run("returns error on footer write failure", func(t *testing.T) {
		t.Parallel()

		ew := &errorWriter{err: errTestWriteFailed}
		writer := NewStreamingXMLWriter(ew)

		err := writer.WriteFooter("root")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "write root end")
	})
}

func TestStreamingXMLWriter_WriteRow(t *testing.T) {
	t.Parallel()

	type TestItem struct {
		XMLName xml.Name `xml:"item"`
		ID      string   `xml:"id"`
		Value   int      `xml:"value"`
	}

	t.Run("writes row as XML element", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingXMLWriter(buf)

		require.NoError(t, writer.WriteHeader("items"))

		item := TestItem{ID: "test-123", Value: 42}
		err := writer.WriteRow("item", item)
		require.NoError(t, err)

		require.NoError(t, writer.Flush())
		require.NoError(t, writer.WriteFooter("items"))

		output := buf.String()
		assert.Contains(t, output, "<item>")
		assert.Contains(t, output, "<id>test-123</id>")
		assert.Contains(t, output, "<value>42</value>")
		assert.Equal(t, int64(1), writer.RecordCount())
	})

	t.Run("writes multiple rows", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingXMLWriter(buf)

		require.NoError(t, writer.WriteHeader("items"))

		for i := 0; i < 10; i++ {
			require.NoError(t, writer.WriteRow("item", TestItem{ID: "id", Value: i}))
		}

		require.NoError(t, writer.Flush())
		require.NoError(t, writer.WriteFooter("items"))

		assert.Equal(t, int64(10), writer.RecordCount())
	})

	t.Run("returns error when writer fails", func(t *testing.T) {
		t.Parallel()

		ew := &errorWriter{err: errTestWriteFailed}
		writer := NewStreamingXMLWriter(ew)

		err := writer.WriteRow("item", TestItem{ID: "test-err", Value: 1})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "encode element")
	})
}

func TestStreamingXMLWriter_Flush(t *testing.T) {
	t.Parallel()

	t.Run("flushes successfully", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		writer := NewStreamingXMLWriter(buf)

		require.NoError(t, writer.WriteHeader("root"))
		err := writer.Flush()
		require.NoError(t, err)
	})
}

func TestStreamingXMLWriter_RecordCount(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	writer := NewStreamingXMLWriter(buf)

	assert.Equal(t, int64(0), writer.RecordCount())

	_ = writer.WriteRow("item", struct{ ID int }{ID: 1})
	assert.Equal(t, int64(1), writer.RecordCount())
}

func TestStreamingCSVWriter_EmptyStream(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	writer := NewStreamingCSVWriter(buf)

	require.NoError(t, writer.WriteMatchedHeader())
	require.NoError(t, writer.Flush())

	reader := csv.NewReader(strings.NewReader(buf.String()))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(0), writer.RecordCount())
}

func TestStreamingCSVWriter_LargeDataSet(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	writer := NewStreamingCSVWriter(buf)

	require.NoError(t, writer.WriteMatchedHeader())

	const rowCount = 10000

	for i := 0; i < rowCount; i++ {
		item := &entities.MatchedItem{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(int64(i)),
			Currency:      "USD",
			Date:          time.Now().UTC(),
		}
		require.NoError(t, writer.WriteMatchedRow(item))
	}

	require.NoError(t, writer.Flush())
	assert.Equal(t, int64(rowCount), writer.RecordCount())

	reader := csv.NewReader(strings.NewReader(buf.String()))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, rows, rowCount+1)
}

func TestStreamingJSONWriter_EmptyArray(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	writer := NewStreamingJSONWriter(buf)

	require.NoError(t, writer.WriteArrayStart())
	require.NoError(t, writer.WriteArrayEnd())

	var result []any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))
	assert.Empty(t, result)
	assert.Equal(t, int64(0), writer.RecordCount())
}

func TestStreamingJSONWriter_LargeDataSet(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	writer := NewStreamingJSONWriter(buf)

	require.NoError(t, writer.WriteArrayStart())

	const rowCount = 1000

	for i := 0; i < rowCount; i++ {
		require.NoError(t, writer.WriteRow(map[string]int{"id": i}))
	}

	require.NoError(t, writer.WriteArrayEnd())

	var result []map[string]int
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))
	assert.Len(t, result, rowCount)
	assert.Equal(t, int64(rowCount), writer.RecordCount())
}

func TestStreamingXMLWriter_LargeDataSet(t *testing.T) {
	t.Parallel()

	type TestItem struct {
		XMLName xml.Name `xml:"item"`
		ID      int      `xml:"id"`
	}

	type Items struct {
		Items []TestItem `xml:"item"`
	}

	buf := &bytes.Buffer{}
	writer := NewStreamingXMLWriter(buf)

	require.NoError(t, writer.WriteHeader("items"))

	const rowCount = 1000
	for i := 0; i < rowCount; i++ {
		require.NoError(t, writer.WriteRow("item", TestItem{ID: i}))
	}

	require.NoError(t, writer.Flush())
	require.NoError(t, writer.WriteFooter("items"))

	var result Items
	require.NoError(t, xml.Unmarshal(buf.Bytes(), &result))
	assert.Len(t, result.Items, rowCount)
	assert.Equal(t, int64(rowCount), writer.RecordCount())
}

type errorWriter struct {
	err error
}

func (ew *errorWriter) Write(_ []byte) (int, error) {
	return 0, ew.err
}

type conditionalErrorWriter struct {
	writes    int
	failAfter int
}

func (cew *conditionalErrorWriter) Write(data []byte) (int, error) {
	cew.writes++
	if cew.writes > cew.failAfter {
		return 0, errTestConditionalWriteError
	}

	return len(data), nil
}
