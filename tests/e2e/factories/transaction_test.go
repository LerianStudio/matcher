//go:build e2e

package factories

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionData_Structure(t *testing.T) {
	td := TransactionData{
		ID:          "txn-001",
		Amount:      "100.50",
		Currency:    "USD",
		Date:        "2024-01-15",
		Description: "Test payment",
	}

	assert.Equal(t, "txn-001", td.ID)
	assert.Equal(t, "100.50", td.Amount)
	assert.Equal(t, "USD", td.Currency)
	assert.Equal(t, "2024-01-15", td.Date)
	assert.Equal(t, "Test payment", td.Description)
}

func TestNewCSVBuilder(t *testing.T) {
	builder := NewCSVBuilder("test-prefix")

	require.NotNil(t, builder)
	assert.Equal(t, "test-prefix", builder.prefix)
	assert.Empty(t, builder.rows)
}

func TestCSVBuilder_AddRow(t *testing.T) {
	builder := NewCSVBuilder("pfx").
		AddRow("001", "100.00", "USD", "2024-01-01", "Payment 1").
		AddRow("002", "200.00", "EUR", "2024-01-02", "Payment 2")

	assert.Len(t, builder.rows, 2)
	assert.Equal(t, "pfx-001", builder.rows[0].ID)
	assert.Equal(t, "pfx-002", builder.rows[1].ID)
}

func TestCSVBuilder_AddRowRaw(t *testing.T) {
	builder := NewCSVBuilder("ignored").
		AddRowRaw("exact-id-001", "100.00", "USD", "2024-01-01", "Raw payment")

	assert.Len(t, builder.rows, 1)
	assert.Equal(t, "exact-id-001", builder.rows[0].ID)
}

func TestCSVBuilder_AddRowf(t *testing.T) {
	builder := NewCSVBuilder("fmt").
		AddRowf("TXN-%d", 150.75, "GBP", "2024-02-15", "Formatted payment %d", 42)

	assert.Len(t, builder.rows, 1)
	assert.Equal(t, "fmt-TXN-42", builder.rows[0].ID)
	assert.Equal(t, "150.75", builder.rows[0].Amount)
	assert.Equal(t, "GBP", builder.rows[0].Currency)
	assert.Equal(t, "Formatted payment 42", builder.rows[0].Description)
}

func TestCSVBuilder_Build(t *testing.T) {
	builder := NewCSVBuilder("test").
		AddRow("001", "100.00", "USD", "2024-01-01", "Payment one").
		AddRow("002", "200.00", "EUR", "2024-01-02", "Payment two")

	csv := builder.Build()

	assert.NotEmpty(t, csv)
	content := string(csv)

	lines := strings.Split(content, "\n")
	assert.GreaterOrEqual(t, len(lines), 3)

	assert.Equal(t, "id,amount,currency,date,description", lines[0])
	assert.Equal(t, "test-001,100.00,USD,2024-01-01,Payment one", lines[1])
	assert.Equal(t, "test-002,200.00,EUR,2024-01-02,Payment two", lines[2])
}

func TestCSVBuilder_Build_EmptyBuilder(t *testing.T) {
	builder := NewCSVBuilder("empty")

	csv := builder.Build()

	content := string(csv)
	assert.Contains(t, content, "id,amount,currency,date,description")
	lines := strings.Split(strings.TrimSpace(content), "\n")
	assert.Len(t, lines, 1)
}

func TestCSVBuilder_Chaining(t *testing.T) {
	csv := NewCSVBuilder("chain").
		AddRow("1", "10.00", "USD", "2024-01-01", "First").
		AddRow("2", "20.00", "USD", "2024-01-02", "Second").
		AddRowRaw("raw-3", "30.00", "USD", "2024-01-03", "Third").
		Build()

	content := string(csv)
	assert.Contains(t, content, "chain-1")
	assert.Contains(t, content, "chain-2")
	assert.Contains(t, content, "raw-3")
}

func TestNewJSONBuilder(t *testing.T) {
	builder := NewJSONBuilder("json-prefix")

	require.NotNil(t, builder)
	assert.Equal(t, "json-prefix", builder.prefix)
	assert.Empty(t, builder.rows)
}

func TestJSONBuilder_AddRow(t *testing.T) {
	builder := NewJSONBuilder("jpfx").
		AddRow("001", "100.00", "USD", "2024-01-01", "JSON Payment 1").
		AddRow("002", "200.00", "EUR", "2024-01-02", "JSON Payment 2")

	assert.Len(t, builder.rows, 2)
	assert.Equal(t, "jpfx-001", builder.rows[0].ID)
	assert.Equal(t, "jpfx-002", builder.rows[1].ID)
}

func TestJSONBuilder_AddRowRaw(t *testing.T) {
	builder := NewJSONBuilder("ignored").
		AddRowRaw("exact-json-id", "150.00", "GBP", "2024-03-01", "Raw JSON")

	assert.Len(t, builder.rows, 1)
	assert.Equal(t, "exact-json-id", builder.rows[0].ID)
}

func TestJSONBuilder_Build(t *testing.T) {
	builder := NewJSONBuilder("test").
		AddRow("001", "100.00", "USD", "2024-01-01", "Payment one").
		AddRow("002", "200.00", "EUR", "2024-01-02", "Payment two")

	jsonData := builder.Build()

	assert.NotEmpty(t, jsonData)

	var parsed []map[string]string
	err := json.Unmarshal(jsonData, &parsed)
	require.NoError(t, err)

	assert.Len(t, parsed, 2)
	assert.Equal(t, "test-001", parsed[0]["id"])
	assert.Equal(t, "100.00", parsed[0]["amount"])
	assert.Equal(t, "USD", parsed[0]["currency"])
	assert.Equal(t, "2024-01-01", parsed[0]["date"])
	assert.Equal(t, "Payment one", parsed[0]["description"])

	assert.Equal(t, "test-002", parsed[1]["id"])
	assert.Equal(t, "200.00", parsed[1]["amount"])
	assert.Equal(t, "EUR", parsed[1]["currency"])
}

func TestJSONBuilder_Build_Empty(t *testing.T) {
	builder := NewJSONBuilder("empty")

	jsonData := builder.Build()

	assert.Equal(t, "[]", string(jsonData))

	var parsed []any
	err := json.Unmarshal(jsonData, &parsed)
	require.NoError(t, err)
	assert.Empty(t, parsed)
}

func TestJSONBuilder_Build_SingleRow(t *testing.T) {
	builder := NewJSONBuilder("single").
		AddRow("001", "50.00", "CAD", "2024-05-01", "Single entry")

	jsonData := builder.Build()

	var parsed []map[string]string
	err := json.Unmarshal(jsonData, &parsed)
	require.NoError(t, err)

	assert.Len(t, parsed, 1)
	assert.Equal(t, "single-001", parsed[0]["id"])
}

func TestQuickCSV(t *testing.T) {
	csv := QuickCSV("quick", "txn1", "99.99", "USD", "2024-06-01", "Quick payment")

	content := string(csv)
	assert.Contains(t, content, "id,amount,currency,date,description")
	assert.Contains(t, content, "quick-txn1")
	assert.Contains(t, content, "99.99")
	assert.Contains(t, content, "USD")
	assert.Contains(t, content, "2024-06-01")
	assert.Contains(t, content, "Quick payment")
}

func TestQuickJSON(t *testing.T) {
	jsonData := QuickJSON("qjson", "txn2", "75.50", "GBP", "2024-07-01", "Quick JSON")

	var parsed []map[string]string
	err := json.Unmarshal(jsonData, &parsed)
	require.NoError(t, err)

	assert.Len(t, parsed, 1)
	assert.Equal(t, "qjson-txn2", parsed[0]["id"])
	assert.Equal(t, "75.50", parsed[0]["amount"])
	assert.Equal(t, "GBP", parsed[0]["currency"])
	assert.Equal(t, "2024-07-01", parsed[0]["date"])
	assert.Equal(t, "Quick JSON", parsed[0]["description"])
}

func TestMatchingPairCSV(t *testing.T) {
	ledger, bank := MatchingPairCSV("match", "TX001", "500.00", "USD", "2024-08-01")

	ledgerContent := string(ledger)
	bankContent := string(bank)

	assert.Contains(t, ledgerContent, "id,amount,currency,date,description")
	assert.Contains(t, ledgerContent, "match-LEDGER-TX001")
	assert.Contains(t, ledgerContent, "500.00")
	assert.Contains(t, ledgerContent, "USD")
	assert.Contains(t, ledgerContent, "ledger entry")

	assert.Contains(t, bankContent, "id,amount,currency,date,description")
	assert.Contains(t, bankContent, "match-bank-tx001")
	assert.Contains(t, bankContent, "500.00")
	assert.Contains(t, bankContent, "USD")
	assert.Contains(t, bankContent, "bank statement")
}

func TestMatchingPairCSV_CaseDifference(t *testing.T) {
	ledger, bank := MatchingPairCSV("TEST", "ABC", "100.00", "EUR", "2024-01-01")

	ledgerContent := string(ledger)
	bankContent := string(bank)

	assert.Contains(t, ledgerContent, "TEST-LEDGER-ABC")
	assert.Contains(t, bankContent, "test-bank-abc")
}

func TestCSVBuilder_ValidCSVFormat(t *testing.T) {
	builder := NewCSVBuilder("validate").
		AddRow("001", "100.00", "USD", "2024-01-15", "Test transaction")

	csv := builder.Build()
	content := string(csv)

	lines := strings.Split(content, "\n")

	headerFields := strings.Split(lines[0], ",")
	assert.Len(t, headerFields, 5)
	assert.Equal(t, "id", headerFields[0])
	assert.Equal(t, "amount", headerFields[1])
	assert.Equal(t, "currency", headerFields[2])
	assert.Equal(t, "date", headerFields[3])
	assert.Equal(t, "description", headerFields[4])

	dataFields := strings.Split(lines[1], ",")
	assert.Len(t, dataFields, 5)
}

func TestJSONBuilder_ValidJSONFormat(t *testing.T) {
	builder := NewJSONBuilder("validate").
		AddRow("001", "100.00", "USD", "2024-01-15", "Test transaction").
		AddRow("002", "200.00", "EUR", "2024-01-16", "Another transaction")

	jsonData := builder.Build()

	var result any
	err := json.Unmarshal(jsonData, &result)
	require.NoError(t, err)

	arr, ok := result.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 2)
}

func TestCSVBuilder_SpecialCharacters(t *testing.T) {
	builder := NewCSVBuilder("special").
		AddRowRaw("id-with-dash", "100.00", "USD", "2024-01-01", "Description with spaces")

	csv := builder.Build()
	content := string(csv)

	assert.Contains(t, content, "id-with-dash")
	assert.Contains(t, content, "Description with spaces")
}

func TestJSONBuilder_MultipleFields(t *testing.T) {
	builder := NewJSONBuilder("multi").
		AddRow("T1", "1.00", "USD", "2024-01-01", "One").
		AddRow("T2", "2.00", "EUR", "2024-01-02", "Two").
		AddRow("T3", "3.00", "GBP", "2024-01-03", "Three")

	jsonData := builder.Build()

	var parsed []map[string]string
	err := json.Unmarshal(jsonData, &parsed)
	require.NoError(t, err)

	assert.Len(t, parsed, 3)

	for _, item := range parsed {
		assert.Contains(t, item, "id")
		assert.Contains(t, item, "amount")
		assert.Contains(t, item, "currency")
		assert.Contains(t, item, "date")
		assert.Contains(t, item, "description")
	}
}
