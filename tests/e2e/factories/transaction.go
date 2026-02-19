//go:build e2e

package factories

import (
	"fmt"
	"strings"
)

// TransactionData represents a single transaction for test data generation.
type TransactionData struct {
	ID          string
	Amount      string
	Currency    string
	Date        string
	Description string
}

// CSVBuilder builds CSV content for ingestion tests.
type CSVBuilder struct {
	prefix string
	rows   []TransactionData
}

// NewCSVBuilder creates a new CSV builder with a unique prefix.
func NewCSVBuilder(prefix string) *CSVBuilder {
	return &CSVBuilder{
		prefix: prefix,
		rows:   make([]TransactionData, 0),
	}
}

// AddRow adds a transaction row.
func (b *CSVBuilder) AddRow(id, amount, currency, date, description string) *CSVBuilder {
	b.rows = append(b.rows, TransactionData{
		ID:          fmt.Sprintf("%s-%s", b.prefix, id),
		Amount:      amount,
		Currency:    currency,
		Date:        date,
		Description: description,
	})
	return b
}

// AddRowRaw adds a transaction row without ID prefix.
func (b *CSVBuilder) AddRowRaw(id, amount, currency, date, description string) *CSVBuilder {
	b.rows = append(b.rows, TransactionData{
		ID:          id,
		Amount:      amount,
		Currency:    currency,
		Date:        date,
		Description: description,
	})
	return b
}

// AddRowf adds a transaction row with formatted values.
func (b *CSVBuilder) AddRowf(
	idPattern string,
	amount float64,
	currency, date, descPattern string,
	args ...any,
) *CSVBuilder {
	id := fmt.Sprintf(idPattern, args...)
	desc := fmt.Sprintf(descPattern, args...)
	return b.AddRow(id, fmt.Sprintf("%.2f", amount), currency, date, desc)
}

// Build generates the CSV content.
func (b *CSVBuilder) Build() []byte {
	var sb strings.Builder
	sb.WriteString("id,amount,currency,date,description\n")
	for _, row := range b.rows {
		sb.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s\n",
			row.ID, row.Amount, row.Currency, row.Date, row.Description))
	}
	return []byte(sb.String())
}

// JSONBuilder builds JSON content for ingestion tests.
type JSONBuilder struct {
	prefix string
	rows   []TransactionData
}

// NewJSONBuilder creates a new JSON builder with a unique prefix.
func NewJSONBuilder(prefix string) *JSONBuilder {
	return &JSONBuilder{
		prefix: prefix,
		rows:   make([]TransactionData, 0),
	}
}

// AddRow adds a transaction row.
func (b *JSONBuilder) AddRow(id, amount, currency, date, description string) *JSONBuilder {
	b.rows = append(b.rows, TransactionData{
		ID:          fmt.Sprintf("%s-%s", b.prefix, id),
		Amount:      amount,
		Currency:    currency,
		Date:        date,
		Description: description,
	})
	return b
}

// AddRowRaw adds a transaction row without ID prefix.
func (b *JSONBuilder) AddRowRaw(id, amount, currency, date, description string) *JSONBuilder {
	b.rows = append(b.rows, TransactionData{
		ID:          id,
		Amount:      amount,
		Currency:    currency,
		Date:        date,
		Description: description,
	})
	return b
}

// Build generates the JSON content.
func (b *JSONBuilder) Build() []byte {
	if len(b.rows) == 0 {
		return []byte("[]")
	}

	var sb strings.Builder
	sb.WriteString("[\n")
	for i, row := range b.rows {
		sb.WriteString(
			fmt.Sprintf(
				`  {"id":"%s","amount":"%s","currency":"%s","date":"%s","description":"%s"}`,
				row.ID,
				row.Amount,
				row.Currency,
				row.Date,
				row.Description,
			),
		)
		if i < len(b.rows)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString("]")
	return []byte(sb.String())
}

// QuickCSV generates simple CSV content for a single transaction.
func QuickCSV(prefix, id, amount, currency, date, description string) []byte {
	return NewCSVBuilder(prefix).AddRow(id, amount, currency, date, description).Build()
}

// QuickJSON generates simple JSON content for a single transaction.
func QuickJSON(prefix, id, amount, currency, date, description string) []byte {
	return NewJSONBuilder(prefix).AddRow(id, amount, currency, date, description).Build()
}

// MatchingPairCSV generates two CSVs with matching transactions.
func MatchingPairCSV(prefix, id, amount, currency, date string) (ledger, bank []byte) {
	ledgerID := fmt.Sprintf("%s-LEDGER-%s", prefix, id)
	bankID := fmt.Sprintf("%s-BANK-%s", prefix, id)

	ledger = NewCSVBuilder("").
		AddRowRaw(ledgerID, amount, currency, date, "ledger entry").
		Build()
	bank = NewCSVBuilder("").
		AddRowRaw(strings.ToLower(bankID), amount, currency, date, "bank statement").
		Build()

	return ledger, bank
}
