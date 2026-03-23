//go:build unit

package parsers

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestNormalizeTransactionRequiresMappings(t *testing.T) {
	t.Parallel()

	_, err := mappingFromFieldMap(nil)
	require.Error(t, err)

	_, err = mappingFromFieldMap(&shared.FieldMap{Mapping: map[string]any{}})
	require.Error(t, err)

	_, err = mappingFromFieldMap(&shared.FieldMap{Mapping: map[string]any{"external_id": "id"}})
	require.Error(t, err)
}

func TestNormalizeTransactionSuccess(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	csvData := "id,amount,currency,date,desc\n1,10.00,USD,2024-01-01,payment\n"

	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.Empty(t, result.Errors)

	transaction := result.Transactions[0]
	require.Equal(t, "1", transaction.ExternalID)
	require.Equal(t, "USD", transaction.Currency)
	require.True(t, transaction.Amount.Equal(decimal.NewFromFloat(10.00)))
	require.Equal(t, "payment", transaction.Description)
}

func TestNormalizeTransactionRejectsInvalidCurrency(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
	}}

	csvData := "id,amount,currency,date\n1,10.00,INVALID_CURRENCY,2024-01-01\n"

	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Empty(t, result.Transactions)
	require.Len(t, result.Errors, 1)
	require.Equal(t, "currency", result.Errors[0].Field)
	require.Contains(t, result.Errors[0].Message, "ISO 4217")
}

func TestNormalizeTransactionCurrencyValidation(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
	}}

	t.Run("valid ISO 4217 code accepted", func(t *testing.T) {
		t.Parallel()

		validCSV := "id,amount,currency,date\n1,10.00,USD,2024-01-01\n"
		result, err := parser.Parse(ctx, strings.NewReader(validCSV), job, fieldMap)
		require.NoError(t, err)
		require.Len(t, result.Transactions, 1)
		require.Empty(t, result.Errors)
	})

	t.Run("lowercase converted to uppercase", func(t *testing.T) {
		t.Parallel()

		lowerCSV := "id,amount,currency,date\n1,10.00,usd,2024-01-01\n"
		result, err := parser.Parse(ctx, strings.NewReader(lowerCSV), job, fieldMap)
		require.NoError(t, err)
		require.Len(t, result.Transactions, 1)
		require.Empty(t, result.Errors)
		require.Equal(t, "USD", result.Transactions[0].Currency)
	})

	t.Run("too long currency rejected", func(t *testing.T) {
		t.Parallel()

		invalidCSV := "id,amount,currency,date\n1,10.00,USDD,2024-01-01\n"
		result, err := parser.Parse(ctx, strings.NewReader(invalidCSV), job, fieldMap)
		require.NoError(t, err)
		require.Empty(t, result.Transactions)
		require.Len(t, result.Errors, 1)
		require.Equal(t, "currency", result.Errors[0].Field)
	})

	t.Run("non-ISO code rejected", func(t *testing.T) {
		t.Parallel()

		invalidCSV := "id,amount,currency,date\n1,10.00,ABC,2024-01-01\n"
		result, err := parser.Parse(ctx, strings.NewReader(invalidCSV), job, fieldMap)
		require.NoError(t, err)
		require.Empty(t, result.Transactions)
		require.Len(t, result.Errors, 1)
		require.Equal(t, "currency", result.Errors[0].Field)
		require.Contains(t, result.Errors[0].Message, "ISO 4217")
	})
}

func TestJSONParser(t *testing.T) {
	t.Parallel()

	parser := NewJSONParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.json", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	jsonData := `[{"id":"1","amount":"5.00","currency":"USD","date":"2024-01-02","desc":"fee"}]`

	result, err := parser.Parse(ctx, strings.NewReader(jsonData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.Empty(t, result.Errors)

	transaction := result.Transactions[0]
	require.Equal(t, "1", transaction.ExternalID)
	require.Equal(t, "USD", transaction.Currency)
	require.True(t, transaction.Amount.Equal(decimal.NewFromFloat(5.00)))
	require.Equal(t, "fee", transaction.Description)

	emptyResult, err := parser.Parse(ctx, strings.NewReader(`[]`), job, fieldMap)
	require.NoError(t, err)
	require.Empty(t, emptyResult.Transactions)
	require.Empty(t, emptyResult.Errors)

	_, err = parser.Parse(ctx, strings.NewReader(`invalid`), job, fieldMap)
	require.Error(t, err)

	_, err = parser.Parse(
		ctx,
		strings.NewReader(`[{"id":"1","amount":"5.00","currency":"USD","date":"2024-01-02",}]`),
		job,
		fieldMap,
	)
	require.Error(t, err)

	partialData := `[{"id":"1","currency":"USD","date":"2024-01-02"}]`

	partialResult, err := parser.Parse(ctx, strings.NewReader(partialData), job, fieldMap)
	require.NoError(t, err)
	require.Empty(t, partialResult.Transactions)
	require.Len(t, partialResult.Errors, 1)
}

func TestXMLParser(t *testing.T) {
	t.Parallel()

	parser := NewXMLParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.xml", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "external_id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}}

	xmlData := `<transactions><transaction><external_id>1</external_id><amount>9.99</amount><currency>USD</currency><date>2024-01-03</date><description>charge</description></transaction></transactions>`

	result, err := parser.Parse(ctx, strings.NewReader(xmlData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.Empty(t, result.Errors)

	transaction := result.Transactions[0]
	require.Equal(t, "1", transaction.ExternalID)
	require.Equal(t, "USD", transaction.Currency)
	require.True(t, transaction.Amount.Equal(decimal.NewFromFloat(9.99)))
	require.Equal(t, "charge", transaction.Description)

	emptyResult, err := parser.Parse(
		ctx,
		strings.NewReader(`<transactions></transactions>`),
		job,
		fieldMap,
	)
	require.NoError(t, err)
	require.Empty(t, emptyResult.Transactions)
	require.Empty(t, emptyResult.Errors)

	_, err = parser.Parse(
		ctx,
		strings.NewReader(`<transactions><transaction><external_id>1</external_id></transactions>`),
		job,
		fieldMap,
	)
	require.Error(t, err)

	_, err = parser.Parse(
		ctx,
		strings.NewReader(
			`<transactions><transaction><external_id>1</external_id><amount>9.99</amount></transaction>`,
		),
		job,
		fieldMap,
	)
	require.Error(t, err)

	partialData := `<transactions><transaction><external_id>1</external_id><currency>USD</currency><date>2024-01-03</date></transaction></transactions>`

	partialResult, err := parser.Parse(ctx, strings.NewReader(partialData), job, fieldMap)
	require.NoError(t, err)
	require.Empty(t, partialResult.Transactions)
	require.Len(t, partialResult.Errors, 1)
}

func TestNormalizeTransactionMetadataExcludesMappedFields(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	csvData := "id,amount,currency,date,desc,extra_info,notes\n1,10.00,USD,2024-01-01,payment,some_extra,some_notes\n"

	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.Empty(t, result.Errors)

	transaction := result.Transactions[0]

	// Mapped fields must NOT appear in metadata (they are in dedicated columns)
	require.NotContains(t, transaction.Metadata, "id")
	require.NotContains(t, transaction.Metadata, "amount")
	require.NotContains(t, transaction.Metadata, "currency")
	require.NotContains(t, transaction.Metadata, "date")
	require.NotContains(t, transaction.Metadata, "desc")

	// Unmapped fields MUST appear in metadata
	require.Equal(t, "some_extra", transaction.Metadata["extra_info"])
	require.Equal(t, "some_notes", transaction.Metadata["notes"])
}

func TestNormalizeTransactionMetadataNoExtraFields(t *testing.T) {
	t.Parallel()

	parser := NewCSVParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
	}}

	// CSV with only mapped columns and no extras
	csvData := "id,amount,currency,date\n1,10.00,USD,2024-01-01\n"

	result, err := parser.Parse(ctx, strings.NewReader(csvData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)

	transaction := result.Transactions[0]

	// All fields are mapped, so metadata should be empty
	require.Empty(t, transaction.Metadata)
}

func TestJSONParser_FormulaInjectionSanitized(t *testing.T) {
	t.Parallel()

	parser := NewJSONParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.json", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "desc",
	}}

	jsonData := `[{"id":"=MALICIOUS_ID","amount":"100.00","currency":"USD","date":"2024-01-15","desc":"=HYPERLINK(\"http://evil.com\",\"Click\")","extra":"@SUM(1+1)"}]`

	result, err := parser.Parse(ctx, strings.NewReader(jsonData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.Empty(t, result.Errors)

	tx := result.Transactions[0]
	// external_id is stored verbatim (raw) for dedup hash integrity and search.
	// Formula injection is a display concern handled at output boundaries.
	assert.Equal(t, "=MALICIOUS_ID", tx.ExternalID)
	// Description should be sanitized via getStringValue
	assert.Equal(t, "'=HYPERLINK(\"http://evil.com\",\"Click\")", tx.Description)
	// Metadata string values should be sanitized via buildMetadata
	assert.Equal(t, "'@SUM(1+1)", tx.Metadata["extra"])
}

func TestXMLParser_FormulaInjectionSanitized(t *testing.T) {
	t.Parallel()

	parser := NewXMLParser()
	ctx := t.Context()

	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.xml", 100)
	require.NoError(t, err)

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "external_id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}}

	xmlData := `<transactions><transaction>` +
		`<external_id>=MALICIOUS_ID</external_id>` +
		`<amount>100.00</amount>` +
		`<currency>USD</currency>` +
		`<date>2024-01-15</date>` +
		`<description>=HYPERLINK("http://evil.com","Click")</description>` +
		`<extra>@SUM(1+1)</extra>` +
		`</transaction></transactions>`

	result, err := parser.Parse(ctx, strings.NewReader(xmlData), job, fieldMap)
	require.NoError(t, err)
	require.Len(t, result.Transactions, 1)
	require.Empty(t, result.Errors)

	tx := result.Transactions[0]
	// external_id is stored verbatim (raw) for dedup hash integrity and search.
	// Formula injection is a display concern handled at output boundaries.
	assert.Equal(t, "=MALICIOUS_ID", tx.ExternalID)
	// Description should be sanitized via getStringValue
	assert.Equal(t, "'=HYPERLINK(\"http://evil.com\",\"Click\")", tx.Description)
	// Metadata string values should be sanitized via buildMetadata
	assert.Equal(t, "'@SUM(1+1)", tx.Metadata["extra"])
}

func Test_tenantIDFromContext_ValidUUID(t *testing.T) {
	t.Parallel()

	expected := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, expected.String())

	result, err := tenantIDFromContext(ctx)

	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func Test_tenantIDFromContext_DefaultTenant(t *testing.T) {
	t.Parallel()

	// context.Background() has no tenant value set, so auth.GetTenantID
	// falls back to the default tenant ID constant.
	ctx := context.Background()

	result, err := tenantIDFromContext(ctx)

	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, result, "should not return uuid.Nil for default tenant")

	expected, parseErr := uuid.Parse(auth.DefaultTenantID)
	require.NoError(t, parseErr)
	require.Equal(t, expected, result)
}

func Test_tenantIDFromContext_InvalidUUIDString(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")

	result, err := tenantIDFromContext(ctx)

	require.Error(t, err)
	require.Equal(t, uuid.Nil, result)
	require.Contains(t, err.Error(), "not-a-valid-uuid")
}
