//go:build unit

package parsers

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func FuzzCSVParser(f *testing.F) {
	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
	}}

	f.Add("id,amount,currency,date\n1,10.00,USD,2024-01-01\n")
	f.Add("id,amount,currency,date\n1,INVALID,USD,2024-01-01\n")

	f.Fuzz(func(t *testing.T, data string) {
		parser := NewCSVParser()
		ctx := t.Context()

		localJob, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 100)
		if err != nil {
			return
		}

		_, err = parser.Parse(ctx, strings.NewReader(data), localJob, fieldMap)
		if err != nil {
			return
		}
	})
}

func FuzzJSONParser(f *testing.F) {
	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
	}}

	f.Add(`[{"id":"1","amount":"5.00","currency":"USD","date":"2024-01-02"}]`)
	f.Add(`{"id":"1","amount":"INVALID","currency":"USD","date":"2024-01-02"}`)

	f.Fuzz(func(t *testing.T, data string) {
		parser := NewJSONParser()
		ctx := t.Context()

		localJob, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.json", 100)
		if err != nil {
			return
		}

		_, err = parser.Parse(ctx, strings.NewReader(data), localJob, fieldMap)
		if err != nil {
			return
		}
	})
}

func FuzzXMLParser(f *testing.F) {
	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "external_id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
	}}

	f.Add(
		`<transactions><transaction><external_id>1</external_id><amount>9.99</amount><currency>USD</currency><date>2024-01-03</date></transaction></transactions>`,
	)
	f.Add(
		`<transactions><transaction><external_id>1</external_id><amount>bad</amount><currency>USD</currency><date>2024-01-03</date></transaction></transactions>`,
	)

	f.Fuzz(func(t *testing.T, data string) {
		parser := NewXMLParser()
		ctx := t.Context()

		localJob, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.xml", 100)
		if err != nil {
			return
		}

		_, err = parser.Parse(ctx, strings.NewReader(data), localJob, fieldMap)
		if err != nil {
			return
		}
	})
}
