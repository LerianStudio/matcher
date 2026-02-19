package common

import (
	"database/sql"
	"time"

	"github.com/shopspring/decimal"
)

// JobPostgreSQLModel maps to ingestion_jobs table (000001_init_schema.up.sql:71-81).
type JobPostgreSQLModel struct {
	ID          string
	ContextID   string
	SourceID    string
	Status      string
	StartedAt   time.Time
	CompletedAt sql.NullTime
	Metadata    []byte
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// TransactionPostgreSQLModel maps to transactions table (000001_init_schema.up.sql:84-104).
type TransactionPostgreSQLModel struct {
	ID                  string
	IngestionJobID      string
	SourceID            string
	ExternalID          string
	Amount              decimal.Decimal
	Currency            string
	AmountBase          decimal.NullDecimal
	BaseCurrency        sql.NullString
	FXRate              decimal.NullDecimal
	FXRateSource        sql.NullString
	FXRateEffectiveDate sql.NullTime
	ExtractionStatus    string
	Date                time.Time
	Description         sql.NullString
	Status              string
	Metadata            []byte
	CreatedAt           time.Time
	UpdatedAt           time.Time
}
