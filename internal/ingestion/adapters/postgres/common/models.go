// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package common

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// JobPostgreSQLModel maps to ingestion_jobs table (000001_init_schema.up.sql:71-81).
type JobPostgreSQLModel struct {
	ID          uuid.UUID
	ContextID   uuid.UUID
	SourceID    uuid.UUID
	Status      string
	StartedAt   time.Time
	CompletedAt sql.NullTime
	Metadata    []byte
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// TransactionPostgreSQLModel maps to transactions table (000001_init_schema.up.sql:84-104).
type TransactionPostgreSQLModel struct {
	ID                  uuid.UUID
	IngestionJobID      uuid.UUID
	SourceID            uuid.UUID
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
