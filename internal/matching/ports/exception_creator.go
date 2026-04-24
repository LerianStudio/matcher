// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package ports

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/matching/domain/repositories"
)

//go:generate mockgen -destination=mocks/exception_creator_mock.go -package=mocks . ExceptionCreator

// ExceptionTransactionInput contains transaction data required for severity classification.
// Implements PRD AC-002 severity classification based on amount, age, and source type.
type ExceptionTransactionInput struct {
	TransactionID   uuid.UUID
	AmountAbsBase   decimal.Decimal
	TransactionDate time.Time
	SourceType      string
	FXMissing       bool
	Reason          string
}

// ExceptionCreator creates exception records for unmatched transactions.
type ExceptionCreator interface {
	CreateExceptions(
		ctx context.Context,
		contextID, runID uuid.UUID,
		inputs []ExceptionTransactionInput,
		regulatorySourceTypes []string,
	) error
	CreateExceptionsWithTx(
		ctx context.Context,
		tx repositories.Tx,
		contextID, runID uuid.UUID,
		inputs []ExceptionTransactionInput,
		regulatorySourceTypes []string,
	) error
}
