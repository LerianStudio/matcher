// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package common provides shared utilities for postgres adapters.
package common

import (
	"errors"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
)

// ErrTransactionIDsEmpty is returned when BuildMarkStatusQuery is called
// with an empty or nil transactionIDs slice.
var ErrTransactionIDsEmpty = errors.New("transaction IDs must not be empty")

// BuildMarkStatusQuery builds a SQL UPDATE statement that sets the status
// column on transactions filtered by context ID and transaction IDs.
// This is shared infrastructure so that multiple bounded contexts can
// build the same query without importing each other's adapters.
// It returns an error when transactionIDs is empty or nil.
func BuildMarkStatusQuery(
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
	status string,
) (squirrel.UpdateBuilder, error) {
	if len(transactionIDs) == 0 {
		return squirrel.UpdateBuilder{}, ErrTransactionIDsEmpty
	}

	queryBuilder := squirrel.Update("transactions").
		Set("status", status).
		Set("updated_at", squirrel.Expr("NOW()"))

	stringIDs := make([]string, 0, len(transactionIDs))
	for _, id := range transactionIDs {
		stringIDs = append(stringIDs, id.String())
	}

	return queryBuilder.
		Where(
			squirrel.Expr(
				"source_id IN (SELECT id FROM reconciliation_sources WHERE context_id = ?)",
				contextID.String(),
			),
		).
		Where(squirrel.Eq{"id": stringIDs}).
		PlaceholderFormat(squirrel.Dollar), nil
}

// BuildMarkMatchedQuery builds the SQL to mark transactions as matched.
func BuildMarkMatchedQuery(contextID uuid.UUID, transactionIDs []uuid.UUID) (squirrel.UpdateBuilder, error) {
	return BuildMarkStatusQuery(contextID, transactionIDs, "MATCHED")
}

// BuildMarkPendingReviewQuery builds the SQL to mark transactions as pending review.
func BuildMarkPendingReviewQuery(
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) (squirrel.UpdateBuilder, error) {
	return BuildMarkStatusQuery(contextID, transactionIDs, "PENDING_REVIEW")
}

// BuildMarkUnmatchedQuery builds the SQL to mark transactions as unmatched.
func BuildMarkUnmatchedQuery(
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) (squirrel.UpdateBuilder, error) {
	return BuildMarkStatusQuery(contextID, transactionIDs, "UNMATCHED")
}
