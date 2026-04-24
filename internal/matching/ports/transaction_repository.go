// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:generate mockgen -destination=mocks/transaction_repository_mock.go -package=mocks . TransactionRepository

package ports

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// TransactionRepository loads and updates candidate transactions for matching.
// Contract:
// - Returns only extraction_status=COMPLETE and status=UNMATCHED transactions.
// - Returned ordering must be deterministic for identical query parameters.
// - Ordering must be stable for pagination: date ASC, id ASC (tie-breaker).
// - startInclusive/endInclusive are optional; nil means unbounded.
// - limit must be > 0; offset must be >= 0.
// - Tenant scoping and authorization happen in adapters; caller provides contextID.
// - Ingestion Transaction is treated as a shared-kernel type for matching.
//
// Mark* operations are only exposed in transactional form (`*WithTx`) because
// every matching write path composes multiple repositories inside a single
// tenant-scoped transaction. Use WithTx to start one.
type TransactionRepository interface {
	ListUnmatchedByContext(
		ctx context.Context,
		contextID uuid.UUID,
		startInclusive *time.Time,
		endInclusive *time.Time,
		limit int,
		offset int,
	) ([]*shared.Transaction, error)
	FindByContextAndIDs(
		ctx context.Context,
		contextID uuid.UUID,
		transactionIDs []uuid.UUID,
	) ([]*shared.Transaction, error)
	MarkMatchedWithTx(
		ctx context.Context,
		tx repositories.Tx,
		contextID uuid.UUID,
		transactionIDs []uuid.UUID,
	) error
	MarkPendingReviewWithTx(
		ctx context.Context,
		tx repositories.Tx,
		contextID uuid.UUID,
		transactionIDs []uuid.UUID,
	) error
	MarkUnmatchedWithTx(
		ctx context.Context,
		tx repositories.Tx,
		contextID uuid.UUID,
		transactionIDs []uuid.UUID,
	) error
	WithTx(ctx context.Context, fn func(repositories.Tx) error) error
}
