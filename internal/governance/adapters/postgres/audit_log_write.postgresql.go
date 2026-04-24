// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/hashchain"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Create inserts a new audit log entry.
func (repo *Repository) Create(
	ctx context.Context,
	auditLog *entities.AuditLog,
) (*entities.AuditLog, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if auditLog == nil {
		return nil, ErrAuditLogRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_audit_log")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.AuditLog, error) {
			return repo.executeCreate(ctx, tx, auditLog)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create audit log transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create audit log", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to create audit log", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithTx inserts a new audit log entry using the provided transaction.
// This enables atomic audit logging within the same transaction as the audited operation.
func (repo *Repository) CreateWithTx(
	ctx context.Context,
	tx repositories.Tx,
	auditLog *entities.AuditLog,
) (*entities.AuditLog, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if auditLog == nil {
		return nil, ErrAuditLogRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_audit_log_with_tx")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.AuditLog, error) {
			return repo.executeCreate(ctx, innerTx, auditLog)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create audit log transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create audit log", wrappedErr)

		libLog.SafeError(logger, ctx, "failed to create audit log", wrappedErr, runtime.IsProductionMode())

		return nil, wrappedErr
	}

	return result, nil
}

// executeCreate performs the actual audit log creation within a transaction.
// It computes the hash chain by:
//  1. Acquiring the next sequence number for the tenant
//  2. Fetching the previous record's hash (or genesis hash for first record)
//  3. Computing the record hash and inserting with all hash chain fields.
//
// Timestamp precision: CreatedAt is truncated to microseconds before hashing
// and insertion to match PostgreSQL TIMESTAMPTZ storage precision. Without this,
// nanosecond-precision timestamps produce hashes that cannot be verified after
// a round-trip through PostgreSQL.
func (repo *Repository) executeCreate(
	ctx context.Context,
	tx *sql.Tx,
	auditLog *entities.AuditLog,
) (*entities.AuditLog, error) {
	tenantID, err := getTenantIDFromContext(ctx)
	if err == nil {
		auditLog.TenantID = tenantID
	} else {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("tenant ID not found in context, using pre-set value on audit log: %v", err))
	}

	// Normalize timestamp to microsecond precision before hashing.
	// PostgreSQL TIMESTAMPTZ stores at most microsecond resolution; without
	// truncation the hash is computed from nanosecond-precision JSON but the
	// stored timestamp loses the extra digits, making VerifyRecordHash fail
	// on any subsequent read-back.
	auditLog.CreatedAt = auditLog.CreatedAt.Truncate(time.Microsecond)

	tenantSeq, err := repo.acquireNextSequence(ctx, tx, auditLog.TenantID)
	if err != nil {
		return nil, fmt.Errorf("acquire sequence: %w", err)
	}

	prevHash, err := repo.getPreviousHash(ctx, tx, auditLog.TenantID, tenantSeq)
	if err != nil {
		return nil, fmt.Errorf("get previous hash: %w", err)
	}

	recordData := hashchain.RecordData{
		ID:          auditLog.ID,
		TenantID:    auditLog.TenantID,
		TenantSeq:   tenantSeq,
		EntityType:  auditLog.EntityType,
		EntityID:    auditLog.EntityID,
		Action:      auditLog.Action,
		ActorID:     auditLog.ActorID,
		Changes:     json.RawMessage(auditLog.Changes),
		CreatedAt:   auditLog.CreatedAt,
		HashVersion: hashchain.HashVersion,
	}

	recordHash, err := hashchain.ComputeRecordHash(prevHash, recordData)
	if err != nil {
		return nil, fmt.Errorf("compute record hash: %w", err)
	}

	row := tx.QueryRowContext(
		ctx,
		`INSERT INTO audit_logs (id, tenant_id, entity_type, entity_id, action, actor_id, changes, created_at, tenant_seq, prev_hash, record_hash, hash_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		 RETURNING `+auditLogColumns,
		auditLog.ID,
		auditLog.TenantID,
		auditLog.EntityType,
		auditLog.EntityID,
		auditLog.Action,
		auditLog.ActorID,
		auditLog.Changes,
		auditLog.CreatedAt,
		tenantSeq,
		prevHash,
		recordHash,
		hashchain.HashVersion,
	)

	return scanAuditLog(row)
}

// acquireNextSequence atomically increments and returns the next sequence number for a tenant.
// Uses INSERT ... ON CONFLICT for upsert to handle first-time tenant initialization.
//
// CONCURRENCY SAFETY: Before the upsert, this method acquires an exclusive row lock
// (SELECT ... FOR UPDATE) on the tenant's chain state row. This serializes all hash
// chain writes per tenant, preventing race conditions where concurrent writers could
// observe stale previous hashes via getPreviousHash and fork the hash chain.
//
// The lock is held until the transaction commits, ensuring:
//  1. Only one writer progresses past this point per tenant at a time.
//  2. The subsequent getPreviousHash sees the prior writer's fully committed data.
//
// For the first record (no chain_state row yet), the SELECT returns sql.ErrNoRows
// and the upsert INSERT path atomically creates and locks the new row.
func (repo *Repository) acquireNextSequence(
	ctx context.Context,
	tx *sql.Tx,
	tenantID uuid.UUID,
) (int64, error) {
	// Acquire exclusive row lock on existing chain state to serialize writers.
	// Without this lock, concurrent writers could read stale previous hashes,
	// forking the hash chain (see getPreviousHash for the read side).
	var lockPlaceholder int

	err := tx.QueryRowContext(
		ctx,
		`SELECT 1 FROM audit_log_chain_state WHERE tenant_id = $1 FOR UPDATE`,
		tenantID,
	).Scan(&lockPlaceholder)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("lock chain state: %w", err)
	}

	// sql.ErrNoRows means first record for this tenant; the INSERT path below
	// will atomically create and lock the row.

	var nextSeq int64

	err = tx.QueryRowContext(
		ctx,
		`INSERT INTO audit_log_chain_state (tenant_id, next_seq)
		 VALUES ($1, 2)
		 ON CONFLICT (tenant_id) DO UPDATE SET next_seq = audit_log_chain_state.next_seq + 1
		 RETURNING next_seq - 1`,
		tenantID,
	).Scan(&nextSeq)
	if err != nil {
		return 0, fmt.Errorf("upsert chain state: %w", err)
	}

	return nextSeq, nil
}

// getPreviousHash retrieves the hash of the previous record in the chain.
// Returns genesis hash (32 zero bytes) for the first record (seq=1).
//
// SAFETY: This query does not use FOR SHARE/FOR UPDATE because the exclusive
// row lock acquired by acquireNextSequence on audit_log_chain_state serializes
// all writers for the same tenant. By the time this method executes, any prior
// writer has already committed its audit_logs INSERT (it had to release the
// chain_state row lock that we now hold, which requires its transaction to commit).
func (repo *Repository) getPreviousHash(
	ctx context.Context,
	tx *sql.Tx,
	tenantID uuid.UUID,
	currentSeq int64,
) ([]byte, error) {
	if currentSeq == 1 {
		return hashchain.GenesisHash(), nil
	}

	var prevHash []byte

	err := tx.QueryRowContext(
		ctx,
		`SELECT record_hash FROM audit_logs WHERE tenant_id = $1 AND tenant_seq = $2`,
		tenantID,
		currentSeq-1,
	).Scan(&prevHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: seq %d", ErrPreviousRecordNotFound, currentSeq-1)
		}

		return nil, fmt.Errorf("query previous hash: %w", err)
	}

	return prevHash, nil
}
