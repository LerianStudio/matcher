// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// --- acquireNextSequence ---

func TestAcquireNextSequence_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	tenantID := defaultTenantUUID()

	mock.ExpectBegin()
	// Mock row-level lock acquisition (existing chain state row)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(int64(5)))

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	seq, err := repo.acquireNextSequence(context.Background(), tx, tenantID)
	require.NoError(t, err)
	require.Equal(t, int64(5), seq)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAcquireNextSequence_Error(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	tenantID := defaultTenantUUID()

	mock.ExpectBegin()
	// Mock row-level lock (first record: no existing chain state)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(errTestConnectionFailed)

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	_, err = repo.acquireNextSequence(context.Background(), tx, tenantID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "upsert chain state")
	require.NoError(t, mock.ExpectationsWereMet())
}

// --- getPreviousHash ---

func TestGetPreviousHash_GenesisHash(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	hash, err := repo.getPreviousHash(context.Background(), tx, defaultTenantUUID(), 1)
	require.NoError(t, err)
	require.NotNil(t, hash)
	require.Len(t, hash, 32)
	// Genesis hash should be all zeros
	for _, b := range hash {
		require.Equal(t, byte(0), b)
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetPreviousHash_NonGenesisSuccess(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	tenantID := defaultTenantUUID()
	expectedHash := make([]byte, 32)
	expectedHash[0] = 0xAB

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT record_hash FROM audit_logs WHERE tenant_id = \$1 AND tenant_seq = \$2`).
		WithArgs(tenantID, int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"record_hash"}).AddRow(expectedHash))

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	hash, err := repo.getPreviousHash(context.Background(), tx, tenantID, 5)
	require.NoError(t, err)
	require.Equal(t, expectedHash, hash)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetPreviousHash_NonGenesisNotFound(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	tenantID := defaultTenantUUID()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT record_hash FROM audit_logs WHERE tenant_id = \$1 AND tenant_seq = \$2`).
		WithArgs(tenantID, int64(4)).
		WillReturnError(sql.ErrNoRows)

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	_, err = repo.getPreviousHash(context.Background(), tx, tenantID, 5)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPreviousRecordNotFound)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetPreviousHash_NonGenesisQueryError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	tenantID := defaultTenantUUID()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT record_hash FROM audit_logs WHERE tenant_id = \$1 AND tenant_seq = \$2`).
		WithArgs(tenantID, int64(2)).
		WillReturnError(errTestConnectionFailed)

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	_, err = repo.getPreviousHash(context.Background(), tx, tenantID, 3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "query previous hash")
	require.NoError(t, mock.ExpectationsWereMet())
}

// --- executeCreate error paths ---

func TestExecuteCreate_AcquireSequenceError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	tenantID := defaultTenantUUID()

	auditLog := &entities.AuditLog{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		EntityType: "match_run",
		EntityID:   uuid.New(),
		Action:     "CREATE",
		Changes:    []byte(`{}`),
		CreatedAt:  time.Now().UTC(),
	}

	mock.ExpectBegin()
	// Mock row-level lock (first record: no existing chain state)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(errTestDatabaseError)

	ctx := contextWithTenant()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	_, err = repo.executeCreate(ctx, tx, auditLog)
	require.Error(t, err)
	require.Contains(t, err.Error(), "acquire sequence")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_GetPreviousHashError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	tenantID := defaultTenantUUID()

	auditLog := &entities.AuditLog{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		EntityType: "match_run",
		EntityID:   uuid.New(),
		Action:     "CREATE",
		Changes:    []byte(`{}`),
		CreatedAt:  time.Now().UTC(),
	}

	mock.ExpectBegin()
	// Mock row-level lock (existing chain state row for seq > 1)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	// acquireNextSequence returns seq=5 (non-genesis, needs previous hash)
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(int64(5)))
	// getPreviousHash fails
	mock.ExpectQuery(`SELECT record_hash FROM audit_logs WHERE tenant_id = \$1 AND tenant_seq = \$2`).
		WithArgs(tenantID, int64(4)).
		WillReturnError(errTestConnectionFailed)

	ctx := contextWithTenant()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	_, err = repo.executeCreate(ctx, tx, auditLog)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get previous hash")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_NoTenantInContext(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	auditLogID := uuid.New()
	presetTenantID := uuid.New()
	entityID := uuid.New()
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	changes := []byte(`{"field":"value"}`)

	auditLog := &entities.AuditLog{
		ID:         auditLogID,
		TenantID:   presetTenantID,
		EntityType: "context",
		EntityID:   entityID,
		Action:     "UPDATE",
		Changes:    changes,
		CreatedAt:  createdAt,
	}

	tenantSeq := int64(1)
	prevHash := make([]byte, 32)
	recordHash := make([]byte, 32)

	mock.ExpectBegin()
	// Mock row-level lock (first record: no existing chain state for this preset tenant)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(presetTenantID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(presetTenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(tenantSeq))
	mock.ExpectQuery(`INSERT INTO audit_logs`).
		WithArgs(
			auditLogID, presetTenantID, "context", entityID, "UPDATE", nil, changes, createdAt,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows(auditLogTestColumns).
			AddRow(auditLogID, presetTenantID, "context", entityID, "UPDATE", nil, changes, createdAt,
				tenantSeq, prevHash, recordHash, int16(1)))
	mock.ExpectCommit()

	// Use context with invalid tenant so getTenantIDFromContext fails
	// and the pre-set TenantID on auditLog is used instead
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-uuid")

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	result, err := repo.executeCreate(ctx, tx, auditLog)
	require.NoError(t, err)
	require.NotNil(t, result)
	// Tenant ID should remain the pre-set value
	require.Equal(t, presetTenantID, result.TenantID)

	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

// --- Create via setupMockRepository ---

func TestCreate_SuccessViaMock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	auditLogID := uuid.New()
	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	actorID := "user@test.com"
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	changes := []byte(`{"field":"value"}`)

	tenantSeq := int64(1)
	prevHash := make([]byte, 32)
	recordHash := make([]byte, 32)

	ctx := contextWithTenant()

	mock.ExpectBegin()
	// Mock row-level lock acquisition (first record: no existing chain state)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(tenantSeq))
	mock.ExpectQuery(`INSERT INTO audit_logs`).
		WithArgs(
			auditLogID, tenantID, "source", entityID, "CREATE", &actorID, changes, createdAt,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows(auditLogTestColumns).
			AddRow(auditLogID, tenantID, "source", entityID, "CREATE", &actorID, changes, createdAt,
				tenantSeq, prevHash, recordHash, int16(1)))
	mock.ExpectCommit()

	auditLog := &entities.AuditLog{
		ID:         auditLogID,
		TenantID:   uuid.New(),
		EntityType: "source",
		EntityID:   entityID,
		Action:     "CREATE",
		ActorID:    &actorID,
		Changes:    changes,
		CreatedAt:  createdAt,
	}

	result, err := repo.Create(ctx, auditLog)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, auditLogID, result.ID)
	require.Equal(t, tenantID, result.TenantID)
	require.Equal(t, "source", result.EntityType)
}

func TestCreate_TransactionError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	ctx := contextWithTenant()

	mock.ExpectBegin()
	// Mock row-level lock (first record: no existing chain state)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	auditLog := &entities.AuditLog{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		EntityType: "match_run",
		EntityID:   uuid.New(),
		Action:     "CREATE",
		Changes:    []byte(`{}`),
		CreatedAt:  time.Now().UTC(),
	}

	_, err := repo.Create(ctx, auditLog)
	require.Error(t, err)
	require.Contains(t, err.Error(), "create audit log transaction")
}

func TestCreate_MissingTenantID(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})
	// Empty context - auth.GetTenantID returns default which is valid
	// so we use an invalid tenant
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "bad-uuid")

	auditLog := &entities.AuditLog{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		EntityType: "test",
		EntityID:   uuid.New(),
		Action:     "CREATE",
		Changes:    []byte(`{}`),
		CreatedAt:  time.Now().UTC(),
	}

	// Create will still succeed as executeCreate allows fallback to pre-set TenantID
	// but the error will come from fakeInfrastructureProvider's transaction handling
	// The path we care about is that Create delegates to WithTenantTxProvider
	_, err := repo.Create(ctx, auditLog)
	// It will fail because fakeInfrastructureProvider doesn't support real transactions
	require.Error(t, err)
}

// --- GetByID error paths ---

func TestGetByID_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})
	ctx := context.Background()

	// fakeInfrastructureProvider returns nil database leases,
	// so the repo fails when trying to acquire a read connection.
	_, err := repo.GetByID(ctx, uuid.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), "postgres connection is required")
}

func TestGetByID_DatabaseError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	logID := uuid.New()
	ctx := contextWithTenant()

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE id = \$1 AND tenant_id = \$2`).
		WithArgs(logID, tenantID).
		WillReturnError(errTestConnectionFailed)

	_, err := repo.GetByID(ctx, logID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get audit log by id transaction")
}

// --- ListByEntity additional coverage ---

func TestListByEntity_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})
	ctx := context.Background()

	// fakeInfrastructureProvider returns nil database leases,
	// so the repo fails when trying to acquire a read connection.
	_, _, err := repo.ListByEntity(ctx, "entity", uuid.New(), nil, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "postgres connection is required")
}

func TestListByEntity_EmptyResult(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	ctx := contextWithTenant()

	rows := sqlmock.NewRows(auditLogTestColumns)

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 AND entity_type = \$2 AND entity_id = \$3 ORDER BY`).
		WithArgs(tenantID, "match_run", entityID).
		WillReturnRows(rows)

	logs, nextCursor, err := repo.ListByEntity(ctx, "match_run", entityID, nil, 10)
	require.NoError(t, err)
	require.Empty(t, logs)
	require.Empty(t, nextCursor)
}

func TestListByEntity_ScanError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	ctx := contextWithTenant()

	// Return rows with wrong column count to trigger scan error
	rows := sqlmock.NewRows([]string{"id"}).AddRow("not-a-uuid")

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 AND entity_type = \$2 AND entity_id = \$3 ORDER BY`).
		WithArgs(tenantID, "match_run", entityID).
		WillReturnRows(rows)

	_, _, err := repo.ListByEntity(ctx, "match_run", entityID, nil, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "list audit logs by entity transaction")
}

// --- List additional coverage ---

func TestList_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := contextWithTenant()

	_, _, err := repo.List(ctx, entities.AuditLogFilter{}, nil, 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestList_NilConnection(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})
	ctx := context.Background()

	// fakeInfrastructureProvider returns nil database leases,
	// so the repo fails when trying to acquire a read connection.
	_, _, err := repo.List(ctx, entities.AuditLogFilter{}, nil, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "postgres connection is required")
}

func TestList_EmptyResult(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	ctx := contextWithTenant()

	rows := sqlmock.NewRows(auditLogTestColumns)

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 ORDER BY`).
		WithArgs(tenantID).
		WillReturnRows(rows)

	logs, nextCursor, err := repo.List(ctx, entities.AuditLogFilter{}, nil, 10)
	require.NoError(t, err)
	require.Empty(t, logs)
	require.Empty(t, nextCursor)
}

func TestList_WithPagination(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	ctx := contextWithTenant()
	limit := 2
	changes := []byte(`{}`)
	createdAt1 := time.Now().UTC()
	createdAt2 := createdAt1.Add(-time.Minute)
	createdAt3 := createdAt2.Add(-time.Minute)
	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	tenantSeq, prevHash, recordHash, hashVersion := defaultHashChainValues()
	rows := sqlmock.NewRows(auditLogTestColumns).
		AddRow(id1, tenantID, "source", uuid.New(), "CREATE", nil, changes, createdAt1,
			tenantSeq, prevHash, recordHash, hashVersion).
		AddRow(id2, tenantID, "source", uuid.New(), "UPDATE", nil, changes, createdAt2,
			tenantSeq+1, prevHash, recordHash, hashVersion).
		AddRow(id3, tenantID, "source", uuid.New(), "DELETE", nil, changes, createdAt3,
			tenantSeq+2, prevHash, recordHash, hashVersion)

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 ORDER BY`).
		WithArgs(tenantID).
		WillReturnRows(rows)

	logs, nextCursor, err := repo.List(ctx, entities.AuditLogFilter{}, nil, limit)
	require.NoError(t, err)
	require.Len(t, logs, 2)
	require.NotEmpty(t, nextCursor, "expected a next cursor for pagination")
}

func TestList_ScanError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	ctx := contextWithTenant()

	// Return rows with wrong column count to trigger scan error
	rows := sqlmock.NewRows([]string{"id"}).AddRow("not-a-uuid")

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 ORDER BY`).
		WithArgs(tenantID).
		WillReturnRows(rows)

	_, _, err := repo.List(ctx, entities.AuditLogFilter{}, nil, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "list audit logs transaction")
}

// --- ScanAuditLog with nullable hash fields ---

func TestScanAuditLog_NullableHashFields(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	tenantID := uuid.New()
	entityID := uuid.New()
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	changes := []byte(`{}`)

	log, err := scanAuditLog(fakeScanner{scan: func(dest ...any) error {
		if ptr, ok := dest[0].(*uuid.UUID); ok {
			*ptr = id
		}

		if ptr, ok := dest[1].(*uuid.UUID); ok {
			*ptr = tenantID
		}

		if ptr, ok := dest[2].(*string); ok {
			*ptr = "source"
		}

		if ptr, ok := dest[3].(*uuid.UUID); ok {
			*ptr = entityID
		}

		if ptr, ok := dest[4].(*string); ok {
			*ptr = "DELETE"
		}

		// ActorID is nil (no actor)
		if ptr, ok := dest[5].(**string); ok {
			*ptr = nil
		}

		if ptr, ok := dest[6].(*[]byte); ok {
			*ptr = changes
		}

		if ptr, ok := dest[7].(*time.Time); ok {
			*ptr = createdAt
		}

		// NullInt64 with Valid=false (no tenant_seq)
		if ptr, ok := dest[8].(*sql.NullInt64); ok {
			*ptr = sql.NullInt64{Valid: false}
		}

		// Empty prev_hash and record_hash
		if ptr, ok := dest[9].(*[]byte); ok {
			*ptr = nil
		}

		if ptr, ok := dest[10].(*[]byte); ok {
			*ptr = nil
		}

		// NullInt16 with Valid=false (no hash_version)
		if ptr, ok := dest[11].(*sql.NullInt16); ok {
			*ptr = sql.NullInt16{Valid: false}
		}

		return nil
	}})

	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, id, log.ID)
	require.Nil(t, log.ActorID)
	require.Equal(t, int64(0), log.TenantSeq) // Not valid, so stays zero
	require.Nil(t, log.PrevHash)
	require.Nil(t, log.RecordHash)
	require.Equal(t, int16(0), log.HashVersion) // Not valid, so stays zero
}

// --- validateListByEntityParams ---

func TestValidateListByEntityParams_NilRepo(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	_, err := repo.validateListByEntityParams("entity", uuid.New(), 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestValidateListByEntityParams_EmptyEntityType(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})

	_, err := repo.validateListByEntityParams("", uuid.New(), 10)
	require.ErrorIs(t, err, entities.ErrEntityTypeRequired)
}

func TestValidateListByEntityParams_Success(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})

	trimmed, err := repo.validateListByEntityParams("  match_run  ", uuid.New(), 10)
	require.NoError(t, err)
	require.Equal(t, "match_run", trimmed)
}

// --- applyAuditLogFilter additional coverage ---

func TestApplyAuditLogFilter_EmptyFilter(t *testing.T) {
	t.Parallel()

	filter := entities.AuditLogFilter{}

	qb := squirrelSelectAll()
	qb = applyAuditLogFilter(qb, filter, nil)

	query, args, err := qb.ToSql()
	require.NoError(t, err)
	// No additional WHERE clauses
	require.NotContains(t, query, "actor_id")
	require.NotContains(t, query, "action")
	require.NotContains(t, query, "entity_type")
	require.NotContains(t, query, "created_at")
	require.Empty(t, args)
}

func TestApplyAuditLogFilter_OnlyActionFilter(t *testing.T) {
	t.Parallel()

	action := "UPDATE"
	filter := entities.AuditLogFilter{Action: &action}

	qb := squirrelSelectAll()
	qb = applyAuditLogFilter(qb, filter, nil)

	query, args, err := qb.ToSql()
	require.NoError(t, err)
	require.Contains(t, query, "action")
	require.Contains(t, args, action)
}

func TestApplyAuditLogFilter_OnlyEntityTypeFilter(t *testing.T) {
	t.Parallel()

	entityType := "source"
	filter := entities.AuditLogFilter{EntityType: &entityType}

	qb := squirrelSelectAll()
	qb = applyAuditLogFilter(qb, filter, nil)

	query, args, err := qb.ToSql()
	require.NoError(t, err)
	require.Contains(t, query, "entity_type")
	require.Contains(t, args, entityType)
}

func TestApplyAuditLogFilter_OnlyDateFrom(t *testing.T) {
	t.Parallel()

	dateFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	filter := entities.AuditLogFilter{DateFrom: &dateFrom}

	qb := squirrelSelectAll()
	qb = applyAuditLogFilter(qb, filter, nil)

	query, args, err := qb.ToSql()
	require.NoError(t, err)
	require.Contains(t, query, "created_at >= $")
	require.NotContains(t, query, "created_at <= $")
	require.Contains(t, args, dateFrom)
}

func TestApplyAuditLogFilter_OnlyDateTo(t *testing.T) {
	t.Parallel()

	dateTo := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)
	filter := entities.AuditLogFilter{DateTo: &dateTo}

	qb := squirrelSelectAll()
	qb = applyAuditLogFilter(qb, filter, nil)

	query, args, err := qb.ToSql()
	require.NoError(t, err)
	require.NotContains(t, query, "created_at >= $")
	require.Contains(t, query, "created_at <= $")
	require.Contains(t, args, dateTo)
}

// --- List with cursor and filter combined ---

func TestList_WithCursorAndFilter(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	ctx := contextWithTenant()

	actor := "admin@test.com"
	action := "CREATE"
	cursorTime := time.Now().UTC()
	cursorID := uuid.New()
	cursor := &sharedhttp.TimestampCursor{Timestamp: cursorTime, ID: cursorID}
	filter := entities.AuditLogFilter{
		Actor:  &actor,
		Action: &action,
	}

	auditLogID := uuid.New()
	entityID := uuid.New()
	createdAt := cursorTime.Add(-time.Hour)
	changes := []byte(`{}`)
	tenantSeq, prevHash, recordHash, hashVersion := defaultHashChainValues()

	rows := sqlmock.NewRows(auditLogTestColumns).
		AddRow(auditLogID, tenantID, "match_run", entityID, action, &actor, changes, createdAt,
			tenantSeq, prevHash, recordHash, hashVersion)

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 AND actor_id = \$2 AND action = \$3 AND \(created_at, id\) < \(\$4, \$5\) ORDER BY`).
		WithArgs(tenantID, actor, action, cursorTime, cursorID).
		WillReturnRows(rows)

	logs, _, err := repo.List(ctx, filter, cursor, 10)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, auditLogID, logs[0].ID)
}

// --- CreateWithTx via setupMockRepository ---

func TestCreateWithTx_SuccessViaMock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	auditLogID := uuid.New()
	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	changes := []byte(`{"field":"value"}`)

	tenantSeq := int64(1)
	prevHash := make([]byte, 32)
	recordHash := make([]byte, 32)

	ctx := contextWithTenant()

	// CreateWithTx uses WithTenantTxOrExistingProvider which will use the existing tx
	mock.ExpectBegin()
	// Mock row-level lock acquisition (first record: no existing chain state)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(tenantSeq))
	mock.ExpectQuery(`INSERT INTO audit_logs`).
		WithArgs(
			auditLogID, tenantID, "source", entityID, "CREATE", nil, changes, createdAt,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows(auditLogTestColumns).
			AddRow(auditLogID, tenantID, "source", entityID, "CREATE", nil, changes, createdAt,
				tenantSeq, prevHash, recordHash, int16(1)))
	mock.ExpectCommit()

	auditLog := &entities.AuditLog{
		ID:         auditLogID,
		TenantID:   uuid.New(),
		EntityType: "source",
		EntityID:   entityID,
		Action:     "CREATE",
		Changes:    changes,
		CreatedAt:  createdAt,
	}

	// Get a tx from the mock provider's database connection
	pgConn, err := repo.provider.GetPrimaryDB(ctx)
	require.NoError(t, err)
	require.NotNil(t, pgConn)
	require.NotNil(t, pgConn.DB())

	tx, err := pgConn.DB().BeginTx(ctx, nil)
	require.NoError(t, err)

	result, err := repo.CreateWithTx(ctx, tx, auditLog)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, auditLogID, result.ID)

	require.NoError(t, tx.Commit())
}

// squirrelSelectAll is a helper for filter tests.
func squirrelSelectAll() squirrel.SelectBuilder {
	return squirrel.Select("*").From("audit_logs").PlaceholderFormat(squirrel.Dollar)
}
