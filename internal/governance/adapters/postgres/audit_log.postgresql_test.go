//go:build unit

package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/hashchain"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

var (
	errTestScanFailed          = errors.New("scan failed")
	errTestConstraintViolation = errors.New("constraint violation")
	errTestConnectionFailed    = errors.New("connection failed")
	errTestDatabaseError       = errors.New("database error")
)

// auditLogTestColumns matches the 12 columns returned by auditLogColumns constant.
var auditLogTestColumns = []string{
	"id", "tenant_id", "entity_type", "entity_id", "action", "actor_id", "changes", "created_at",
	"tenant_seq", "prev_hash", "record_hash", "hash_version",
}

// defaultHashChainValues returns the default hash chain values for test mocks.
// Returns: tenant_seq (int64), prev_hash ([]byte), record_hash ([]byte), hash_version (int16)
func defaultHashChainValues() (int64, []byte, []byte, int16) {
	return int64(1), make([]byte, 32), make([]byte, 32), int16(1)
}

type fakeInfrastructureProvider struct{}

func (f *fakeInfrastructureProvider) GetRedisConnection(
	_ context.Context,
) (*ports.RedisConnectionLease, error) {
	return nil, nil
}

func (f *fakeInfrastructureProvider) BeginTx(ctx context.Context) (*ports.TxLease, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create sqlmock: %w", err)
	}

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return ports.NewTxLease(tx, nil), nil
}

func (f *fakeInfrastructureProvider) GetReplicaDB(_ context.Context) (*ports.DBLease, error) {
	return nil, nil
}

func (f *fakeInfrastructureProvider) GetPrimaryDB(_ context.Context) (*ports.DBLease, error) {
	return nil, nil
}

var _ ports.InfrastructureProvider = (*fakeInfrastructureProvider)(nil)

type fakeScanner struct {
	scan func(dest ...any) error
}

func (f fakeScanner) Scan(dest ...any) error {
	return f.scan(dest...)
}

type timeTruncatedToMicrosecondArg struct {
	expected time.Time
}

func (arg timeTruncatedToMicrosecondArg) Match(value driver.Value) bool {
	actual, ok := value.(time.Time)
	if !ok {
		return false
	}

	return actual.Equal(arg.expected.Truncate(time.Microsecond))
}

// byteSliceArg is a sqlmock ArgumentMatcher that compares the driver value
// to an expected byte slice. Using this instead of sqlmock.AnyArg() for
// prev_hash / record_hash turns wire-drift into a loud test failure: if
// executeCreate starts passing the wrong hash bytes to INSERT, the
// expectation will not match and sqlmock will report a detailed diff
// identifying the regressing column.
type byteSliceArg struct {
	expected []byte
}

func (arg byteSliceArg) Match(value driver.Value) bool {
	actual, ok := value.([]byte)
	if !ok {
		return false
	}

	if len(actual) != len(arg.expected) {
		return false
	}

	for i := range actual {
		if actual[i] != arg.expected[i] {
			return false
		}
	}

	return true
}

func TestRepository_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, err := repo.Create(ctx, &entities.AuditLog{})
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.GetByID(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, _, err = repo.ListByEntity(ctx, "entity", uuid.New(), nil, 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_CreateValidation(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	_, err := repo.Create(ctx, nil)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_GetByIDValidation(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	_, err := repo.GetByID(ctx, uuid.Nil)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_ListByEntityValidation(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	_, _, err := repo.ListByEntity(ctx, "entity", uuid.New(), nil, 10)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_ListByEntityParamValidation(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})
	ctx := context.Background()

	_, _, err := repo.ListByEntity(ctx, " ", uuid.New(), nil, 10)
	require.ErrorIs(t, err, entities.ErrEntityTypeRequired)

	_, _, err = repo.ListByEntity(ctx, "entity", uuid.Nil, nil, 10)
	require.ErrorIs(t, err, entities.ErrEntityIDRequired)

	_, _, err = repo.ListByEntity(ctx, "entity", uuid.New(), nil, 0)
	require.ErrorIs(t, err, ErrLimitMustBePositive)

	_, _, err = repo.ListByEntity(ctx, "entity", uuid.New(), nil, -1)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
}

func TestRepositorySentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrRepositoryNotInitialized", ErrRepositoryNotInitialized},
		{"ErrAuditLogRequired", ErrAuditLogRequired},
		{"ErrIDRequired", ErrIDRequired},
		{"ErrTenantIDRequired", entities.ErrTenantIDRequired},
		{"ErrEntityTypeRequired", entities.ErrEntityTypeRequired},
		{"ErrEntityIDRequired", entities.ErrEntityIDRequired},
		{"ErrLimitMustBePositive", ErrLimitMustBePositive},
		{"ErrNilScanner", ErrNilScanner},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestScanAuditLog(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	tenantID := uuid.New()
	entityID := uuid.New()
	actorID := "actor-1"
	changes := []byte("payload")
	createdAt := time.Now().UTC()
	tenantSeq := int64(5)
	prevHash := make([]byte, 32)
	recordHash := make([]byte, 32)
	hashVersion := int16(1)

	log, err := scanAuditLog(fakeScanner{scan: func(dest ...any) error {
		if ptr, ok := dest[0].(*uuid.UUID); ok {
			*ptr = id
		}

		if ptr, ok := dest[1].(*uuid.UUID); ok {
			*ptr = tenantID
		}

		if ptr, ok := dest[2].(*string); ok {
			*ptr = "match_run"
		}

		if ptr, ok := dest[3].(*uuid.UUID); ok {
			*ptr = entityID
		}

		if ptr, ok := dest[4].(*string); ok {
			*ptr = "CREATED"
		}

		if ptr, ok := dest[5].(**string); ok {
			*ptr = &actorID
		}

		if ptr, ok := dest[6].(*[]byte); ok {
			*ptr = changes
		}

		if ptr, ok := dest[7].(*time.Time); ok {
			*ptr = createdAt
		}

		if ptr, ok := dest[8].(*sql.NullInt64); ok {
			*ptr = sql.NullInt64{Int64: tenantSeq, Valid: true}
		}

		if ptr, ok := dest[9].(*[]byte); ok {
			*ptr = prevHash
		}

		if ptr, ok := dest[10].(*[]byte); ok {
			*ptr = recordHash
		}

		if ptr, ok := dest[11].(*sql.NullInt16); ok {
			*ptr = sql.NullInt16{Int16: hashVersion, Valid: true}
		}

		return nil
	}})

	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, id, log.ID)
	require.Equal(t, tenantID, log.TenantID)
	require.Equal(t, "match_run", log.EntityType)
	require.Equal(t, entityID, log.EntityID)
	require.Equal(t, "CREATED", log.Action)
	require.NotNil(t, log.ActorID)
	require.Equal(t, actorID, *log.ActorID)
	require.Equal(t, changes, log.Changes)
	require.Equal(t, createdAt, log.CreatedAt)
	require.Equal(t, tenantSeq, log.TenantSeq)
	require.Equal(t, prevHash, log.PrevHash)
	require.Equal(t, recordHash, log.RecordHash)
	require.Equal(t, hashVersion, log.HashVersion)
}

func TestScanAuditLog_NormalizesCreatedAtToUTC(t *testing.T) {
	t.Parallel()

	offsetInstant := time.Date(2026, 2, 16, 11, 4, 30, 0, time.FixedZone("BRT", -3*60*60))

	log, err := scanAuditLog(fakeScanner{scan: func(dest ...any) error {
		*(dest[0].(*uuid.UUID)) = uuid.New()
		*(dest[1].(*uuid.UUID)) = uuid.New()
		*(dest[2].(*string)) = "match_run"
		*(dest[3].(*uuid.UUID)) = uuid.New()
		*(dest[4].(*string)) = "UPDATED"
		*(dest[5].(**string)) = nil
		*(dest[6].(*[]byte)) = []byte(`{"field":"value"}`)
		*(dest[7].(*time.Time)) = offsetInstant
		*(dest[8].(*sql.NullInt64)) = sql.NullInt64{Valid: false}
		*(dest[9].(*[]byte)) = make([]byte, 32)
		*(dest[10].(*[]byte)) = make([]byte, 32)
		*(dest[11].(*sql.NullInt16)) = sql.NullInt16{Valid: false}

		return nil
	}})

	require.NoError(t, err)
	require.NotNil(t, log)
	assert.Equal(t, time.UTC, log.CreatedAt.Location())
	assert.Equal(t, offsetInstant.UTC(), log.CreatedAt)
}

func TestScanAuditLogError(t *testing.T) {
	t.Parallel()

	expectedErr := errTestScanFailed
	_, err := scanAuditLog(fakeScanner{scan: func(_ ...any) error {
		return expectedErr
	}})

	require.Error(t, err)
	require.Contains(t, err.Error(), "scanning audit log")
}

func TestScanAuditLog_NilScanner(t *testing.T) {
	t.Parallel()

	_, err := scanAuditLog(nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilScanner)
}

func TestExecuteCreate_SQLGeneration(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	auditLogID := uuid.New()
	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	actorID := "user@test.com"
	createdAt := time.Now().UTC()
	changes := []byte(`{"name":"test"}`)

	auditLog := &entities.AuditLog{
		ID:         auditLogID,
		TenantID:   uuid.New(),
		EntityType: "reconciliation_context",
		EntityID:   entityID,
		Action:     "CREATE",
		ActorID:    &actorID,
		Changes:    changes,
		CreatedAt:  createdAt,
	}

	// Hash chain values
	tenantSeq := int64(1)
	prevHash := make([]byte, 32)   // Genesis hash
	recordHash := make([]byte, 32) // Computed hash (mock value)

	mock.ExpectBegin()
	// Mock row-level lock acquisition (first record: no existing chain state)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(sql.ErrNoRows)
	// Mock acquireNextSequence upsert
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(tenantSeq))
	// Mock INSERT with RETURNING columns
	mock.ExpectQuery(`INSERT INTO audit_logs`).
		WithArgs(
			auditLogID, tenantID, "reconciliation_context", entityID, "CREATE", &actorID, changes, timeTruncatedToMicrosecondArg{expected: createdAt},
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "entity_type", "entity_id", "action", "actor_id", "changes", "created_at",
			"tenant_seq", "prev_hash", "record_hash", "hash_version",
		}).AddRow(auditLogID, tenantID, "reconciliation_context", entityID, "CREATE", &actorID, changes, createdAt,
			tenantSeq, prevHash, recordHash, int16(1)))
	mock.ExpectCommit()

	ctx := contextWithTenant()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	result, err := repo.executeCreate(ctx, tx, auditLog)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, auditLogID, result.ID)
	require.Equal(t, tenantID, result.TenantID)
	require.Equal(t, "reconciliation_context", result.EntityType)
	require.Equal(t, tenantSeq, result.TenantSeq)

	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestExecuteCreate_ExactHashComputation_Genesis asserts that the bytes
// passed to INSERT for prev_hash and record_hash match the hashchain
// package's canonical output for a first-record (seq=1) genesis case.
//
// Contrasts with TestExecuteCreate_SQLGeneration above which accepts any
// hash bytes via sqlmock.AnyArg() — that test catches SQL-shape drift but
// not wire drift in the hash computation (e.g., a regression that mutates
// the RecordData struct order, swaps prevHash and recordHash arguments,
// or skips CreatedAt truncation before hashing).
func TestExecuteCreate_ExactHashComputation_Genesis(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	// Deterministic inputs — if any of these changed the production code
	// would compute a different record hash and the byteSliceArg matcher
	// below would reject it. We lock them here so the test is hermetic.
	auditLogID := uuid.MustParse("6d3b45b4-50b6-4e9e-b6e8-2e0b8f1f0b9e")
	entityID := uuid.MustParse("9a9c4e0a-8c4b-4b3a-9a9c-4e0a8c4b4b3a")
	actorID := "hash-test-actor"
	// Deliberately sub-microsecond so we also prove the truncation step
	// is inside the hash computation. If executeCreate stops truncating
	// CreatedAt before hashing, the computed hash will differ from the
	// expected bytes and this test will fail loudly.
	createdAt := time.Date(2026, 4, 18, 12, 30, 45, 789_123_456, time.UTC)
	truncated := createdAt.Truncate(time.Microsecond)
	changes := []byte(`{"entity":"context","v":1}`)
	tenantID := defaultTenantUUID()

	auditLog := &entities.AuditLog{
		ID:         auditLogID,
		TenantID:   tenantID,
		EntityType: "reconciliation_context",
		EntityID:   entityID,
		Action:     "CREATE",
		ActorID:    &actorID,
		Changes:    changes,
		CreatedAt:  createdAt,
	}

	// Genesis case: prev_hash is 32 zero bytes returned by GenesisHash().
	prevHash := hashchain.GenesisHash()
	tenantSeq := int64(1)

	// Compute the expected record hash with the SAME truncated CreatedAt
	// the production code will feed to ComputeRecordHash. Any drift in
	// the RecordData struct, the hash-version constant, or the truncation
	// step will make executeCreate emit different bytes and fail the
	// byteSliceArg matcher below.
	expectedRecordHash, err := hashchain.ComputeRecordHash(prevHash, hashchain.RecordData{
		ID:          auditLogID,
		TenantID:    tenantID,
		TenantSeq:   tenantSeq,
		EntityType:  "reconciliation_context",
		EntityID:    entityID,
		Action:      "CREATE",
		ActorID:     &actorID,
		Changes:     json.RawMessage(changes),
		CreatedAt:   truncated,
		HashVersion: hashchain.HashVersion,
	})
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(tenantSeq))

	// The critical assertion: args 9 (prev_hash), 10 (record_hash) and 11
	// (hash_version) must match exactly. No sqlmock.AnyArg() allowed here.
	mock.ExpectQuery(`INSERT INTO audit_logs`).
		WithArgs(
			auditLogID,
			tenantID,
			"reconciliation_context",
			entityID,
			"CREATE",
			&actorID,
			changes,
			timeTruncatedToMicrosecondArg{expected: createdAt},
			tenantSeq,
			byteSliceArg{expected: prevHash},
			byteSliceArg{expected: expectedRecordHash},
			int16(hashchain.HashVersion),
		).
		WillReturnRows(sqlmock.NewRows(auditLogTestColumns).
			AddRow(auditLogID, tenantID, "reconciliation_context", entityID, "CREATE",
				&actorID, changes, truncated, tenantSeq, prevHash, expectedRecordHash, int16(hashchain.HashVersion)))
	mock.ExpectCommit()

	ctx := contextWithTenant()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	result, err := repo.executeCreate(ctx, tx, auditLog)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, expectedRecordHash, result.RecordHash,
		"returned record hash must match the canonical computation")
	assert.Equal(t, prevHash, result.PrevHash,
		"returned prev hash must be genesis for the first record")
	assert.Equal(t, truncated, result.CreatedAt,
		"CreatedAt must round-trip with microsecond truncation applied")

	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestExecuteCreate_ExactHashComputation_ChainLink asserts that a record
// at seq=N (N>1) inherits the previous record's hash and chains correctly.
// Exercises the non-genesis path in getPreviousHash (where the SELECT runs
// a live query against audit_logs rather than short-circuiting to
// GenesisHash()) and the delete-drift-guard at the wire level.
func TestExecuteCreate_ExactHashComputation_ChainLink(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	auditLogID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	entityID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	actorID := "chain-test-actor"
	createdAt := time.Date(2026, 4, 18, 15, 0, 0, 0, time.UTC)
	changes := []byte(`{"step":"N"}`)
	tenantID := defaultTenantUUID()

	auditLog := &entities.AuditLog{
		ID:         auditLogID,
		TenantID:   tenantID,
		EntityType: "match_run",
		EntityID:   entityID,
		Action:     "UPDATE",
		ActorID:    &actorID,
		Changes:    changes,
		CreatedAt:  createdAt,
	}

	// Seq = 7 — an arbitrary N > 1; the exact value matters only insofar
	// as getPreviousHash is asked for seq-1 = 6.
	tenantSeq := int64(7)

	// A non-trivial "previous" hash for seq=6 that the test will feed back
	// through the SELECT. If executeCreate ignored this bytes and sent the
	// genesis zero-hash instead, byteSliceArg would reject the INSERT.
	prevHash := make([]byte, 32)
	for i := range prevHash {
		prevHash[i] = byte(i + 1) // deterministic non-zero fixture: 0x01..0x20
	}

	expectedRecordHash, err := hashchain.ComputeRecordHash(prevHash, hashchain.RecordData{
		ID:          auditLogID,
		TenantID:    tenantID,
		TenantSeq:   tenantSeq,
		EntityType:  "match_run",
		EntityID:    entityID,
		Action:      "UPDATE",
		ActorID:     &actorID,
		Changes:     json.RawMessage(changes),
		CreatedAt:   createdAt,
		HashVersion: hashchain.HashVersion,
	})
	require.NoError(t, err)

	mock.ExpectBegin()
	// Lock query: seq > 1 so the chain_state row already exists.
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(tenantSeq))
	// getPreviousHash executes this SELECT for seq-1 = 6.
	mock.ExpectQuery(`SELECT record_hash FROM audit_logs WHERE tenant_id = \$1 AND tenant_seq = \$2`).
		WithArgs(tenantID, tenantSeq-1).
		WillReturnRows(sqlmock.NewRows([]string{"record_hash"}).AddRow(prevHash))

	mock.ExpectQuery(`INSERT INTO audit_logs`).
		WithArgs(
			auditLogID,
			tenantID,
			"match_run",
			entityID,
			"UPDATE",
			&actorID,
			changes,
			timeTruncatedToMicrosecondArg{expected: createdAt},
			tenantSeq,
			byteSliceArg{expected: prevHash},
			byteSliceArg{expected: expectedRecordHash},
			int16(hashchain.HashVersion),
		).
		WillReturnRows(sqlmock.NewRows(auditLogTestColumns).
			AddRow(auditLogID, tenantID, "match_run", entityID, "UPDATE",
				&actorID, changes, createdAt, tenantSeq, prevHash, expectedRecordHash, int16(hashchain.HashVersion)))
	mock.ExpectCommit()

	ctx := contextWithTenant()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	result, err := repo.executeCreate(ctx, tx, auditLog)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, prevHash, result.PrevHash,
		"prev_hash must match the previous record's hash returned from SELECT")
	assert.Equal(t, expectedRecordHash, result.RecordHash,
		"record_hash must chain from the previous hash and current record data")

	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_InsertError(t *testing.T) {
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
		Action:     "UPDATE",
		Changes:    []byte(`{}`),
		CreatedAt:  time.Now().UTC(),
	}

	mock.ExpectBegin()
	// Mock row-level lock acquisition (first record: no existing chain state)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(sql.ErrNoRows)
	// Mock acquireNextSequence upsert
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(int64(1)))
	// Mock INSERT to fail
	mock.ExpectQuery(`INSERT INTO audit_logs`).
		WillReturnError(errTestConstraintViolation)

	ctx := contextWithTenant()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	_, err = repo.executeCreate(ctx, tx, auditLog)
	require.Error(t, err)
	require.Contains(t, err.Error(), "constraint violation")
}

func TestApplyAuditLogFilter(t *testing.T) {
	t.Parallel()

	t.Run("applies actor filter", func(t *testing.T) {
		t.Parallel()

		actor := "user@test.com"
		filter := entities.AuditLogFilter{Actor: &actor}

		qb := squirrel.Select("*").From("audit_logs").PlaceholderFormat(squirrel.Dollar)
		qb = applyAuditLogFilter(qb, filter, nil)

		query, args, err := qb.ToSql()
		require.NoError(t, err)
		require.Contains(t, query, "actor_id")
		require.Contains(t, args, actor)
	})

	t.Run("applies date range filter", func(t *testing.T) {
		t.Parallel()

		dateFrom := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		dateTo := time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC)
		filter := entities.AuditLogFilter{DateFrom: &dateFrom, DateTo: &dateTo}

		qb := squirrel.Select("*").From("audit_logs").PlaceholderFormat(squirrel.Dollar)
		qb = applyAuditLogFilter(qb, filter, nil)

		query, args, err := qb.ToSql()
		require.NoError(t, err)
		require.Contains(t, query, "created_at >= $")
		require.Contains(t, query, "created_at <= $")
		require.Contains(t, args, dateFrom)
		require.Contains(t, args, dateTo)
	})

	t.Run("applies cursor filter", func(t *testing.T) {
		t.Parallel()

		cursorTime := time.Now().UTC()
		cursorID := uuid.New()
		cursor := &sharedhttp.TimestampCursor{Timestamp: cursorTime, ID: cursorID}
		filter := entities.AuditLogFilter{}

		qb := squirrel.Select("*").From("audit_logs").PlaceholderFormat(squirrel.Dollar)
		qb = applyAuditLogFilter(qb, filter, cursor)

		query, _, err := qb.ToSql()
		require.NoError(t, err)
		require.Contains(t, query, "(created_at, id) < ")
	})

	t.Run("applies all filters together", func(t *testing.T) {
		t.Parallel()

		actor := "admin"
		action := "DELETE"
		entityType := "source"
		filter := entities.AuditLogFilter{
			Actor:      &actor,
			Action:     &action,
			EntityType: &entityType,
		}

		qb := squirrel.Select("*").From("audit_logs").PlaceholderFormat(squirrel.Dollar)
		qb = applyAuditLogFilter(qb, filter, nil)

		query, args, err := qb.ToSql()
		require.NoError(t, err)
		require.Contains(t, query, "actor_id")
		require.Contains(t, query, "action")
		require.Contains(t, query, "entity_type")
		require.Contains(t, args, actor)
		require.Contains(t, args, action)
		require.Contains(t, args, entityType)
	})
}

// setupMockRepository creates a repository with sqlmock for testing database interactions.
func setupMockRepository(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	finish := func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}

	return repo, mock, finish
}

// TEST HELPERS - These functions are for unit tests only and should not be used
// in production authentication flows.

// contextWithTenant creates a context with the default tenant ID for testing.
// This function sets auth.TenantIDKey directly in the context for test isolation.
// Production code must extract tenant IDs from JWT claims via auth middleware.
// Uses auth.DefaultTenantID to skip schema SET commands in tests.
func contextWithTenant() context.Context {
	return context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
}

// defaultTenantUUID returns the default tenant ID as a UUID for test data.
// This is a test helper that parses auth.DefaultTenantID for use in mock expectations.
func defaultTenantUUID() uuid.UUID {
	id, _ := uuid.Parse(auth.DefaultTenantID)
	return id
}

func TestGetByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	auditLogID := uuid.New()
	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	actorID := "user@test.com"
	createdAt := time.Now().UTC()
	changes := []byte(`{"field":"value"}`)

	ctx := contextWithTenant()

	query := regexp.QuoteMeta(
		`SELECT ` + auditLogColumns + ` FROM audit_logs WHERE id = $1 AND tenant_id = $2`,
	)

	tenantSeq, prevHash, recordHash, hashVersion := defaultHashChainValues()
	rows := sqlmock.NewRows(auditLogTestColumns).
		AddRow(auditLogID, tenantID, "reconciliation_context", entityID, "CREATE", &actorID, changes, createdAt,
			tenantSeq, prevHash, recordHash, hashVersion)

	mock.ExpectQuery(query).WithArgs(auditLogID, tenantID).WillReturnRows(rows)

	result, err := repo.GetByID(ctx, auditLogID)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, auditLogID, result.ID)
	require.Equal(t, tenantID, result.TenantID)
	require.Equal(t, "reconciliation_context", result.EntityType)
	require.Equal(t, entityID, result.EntityID)
	require.Equal(t, "CREATE", result.Action)
	require.NotNil(t, result.ActorID)
	require.Equal(t, actorID, *result.ActorID)
	require.Equal(t, changes, result.Changes)
	require.Equal(t, createdAt, result.CreatedAt)
}

func TestGetByID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	missingID := uuid.New()
	ctx := contextWithTenant()

	query := regexp.QuoteMeta(
		`SELECT ` + auditLogColumns + ` FROM audit_logs WHERE id = $1 AND tenant_id = $2`,
	)

	mock.ExpectQuery(query).WithArgs(missingID, tenantID).WillReturnError(sql.ErrNoRows)

	_, err := repo.GetByID(ctx, missingID)
	require.ErrorIs(t, err, ErrAuditLogNotFound)
}

func TestGetByID_NilID(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})
	ctx := contextWithTenant()

	_, err := repo.GetByID(ctx, uuid.Nil)
	require.ErrorIs(t, err, ErrIDRequired)
}

func TestGetByID_InvalidTenantID(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})
	// Context with invalid (non-UUID) tenant ID
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")

	_, err := repo.GetByID(ctx, uuid.New())
	require.ErrorIs(t, err, entities.ErrTenantIDRequired)
}

func TestListByEntity_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	entityType := "match_run"
	limit := 10

	ctx := contextWithTenant()

	auditLogID1 := uuid.New()
	auditLogID2 := uuid.New()
	createdAt1 := time.Now().UTC()
	createdAt2 := createdAt1.Add(-time.Hour)
	changes := []byte(`{}`)

	tenantSeq, prevHash, recordHash, hashVersion := defaultHashChainValues()
	rows := sqlmock.NewRows(auditLogTestColumns).
		AddRow(auditLogID1, tenantID, entityType, entityID, "CREATE", nil, changes, createdAt1,
			tenantSeq, prevHash, recordHash, hashVersion).
		AddRow(auditLogID2, tenantID, entityType, entityID, "UPDATE", nil, changes, createdAt2,
			tenantSeq+1, prevHash, recordHash, hashVersion)

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 AND entity_type = \$2 AND entity_id = \$3 ORDER BY`).
		WithArgs(tenantID, entityType, entityID).WillReturnRows(rows)

	logs, nextCursor, err := repo.ListByEntity(ctx, entityType, entityID, nil, limit)
	require.NoError(t, err)
	require.Len(t, logs, 2)
	require.Empty(t, nextCursor)
	require.Equal(t, auditLogID1, logs[0].ID)
	require.Equal(t, auditLogID2, logs[1].ID)
}

func TestListByEntity_WithCursor(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	entityType := "match_run"
	limit := 10

	ctx := contextWithTenant()

	cursorTime := time.Now().UTC()
	cursorID := uuid.New()
	cursor := &sharedhttp.TimestampCursor{Timestamp: cursorTime, ID: cursorID}

	auditLogID := uuid.New()
	createdAt := cursorTime.Add(-time.Hour)
	changes := []byte(`{}`)

	tenantSeq, prevHash, recordHash, hashVersion := defaultHashChainValues()
	rows := sqlmock.NewRows(auditLogTestColumns).
		AddRow(auditLogID, tenantID, entityType, entityID, "UPDATE", nil, changes, createdAt,
			tenantSeq, prevHash, recordHash, hashVersion)

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 AND entity_type = \$2 AND entity_id = \$3 AND \(created_at, id\) < \(\$4, \$5\) ORDER BY`).
		WithArgs(tenantID, entityType, entityID, cursorTime, cursorID).
		WillReturnRows(rows)

	logs, nextCursor, err := repo.ListByEntity(ctx, entityType, entityID, cursor, limit)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Empty(t, nextCursor)
	require.Equal(t, auditLogID, logs[0].ID)
}

func TestListByEntity_WithPagination(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	entityType := "reconciliation_context"
	limit := 2

	ctx := contextWithTenant()

	changes := []byte(`{}`)
	createdAt1 := time.Now().UTC()
	createdAt2 := createdAt1.Add(-time.Minute)
	createdAt3 := createdAt2.Add(-time.Minute)
	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	tenantSeq, prevHash, recordHash, hashVersion := defaultHashChainValues()
	rows := sqlmock.NewRows(auditLogTestColumns).
		AddRow(id1, tenantID, entityType, entityID, "CREATE", nil, changes, createdAt1,
			tenantSeq, prevHash, recordHash, hashVersion).
		AddRow(id2, tenantID, entityType, entityID, "UPDATE", nil, changes, createdAt2,
			tenantSeq+1, prevHash, recordHash, hashVersion).
		AddRow(id3, tenantID, entityType, entityID, "DELETE", nil, changes, createdAt3,
			tenantSeq+2, prevHash, recordHash, hashVersion)

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 AND entity_type = \$2 AND entity_id = \$3 ORDER BY`).
		WithArgs(tenantID, entityType, entityID).WillReturnRows(rows)

	logs, nextCursor, err := repo.ListByEntity(ctx, entityType, entityID, nil, limit)
	require.NoError(t, err)
	require.Len(t, logs, 2)
	require.NotEmpty(t, nextCursor)
}

func TestListByEntity_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	ctx := contextWithTenant()

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 AND entity_type = \$2 AND entity_id = \$3 ORDER BY`).
		WithArgs(tenantID, "match_run", entityID).
		WillReturnError(errTestConnectionFailed)

	_, _, err := repo.ListByEntity(ctx, "match_run", entityID, nil, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "list audit logs by entity transaction")
}

func TestList_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	limit := 10
	ctx := contextWithTenant()

	auditLogID := uuid.New()
	entityID := uuid.New()
	createdAt := time.Now().UTC()
	changes := []byte(`{}`)

	tenantSeq, prevHash, recordHash, hashVersion := defaultHashChainValues()
	rows := sqlmock.NewRows(auditLogTestColumns).
		AddRow(auditLogID, tenantID, "source", entityID, "DELETE", nil, changes, createdAt,
			tenantSeq, prevHash, recordHash, hashVersion)

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 ORDER BY`).
		WithArgs(tenantID).
		WillReturnRows(rows)

	logs, nextCursor, err := repo.List(ctx, entities.AuditLogFilter{}, nil, limit)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Empty(t, nextCursor)
	require.Equal(t, auditLogID, logs[0].ID)
}

func TestList_WithFilters(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	limit := 10
	ctx := contextWithTenant()

	actor := "admin@test.com"
	action := "CREATE"
	entityType := "reconciliation_context"
	filter := entities.AuditLogFilter{
		Actor:      &actor,
		Action:     &action,
		EntityType: &entityType,
	}

	auditLogID := uuid.New()
	entityID := uuid.New()
	createdAt := time.Now().UTC()
	changes := []byte(`{}`)

	tenantSeq, prevHash, recordHash, hashVersion := defaultHashChainValues()
	rows := sqlmock.NewRows(auditLogTestColumns).
		AddRow(auditLogID, tenantID, entityType, entityID, action, &actor, changes, createdAt,
			tenantSeq, prevHash, recordHash, hashVersion)

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 AND actor_id = \$2 AND action = \$3 AND entity_type = \$4 ORDER BY`).
		WithArgs(tenantID, actor, action, entityType).
		WillReturnRows(rows)

	logs, nextCursor, err := repo.List(ctx, filter, nil, limit)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Empty(t, nextCursor)
	require.Equal(t, auditLogID, logs[0].ID)
	require.NotNil(t, logs[0].ActorID)
	require.Equal(t, actor, *logs[0].ActorID)
}

func TestList_WithDateRangeFilter(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	limit := 10
	ctx := contextWithTenant()

	dateFrom := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	dateTo := time.Date(2025, 1, 31, 23, 59, 59, 999999999, time.UTC)
	filter := entities.AuditLogFilter{
		DateFrom: &dateFrom,
		DateTo:   &dateTo,
	}

	auditLogID := uuid.New()
	entityID := uuid.New()
	createdAt := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	changes := []byte(`{}`)

	tenantSeq, prevHash, recordHash, hashVersion := defaultHashChainValues()
	rows := sqlmock.NewRows(auditLogTestColumns).
		AddRow(auditLogID, tenantID, "match_run", entityID, "UPDATE", nil, changes, createdAt,
			tenantSeq, prevHash, recordHash, hashVersion)

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 AND created_at >= \$2 AND created_at <= \$3 ORDER BY`).
		WithArgs(tenantID, dateFrom, dateTo).
		WillReturnRows(rows)

	logs, _, err := repo.List(ctx, filter, nil, limit)
	require.NoError(t, err)
	require.Len(t, logs, 1)
}

func TestList_WithCursor(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	limit := 10
	ctx := contextWithTenant()

	cursorTime := time.Now().UTC()
	cursorID := uuid.New()
	cursor := &sharedhttp.TimestampCursor{Timestamp: cursorTime, ID: cursorID}

	auditLogID := uuid.New()
	entityID := uuid.New()
	createdAt := cursorTime.Add(-time.Hour)
	changes := []byte(`{}`)

	tenantSeq, prevHash, recordHash, hashVersion := defaultHashChainValues()
	rows := sqlmock.NewRows(auditLogTestColumns).
		AddRow(auditLogID, tenantID, "source", entityID, "CREATE", nil, changes, createdAt,
			tenantSeq, prevHash, recordHash, hashVersion)

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1 AND \(created_at, id\) < \(\$2, \$3\) ORDER BY`).
		WithArgs(tenantID, cursorTime, cursorID).
		WillReturnRows(rows)

	logs, _, err := repo.List(ctx, entities.AuditLogFilter{}, cursor, limit)
	require.NoError(t, err)
	require.Len(t, logs, 1)
}

func TestList_InvalidLimit(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})
	ctx := contextWithTenant()

	_, _, err := repo.List(ctx, entities.AuditLogFilter{}, nil, 0)
	require.ErrorIs(t, err, ErrLimitMustBePositive)

	_, _, err = repo.List(ctx, entities.AuditLogFilter{}, nil, -1)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
}

func TestList_InvalidTenantID(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})
	// Context with invalid (non-UUID) tenant ID
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")

	_, _, err := repo.List(ctx, entities.AuditLogFilter{}, nil, 10)
	require.ErrorIs(t, err, entities.ErrTenantIDRequired)
}

func TestList_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	tenantID := defaultTenantUUID()
	ctx := contextWithTenant()

	mock.ExpectQuery(`SELECT .+ FROM audit_logs WHERE tenant_id = \$1`).
		WithArgs(tenantID).
		WillReturnError(errTestDatabaseError)

	_, _, err := repo.List(ctx, entities.AuditLogFilter{}, nil, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "list audit logs transaction")
}

func TestSafeLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    int
		expected uint64
	}{
		{"positive value", 10, 10},
		{"zero", 0, 0},
		{"negative value", -5, 0},
		{"large positive", 1000000, 1000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, safeLimit(tt.input))
		})
	}
}

func TestBuildNextCursor(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	makeLog := func(id uuid.UUID, createdAt time.Time) *entities.AuditLog {
		return &entities.AuditLog{ID: id, CreatedAt: createdAt}
	}

	tests := []struct {
		name            string
		logs            []*entities.AuditLog
		limit           int
		expectTrimmed   int
		expectHasCursor bool
	}{
		{
			name:            "no logs",
			logs:            nil,
			limit:           10,
			expectTrimmed:   0,
			expectHasCursor: false,
		},
		{
			name:            "logs equal to limit",
			logs:            []*entities.AuditLog{makeLog(id1, now)},
			limit:           1,
			expectTrimmed:   1,
			expectHasCursor: false,
		},
		{
			name:            "logs less than limit",
			logs:            []*entities.AuditLog{makeLog(id1, now)},
			limit:           10,
			expectTrimmed:   1,
			expectHasCursor: false,
		},
		{
			name: "logs exceed limit - returns cursor",
			logs: []*entities.AuditLog{
				makeLog(id1, now),
				makeLog(id2, now.Add(-time.Second)),
				makeLog(id3, now.Add(-2*time.Second)),
			},
			limit:           2,
			expectTrimmed:   2,
			expectHasCursor: true,
		},
		{
			name:            "zero limit returns all logs without cursor",
			logs:            []*entities.AuditLog{makeLog(id1, now)},
			limit:           0,
			expectTrimmed:   1,
			expectHasCursor: false,
		},
		{
			name:            "negative limit returns all logs without cursor",
			logs:            []*entities.AuditLog{makeLog(id1, now)},
			limit:           -1,
			expectTrimmed:   1,
			expectHasCursor: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, cursor, err := buildNextCursor(tt.logs, tt.limit)
			require.NoError(t, err)

			require.Len(t, result, tt.expectTrimmed)

			if tt.expectHasCursor {
				require.NotEmpty(t, cursor, "expected a cursor")
			} else {
				require.Empty(t, cursor, "expected no cursor")
			}
		})
	}
}

func TestBuildNextCursorWithEncoder_EncodeFailure(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	logs := []*entities.AuditLog{
		{ID: uuid.New(), CreatedAt: now},
		{ID: uuid.New(), CreatedAt: now.Add(-time.Second)},
	}

	trimmed, cursor, err := buildNextCursorWithEncoder(
		logs,
		1,
		func(_ time.Time, _ uuid.UUID) (string, error) {
			return "", errTestDatabaseError
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, errTestDatabaseError)
	assert.Contains(t, err.Error(), "encode next cursor")
	require.Len(t, trimmed, 1)
	assert.Empty(t, cursor)
}

func TestBuildNextCursorWithEncoder_NilEncoder(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	logs := []*entities.AuditLog{
		{ID: uuid.New(), CreatedAt: now},
		{ID: uuid.New(), CreatedAt: now.Add(-time.Second)},
	}

	trimmed, cursor, err := buildNextCursorWithEncoder(logs, 1, nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCursorEncoderRequired)
	assert.Contains(t, err.Error(), "encode next cursor")
	require.Len(t, trimmed, 1)
	assert.Empty(t, cursor)
}

func TestCreate_NilAuditLog(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})
	ctx := contextWithTenant()

	_, err := repo.Create(ctx, nil)
	require.ErrorIs(t, err, ErrAuditLogRequired)
}

func TestCreateWithTx_NilAuditLog(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})
	ctx := contextWithTenant()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = repo.CreateWithTx(ctx, tx, nil)
	require.ErrorIs(t, err, ErrAuditLogRequired)
}

func TestCreateWithTx_NilTransaction(t *testing.T) {
	t.Parallel()

	repo := NewRepository(&fakeInfrastructureProvider{})
	ctx := contextWithTenant()

	auditLog := &entities.AuditLog{
		ID:         uuid.New(),
		TenantID:   defaultTenantUUID(),
		EntityType: "test",
		EntityID:   uuid.New(),
		Action:     "CREATE",
		Changes:    []byte(`{}`),
		CreatedAt:  time.Now().UTC(),
	}

	_, err := repo.CreateWithTx(ctx, nil, auditLog)
	require.ErrorIs(t, err, ErrTransactionRequired)
}

func TestCreateWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := contextWithTenant()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	auditLog := &entities.AuditLog{
		ID:         uuid.New(),
		TenantID:   defaultTenantUUID(),
		EntityType: "test",
		EntityID:   uuid.New(),
		Action:     "CREATE",
		Changes:    []byte(`{}`),
		CreatedAt:  time.Now().UTC(),
	}

	_, err = repo.CreateWithTx(ctx, tx, auditLog)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestExecuteCreate_LargeJSONPayload(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	auditLogID := uuid.New()
	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	createdAt := time.Now().UTC()

	// Generate a large JSON payload (~1MB)
	largePayload := make([]byte, 0, 1024*1024)
	largePayload = append(largePayload, []byte(`{"data":"`)...)

	for i := 0; i < 1024*1024-20; i++ {
		largePayload = append(largePayload, 'x')
	}

	largePayload = append(largePayload, []byte(`"}`)...)

	auditLog := &entities.AuditLog{
		ID:         auditLogID,
		TenantID:   uuid.New(),
		EntityType: "large_entity",
		EntityID:   entityID,
		Action:     "CREATE",
		Changes:    largePayload,
		CreatedAt:  createdAt,
	}

	// Hash chain values
	tenantSeq := int64(1)
	prevHash := make([]byte, 32)   // Genesis hash (32 zero bytes)
	recordHash := make([]byte, 32) // Computed hash (mock value)

	mock.ExpectBegin()
	// Mock row-level lock acquisition (first record: no existing chain state)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(sql.ErrNoRows)
	// Mock acquireNextSequence upsert: INSERT INTO audit_log_chain_state ... RETURNING next_seq - 1
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(tenantSeq))
	// Mock INSERT with RETURNING columns (hash values are computed dynamically, use sqlmock.AnyArg())
	mock.ExpectQuery(`INSERT INTO audit_logs`).
		WithArgs(
			auditLogID, tenantID, "large_entity", entityID, "CREATE", nil, largePayload, timeTruncatedToMicrosecondArg{expected: createdAt},
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "entity_type", "entity_id", "action", "actor_id", "changes", "created_at",
			"tenant_seq", "prev_hash", "record_hash", "hash_version",
		}).AddRow(auditLogID, tenantID, "large_entity", entityID, "CREATE", nil, largePayload, createdAt,
			tenantSeq, prevHash, recordHash, int16(1)))
	mock.ExpectCommit()

	ctx := contextWithTenant()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	result, err := repo.executeCreate(ctx, tx, auditLog)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, auditLogID, result.ID)
	require.Equal(t, len(largePayload), len(result.Changes))
	require.Equal(t, tenantSeq, result.TenantSeq)

	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_EmptyJSONPayload(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	auditLogID := uuid.New()
	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	createdAt := time.Now().UTC()
	emptyPayload := []byte(`{}`)

	auditLog := &entities.AuditLog{
		ID:         auditLogID,
		TenantID:   uuid.New(),
		EntityType: "empty_changes",
		EntityID:   entityID,
		Action:     "DELETE",
		Changes:    emptyPayload,
		CreatedAt:  createdAt,
	}

	// Hash chain values
	tenantSeq := int64(1)
	prevHash := make([]byte, 32)   // Genesis hash
	recordHash := make([]byte, 32) // Computed hash (mock value)

	mock.ExpectBegin()
	// Mock row-level lock acquisition (first record: no existing chain state)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(sql.ErrNoRows)
	// Mock acquireNextSequence upsert
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(tenantSeq))
	// Mock INSERT with RETURNING columns
	mock.ExpectQuery(`INSERT INTO audit_logs`).
		WithArgs(
			auditLogID, tenantID, "empty_changes", entityID, "DELETE", nil, emptyPayload, timeTruncatedToMicrosecondArg{expected: createdAt},
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "entity_type", "entity_id", "action", "actor_id", "changes", "created_at",
			"tenant_seq", "prev_hash", "record_hash", "hash_version",
		}).AddRow(auditLogID, tenantID, "empty_changes", entityID, "DELETE", nil, emptyPayload, createdAt,
			tenantSeq, prevHash, recordHash, int16(1)))
	mock.ExpectCommit()

	ctx := contextWithTenant()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	result, err := repo.executeCreate(ctx, tx, auditLog)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, emptyPayload, result.Changes)
	require.Equal(t, tenantSeq, result.TenantSeq)

	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_NullJSONPayload(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	auditLogID := uuid.New()
	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	createdAt := time.Now().UTC()

	// Note: The hash chain requires valid JSON. Use "null" which is valid JSON.
	// Empty []byte{} is not valid JSON and will fail canonicalization.
	nullChanges := []byte(`null`)

	auditLog := &entities.AuditLog{
		ID:         auditLogID,
		TenantID:   uuid.New(),
		EntityType: "null_changes",
		EntityID:   entityID,
		Action:     "READ",
		Changes:    nullChanges,
		CreatedAt:  createdAt,
	}

	// Hash chain values
	tenantSeq := int64(1)
	prevHash := make([]byte, 32)   // Genesis hash
	recordHash := make([]byte, 32) // Computed hash (mock value)

	mock.ExpectBegin()
	// Mock row-level lock acquisition (first record: no existing chain state)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(sql.ErrNoRows)
	// Mock acquireNextSequence upsert
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(tenantSeq))
	// Mock INSERT with RETURNING columns
	mock.ExpectQuery(`INSERT INTO audit_logs`).
		WithArgs(
			auditLogID, tenantID, "null_changes", entityID, "READ", nil, nullChanges, timeTruncatedToMicrosecondArg{expected: createdAt},
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "entity_type", "entity_id", "action", "actor_id", "changes", "created_at",
			"tenant_seq", "prev_hash", "record_hash", "hash_version",
		}).AddRow(auditLogID, tenantID, "null_changes", entityID, "READ", nil, nullChanges, createdAt,
			tenantSeq, prevHash, recordHash, int16(1)))
	mock.ExpectCommit()

	ctx := contextWithTenant()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	result, err := repo.executeCreate(ctx, tx, auditLog)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, nullChanges, result.Changes)
	require.Equal(t, tenantSeq, result.TenantSeq)

	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_TruncatesCreatedAtToMicroseconds(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	auditLogID := uuid.New()
	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	createdAt := time.Date(2026, 2, 16, 14, 4, 30, 123456789, time.UTC)
	expectedCreatedAt := createdAt.Truncate(time.Microsecond)
	changes := []byte(`{"precision":"test"}`)

	auditLog := &entities.AuditLog{
		ID:         auditLogID,
		TenantID:   uuid.New(),
		EntityType: "precision_test",
		EntityID:   entityID,
		Action:     "UPDATE",
		Changes:    changes,
		CreatedAt:  createdAt,
	}

	tenantSeq := int64(1)
	prevHash := make([]byte, 32)
	recordHash := make([]byte, 32)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(tenantSeq))
	mock.ExpectQuery(`INSERT INTO audit_logs`).
		WithArgs(
			auditLogID,
			tenantID,
			"precision_test",
			entityID,
			"UPDATE",
			nil,
			changes,
			timeTruncatedToMicrosecondArg{expected: createdAt},
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows(auditLogTestColumns).AddRow(
			auditLogID,
			tenantID,
			"precision_test",
			entityID,
			"UPDATE",
			nil,
			changes,
			expectedCreatedAt,
			tenantSeq,
			prevHash,
			recordHash,
			int16(1),
		))
	mock.ExpectCommit()

	ctx := contextWithTenant()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	result, err := repo.executeCreate(ctx, tx, auditLog)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, expectedCreatedAt, auditLog.CreatedAt)
	require.Equal(t, expectedCreatedAt, result.CreatedAt)

	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecuteCreate_LockAcquisitionError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	tenantID := defaultTenantUUID()

	auditLog := &entities.AuditLog{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		EntityType: "test_entity",
		EntityID:   uuid.New(),
		Action:     "CREATE",
		Changes:    []byte(`{}`),
		CreatedAt:  time.Now().UTC(),
	}

	mock.ExpectBegin()
	// Mock lock acquisition to fail with a non-ErrNoRows error (e.g., connection lost)
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnError(errTestConnectionFailed)

	ctx := contextWithTenant()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	_, err = repo.executeCreate(ctx, tx, auditLog)
	require.Error(t, err)
	require.Contains(t, err.Error(), "lock chain state")
	require.ErrorIs(t, err, errTestConnectionFailed)
}

func TestExecuteCreate_ExistingChainState(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	auditLogID := uuid.New()
	tenantID := defaultTenantUUID()
	entityID := uuid.New()
	createdAt := time.Now().UTC()
	changes := []byte(`{"field":"value"}`)

	auditLog := &entities.AuditLog{
		ID:         auditLogID,
		TenantID:   uuid.New(),
		EntityType: "match_run",
		EntityID:   entityID,
		Action:     "UPDATE",
		Changes:    changes,
		CreatedAt:  createdAt,
	}

	tenantSeq := int64(5)
	prevHash := make([]byte, 32)
	recordHash := make([]byte, 32)

	mock.ExpectBegin()
	// Mock lock acquisition — row exists, FOR UPDATE lock acquired successfully
	mock.ExpectQuery(`SELECT 1 FROM audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	// Mock acquireNextSequence upsert — increments existing sequence
	mock.ExpectQuery(`INSERT INTO audit_log_chain_state`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(tenantSeq))
	// Mock getPreviousHash — seq > 1, queries audit_logs for prior record's hash
	mock.ExpectQuery(`SELECT record_hash FROM audit_logs`).
		WithArgs(tenantID, tenantSeq-1).
		WillReturnRows(sqlmock.NewRows([]string{"record_hash"}).AddRow(prevHash))
	// Mock INSERT with RETURNING columns
	mock.ExpectQuery(`INSERT INTO audit_logs`).
		WithArgs(
			auditLogID, tenantID, "match_run", entityID, "UPDATE", nil, changes, timeTruncatedToMicrosecondArg{expected: createdAt},
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows(auditLogTestColumns).AddRow(
			auditLogID, tenantID, "match_run", entityID, "UPDATE", nil, changes, createdAt,
			tenantSeq, prevHash, recordHash, int16(1)))
	mock.ExpectCommit()

	ctx := contextWithTenant()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	repo := &Repository{provider: &fakeInfrastructureProvider{}}
	result, err := repo.executeCreate(ctx, tx, auditLog)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, auditLogID, result.ID)
	require.Equal(t, tenantSeq, result.TenantSeq)

	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetTenantIDFromContext_MissingTenantKey(t *testing.T) {
	t.Parallel()

	// When no tenant key is set, auth.GetTenantID returns the default tenant ID
	// which is a valid UUID, so getTenantIDFromContext should succeed
	ctx := context.Background()

	tenantID, err := getTenantIDFromContext(ctx)
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, tenantID)
}

func TestGetTenantIDFromContext_InvalidFormat(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-uuid")

	_, err := getTenantIDFromContext(ctx)
	require.ErrorIs(t, err, entities.ErrTenantIDRequired)
	require.Contains(t, err.Error(), "invalid tenant id format")
}

func TestGetTenantIDFromContext_ValidUUID(t *testing.T) {
	t.Parallel()

	expectedID := uuid.New()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, expectedID.String())

	tenantID, err := getTenantIDFromContext(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedID, tenantID)
}
