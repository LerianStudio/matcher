// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

// Pins post-fix contract for fix-actor-mapping-pseudonymization-bypass.
//
// These sqlmock tests encode the post-fix repository contract:
//
//   - The SQL changes from `INSERT ... ON CONFLICT (actor_id) DO UPDATE SET
//     display_name = COALESCE(EXCLUDED.display_name, ...)` to
//     `INSERT ... ON CONFLICT (actor_id) DO NOTHING RETURNING ...`.
//   - When RETURNING yields a row (fresh actor_id), the repository returns it.
//   - When RETURNING yields nothing (actor_id already exists), the repository
//     SELECTs the current row and compares it to the payload:
//   - Identical → returns the existing entity (idempotent success).
//   - Different OR redacted → returns ErrActorMappingImmutable.
//   - Both the INSERT and the SELECT run inside the same transaction to
//     close the TOCTOU window where a concurrent UPDATE could overwrite
//     [REDACTED] between the read and the write.
//
// These tests guard the post-fix repository: (a) emits INSERT ... ON
// CONFLICT (actor_id) DO NOTHING RETURNING, (b) implements the post-INSERT
// comparison path, and (c) exposes ErrActorMappingImmutable at this layer.
package actormapping

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// newInsertOnConflictDoNothingQueryRegex matches the post-fix INSERT statement.
// We use a regex (rather than QuoteMeta) because squirrel's exact column-list
// formatting is an implementation detail; this guards the contract by checking
// the load-bearing tokens: INSERT INTO actor_mapping, the five columns
// (actor_id, display_name, email, created_at, updated_at), ON CONFLICT
// (actor_id) DO NOTHING, RETURNING the same five columns.
//
// Whitespace runs between tokens are matched with `\s+` (not `.*`) so the
// regex does not silently accept arbitrary SQL injected between, say,
// `DO NOTHING` and `RETURNING`. The column-list and VALUES segments use
// scoped wildcards (`[^)]*`) bounded by parentheses to keep them tight
// without coupling to squirrel's exact comma/space layout.
func newInsertOnConflictDoNothingQueryRegex() string {
	return `INSERT\s+INTO\s+actor_mapping\s*\([^)]*actor_id[^)]*\)\s+VALUES\s*\([^)]*\)\s+` +
		`ON\s+CONFLICT\s*\(\s*actor_id\s*\)\s+DO\s+NOTHING\s+RETURNING\s+` +
		`actor_id,\s*display_name,\s*email,\s*created_at,\s*updated_at`
}

// newSelectByActorIDQueryRegex matches the post-fix SELECT used by the
// repository to read the current row when INSERT returned no rows.
func newSelectByActorIDQueryRegex() string {
	return `SELECT\s+actor_id,\s*display_name,\s*email,\s*created_at,\s*updated_at\s+FROM\s+actor_mapping\s+WHERE\s+actor_id\s*=\s*\$1`
}

// AC1 (SQL layer) — fresh actor_id, INSERT ... ON CONFLICT DO NOTHING
// RETURNING yields the new row; no follow-up SELECT is needed.
func TestUpsert_NewActor_InsertReturnsRow_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	now := time.Now().UTC()
	displayName := "John Doe"
	email := "john@example.com"

	mapping := &entities.ActorMapping{
		ActorID:     "actor-new-101",
		DisplayName: &displayName,
		Email:       &email,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	mock.ExpectBegin()
	mock.ExpectQuery(newInsertOnConflictDoNothingQueryRegex()).
		WithArgs("actor-new-101", &displayName, &email, now, now).
		WillReturnRows(sqlmock.NewRows(actorMappingTestColumns).
			AddRow("actor-new-101", &displayName, &email, now, now))
	mock.ExpectCommit()

	result, err := repo.Upsert(ctx, mapping)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "actor-new-101", result.ActorID)
	require.NotNil(t, result.DisplayName)
	require.Equal(t, "John Doe", *result.DisplayName)
	require.NotNil(t, result.Email)
	require.Equal(t, "john@example.com", *result.Email)
}

// AC2 (SQL layer) — actor_id exists, payload matches existing row exactly.
// INSERT ... ON CONFLICT DO NOTHING returns no rows; the follow-up SELECT
// returns the current row; the repository sees the values match and
// returns the existing entity with no error (idempotent success).
func TestUpsert_IdempotentSameValues_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	createdAt := time.Now().UTC().Add(-time.Hour)
	updatedAt := createdAt
	displayName := "Jane Roe"
	email := "jane@example.com"

	payload := &entities.ActorMapping{
		ActorID:     "actor-idem-102",
		DisplayName: &displayName,
		Email:       &email,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}

	mock.ExpectBegin()
	// INSERT returns zero rows on conflict.
	mock.ExpectQuery(newInsertOnConflictDoNothingQueryRegex()).
		WithArgs("actor-idem-102", &displayName, &email, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows(actorMappingTestColumns))
	// SELECT returns the existing (identical) row.
	mock.ExpectQuery(newSelectByActorIDQueryRegex()).
		WithArgs("actor-idem-102").
		WillReturnRows(sqlmock.NewRows(actorMappingTestColumns).
			AddRow("actor-idem-102", &displayName, &email, createdAt, updatedAt))
	mock.ExpectCommit()

	result, err := repo.Upsert(ctx, payload)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "actor-idem-102", result.ActorID)
	require.NotNil(t, result.DisplayName)
	require.Equal(t, "Jane Roe", *result.DisplayName)
	require.NotNil(t, result.Email)
	require.Equal(t, "jane@example.com", *result.Email)
	require.Equal(t, updatedAt, result.UpdatedAt) // idempotent path preserves updated_at
}

// AC3 / AC4 (SQL layer) — actor_id exists, payload differs from existing row.
// INSERT returns no rows; SELECT shows a different email; repository
// returns ErrActorMappingImmutable and rolls back.
func TestUpsert_DifferentValues_ReturnsImmutable_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	createdAt := time.Now().UTC().Add(-time.Hour)
	updatedAt := createdAt
	originalDisplayName := "Original Name"
	originalEmail := "original@example.com"

	newDisplayName := "Original Name" // same
	newEmail := "different@example.com"

	payload := &entities.ActorMapping{
		ActorID:     "actor-mut-103",
		DisplayName: &newDisplayName,
		Email:       &newEmail,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}

	mock.ExpectBegin()
	mock.ExpectQuery(newInsertOnConflictDoNothingQueryRegex()).
		WithArgs("actor-mut-103", &newDisplayName, &newEmail, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows(actorMappingTestColumns))
	mock.ExpectQuery(newSelectByActorIDQueryRegex()).
		WithArgs("actor-mut-103").
		WillReturnRows(sqlmock.NewRows(actorMappingTestColumns).
			AddRow("actor-mut-103", &originalDisplayName, &originalEmail, createdAt, updatedAt))
	mock.ExpectRollback()

	result, err := repo.Upsert(ctx, payload)
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrActorMappingImmutable)
}

// AC5 (SQL layer) — pentest PoC reproduction at the repository level.
// Existing row is in the [REDACTED] state; attacker sends plaintext PII.
// The post-fix repository compares the SELECT result to the payload,
// sees they differ, and returns ErrActorMappingImmutable. The [REDACTED]
// row is preserved.
func TestUpsert_OverRedacted_ReturnsImmutable_Sqlmock(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	createdAt := time.Now().UTC().Add(-time.Hour)
	updatedAt := createdAt
	redacted := "[REDACTED]"

	attackerDisplayName := "Attacker Name"
	attackerEmail := "attacker@evil.example"

	payload := &entities.ActorMapping{
		ActorID:     "actor-pseudo-104",
		DisplayName: &attackerDisplayName,
		Email:       &attackerEmail,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}

	mock.ExpectBegin()
	mock.ExpectQuery(newInsertOnConflictDoNothingQueryRegex()).
		WithArgs("actor-pseudo-104", &attackerDisplayName, &attackerEmail, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows(actorMappingTestColumns))
	mock.ExpectQuery(newSelectByActorIDQueryRegex()).
		WithArgs("actor-pseudo-104").
		WillReturnRows(sqlmock.NewRows(actorMappingTestColumns).
			AddRow("actor-pseudo-104", &redacted, &redacted, createdAt, updatedAt))
	mock.ExpectRollback()

	result, err := repo.Upsert(ctx, payload)
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrActorMappingImmutable)
}

// Regression: ensure the sentinel is exported from the actormapping
// package so handler and service layers can errors.Is against it.
func TestErrActorMappingImmutable_Exported(t *testing.T) {
	t.Parallel()

	require.Error(t, ErrActorMappingImmutable)
	require.NotEmpty(t, ErrActorMappingImmutable.Error())
}
