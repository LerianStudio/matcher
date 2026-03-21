// Copyright 2025 Lerian Studio.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/secretcodec"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface check.
var _ ports.Store = (*Store)(nil)

// ErrRevisionRowUpdateMismatch indicates that updating the target revision row
// did not affect exactly one row.
var ErrRevisionRowUpdateMismatch = errors.New("postgres store: revision row update mismatch")

// Store implements ports.Store backed by PostgreSQL with optimistic concurrency
// control and LISTEN/NOTIFY change propagation.
//
// SECURITY: All table/schema/channel names in this package are constructed from
// identifiers validated at bootstrap against ^[a-z_][a-z0-9_]{0,62}$ (see
// bootstrap.ValidatePostgresObjectNames). They are operator-controlled, never
// user input. Raw SQL concatenation via qualify() is safe under these
// constraints.
type Store struct {
	db             *sql.DB
	schema         string
	entriesTable   string
	historyTable   string
	revisionTable  string
	notifyChannel  string
	secretCodec    *secretcodec.Codec
	applyBehaviors map[string]domain.ApplyBehavior
}

// notifyPayload is the JSON structure sent via pg_notify on each Put.
type notifyPayload struct {
	Kind          string `json:"kind"`
	Scope         string `json:"scope"`
	Subject       string `json:"subject"`
	Revision      uint64 `json:"revision"`
	ApplyBehavior string `json:"apply_behavior,omitempty"`
}

// qualifiedEntries returns the schema-qualified entries table name.
func (store *Store) qualifiedEntries() string {
	return qualify(store.schema, store.entriesTable)
}

// qualifiedHistory returns the schema-qualified history table name.
func (store *Store) qualifiedHistory() string {
	return qualify(store.schema, store.historyTable)
}

// qualifiedRevisions returns the schema-qualified revisions table name.
func (store *Store) qualifiedRevisions() string {
	return qualify(store.schema, store.revisionTable)
}

// Get retrieves all entries for a target at its current revision.
// If the target has never been written, it returns an empty ReadResult with
// RevisionZero.
func (store *Store) Get(ctx context.Context, target domain.Target) (ports.ReadResult, error) {
	if store == nil || store.db == nil {
		return ports.ReadResult{}, ErrNilDB
	}

	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return ports.ReadResult{}, fmt.Errorf("postgres store get: begin tx: %w", err)
	}

	defer func() {
		_ = tx.Rollback() // no-op after Commit
	}()

	// #nosec G202 -- table identifier is validated in bootstrap (operator-controlled, not user input).
	query := `SELECT key, value, revision, updated_at, updated_by, source
		 FROM ` + store.qualifiedEntries() + `
		 WHERE kind=$1 AND scope=$2 AND subject=$3`

	rows, err := tx.QueryContext(ctx, query,
		string(target.Kind), string(target.Scope), target.SubjectID,
	)
	if err != nil {
		return ports.ReadResult{}, fmt.Errorf("postgres store get: query: %w", err)
	}

	defer rows.Close()

	var entries []domain.Entry

	for rows.Next() {
		var (
			key       string
			valueRaw  []byte
			revision  uint64
			updatedAt time.Time
			updatedBy string
			source    string
		)

		if err := rows.Scan(&key, &valueRaw, &revision, &updatedAt, &updatedBy, &source); err != nil {
			return ports.ReadResult{}, fmt.Errorf("postgres store get: scan: %w", err)
		}

		var value any

		if valueRaw != nil {
			decodedValue, err := decodeJSONValue(valueRaw)
			if err != nil {
				return ports.ReadResult{}, fmt.Errorf("postgres store get: unmarshal value for key %q: %w", key, err)
			}

			decodedValue, err = store.decryptValue(target, key, decodedValue)
			if err != nil {
				return ports.ReadResult{}, fmt.Errorf("postgres store get: decrypt value for key %q: %w", key, err)
			}

			value = decodedValue
		}

		entries = append(entries, domain.Entry{
			Kind:      target.Kind,
			Scope:     target.Scope,
			Subject:   target.SubjectID,
			Key:       key,
			Value:     value,
			Revision:  domain.Revision(revision),
			UpdatedAt: updatedAt,
			UpdatedBy: updatedBy,
			Source:    source,
		})
	}

	if err := rows.Err(); err != nil {
		return ports.ReadResult{}, fmt.Errorf("postgres store get: rows iteration: %w", err)
	}

	revision, err := store.readRevision(ctx, tx, target)
	if err != nil {
		return ports.ReadResult{}, fmt.Errorf("postgres store get: read revision: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return ports.ReadResult{}, fmt.Errorf("postgres store get: commit tx: %w", err)
	}

	return ports.ReadResult{
		Entries:  entries,
		Revision: revision,
	}, nil
}

// Put atomically writes a batch of operations for a target. It uses a
// transaction with SELECT ... FOR UPDATE to enforce optimistic concurrency.
// If the expected revision does not match, it returns domain.ErrRevisionMismatch
// alongside the current revision in storage.
//
// On success it emits a pg_notify event so that LISTEN-based watchers can
// detect changes without polling.
func (store *Store) Put(
	ctx context.Context,
	target domain.Target,
	ops []ports.WriteOp,
	expected domain.Revision,
	actor domain.Actor,
	source string,
) (domain.Revision, error) {
	if store == nil || store.db == nil {
		return domain.RevisionZero, ErrNilDB
	}

	if len(ops) == 0 {
		revision, err := store.readRevision(ctx, store.db, target)
		if err != nil {
			return domain.RevisionZero, fmt.Errorf("postgres store put: read revision for empty batch: %w", err)
		}

		return revision, nil
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.RevisionZero, fmt.Errorf("postgres store put: begin tx: %w", err)
	}

	defer func() {
		_ = tx.Rollback() // no-op after Commit
	}()

	newRevision, err := store.putInTx(ctx, tx, target, ops, expected, actor, source)
	if err != nil {
		return newRevision, fmt.Errorf("postgres store put: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return domain.RevisionZero, fmt.Errorf("postgres store put: commit: %w", err)
	}

	return newRevision, nil
}

// putInTx performs the core Put logic within an existing transaction: revision
// locking, operation application, revision update, and notification.
func (store *Store) putInTx(
	ctx context.Context,
	tx *sql.Tx,
	target domain.Target,
	ops []ports.WriteOp,
	expected domain.Revision,
	actor domain.Actor,
	source string,
) (domain.Revision, error) {
	now := time.Now().UTC()

	if err := store.ensureRevisionRow(ctx, tx, target, actor, source, now); err != nil {
		return domain.RevisionZero, fmt.Errorf("ensure revision row: %w", err)
	}

	// Acquire a row-level lock on the target revision row.
	currentRev, err := store.lockAndReadRevision(ctx, tx, target)
	if err != nil {
		return domain.RevisionZero, fmt.Errorf("lock revision: %w", err)
	}

	if expected != currentRev {
		return currentRev, domain.ErrRevisionMismatch
	}

	newRevision := expected.Next()

	for _, op := range ops {
		if err := store.applyOp(ctx, tx, target, op, newRevision, now, actor, source); err != nil {
			return domain.RevisionZero, fmt.Errorf("apply op key %q: %w", op.Key, err)
		}
	}

	if err := store.updateRevisionRow(ctx, tx, target, newRevision, store.escalateBehavior(ops), now, actor, source); err != nil {
		return domain.RevisionZero, fmt.Errorf("update revision row: %w", err)
	}

	if err := store.notify(ctx, tx, target, newRevision, store.escalateBehavior(ops)); err != nil {
		return domain.RevisionZero, fmt.Errorf("notify: %w", err)
	}

	return newRevision, nil
}

// sqlQueryRower is a minimal QueryRowContext surface shared by sql.DB and sql.Tx.
type sqlQueryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// readRevision reads the current revision for a target from the revision table.
