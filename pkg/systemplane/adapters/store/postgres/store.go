// Copyright 2025 Lerian Studio.

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
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
func (store *Store) readRevision(
	ctx context.Context,
	queryRower sqlQueryRower,
	target domain.Target,
) (domain.Revision, error) {
	query := `SELECT revision FROM ` + store.qualifiedRevisions() + ` WHERE kind=$1 AND scope=$2 AND subject=$3`

	var rev uint64

	err := queryRower.QueryRowContext(ctx, query,
		string(target.Kind), string(target.Scope), target.SubjectID,
	).Scan(&rev)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RevisionZero, nil
	}

	if err != nil {
		return domain.RevisionZero, fmt.Errorf("select revision: %w", err)
	}

	return domain.Revision(rev), nil
}

// ensureRevisionRow guarantees the target has a row in the revision table.
func (store *Store) ensureRevisionRow(
	ctx context.Context,
	tx *sql.Tx,
	target domain.Target,
	actor domain.Actor,
	source string,
	now time.Time,
) error {
	// #nosec G202 -- table identifier is validated in bootstrap (operator-controlled, not user input).
	query := `INSERT INTO ` + store.qualifiedRevisions() + ` (kind, scope, subject, revision, updated_at, updated_by, source)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (kind, scope, subject) DO NOTHING`

	_, err := tx.ExecContext(ctx, query,
		string(target.Kind),
		string(target.Scope),
		target.SubjectID,
		domain.RevisionZero.Uint64(),
		now,
		actor.ID,
		source,
	)

	return err
}

// lockAndReadRevision selects the target revision row FOR UPDATE to prevent
// concurrent writers from racing on optimistic revision checks.
func (store *Store) lockAndReadRevision(
	ctx context.Context,
	tx *sql.Tx,
	target domain.Target,
) (domain.Revision, error) {
	query := `SELECT revision
		 FROM ` + store.qualifiedRevisions() + `
		 WHERE kind=$1 AND scope=$2 AND subject=$3
		 FOR UPDATE`

	var rev uint64

	err := tx.QueryRowContext(ctx, query,
		string(target.Kind), string(target.Scope), target.SubjectID,
	).Scan(&rev)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RevisionZero, nil
	}

	if err != nil {
		return domain.RevisionZero, fmt.Errorf("select revision for update: %w", err)
	}

	return domain.Revision(rev), nil
}

// updateRevisionRow persists the new target revision in the revision table.
func (store *Store) updateRevisionRow(
	ctx context.Context,
	tx *sql.Tx,
	target domain.Target,
	revision domain.Revision,
	behavior domain.ApplyBehavior,
	now time.Time,
	actor domain.Actor,
	source string,
) error {
	// #nosec G202 -- table identifier is validated in bootstrap (operator-controlled, not user input).
	query := `UPDATE ` + store.qualifiedRevisions() + `
		 SET revision=$4, apply_behavior=$5, updated_at=$6, updated_by=$7, source=$8
		 WHERE kind=$1 AND scope=$2 AND subject=$3`

	result, err := tx.ExecContext(ctx, query,
		string(target.Kind),
		string(target.Scope),
		target.SubjectID,
		revision.Uint64(),
		string(behavior),
		now,
		actor.ID,
		source,
	)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected != 1 {
		return fmt.Errorf("%w: expected 1 updated row, got %d", ErrRevisionRowUpdateMismatch, rowsAffected)
	}

	return nil
}

// applyOp processes a single WriteOp: fetches the old value for history,
// upserts or deletes the entry, then inserts the history record.
func (store *Store) applyOp(
	ctx context.Context,
	tx *sql.Tx,
	target domain.Target,
	op ports.WriteOp,
	newRevision domain.Revision,
	now time.Time,
	actor domain.Actor,
	source string,
) error {
	oldValueRaw, err := store.fetchOldValue(ctx, tx, target, op.Key)
	if err != nil {
		return fmt.Errorf("fetch old value: %w", err)
	}

	if op.Reset || domain.IsNilValue(op.Value) {
		if err := store.deleteEntry(ctx, tx, target, op.Key); err != nil {
			return fmt.Errorf("delete entry: %w", err)
		}
	} else {
		if err := store.upsertEntry(ctx, tx, target, op, newRevision, now, actor, source); err != nil {
			return fmt.Errorf("upsert entry: %w", err)
		}
	}

	if err := store.insertHistory(ctx, tx, target, op, oldValueRaw, newRevision, now, actor, source); err != nil {
		return fmt.Errorf("insert history: %w", err)
	}

	return nil
}

// fetchOldValue retrieves the current JSONB value for a key, or nil if the
// key does not exist.
func (store *Store) fetchOldValue(
	ctx context.Context,
	tx *sql.Tx,
	target domain.Target,
	key string,
) ([]byte, error) {
	query := `SELECT value FROM ` + store.qualifiedEntries() + ` WHERE kind=$1 AND scope=$2 AND subject=$3 AND key=$4`

	var raw []byte

	err := tx.QueryRowContext(ctx, query,
		string(target.Kind), string(target.Scope), target.SubjectID, key,
	).Scan(&raw)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("select old value: %w", err)
	}

	return raw, nil
}

// deleteEntry removes an entry from the entries table.
func (store *Store) deleteEntry(
	ctx context.Context,
	tx *sql.Tx,
	target domain.Target,
	key string,
) error {
	// #nosec G202 -- table identifier is validated in bootstrap (operator-controlled, not user input).
	query := `DELETE FROM ` + store.qualifiedEntries() + ` WHERE kind=$1 AND scope=$2 AND subject=$3 AND key=$4`

	_, err := tx.ExecContext(ctx, query,
		string(target.Kind), string(target.Scope), target.SubjectID, key,
	)

	return err
}

// upsertEntry inserts or updates an entry using ON CONFLICT.
func (store *Store) upsertEntry(
	ctx context.Context,
	tx *sql.Tx,
	target domain.Target,
	op ports.WriteOp,
	revision domain.Revision,
	now time.Time,
	actor domain.Actor,
	source string,
) error {
	valueForStorage, err := store.encryptValue(target, op.Key, op.Value)
	if err != nil {
		return fmt.Errorf("encrypt value: %w", err)
	}

	valueBytes, err := json.Marshal(valueForStorage)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	// #nosec G202 -- table identifier is validated in bootstrap (operator-controlled, not user input).
	query := `INSERT INTO ` + store.qualifiedEntries() + ` (kind, scope, subject, key, value, revision, updated_at, updated_by, source)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (kind, scope, subject, key) DO UPDATE
		 SET value=$5, revision=$6, updated_at=$7, updated_by=$8, source=$9`

	_, err = tx.ExecContext(ctx, query,
		string(target.Kind), string(target.Scope), target.SubjectID, op.Key,
		valueBytes, revision.Uint64(), now, actor.ID, source,
	)

	return err
}

// insertHistory appends an audit record to the history table.
func (store *Store) insertHistory(
	ctx context.Context,
	tx *sql.Tx,
	target domain.Target,
	op ports.WriteOp,
	oldValueRaw []byte,
	revision domain.Revision,
	now time.Time,
	actor domain.Actor,
	source string,
) error {
	var newValueRaw []byte

	if !op.Reset && !domain.IsNilValue(op.Value) {
		valueForHistory, err := store.encryptValue(target, op.Key, op.Value)
		if err != nil {
			return fmt.Errorf("encrypt new value for history: %w", err)
		}

		b, err := json.Marshal(valueForHistory)
		if err != nil {
			return fmt.Errorf("marshal new value for history: %w", err)
		}

		newValueRaw = b
	}

	// #nosec G202 -- table identifier is validated in bootstrap (operator-controlled, not user input).
	query := `INSERT INTO ` + store.qualifiedHistory() + ` (kind, scope, subject, key, old_value, new_value, revision, actor_id, changed_at, source)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	_, err := tx.ExecContext(ctx, query,
		string(target.Kind), string(target.Scope), target.SubjectID, op.Key,
		nullableJSONB(oldValueRaw), nullableJSONB(newValueRaw),
		revision.Uint64(), actor.ID, now, source,
	)

	return err
}

func (store *Store) encryptValue(target domain.Target, key string, value any) (any, error) {
	if store == nil || store.secretCodec == nil {
		return value, nil
	}

	return store.secretCodec.Encrypt(target, key, value)
}

func (store *Store) decryptValue(target domain.Target, key string, value any) (any, error) {
	if store == nil || store.secretCodec == nil {
		return value, nil
	}

	return store.secretCodec.Decrypt(target, key, value)
}

// notify sends a pg_notify event with a JSON payload describing the change.
func (store *Store) notify(
	ctx context.Context,
	tx *sql.Tx,
	target domain.Target,
	revision domain.Revision,
	behavior domain.ApplyBehavior,
) error {
	payload := notifyPayload{
		Kind:          string(target.Kind),
		Scope:         string(target.Scope),
		Subject:       target.SubjectID,
		Revision:      revision.Uint64(),
		ApplyBehavior: string(behavior),
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal notify payload: %w", err)
	}

	_, err = tx.ExecContext(ctx, "SELECT pg_notify($1, $2)", store.notifyChannel, string(payloadBytes))

	return err
}

func (store *Store) escalateBehavior(ops []ports.WriteOp) domain.ApplyBehavior {
	if store == nil {
		return domain.ApplyBundleRebuild
	}

	escalation := domain.ApplyLiveRead
	for _, op := range ops {
		behavior, ok := store.applyBehaviors[op.Key]
		if !ok {
			return domain.ApplyBundleRebuild
		}
		if behavior.Strength() > escalation.Strength() {
			escalation = behavior
		}
	}

	return escalation
}

// nullableJSONB returns nil (SQL NULL) for empty/nil byte slices, or the raw
// bytes otherwise.
func nullableJSONB(b []byte) any {
	if len(b) == 0 {
		return nil
	}

	return b
}
