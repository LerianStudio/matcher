// Copyright 2025 Lerian Studio.

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

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
	if err != nil {
		return fmt.Errorf("delete entry exec: %w", err)
	}

	return nil
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
	if err != nil {
		return fmt.Errorf("upsert entry exec: %w", err)
	}

	return nil
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
	if err != nil {
		return fmt.Errorf("insert history exec: %w", err)
	}

	return nil
}
