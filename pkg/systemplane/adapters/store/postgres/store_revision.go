// Copyright 2025 Lerian Studio.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

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
	if err != nil {
		return fmt.Errorf("insert revision row: %w", err)
	}

	return nil
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
		return fmt.Errorf("update revision row exec: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("update revision row rows affected: %w", err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("%w: expected 1 updated row, got %d", ErrRevisionRowUpdateMismatch, rowsAffected)
	}

	return nil
}
