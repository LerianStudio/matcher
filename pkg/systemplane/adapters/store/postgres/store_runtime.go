// Copyright 2025 Lerian Studio.

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

func (store *Store) encryptValue(target domain.Target, key string, value any) (any, error) {
	if store == nil || store.secretCodec == nil {
		return value, nil
	}

	encryptedValue, err := store.secretCodec.Encrypt(target, key, value)
	if err != nil {
		return nil, fmt.Errorf("postgres store encrypt value %q: %w", key, err)
	}

	return encryptedValue, nil
}

func (store *Store) decryptValue(target domain.Target, key string, value any) (any, error) {
	if store == nil || store.secretCodec == nil {
		return value, nil
	}

	decryptedValue, err := store.secretCodec.Decrypt(target, key, value)
	if err != nil {
		return nil, fmt.Errorf("postgres store decrypt value %q: %w", key, err)
	}

	return decryptedValue, nil
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
	if err != nil {
		return fmt.Errorf("notify exec: %w", err)
	}

	return nil
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
