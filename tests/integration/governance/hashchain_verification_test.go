//go:build integration

package governance

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	governancePostgres "github.com/LerianStudio/matcher/internal/governance/adapters/postgres"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/hashchain"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/tests/integration"
)

// hashTestCtx returns a context with both tenant ID and tenant slug set,
// suitable for governance operations that require full tenant context.
func hashTestCtx(t *testing.T, h *integration.TestHarness) context.Context {
	t.Helper()

	ctx := h.Ctx()
	ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

	return ctx
}

// auditLogToChainRecord converts an AuditLog entity (as returned by the repository)
// into a hashchain.ChainRecord suitable for VerifyChain.
func auditLogToChainRecord(log *entities.AuditLog) hashchain.ChainRecord {
	return hashchain.ChainRecord{
		TenantSeq:   log.TenantSeq,
		PrevHash:    log.PrevHash,
		RecordHash:  log.RecordHash,
		HashVersion: log.HashVersion,
		Data: hashchain.RecordData{
			ID:          log.ID,
			TenantID:    log.TenantID,
			TenantSeq:   log.TenantSeq,
			EntityType:  log.EntityType,
			EntityID:    log.EntityID,
			Action:      log.Action,
			ActorID:     log.ActorID,
			Changes:     json.RawMessage(log.Changes),
			CreatedAt:   log.CreatedAt,
			HashVersion: log.HashVersion,
		},
	}
}

// readChainByTenantSeq reads audit logs ordered by tenant_seq ASC using raw SQL.
// This bypasses the repository's read methods to get records in chain order
// (the repository's ListByEntity returns created_at DESC which is wrong for chain verification).
func readChainByTenantSeq(
	ctx context.Context,
	t *testing.T,
	h *integration.TestHarness,
	entityType string,
	entityID uuid.UUID,
) []*entities.AuditLog {
	t.Helper()

	logs, err := pgcommon.WithTenantTxProvider(
		ctx,
		h.Provider(),
		func(tx *sql.Tx) ([]*entities.AuditLog, error) {
			rows, err := tx.QueryContext(ctx,
				`SELECT id, tenant_id, entity_type, entity_id, action, actor_id,
				        changes, created_at, tenant_seq, prev_hash, record_hash, hash_version
				   FROM audit_logs
				  WHERE entity_type = $1 AND entity_id = $2
				  ORDER BY tenant_seq ASC`,
				entityType, entityID,
			)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			var result []*entities.AuditLog

			for rows.Next() {
				var log entities.AuditLog

				var (
					tenantSeq   sql.NullInt64
					prevHash    []byte
					recordHash  []byte
					hashVersion sql.NullInt16
				)

				if err := rows.Scan(
					&log.ID,
					&log.TenantID,
					&log.EntityType,
					&log.EntityID,
					&log.Action,
					&log.ActorID,
					&log.Changes,
					&log.CreatedAt,
					&tenantSeq,
					&prevHash,
					&recordHash,
					&hashVersion,
				); err != nil {
					return nil, err
				}

				if tenantSeq.Valid {
					log.TenantSeq = tenantSeq.Int64
				}

				log.PrevHash = prevHash
				log.RecordHash = recordHash

				if hashVersion.Valid {
					log.HashVersion = hashVersion.Int16
				}

				result = append(result, &log)
			}

			if err := rows.Err(); err != nil {
				return nil, err
			}

			return result, nil
		},
	)

	require.NoError(t, err)

	return logs
}

// TestIntegration_Governance_HashChain_SingleRecord verifies that a single audit log inserted via the
// repository produces a valid hash chain entry: genesis prev_hash, correct
// record hash, and VerifyRecordHash passes after a Postgres round-trip.
//
// This works because:
//   - CreatedAt is truncated to microsecond precision before hashing (matches TIMESTAMPTZ)
//   - changes column is JSON (not JSONB), preserving exact input bytes
func TestIntegration_Governance_HashChain_SingleRecord(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := governancePostgres.NewRepository(h.Provider())
		ctx := hashTestCtx(t, h)

		entityID := uuid.New()
		changes, err := json.Marshal(map[string]any{"status": "active"})
		require.NoError(t, err)

		auditLog, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			"reconciliation_context",
			entityID,
			"CREATED",
			strPtr("user-single"),
			changes,
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, auditLog)
		require.NoError(t, err)

		// Verify the repo populated hash chain fields
		require.NotZero(t, created.TenantSeq, "tenant_seq must be assigned by DB")
		require.NotEmpty(t, created.PrevHash, "prev_hash must be set")
		require.Len(t, created.PrevHash, hashchain.HashSize, "prev_hash must be 32 bytes")
		require.NotEmpty(t, created.RecordHash, "record_hash must be set")
		require.Len(t, created.RecordHash, hashchain.HashSize, "record_hash must be 32 bytes")
		require.Equal(t, int16(hashchain.HashVersion), created.HashVersion)

		// First record in chain: prev_hash must be genesis
		require.Equal(t, hashchain.GenesisHash(), created.PrevHash,
			"first record's prev_hash must be genesis (32 zero bytes)")

		// Read back from DB and verify hash can be recomputed from read-back data
		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)

		record := hashchain.RecordData{
			ID:          fetched.ID,
			TenantID:    fetched.TenantID,
			TenantSeq:   fetched.TenantSeq,
			EntityType:  fetched.EntityType,
			EntityID:    fetched.EntityID,
			Action:      fetched.Action,
			ActorID:     fetched.ActorID,
			Changes:     json.RawMessage(fetched.Changes),
			CreatedAt:   fetched.CreatedAt,
			HashVersion: fetched.HashVersion,
		}

		err = hashchain.VerifyRecordHash(fetched.RecordHash, fetched.PrevHash, record)
		require.NoError(t, err, "record hash must verify after Postgres round-trip")

		// Verify domain fields survive
		require.Equal(t, entityID, fetched.EntityID)
		require.Equal(t, "reconciliation_context", fetched.EntityType)
		require.Equal(t, "CREATED", fetched.Action)
		require.NotNil(t, fetched.ActorID)
		require.Equal(t, "user-single", *fetched.ActorID)
	})
}

// TestIntegration_Governance_HashChain_ThreeRecordChain creates 3 audit logs chained together and
// verifies the entire chain via VerifyChain after reading back from Postgres.
func TestIntegration_Governance_HashChain_ThreeRecordChain(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := governancePostgres.NewRepository(h.Provider())
		ctx := hashTestCtx(t, h)

		// Use a unique entityID so readChainByTenantSeq isolates our records.
		entityID := uuid.New()
		entityType := "hashchain_3rec"

		actions := []string{"CREATED", "UPDATED", "APPROVED"}

		for i, action := range actions {
			changes, err := json.Marshal(map[string]any{"step": i + 1, "action": action})
			require.NoError(t, err)

			auditLog, err := entities.NewAuditLog(
				ctx,
				h.Seed.TenantID,
				entityType,
				entityID,
				action,
				strPtr("user-chain"),
				changes,
			)
			require.NoError(t, err)

			created, err := repo.Create(ctx, auditLog)
			require.NoError(t, err)
			require.NotNil(t, created)
		}

		// Read back in tenant_seq ASC order
		logs := readChainByTenantSeq(ctx, t, h, entityType, entityID)
		require.Len(t, logs, 3, "expected exactly 3 audit logs")

		// Convert to ChainRecords and verify full chain via VerifyChain
		chain := make([]hashchain.ChainRecord, len(logs))
		for i, log := range logs {
			chain[i] = auditLogToChainRecord(log)
		}

		err := hashchain.VerifyChain(chain)
		require.NoError(t, err, "3-record hash chain must verify after Postgres round-trip")

		// Verify sequential tenant_seq
		for i := 1; i < len(chain); i++ {
			require.Equal(t, chain[i-1].TenantSeq+1, chain[i].TenantSeq,
				"tenant_seq must be consecutive")
		}

		// Verify hash chain linkage: record[i].prev_hash == record[i-1].record_hash
		for i := 1; i < len(logs); i++ {
			require.Equal(t, logs[i-1].RecordHash, logs[i].PrevHash,
				"record[%d].prev_hash must equal record[%d].record_hash", i, i-1)
		}
	})
}

// TestIntegration_Governance_HashChain_BrokenChain_TamperedHash creates a valid 3-record chain, reads it
// back, tampers with record[1]'s RecordHash, and expects VerifyChain to detect
// the invalid record hash.
func TestIntegration_Governance_HashChain_BrokenChain_TamperedHash(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := governancePostgres.NewRepository(h.Provider())
		ctx := hashTestCtx(t, h)

		entityID := uuid.New()
		entityType := "hashchain_tamper_hash"

		for i := 0; i < 3; i++ {
			changes, err := json.Marshal(map[string]any{"idx": i})
			require.NoError(t, err)

			auditLog, err := entities.NewAuditLog(
				ctx,
				h.Seed.TenantID,
				entityType,
				entityID,
				"WRITE",
				nil,
				changes,
			)
			require.NoError(t, err)

			_, err = repo.Create(ctx, auditLog)
			require.NoError(t, err)
		}

		logs := readChainByTenantSeq(ctx, t, h, entityType, entityID)
		require.Len(t, logs, 3)

		chain := make([]hashchain.ChainRecord, len(logs))
		for i, log := range logs {
			chain[i] = auditLogToChainRecord(log)
		}

		// Tamper with record[1]'s stored RecordHash (flip a bit)
		tamperedHash := make([]byte, len(chain[1].RecordHash))
		copy(tamperedHash, chain[1].RecordHash)
		tamperedHash[0] ^= 0xFF
		chain[1].RecordHash = tamperedHash

		err := hashchain.VerifyChain(chain)
		require.Error(t, err, "chain with tampered record hash must fail verification")
		require.ErrorIs(t, err, hashchain.ErrChainBroken,
			"expected ErrChainBroken when record hash is tampered")
		require.ErrorIs(t, err, hashchain.ErrInvalidRecordHash,
			"expected ErrInvalidRecordHash wrapped inside ErrChainBroken")
	})
}

// TestIntegration_Governance_HashChain_BrokenChain_WrongPrevHash creates a valid 3-record chain, reads it
// back, tampers with record[2]'s PrevHash, and expects VerifyChain to detect the
// broken chain link.
func TestIntegration_Governance_HashChain_BrokenChain_WrongPrevHash(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := governancePostgres.NewRepository(h.Provider())
		ctx := hashTestCtx(t, h)

		entityID := uuid.New()
		entityType := "hashchain_wrong_prev"

		for i := 0; i < 3; i++ {
			changes, err := json.Marshal(map[string]any{"idx": i})
			require.NoError(t, err)

			auditLog, err := entities.NewAuditLog(
				ctx,
				h.Seed.TenantID,
				entityType,
				entityID,
				"WRITE",
				nil,
				changes,
			)
			require.NoError(t, err)

			_, err = repo.Create(ctx, auditLog)
			require.NoError(t, err)
		}

		logs := readChainByTenantSeq(ctx, t, h, entityType, entityID)
		require.Len(t, logs, 3)

		chain := make([]hashchain.ChainRecord, len(logs))
		for i, log := range logs {
			chain[i] = auditLogToChainRecord(log)
		}

		// Tamper with record[2]'s PrevHash (flip a bit)
		tamperedPrev := make([]byte, len(chain[2].PrevHash))
		copy(tamperedPrev, chain[2].PrevHash)
		tamperedPrev[0] ^= 0xFF
		chain[2].PrevHash = tamperedPrev

		err := hashchain.VerifyChain(chain)
		require.Error(t, err, "chain with wrong prev_hash must fail verification")
		require.ErrorIs(t, err, hashchain.ErrChainBroken,
			"expected ErrChainBroken when prev_hash is tampered")
	})
}

// TestIntegration_Governance_HashChain_RoundTripPreservesData inserts a record with complex Changes JSON
// and verifies all fields survive the Postgres round-trip, including timestamps,
// UUIDs, actor_id, and json.RawMessage content.
func TestIntegration_Governance_HashChain_RoundTripPreservesData(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		repo := governancePostgres.NewRepository(h.Provider())
		ctx := hashTestCtx(t, h)

		entityID := uuid.New()
		entityType := "hashchain_roundtrip"
		actorID := "admin@lerian.studio"

		complexChanges := map[string]any{
			"before": map[string]any{
				"status":  "PENDING",
				"amount":  1234.56,
				"tags":    []string{"auto", "batch", "fee-verified"},
				"enabled": false,
			},
			"after": map[string]any{
				"status":  "MATCHED",
				"amount":  1234.56,
				"tags":    []string{"auto", "batch", "fee-verified", "confirmed"},
				"enabled": true,
			},
			"metadata": map[string]any{
				"rule_id":      uuid.New().String(),
				"confidence":   98.5,
				"match_run_id": uuid.New().String(),
				"nested": map[string]any{
					"level": 2,
					"items": []int{1, 2, 3},
				},
			},
		}

		changesJSON, err := json.Marshal(complexChanges)
		require.NoError(t, err)

		auditLog, err := entities.NewAuditLog(
			ctx,
			h.Seed.TenantID,
			entityType,
			entityID,
			"MATCHED",
			&actorID,
			changesJSON,
		)
		require.NoError(t, err)

		created, err := repo.Create(ctx, auditLog)
		require.NoError(t, err)

		// Read back via GetByID
		fetched, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)

		// Verify all domain fields survived the round-trip
		require.Equal(t, created.ID, fetched.ID, "ID must survive round-trip")
		require.Equal(t, created.TenantID, fetched.TenantID, "TenantID must survive round-trip")
		require.Equal(t, entityType, fetched.EntityType, "EntityType must survive round-trip")
		require.Equal(t, entityID, fetched.EntityID, "EntityID must survive round-trip")
		require.Equal(t, "MATCHED", fetched.Action, "Action must survive round-trip")
		require.NotNil(t, fetched.ActorID, "ActorID must not be nil")
		require.Equal(t, actorID, *fetched.ActorID, "ActorID value must survive round-trip")

		// Verify Changes JSON round-trip (use JSONEq for order-independent comparison)
		require.JSONEq(t, string(changesJSON), string(fetched.Changes),
			"Changes JSON must survive Postgres round-trip")

		// Verify hash chain fields
		require.NotZero(t, fetched.TenantSeq, "TenantSeq must be assigned")
		require.NotEmpty(t, fetched.PrevHash, "PrevHash must be set")
		require.Len(t, fetched.PrevHash, hashchain.HashSize, "PrevHash must be 32 bytes")
		require.NotEmpty(t, fetched.RecordHash, "RecordHash must be set")
		require.Len(t, fetched.RecordHash, hashchain.HashSize, "RecordHash must be 32 bytes")
		require.Equal(t, int16(hashchain.HashVersion), fetched.HashVersion, "HashVersion must survive round-trip")

		// Verify timestamp precision: PostgreSQL stores microseconds, so
		// we must confirm created_at survives at microsecond resolution.
		require.Equal(t, created.CreatedAt.Truncate(time.Microsecond),
			fetched.CreatedAt.Truncate(time.Microsecond),
			"CreatedAt must survive round-trip at microsecond precision")

		// Verify the record hash can be recomputed from read-back data.
		// This works because:
		//   - CreatedAt is truncated to microsecond before hashing (matches TIMESTAMPTZ)
		//   - changes column is JSON (not JSONB), preserving exact input bytes
		record := hashchain.RecordData{
			ID:          fetched.ID,
			TenantID:    fetched.TenantID,
			TenantSeq:   fetched.TenantSeq,
			EntityType:  fetched.EntityType,
			EntityID:    fetched.EntityID,
			Action:      fetched.Action,
			ActorID:     fetched.ActorID,
			Changes:     json.RawMessage(fetched.Changes),
			CreatedAt:   fetched.CreatedAt,
			HashVersion: fetched.HashVersion,
		}

		err = hashchain.VerifyRecordHash(fetched.RecordHash, fetched.PrevHash, record)
		require.NoError(t, err, "record hash must verify after round-trip with complex JSON")
	})
}

// TestIntegration_Governance_HashChain_GenesisHashIsZeros verifies that GenesisHash returns exactly
// 32 zero bytes — the anchor for every tenant's first hash chain record.
func TestIntegration_Governance_HashChain_GenesisHashIsZeros(t *testing.T) {
	t.Parallel()

	genesis := hashchain.GenesisHash()

	require.Len(t, genesis, hashchain.HashSize,
		"genesis hash must be exactly %d bytes", hashchain.HashSize)

	expected := make([]byte, hashchain.HashSize)
	require.Equal(t, expected, genesis,
		"genesis hash must be 32 zero bytes")

	// Verify immutability: mutating the returned slice must not affect future calls
	genesis[0] = 0xFF

	fresh := hashchain.GenesisHash()
	require.Equal(t, byte(0), fresh[0],
		"GenesisHash must return independent copies; mutation must not leak")
}
