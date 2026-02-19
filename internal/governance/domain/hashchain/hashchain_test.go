//go:build unit

package hashchain

import (
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedtestutil "github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestGenesisHash(t *testing.T) {
	t.Parallel()

	genesis := GenesisHash()
	assert.Len(t, genesis, HashSize)

	for _, b := range genesis {
		assert.Equal(t, byte(0), b)
	}
}

// TestGenesisHash_Immutability verifies the genesis hash cannot be silently corrupted
// by mutating the returned slice. Each call must return an independent copy.
func TestGenesisHash_Immutability(t *testing.T) {
	t.Parallel()

	g1 := GenesisHash()
	g1[0] = 0xFF // mutate the returned copy

	g2 := GenesisHash()
	assert.Equal(t, byte(0), g2[0], "GenesisHash must return independent copies; mutation leaked")
}

// TestRecordData_GoldenHash is a regression test that pins the exact SHA-256 digest
// produced by a known RecordData value. If anyone reorders struct fields, changes
// json tags, or alters the canonicalization, this test will fail immediately.
//
// The expected hash was computed from the canonical JSON:
//
//	SHA-256(genesis_hash || json.Marshal(record))
//
// where genesis_hash is 32 zero bytes.
func TestRecordData_GoldenHash(t *testing.T) {
	t.Parallel()

	actor := "system@lerian.studio"
	record := RecordData{
		ID:          uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		TenantID:    uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		TenantSeq:   1,
		EntityType:  "reconciliation_context",
		EntityID:    uuid.MustParse("00000000-0000-0000-0000-000000000003"),
		Action:      "CREATED",
		ActorID:     &actor,
		Changes:     json.RawMessage(`{"name":"Test Context"}`),
		CreatedAt:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		HashVersion: 1,
	}

	hash, err := ComputeRecordHash(GenesisHash(), record)
	require.NoError(t, err)

	const expectedHex = "059ba92a28c0675d6836298ca2e0355604f548c9afb46090d96dca4d9f8e093f"
	assert.Equal(t, expectedHex, hex.EncodeToString(hash),
		"golden hash mismatch — did you reorder RecordData fields or change json tags?")
}

// TestRecordData_FieldOrder uses reflection to assert the exact declaration order
// of RecordData fields. This complements the golden hash test by catching
// field reordering at compile-time (via test), even before hash computation.
func TestRecordData_FieldOrder(t *testing.T) {
	t.Parallel()

	expectedFields := []string{
		"ID",
		"TenantID",
		"TenantSeq",
		"EntityType",
		"EntityID",
		"Action",
		"ActorID",
		"Changes",
		"CreatedAt",
		"HashVersion",
	}

	rt := reflect.TypeOf(RecordData{})
	actualFields := make([]string, rt.NumField())

	for i := range rt.NumField() {
		actualFields[i] = rt.Field(i).Name
	}

	assert.Equal(t, expectedFields, actualFields,
		"RecordData field order is part of the canonical hash contract — "+
			"do NOT reorder fields without incrementing HashVersion")
}

func TestComputeRecordHash_ValidInput(t *testing.T) {
	t.Parallel()

	record := RecordData{
		ID:          uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		TenantID:    uuid.MustParse("660e8400-e29b-41d4-a716-446655440000"),
		TenantSeq:   1,
		EntityType:  "transaction",
		EntityID:    uuid.MustParse("770e8400-e29b-41d4-a716-446655440000"),
		Action:      "CREATED",
		ActorID:     ptrStr("user@example.com"),
		Changes:     json.RawMessage(`{"field":"value"}`),
		CreatedAt:   time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
		HashVersion: HashVersion,
	}

	hash, err := ComputeRecordHash(GenesisHash(), record)

	require.NoError(t, err)
	assert.Len(t, hash, HashSize)
	assert.NotEqual(t, GenesisHash(), hash)
}

func TestComputeRecordHash_InvalidPrevHashSize(t *testing.T) {
	t.Parallel()

	record := RecordData{
		ID:          uuid.New(),
		TenantID:    uuid.New(),
		TenantSeq:   1,
		EntityType:  "test",
		EntityID:    uuid.New(),
		Action:      "TEST",
		Changes:     json.RawMessage(`{}`),
		CreatedAt:   time.Now().UTC(),
		HashVersion: HashVersion,
	}

	_, err := ComputeRecordHash([]byte{1, 2, 3}, record)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidHashSize)
}

func TestComputeRecordHash_Deterministic(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	record := RecordData{
		ID:          uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		TenantID:    uuid.MustParse("660e8400-e29b-41d4-a716-446655440000"),
		TenantSeq:   1,
		EntityType:  "transaction",
		EntityID:    uuid.MustParse("770e8400-e29b-41d4-a716-446655440000"),
		Action:      "CREATED",
		ActorID:     nil,
		Changes:     json.RawMessage(`{"status":"matched"}`),
		CreatedAt:   fixedTime,
		HashVersion: HashVersion,
	}

	hash1, err := ComputeRecordHash(GenesisHash(), record)
	require.NoError(t, err)

	hash2, err := ComputeRecordHash(GenesisHash(), record)
	require.NoError(t, err)

	assert.Equal(t, hash1, hash2, "same input should produce same hash")
}

func TestComputeRecordHash_NormalizesCreatedAtTimezone(t *testing.T) {
	t.Parallel()

	baseInstant := time.Date(2026, 2, 16, 14, 4, 30, 0, time.UTC)
	localInstant := baseInstant.In(time.FixedZone("BRT", -3*60*60))

	recordUTC := RecordData{
		ID:          uuid.New(),
		TenantID:    uuid.New(),
		TenantSeq:   1,
		EntityType:  "audit",
		EntityID:    uuid.New(),
		Action:      "UPDATED",
		Changes:     json.RawMessage(`{"a":1}`),
		CreatedAt:   baseInstant,
		HashVersion: HashVersion,
	}

	recordLocal := recordUTC
	recordLocal.CreatedAt = localInstant

	hashUTC, err := ComputeRecordHash(GenesisHash(), recordUTC)
	require.NoError(t, err)

	hashLocal, err := ComputeRecordHash(GenesisHash(), recordLocal)
	require.NoError(t, err)

	assert.Equal(t, hashUTC, hashLocal, "same instant with different timezone must hash identically")
}

func TestComputeRecordHash_DifferentInputDifferentHash(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	baseRecord := RecordData{
		ID:          uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		TenantID:    uuid.MustParse("660e8400-e29b-41d4-a716-446655440000"),
		TenantSeq:   1,
		EntityType:  "transaction",
		EntityID:    uuid.MustParse("770e8400-e29b-41d4-a716-446655440000"),
		Action:      "CREATED",
		Changes:     json.RawMessage(`{}`),
		CreatedAt:   fixedTime,
		HashVersion: HashVersion,
	}

	modifiedRecord := baseRecord
	modifiedRecord.Action = "MODIFIED"

	hash1, err := ComputeRecordHash(GenesisHash(), baseRecord)
	require.NoError(t, err)

	hash2, err := ComputeRecordHash(GenesisHash(), modifiedRecord)
	require.NoError(t, err)

	assert.NotEqual(t, hash1, hash2, "different input should produce different hash")
}

func TestVerifyRecordHash_ValidHash(t *testing.T) {
	t.Parallel()

	record := RecordData{
		ID:          sharedtestutil.MustDeterministicUUID("verify-record-hash-valid-id"),
		TenantID:    sharedtestutil.MustDeterministicUUID("verify-record-hash-valid-tenant"),
		TenantSeq:   1,
		EntityType:  "test",
		EntityID:    sharedtestutil.MustDeterministicUUID("verify-record-hash-valid-entity"),
		Action:      "TEST",
		Changes:     json.RawMessage(`{"key":"value"}`),
		CreatedAt:   sharedtestutil.FixedTime(),
		HashVersion: HashVersion,
	}

	hash, err := ComputeRecordHash(GenesisHash(), record)
	require.NoError(t, err)

	err = VerifyRecordHash(hash, GenesisHash(), record)
	assert.NoError(t, err)
}

func TestVerifyRecordHash_TamperedData(t *testing.T) {
	t.Parallel()

	record := RecordData{
		ID:          sharedtestutil.MustDeterministicUUID("verify-record-hash-tampered-id"),
		TenantID:    sharedtestutil.MustDeterministicUUID("verify-record-hash-tampered-tenant"),
		TenantSeq:   1,
		EntityType:  "test",
		EntityID:    sharedtestutil.MustDeterministicUUID("verify-record-hash-tampered-entity"),
		Action:      "CREATED",
		Changes:     json.RawMessage(`{"key":"original"}`),
		CreatedAt:   sharedtestutil.FixedTime(),
		HashVersion: HashVersion,
	}

	hash, err := ComputeRecordHash(GenesisHash(), record)
	require.NoError(t, err)

	record.Changes = json.RawMessage(`{"key":"tampered"}`)

	err = VerifyRecordHash(hash, GenesisHash(), record)
	assert.ErrorIs(t, err, ErrInvalidRecordHash)
}

func TestVerifyChain_ValidChain(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	now := time.Now().UTC()

	record1 := RecordData{
		ID:          uuid.New(),
		TenantID:    tenantID,
		TenantSeq:   1,
		EntityType:  "test",
		EntityID:    uuid.New(),
		Action:      "FIRST",
		Changes:     json.RawMessage(`{}`),
		CreatedAt:   now,
		HashVersion: HashVersion,
	}

	hash1, err := ComputeRecordHash(GenesisHash(), record1)
	require.NoError(t, err)

	record2 := RecordData{
		ID:          uuid.New(),
		TenantID:    tenantID,
		TenantSeq:   2,
		EntityType:  "test",
		EntityID:    uuid.New(),
		Action:      "SECOND",
		Changes:     json.RawMessage(`{}`),
		CreatedAt:   now.Add(time.Second),
		HashVersion: HashVersion,
	}

	hash2, err := ComputeRecordHash(hash1, record2)
	require.NoError(t, err)

	chain := []ChainRecord{
		{TenantSeq: 1, PrevHash: GenesisHash(), RecordHash: hash1, HashVersion: HashVersion, Data: record1},
		{TenantSeq: 2, PrevHash: hash1, RecordHash: hash2, HashVersion: HashVersion, Data: record2},
	}

	err = VerifyChain(chain)
	assert.NoError(t, err)
}

func TestVerifyChain_BrokenChain(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	now := time.Now().UTC()

	record1 := RecordData{
		ID:          uuid.New(),
		TenantID:    tenantID,
		TenantSeq:   1,
		EntityType:  "test",
		EntityID:    uuid.New(),
		Action:      "FIRST",
		Changes:     json.RawMessage(`{}`),
		CreatedAt:   now,
		HashVersion: HashVersion,
	}

	hash1, err := ComputeRecordHash(GenesisHash(), record1)
	require.NoError(t, err)

	record2 := RecordData{
		ID:          uuid.New(),
		TenantID:    tenantID,
		TenantSeq:   2,
		EntityType:  "test",
		EntityID:    uuid.New(),
		Action:      "SECOND",
		Changes:     json.RawMessage(`{}`),
		CreatedAt:   now.Add(time.Second),
		HashVersion: HashVersion,
	}

	hash2, err := ComputeRecordHash(hash1, record2)
	require.NoError(t, err)

	tamperedHash1 := make([]byte, HashSize)
	copy(tamperedHash1, hash1)
	tamperedHash1[0] ^= 0xFF

	chain := []ChainRecord{
		{TenantSeq: 1, PrevHash: GenesisHash(), RecordHash: hash1, HashVersion: HashVersion, Data: record1},
		{TenantSeq: 2, PrevHash: tamperedHash1, RecordHash: hash2, HashVersion: HashVersion, Data: record2},
	}

	err = VerifyChain(chain)
	assert.ErrorIs(t, err, ErrChainBroken)
}

func TestVerifyChain_OutOfOrderSequence(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	now := time.Now().UTC()

	record1 := RecordData{
		ID:          uuid.New(),
		TenantID:    tenantID,
		TenantSeq:   1,
		EntityType:  "test",
		EntityID:    uuid.New(),
		Action:      "FIRST",
		Changes:     json.RawMessage(`{}`),
		CreatedAt:   now,
		HashVersion: HashVersion,
	}

	hash1, err := ComputeRecordHash(GenesisHash(), record1)
	require.NoError(t, err)

	record3 := RecordData{
		ID:          uuid.New(),
		TenantID:    tenantID,
		TenantSeq:   3,
		EntityType:  "test",
		EntityID:    uuid.New(),
		Action:      "THIRD",
		Changes:     json.RawMessage(`{}`),
		CreatedAt:   now.Add(time.Second),
		HashVersion: HashVersion,
	}

	hash3, err := ComputeRecordHash(hash1, record3)
	require.NoError(t, err)

	chain := []ChainRecord{
		{TenantSeq: 1, PrevHash: GenesisHash(), RecordHash: hash1, HashVersion: HashVersion, Data: record1},
		{TenantSeq: 3, PrevHash: hash1, RecordHash: hash3, HashVersion: HashVersion, Data: record3},
	}

	err = VerifyChain(chain)
	assert.ErrorIs(t, err, ErrSequenceOutOfOrder)
}

func TestVerifyChain_EmptyChain(t *testing.T) {
	t.Parallel()

	err := VerifyChain(nil)
	assert.NoError(t, err)

	err = VerifyChain([]ChainRecord{})
	assert.NoError(t, err)
}

func ptrStr(s string) *string {
	return &s
}
