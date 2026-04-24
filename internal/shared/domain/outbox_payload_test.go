// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package shared

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildAuditChangesTruncationMarker(t *testing.T) {
	t.Parallel()

	marker := BuildAuditChangesTruncationMarker(1_500_000, DefaultOutboxMaxPayloadBytes)

	assert.Equal(t, true, marker[TruncatedMarkerKey])
	assert.Equal(t, 1_500_000, marker[TruncatedOriginalSizeKey])
	assert.Equal(t, DefaultOutboxMaxPayloadBytes, marker[TruncatedMaxAllowedKey])
	assert.Equal(t, TruncationNoteAuditDiff, marker[TruncatedNoteKey])
}

func TestBuildAuditChangesTruncationMarker_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	// Governance DTO consumer reads these keys via json.Unmarshal on a
	// RawMessage, so the marker must survive a Marshal → Unmarshal cycle
	// with the expected types preserved.
	marker := BuildAuditChangesTruncationMarker(2048, 1024)

	raw, err := json.Marshal(marker)
	require.NoError(t, err)

	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &decoded))

	var truncated bool
	require.NoError(t, json.Unmarshal(decoded[TruncatedMarkerKey], &truncated))
	assert.True(t, truncated)

	var originalSize int64
	require.NoError(t, json.Unmarshal(decoded[TruncatedOriginalSizeKey], &originalSize))
	assert.Equal(t, int64(2048), originalSize)
}

func TestTruncateIDListIfTooLarge_FitsUnderCap(t *testing.T) {
	t.Parallel()

	ids := make([]uuid.UUID, 100)
	for i := range ids {
		ids[i] = uuid.New()
	}

	out, original := TruncateIDListIfTooLarge(ids, 1024*1024)
	assert.Equal(t, 100, len(out))
	assert.Equal(t, 100, original)
	// Same slice reference — no copy when no truncation.
	assert.Equal(t, &ids[0], &out[0])
}

func TestTruncateIDListIfTooLarge_TrimsToFit(t *testing.T) {
	t.Parallel()

	// Each UUID serializes to 38 bytes (36 chars + 2 quotes) plus 1 byte
	// separator. Pick a cap that forces a clear cutoff at N=10.
	// bracket(2) + 10*38 + 9 = 391 bytes
	ids := make([]uuid.UUID, 100)
	for i := range ids {
		ids[i] = uuid.New()
	}

	out, original := TruncateIDListIfTooLarge(ids, 400)
	assert.Equal(t, 100, original)
	assert.Equal(t, 10, len(out))
	// Serialized size of the result must fit under the cap.
	serialized, err := json.Marshal(out)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(serialized), 400)
}

func TestTruncateIDListIfTooLarge_EmptyList(t *testing.T) {
	t.Parallel()

	out, original := TruncateIDListIfTooLarge(nil, 1024)
	assert.Nil(t, out)
	assert.Equal(t, 0, original)
}

func TestTruncateIDListIfTooLarge_ZeroCap(t *testing.T) {
	t.Parallel()

	ids := []uuid.UUID{uuid.New(), uuid.New()}
	// maxBytes=0 short-circuits: nothing to check, return input.
	out, original := TruncateIDListIfTooLarge(ids, 0)
	assert.Equal(t, ids, out)
	assert.Equal(t, 2, original)
}

func TestTruncateIDListIfTooLarge_CapBelowSingleUUID(t *testing.T) {
	t.Parallel()

	// A cap of 20 bytes cannot fit even one UUID (40-byte minimum
	// serialized size). Binary search must terminate at zero.
	ids := []uuid.UUID{uuid.New(), uuid.New()}
	out, original := TruncateIDListIfTooLarge(ids, 20)
	assert.Empty(t, out)
	assert.Equal(t, 2, original)
}

func TestIDListSerializedSize_MatchesJSONMarshal(t *testing.T) {
	t.Parallel()

	for _, n := range []int{0, 1, 2, 10, 100, 1000} {
		ids := make([]uuid.UUID, n)
		for i := range ids {
			ids[i] = uuid.New()
		}

		serialized, err := json.Marshal(ids)
		require.NoError(t, err)

		// idListSerializedSize must match the actual JSON byte count
		// exactly so the binary search lands on the correct cutoff.
		// json.Marshal emits `null` for nil slices, so skip n=0.
		if n == 0 {
			continue
		}

		assert.Equal(t, len(serialized), idListSerializedSize(n),
			"n=%d: predicted size must match actual JSON size", n)
	}
}
