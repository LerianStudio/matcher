// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// TestArtifactCustodySentinels_AreDistinct guarantees the integrity /
// retrieval / custody sentinels do not collapse into each other via
// errors.Is. Distinctness is load-bearing: bridge-worker retry policy
// decides between terminal and transient by comparing against these
// sentinels, so aliasing would silently break retry correctness.
func TestArtifactCustodySentinels_AreDistinct(t *testing.T) {
	t.Parallel()

	sentinels := map[string]error{
		"integrity":      ports.ErrIntegrityVerificationFailed,
		"retrieval":      ports.ErrArtifactRetrievalFailed,
		"custody":        ports.ErrCustodyStoreFailed,
		"nil-gateway":    ports.ErrNilArtifactRetrievalGateway,
		"nil-verifier":   ports.ErrNilArtifactTrustVerifier,
		"nil-custody":    ports.ErrNilArtifactCustodyStore,
		"descriptor":     ports.ErrArtifactDescriptorRequired,
		"extraction-id":  ports.ErrArtifactExtractionIDRequired,
		"tenant-id":      ports.ErrArtifactTenantIDRequired,
		"hmac-required":  ports.ErrArtifactHMACRequired,
		"cipher-require": ports.ErrArtifactCiphertextRequired,
	}

	for name, a := range sentinels {
		for other, b := range sentinels {
			if name == other {
				continue
			}

			require.NotNil(t, a)
			require.NotNil(t, b)
			assert.False(
				t,
				errors.Is(a, b),
				"sentinel %q must not match %q via errors.Is",
				name, other,
			)
		}
	}
}

// TestArtifactRetrievalDescriptor_ZeroValue is a smoke check that a
// zero-valued descriptor is recognisable. The orchestrator's validator
// guards against the individual fields; this test just proves the struct
// is comparable in a trivial sense.
func TestArtifactRetrievalDescriptor_ZeroValue(t *testing.T) {
	t.Parallel()

	var d ports.ArtifactRetrievalDescriptor
	assert.Equal(t, uuid.Nil, d.ExtractionID)
	assert.Empty(t, d.TenantID)
	assert.Empty(t, d.URL)
}

// TestArtifactCustodyReference_ZeroValue exercises the same idea on the
// output side.
func TestArtifactCustodyReference_ZeroValue(t *testing.T) {
	t.Parallel()

	var r ports.ArtifactCustodyReference
	assert.Empty(t, r.URI)
	assert.Empty(t, r.Key)
	assert.Zero(t, r.Size)
	assert.Empty(t, r.SHA256)
	assert.True(t, r.StoredAt.IsZero())
}

// TestArtifactCustodyWriteInput_ZeroValue proves Content is nil (allowing
// validators to short-circuit without panicking on a nil reader).
func TestArtifactCustodyWriteInput_ZeroValue(t *testing.T) {
	t.Parallel()

	var w ports.ArtifactCustodyWriteInput
	assert.Equal(t, uuid.Nil, w.ExtractionID)
	assert.Empty(t, w.TenantID)
	assert.Nil(t, w.Content)
}
