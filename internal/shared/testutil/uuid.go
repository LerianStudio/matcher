// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package testutil

import (
	"crypto/sha256"
	"fmt"

	"github.com/google/uuid"
)

// MustDeterministicUUID returns a stable UUID derived from the provided seed.
// It generates a deterministic UUID by hashing the seed with SHA-256 and using
// the first 16 bytes as the UUID value.
//
// This function is intended for testing purposes only, where deterministic UUIDs
// help create reproducible test cases.
//
// Panics: This function panics if UUID creation fails, which should never happen
// with valid SHA-256 hash input. The "Must" prefix follows Go convention to indicate
// that this function panics on error rather than returning an error value.
func MustDeterministicUUID(seed string) uuid.UUID {
	hash := sha256.Sum256([]byte(seed))

	id, err := uuid.FromBytes(hash[:16])
	if err != nil {
		panic(
			fmt.Sprintf(
				"testutil: MustDeterministicUUID failed to build UUID from hash slice %x: %v",
				hash[:16],
				err,
			),
		)
	}

	return id
}
