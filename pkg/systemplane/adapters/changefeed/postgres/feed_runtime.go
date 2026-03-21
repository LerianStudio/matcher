// Copyright 2025 Lerian Studio.

package postgres

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

func (feed *Feed) backoff(attempt int) time.Duration {
	base := float64(feed.reconnectMin)
	delay := base * math.Pow(backoffBase, float64(attempt))

	if delay > float64(feed.reconnectMax) {
		delay = float64(feed.reconnectMax)
	}

	// Add jitter: 0-25% of delay.
	randomFactor := secureRandomFactor()
	jitter := delay * jitterRatio * randomFactor

	return time.Duration(delay + jitter)
}

func secureRandomFactor() float64 {
	var randomBytes [8]byte

	if _, err := rand.Read(randomBytes[:]); err != nil {
		return 0
	}

	randomValue := binary.LittleEndian.Uint64(randomBytes[:])

	return float64(randomValue) / float64(math.MaxUint64)
}

// validateRevisionSource ensures that schema and revisionTable identifiers are
// safe for SQL interpolation. Both must be empty (revision tracking disabled)
// or both must match the PostgreSQL identifier pattern. A one-sided config
// (schema set but not table, or vice versa) is rejected to prevent silent
// no-op revision polling.
func (feed *Feed) validateRevisionSource() error {
	schemaSet := feed.schema != ""
	tableSet := feed.revisionTable != ""

	if schemaSet != tableSet {
		return fmt.Errorf("%w: schema and revision table must be configured together", ErrInvalidIdentifier)
	}

	if !schemaSet {
		return nil // Revision tracking disabled — no identifiers to validate.
	}

	if !identifierPattern.MatchString(feed.schema) {
		return fmt.Errorf("%w: schema %q", ErrInvalidIdentifier, feed.schema)
	}

	if !identifierPattern.MatchString(feed.revisionTable) {
		return fmt.Errorf("%w: revision table %q", ErrInvalidIdentifier, feed.revisionTable)
	}

	return nil
}

func (feed *Feed) qualifiedRevisions() string {
	return feed.schema + "." + feed.revisionTable
}
