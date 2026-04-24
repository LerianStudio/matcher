// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package hashchain provides cryptographic hash chain functionality for audit log tamper detection.
//
// # Overview
//
// The hash chain creates a blockchain-like structure where each audit log entry
// contains a hash of its content concatenated with the previous entry's hash.
// This ensures that any modification to a historical record breaks the chain
// and is detectable during verification.
//
// # Hash Structure
//
//	record_hash = SHA-256(prev_hash || canonicalized_content)
//
// Where canonicalized_content is a deterministic JSON representation of the record.
//
// # Genesis Records
//
// The first record in each tenant's chain uses a genesis hash (32 zero bytes)
// as its prev_hash value.
//
// # SOX Compliance
//
// This mechanism supports SOX compliance by:
//   - Detecting unauthorized modifications to the audit trail
//   - Providing cryptographic proof of audit log integrity
//   - Enabling periodic verification via background jobs
package hashchain

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// HashVersion indicates the version of the canonicalization scheme.
// Increment this when changing how records are serialized for hashing.
const HashVersion = 1

// HashSize is the size of a SHA-256 hash in bytes.
const HashSize = 32

// genesisHash is the prev_hash value for the first record in a tenant's chain.
// It consists of 32 zero bytes. Declared as a fixed-size array to prevent
// accidental mutation of the shared package-level value.
var genesisHash = [HashSize]byte{}

// GenesisHash returns a copy of the genesis hash (32 zero bytes).
func GenesisHash() []byte {
	cp := genesisHash
	return cp[:]
}

// Hash chain errors.
var (
	ErrInvalidHashSize    = errors.New("hash must be exactly 32 bytes")
	ErrInvalidRecordHash  = errors.New("record hash does not match computed hash")
	ErrChainBroken        = errors.New("hash chain is broken")
	ErrSequenceOutOfOrder = errors.New("sequence numbers are out of order")
)

// RecordData contains the fields needed to compute an audit log record hash.
// This is the canonical representation that gets hashed.
//
// IMPORTANT — Canonical field order contract:
// The JSON serialization of this struct (via json.Marshal) is fed directly into
// SHA-256 to produce the record hash. Go's encoding/json marshals fields in
// declaration order, so reordering, renaming, or removing fields will change the
// hash output and break verification of ALL historical records. If the struct
// must evolve, increment HashVersion and implement a versioned canonicalization
// strategy. A golden-hash regression test guards this invariant.
type RecordData struct {
	ID          uuid.UUID       `json:"id"`
	TenantID    uuid.UUID       `json:"tenant_id"`
	TenantSeq   int64           `json:"tenant_seq"`
	EntityType  string          `json:"entity_type"`
	EntityID    uuid.UUID       `json:"entity_id"`
	Action      string          `json:"action"`
	ActorID     *string         `json:"actor_id"`
	Changes     json.RawMessage `json:"changes"`
	CreatedAt   time.Time       `json:"created_at"`
	HashVersion int16           `json:"hash_version"`
}

// ComputeRecordHash computes the SHA-256 hash for an audit log record.
// The hash is computed as: SHA-256(prevHash || canonicalJSON(record))
//
// Parameters:
//   - prevHash: The hash of the previous record (32 bytes), or GenesisHash() for first record
//   - record: The record data to hash
//
// Returns the 32-byte SHA-256 hash.
func ComputeRecordHash(prevHash []byte, record RecordData) ([]byte, error) {
	if len(prevHash) != HashSize {
		return nil, fmt.Errorf("%w: got %d bytes", ErrInvalidHashSize, len(prevHash))
	}

	// Normalize CreatedAt to UTC for deterministic hashing.
	// PostgreSQL TIMESTAMPTZ stores the same instant but database drivers may
	// return it in local time, producing different JSON representations
	// (e.g., "...T11:04:30-03:00" vs "...T14:04:30Z") that would yield
	// different SHA-256 hashes for the same logical record.
	record.CreatedAt = record.CreatedAt.UTC()

	canonical, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("failed to canonicalize record: %w", err)
	}

	hasher := sha256.New()
	hasher.Write(prevHash)
	hasher.Write(canonical)

	return hasher.Sum(nil), nil
}

// VerifyRecordHash verifies that a stored record hash matches the computed hash.
func VerifyRecordHash(storedHash, prevHash []byte, record RecordData) error {
	computed, err := ComputeRecordHash(prevHash, record)
	if err != nil {
		return err
	}

	if len(storedHash) != HashSize {
		return fmt.Errorf("%w: stored hash has %d bytes", ErrInvalidHashSize, len(storedHash))
	}

	if subtle.ConstantTimeCompare(computed, storedHash) != 1 {
		return ErrInvalidRecordHash
	}

	return nil
}

// ChainRecord represents a record in the hash chain for verification purposes.
type ChainRecord struct {
	TenantSeq   int64
	PrevHash    []byte
	RecordHash  []byte
	HashVersion int16
	Data        RecordData
}

// VerifyChain verifies the integrity of a sequence of audit log records.
// Records must be provided in ascending tenant_seq order.
// Empty slices return nil. If the first record has TenantSeq == 1, prev_hash is
// validated against GenesisHash(); if TenantSeq > 1, the provided PrevHash is
// trusted to allow partial-chain verification. Callers that require full-chain
// validation should supply records starting at seq 1 or validate the upstream
// PrevHash separately.
//
// Returns nil if the chain is valid, or an error describing the first break.
func VerifyChain(records []ChainRecord) error {
	if len(records) == 0 {
		return nil
	}

	var expectedPrevHash []byte

	for i, rec := range records {
		if i == 0 {
			if rec.TenantSeq == 1 {
				expectedPrevHash = GenesisHash()
			} else {
				expectedPrevHash = rec.PrevHash
			}
		}

		if i > 0 && rec.TenantSeq != records[i-1].TenantSeq+1 {
			return fmt.Errorf("%w: expected seq %d, got %d",
				ErrSequenceOutOfOrder, records[i-1].TenantSeq+1, rec.TenantSeq)
		}

		lengthsEqual := len(expectedPrevHash) == len(rec.PrevHash)
		match := subtle.ConstantTimeCompare(expectedPrevHash, rec.PrevHash)

		if !lengthsEqual || match != 1 {
			return fmt.Errorf("%w at seq %d: prev_hash mismatch", ErrChainBroken, rec.TenantSeq)
		}

		if err := VerifyRecordHash(rec.RecordHash, rec.PrevHash, rec.Data); err != nil {
			return fmt.Errorf("%w at seq %d: %w", ErrChainBroken, rec.TenantSeq, err)
		}

		expectedPrevHash = rec.RecordHash
	}

	return nil
}
