// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/ingestion/ports"
)

// Sentinel errors shared across test files.
var (
	errOutbox             = errors.New("outbox")
	errParse              = errors.New("parse")
	errDatabaseConnection = errors.New("database connection failed")
	errFieldMapQuery      = errors.New("field map query failed")
	errTransactionUpdate  = errors.New("transaction update failed")
	errTransactionFind    = errors.New("transaction find failed")
	errGetParser          = errors.New("parser not found")
)

type fakeRegistry struct{ parser ports.Parser }

func (f fakeRegistry) GetParser(_ string) (ports.Parser, error) { return f.parser, nil }
func (f fakeRegistry) Register(_ ports.Parser)                  {}

type fakeDedupe struct{ err error }

func (f fakeDedupe) CalculateHash(_ uuid.UUID, _ string) string { return "hash" }
func (f fakeDedupe) IsDuplicate(_ context.Context, _ uuid.UUID, _ string) (bool, error) {
	return false, nil
}

func (f fakeDedupe) MarkSeen(_ context.Context, _ uuid.UUID, _ string, _ time.Duration) error {
	return nil
}

func (f fakeDedupe) MarkSeenWithRetry(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ time.Duration,
	_ int,
) error {
	return f.err
}

func (f fakeDedupe) MarkSeenBulk(
	_ context.Context,
	_ uuid.UUID,
	hashes []string,
	_ time.Duration,
) (map[string]bool, error) {
	if f.err != nil {
		return nil, f.err
	}

	result := make(map[string]bool, len(hashes))
	for _, h := range hashes {
		result[h] = true
	}

	return result, nil
}

func (f fakeDedupe) Clear(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func (f fakeDedupe) ClearBatch(_ context.Context, _ uuid.UUID, _ []string) error {
	return nil
}

type recordingDedupe struct{ lastTTL time.Duration }

func (r *recordingDedupe) CalculateHash(_ uuid.UUID, _ string) string { return "hash" }
func (r *recordingDedupe) IsDuplicate(_ context.Context, _ uuid.UUID, _ string) (bool, error) {
	return false, nil
}

func (r *recordingDedupe) MarkSeen(_ context.Context, _ uuid.UUID, _ string, ttl time.Duration) error {
	r.lastTTL = ttl
	return nil
}

func (r *recordingDedupe) MarkSeenWithRetry(_ context.Context, _ uuid.UUID, _ string, ttl time.Duration, _ int) error {
	r.lastTTL = ttl
	return nil
}

func (r *recordingDedupe) MarkSeenBulk(
	_ context.Context,
	_ uuid.UUID,
	hashes []string,
	ttl time.Duration,
) (map[string]bool, error) {
	r.lastTTL = ttl

	result := make(map[string]bool, len(hashes))
	for _, h := range hashes {
		result[h] = true
	}

	return result, nil
}

func (r *recordingDedupe) Clear(_ context.Context, _ uuid.UUID, _ string) error { return nil }
func (r *recordingDedupe) ClearBatch(_ context.Context, _ uuid.UUID, _ []string) error {
	return nil
}
