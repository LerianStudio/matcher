// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

// This file carries the stub types used only by trusted_stream_integration_test.go.
// Keeping them in a sibling file holds the scenario tests themselves under the
// 300-line soft cap while still sharing the helpers.
package command_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	ingestionPorts "github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// trustedStreamFieldMapStub returns a fixed field map regardless of source id.
// The integration test seeds only one source via the harness, so no dispatch
// is required.
type trustedStreamFieldMapStub struct {
	fieldMap *shared.FieldMap
}

func (f *trustedStreamFieldMapStub) FindBySourceID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.FieldMap, error) {
	return f.fieldMap, nil
}

// trustedStreamSourceStub satisfies ingestion ports.SourceRepository for the
// single seeded context/source pair established by the harness.
type trustedStreamSourceStub struct {
	contextID uuid.UUID
	sourceID  uuid.UUID
}

func newTrustedStreamSourceStub(contextID, sourceID uuid.UUID) *trustedStreamSourceStub {
	return &trustedStreamSourceStub{contextID: contextID, sourceID: sourceID}
}

func (s *trustedStreamSourceStub) FindByID(
	_ context.Context,
	contextID, id uuid.UUID,
) (*shared.ReconciliationSource, error) {
	if s.contextID != contextID || s.sourceID != id {
		return nil, fmt.Errorf("source not found")
	}

	return &shared.ReconciliationSource{ID: s.sourceID, ContextID: s.contextID}, nil
}

// trustedStreamFakeDedupe is a minimal in-memory DedupeService that exercises
// MarkSeenWithRetry correctly and rejects repeated hashes. Integration tests
// here never pre-seed duplicates, so the retry-on-duplicate branch stays cold.
type trustedStreamFakeDedupe struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (d *trustedStreamFakeDedupe) CalculateHash(sourceID uuid.UUID, externalID string) string {
	return sourceID.String() + ":" + externalID
}

func (d *trustedStreamFakeDedupe) IsDuplicate(
	_ context.Context,
	_ uuid.UUID,
	hash string,
) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.seen[hash], nil
}

func (d *trustedStreamFakeDedupe) MarkSeen(
	_ context.Context,
	_ uuid.UUID,
	hash string,
	_ time.Duration,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.seen == nil {
		d.seen = map[string]bool{}
	}

	d.seen[hash] = true

	return nil
}

func (d *trustedStreamFakeDedupe) MarkSeenWithRetry(
	_ context.Context,
	_ uuid.UUID,
	hash string,
	_ time.Duration,
	_ int,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.seen == nil {
		d.seen = map[string]bool{}
	}

	if d.seen[hash] {
		return ingestionPorts.ErrDuplicateTransaction
	}

	d.seen[hash] = true

	return nil
}

func (d *trustedStreamFakeDedupe) MarkSeenBulk(
	_ context.Context,
	_ uuid.UUID,
	hashes []string,
	_ time.Duration,
) (map[string]bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.seen == nil {
		d.seen = map[string]bool{}
	}

	result := make(map[string]bool, len(hashes))

	for _, hash := range hashes {
		if d.seen[hash] {
			result[hash] = false

			continue
		}

		d.seen[hash] = true
		result[hash] = true
	}

	return result, nil
}

func (d *trustedStreamFakeDedupe) Clear(_ context.Context, _ uuid.UUID, hash string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.seen, hash)

	return nil
}

func (d *trustedStreamFakeDedupe) ClearBatch(
	_ context.Context,
	_ uuid.UUID,
	hashes []string,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, h := range hashes {
		delete(d.seen, h)
	}

	return nil
}

// trustedStreamPublisher is a no-op IngestionEventPublisher; the outbox is
// the authoritative path for the integration assertions.
type trustedStreamPublisher struct{}

func (*trustedStreamPublisher) PublishIngestionCompleted(
	_ context.Context,
	_ *ingestionEntities.IngestionCompletedEvent,
) error {
	return nil
}

func (*trustedStreamPublisher) PublishIngestionFailed(
	_ context.Context,
	_ *ingestionEntities.IngestionFailedEvent,
) error {
	return nil
}

// Compile-time interface satisfaction checks keep signature drift loud.
var (
	_ ingestionPorts.DedupeService        = (*trustedStreamFakeDedupe)(nil)
	_ ingestionPorts.FieldMapRepository   = (*trustedStreamFieldMapStub)(nil)
	_ ingestionPorts.SourceRepository     = (*trustedStreamSourceStub)(nil)
	_ sharedPorts.IngestionEventPublisher = (*trustedStreamPublisher)(nil)
)
