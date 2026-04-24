// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit && leak

// REFACTOR-014: Bounded-execution test for the schema cache goroutine.
//
// connection_queries.go spawns cacheSchemas in a goroutine whose parent
// context has been stripped via context.WithoutCancel. The internal
// cacheSchemaDeadline is the only termination guarantee. This test
// proves it: a cache whose SetSchema blocks until ctx.Done() must
// return within the deadline, and no goroutine may escape.
package query

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestCacheSchemas_DeadlineBounded_NoLeak(t *testing.T) {
	defer goleak.VerifyNone(t, testutil.LeakOptions()...)

	// A cache whose SetSchema blocks until the caller's ctx is done.
	// Without the internal cacheSchemaDeadline, this would hang forever
	// because the parent context was detached via WithoutCancel before
	// the cacheSchemas goroutine was spawned.
	blockingCache := &mockSchemaCache{
		setSchemaFn: func(ctx context.Context, _ string, _ *sharedPorts.FetcherSchema, _ time.Duration) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	uc, err := NewUseCase(
		&mockFetcherClient{healthy: true},
		&mockConnectionRepo{},
		&mockSchemaRepo{},
		&mockExtractionRepo{},
		nil,
	)
	require.NoError(t, err)
	uc.WithSchemaCache(blockingCache, 30*time.Second)

	connectionID := uuid.New()
	schemas := []*entities.DiscoveredSchema{
		{
			ID:           uuid.New(),
			ConnectionID: connectionID,
			TableName:    "accounts",
			Columns:      []entities.ColumnInfo{{Name: "id"}},
			DiscoveredAt: time.Now().UTC(),
		},
	}

	// Call cacheSchemas synchronously with a fresh ctx so we can observe
	// that the internal deadline terminates it. We use a generous upper
	// bound — the deadline itself is 10s but sub-second is plenty for
	// the context machinery to fire.
	start := time.Now()

	done := make(chan struct{})

	go func() {
		defer close(done)

		uc.cacheSchemas(context.Background(), connectionID, schemas)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		// The deadline is cacheSchemaDeadline (10s). Must return by then
		// plus a small grace margin for scheduling.
		require.Less(t, elapsed, cacheSchemaDeadline+2*time.Second,
			"cacheSchemas must honour its internal deadline")
	case <-time.After(cacheSchemaDeadline + 5*time.Second):
		t.Fatal("cacheSchemas exceeded deadline + grace — internal timeout not enforced")
	}
}
