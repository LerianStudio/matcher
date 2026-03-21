// Copyright 2025 Lerian Studio.

package feedtest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// ---------------------------------------------------------------------------.
// Test helpers
// ---------------------------------------------------------------------------.

// globalTarget returns a validated global config target for use in tests.
func globalTarget(t *testing.T) domain.Target {
	t.Helper()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	return target
}

// testActor returns a fixed actor for use in tests.
func testActor() domain.Actor {
	return domain.Actor{ID: "test-feed-user"}
}

func waitForSubscriptionReady(
	ctx context.Context,
	t *testing.T,
	store ports.Store,
	target domain.Target,
	actor domain.Actor,
	readyCh <-chan struct{},
) domain.Revision {
	t.Helper()

	var revision domain.Revision

	deadline := time.Now().Add(subscribeSignalWaitTimeout)
	attempt := 0

	for time.Now().Before(deadline) {
		attempt++

		newRevision, err := store.Put(ctx, target, []ports.WriteOp{{Key: "ready.probe", Value: attempt}}, revision, actor, "integration-test")
		require.NoError(t, err)

		revision = newRevision

		select {
		case <-readyCh:
			return revision
		case <-time.After(readyProbeRetryInterval):
		}
	}

	t.Fatal("timed out waiting for subscriber readiness")

	return domain.RevisionZero
}
