// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package cross

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestNewFetcherBridgeIntakeAdapter_RejectsNilUseCase(t *testing.T) {
	t.Parallel()

	adapter, err := NewFetcherBridgeIntakeAdapter(nil)
	require.Nil(t, adapter)
	require.ErrorIs(t, err, sharedPorts.ErrNilFetcherBridgeIntake)
}

// TestNewFetcherBridgeIntakeAdapter_AcceptsNonNilUseCase verifies the
// happy-path construction branch of the constructor. The adapter only
// checks for a nil pointer; a zero-valued UseCase pointer is sufficient to
// exercise the successful-return branch without dragging in the full
// ingestion dependency graph.
func TestNewFetcherBridgeIntakeAdapter_AcceptsNonNilUseCase(t *testing.T) {
	t.Parallel()

	adapter, err := NewFetcherBridgeIntakeAdapter(&ingestionCommand.UseCase{})
	require.NoError(t, err)
	require.NotNil(t, adapter)
}

func TestFetcherBridgeIntakeAdapter_NilAdapter_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var adapter *FetcherBridgeIntakeAdapter

	outcome, err := adapter.IngestTrustedContent(context.Background(), sharedPorts.TrustedContentInput{})
	require.Equal(t, sharedPorts.TrustedContentOutcome{}, outcome)
	require.ErrorIs(t, err, sharedPorts.ErrNilFetcherBridgeIntake)
}
