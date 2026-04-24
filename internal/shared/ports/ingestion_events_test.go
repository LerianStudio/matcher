// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

import (
	"context"
	"testing"

	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestIngestionEventPublisherInterface(t *testing.T) {
	t.Parallel()

	// Compile-time interface satisfaction check.
	var _ IngestionEventPublisher = (*mockIngestionEventPublisher)(nil)
}

type mockIngestionEventPublisher struct{}

func (m *mockIngestionEventPublisher) PublishIngestionCompleted(
	_ context.Context,
	_ *sharedDomain.IngestionCompletedEvent,
) error {
	return nil
}

func (m *mockIngestionEventPublisher) PublishIngestionFailed(
	_ context.Context,
	_ *sharedDomain.IngestionFailedEvent,
) error {
	return nil
}
