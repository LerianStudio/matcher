// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

var _ ports.BundleReconciler = (*PublisherReconciler)(nil)

type PublisherReconciler struct {
	logger libLog.Logger
}

func NewPublisherReconciler(logger libLog.Logger) *PublisherReconciler {
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	return &PublisherReconciler{logger: logger}
}

func (r *PublisherReconciler) Name() string {
	return "publisher-reconciler"
}

func (r *PublisherReconciler) Phase() domain.ReconcilerPhase {
	return domain.PhaseValidation
}

func (r *PublisherReconciler) Reconcile(ctx context.Context, previous domain.RuntimeBundle, candidate domain.RuntimeBundle, _ domain.Snapshot) error {
	currentBundle, ok := candidate.(*MatcherBundle)
	if !ok || currentBundle == nil {
		return nil
	}

	var previousBundle *MatcherBundle
	if previous != nil {
		previousBundle, _ = previous.(*MatcherBundle)
	}

	currentConn := currentBundle.RabbitMQConn()
	if currentConn == nil {
		return nil
	}

	if previousBundle != nil && currentConn == previousBundle.RabbitMQConn() {
		return nil
	}

	if err := currentConn.EnsureChannelContext(ctx); err != nil {
		return fmt.Errorf("publisher reconciler ensure rabbitmq channel: %w", err)
	}

	matchingPublisher, ingestionPublisher, err := initEventPublishers(currentConn, r.logger)
	if err != nil {
		return fmt.Errorf("publisher reconciler init publishers: %w", err)
	}

	currentBundle.StagedMatchingPublisher = matchingPublisher
	currentBundle.StagedIngestionPublisher = ingestionPublisher

	return nil
}
