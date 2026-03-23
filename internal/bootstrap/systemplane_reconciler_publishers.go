// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
	tmrabbitmq "github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/rabbitmq"
)

var _ ports.BundleReconciler = (*PublisherReconciler)(nil)

// PublisherReconciler validates staged RabbitMQ publishers for a candidate bundle.
type PublisherReconciler struct {
	logger       libLog.Logger
	configGetter func() *Config
}

// NewPublisherReconciler builds a publisher reconciler with a safe logger fallback.
// configGetter provides access to the current runtime config so the reconciler can
// detect multi-tenant mode and build an rmqManager for staged publishers.
func NewPublisherReconciler(logger libLog.Logger, configGetter func() *Config) *PublisherReconciler {
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	return &PublisherReconciler{logger: logger, configGetter: configGetter}
}

// Name returns the reconciler identifier used in logs and metrics.
func (reconciler *PublisherReconciler) Name() string {
	return "publisher-reconciler"
}

// Phase returns the reconciliation phase for publisher validation.
func (reconciler *PublisherReconciler) Phase() domain.ReconcilerPhase {
	return domain.PhaseValidation
}

// Reconcile validates and stages publishers for the candidate runtime bundle.
func (reconciler *PublisherReconciler) Reconcile(
	ctx context.Context,
	previous domain.RuntimeBundle,
	candidate domain.RuntimeBundle,
	_ domain.Snapshot,
) error {
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

	// Build an rmqManager when multi-tenant mode is enabled so that staged
	// publishers use per-tenant vhost isolation. Without this, a config rebuild
	// would silently downgrade multi-tenant publishers to single-tenant.
	var rmqManager *tmrabbitmq.Manager
	if cfg := reconciler.currentConfig(); cfg != nil && multiTenantModeEnabled(cfg) {
		rmqManager = buildRabbitMQTenantManager(ctx, cfg, reconciler.logger)
	}

	matchingPublisher, ingestionPublisher, err := initEventPublishers(ctx, currentConn, reconciler.logger, rmqManager)
	if err != nil {
		return fmt.Errorf("publisher reconciler init publishers: %w", err)
	}

	currentBundle.StagedMatchingPublisher = matchingPublisher
	currentBundle.StagedIngestionPublisher = ingestionPublisher

	return nil
}

// currentConfig returns the runtime config from the configGetter, or nil.
func (reconciler *PublisherReconciler) currentConfig() *Config {
	if reconciler.configGetter == nil {
		return nil
	}

	return reconciler.configGetter()
}
