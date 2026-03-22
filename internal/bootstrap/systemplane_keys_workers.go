// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"

func matcherKeyDefsWorkers() []domain.KeyDef {
	return []domain.KeyDef{
		// --- Webhook. ---
		{
			Key:              "webhook.timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultWebhookTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Default HTTP timeout for webhook dispatches in seconds",
			Group:            "webhook",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},

		// --- Cleanup Worker. ---
		{
			Key:              "cleanup_worker.enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCleanupEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Enable the cleanup worker background processor",
			Group:            "cleanup_worker",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "cleanup_worker.interval_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCleanupInterval,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Cleanup worker execution interval in seconds",
			Group:            "cleanup_worker",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "cleanup_worker.batch_size",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCleanupBatchSize,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Number of items per cleanup batch",
			Group:            "cleanup_worker",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "cleanup_worker.grace_period_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCleanupGracePeriod,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Grace period before cleanup of expired items in seconds",
			Group:            "cleanup_worker",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}
