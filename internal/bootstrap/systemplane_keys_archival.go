// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import "github.com/LerianStudio/matcher/pkg/systemplane/domain"

func matcherKeyDefsArchival() []domain.KeyDef {
	return concatKeyDefs(
		matcherKeyDefsScheduler(),
		matcherKeyDefsArchivalLifecycle(),
		matcherKeyDefsArchivalStorage(),
		matcherKeyDefsArchivalRuntime(),
	)
}

func matcherKeyDefsScheduler() []domain.KeyDef {
	return []domain.KeyDef{
		// --- Scheduler. ---
		{
			Key:              "scheduler.interval_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultSchedulerInterval,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Scheduler tick interval in seconds",
			Group:            "scheduler",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsArchivalLifecycle() []domain.KeyDef {
	return []domain.KeyDef{
		// --- Archival. ---
		{
			Key:              "archival.enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Enable the audit log archival worker",
			Group:            "archival",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.interval_hours",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalInterval,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Archival worker execution interval in hours",
			Group:            "archival",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.hot_retention_days",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalHotDays,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Days to retain audit logs in hot storage before archival",
			Group:            "archival",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.warm_retention_months",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalWarmMonths,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Months to retain audit logs in warm storage",
			Group:            "archival",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.cold_retention_months",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalColdMonths,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Months to retain audit logs in cold storage",
			Group:            "archival",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.batch_size",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalBatchSize,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Number of audit log records per archival batch",
			Group:            "archival",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsArchivalStorage() []domain.KeyDef {
	return []domain.KeyDef{
		{
			Key:              "archival.storage_bucket",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Object storage bucket for archived audit logs",
			Group:            "archival",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.storage_prefix",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalStoragePrefix,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Object storage key prefix for archived audit logs",
			Group:            "archival",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.storage_class",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalStorageClass,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Object storage class for archived audit logs",
			Group:            "archival",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsArchivalRuntime() []domain.KeyDef {
	return []domain.KeyDef{
		{
			Key:              "archival.partition_lookahead",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalPartitionLA,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Number of future partitions to pre-create for audit log tables",
			Group:            "archival",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.presign_expiry_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalPresignExpiry,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Pre-signed URL expiry for archive downloads in seconds",
			Group:            "archival",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}
