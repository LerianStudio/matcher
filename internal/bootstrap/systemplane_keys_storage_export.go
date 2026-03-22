// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"

func matcherKeyDefsStorageExport() []domain.KeyDef {
	return concatKeyDefs(
		matcherKeyDefsDeduplication(),
		matcherKeyDefsObjectStorage(),
		matcherKeyDefsExportWorker(),
	)
}

func matcherKeyDefsDeduplication() []domain.KeyDef {
	return []domain.KeyDef{
		// --- Deduplication. ---
		{
			Key:              "deduplication.ttl_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultDedupeTTLSec,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Deduplication key TTL in seconds",
			Group:            "deduplication",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsObjectStorage() []domain.KeyDef {
	return []domain.KeyDef{
		// --- Object Storage. ---
		{
			Key:              "object_storage.endpoint",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultObjStorageEndpoint,
			ValueType:        domain.ValueTypeString,
			Validator:        validateHTTPSEndpoint,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "S3-compatible object storage endpoint URL (HTTPS required)",
			Group:            "object_storage",
			Component:        "s3",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "object_storage.region",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultObjStorageRegion,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Object storage region",
			Group:            "object_storage",
			Component:        "s3",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "object_storage.bucket",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultObjStorageBucket,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Object storage bucket name for exports",
			Group:            "object_storage",
			Component:        "s3",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "object_storage.access_key_id",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Secret:           true,
			Description:      "Object storage access key ID",
			Group:            "object_storage",
			Component:        "s3",
			RedactPolicy:     domain.RedactFull,
		},
		{
			Key:              "object_storage.secret_access_key",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Secret:           true,
			Description:      "Object storage secret access key",
			Group:            "object_storage",
			Component:        "s3",
			RedactPolicy:     domain.RedactFull,
		},
		{
			Key:              "object_storage.use_path_style",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultObjStoragePathStyle,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Use path-style addressing for object storage requests",
			Group:            "object_storage",
			Component:        "s3",
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsExportWorker() []domain.KeyDef {
	return []domain.KeyDef{
		// --- Export Worker. ---
		{
			Key:              "export_worker.enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultExportEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Enable the export worker background processor",
			Group:            "export_worker",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "export_worker.poll_interval_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultExportPollInt,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Export worker polling interval in seconds",
			Group:            "export_worker",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "export_worker.page_size",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultExportPageSize,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Number of rows per page in export queries",
			Group:            "export_worker",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "export_worker.presign_expiry_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultExportPresignExp,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Pre-signed URL expiry for export downloads in seconds",
			Group:            "export_worker",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}
