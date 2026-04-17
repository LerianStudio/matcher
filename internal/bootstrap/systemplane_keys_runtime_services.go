// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"

func matcherKeyDefsInfrastructure() []domain.KeyDef {
	return concatKeyDefs(
		matcherKeyDefsInfrastructureRuntime(),
		matcherKeyDefsIdempotency(),
		matcherKeyDefsCallbackRateLimit(),
		matcherKeyDefsFetcherCore(),
		matcherKeyDefsFetcherRuntime(),
		matcherKeyDefsFetcherCustodyRetention(),
		matcherKeyDefsM2M(),
	)
}

func matcherKeyDefsInfrastructureRuntime() []domain.KeyDef {
	return []domain.KeyDef{
		// --- Infrastructure. ---
		{
			Key:              "infrastructure.connect_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultInfraConnectTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Infrastructure connection timeout in seconds",
			Group:            "infrastructure",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "infrastructure.health_check_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultInfraHealthCheckTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Health check probe timeout in seconds",
			Group:            "infrastructure",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsIdempotency() []domain.KeyDef {
	return []domain.KeyDef{
		// --- Idempotency. ---
		{
			Key:              "idempotency.retry_window_sec",
			Kind:             domain.KindSetting,
			AllowedScopes:    settingScopes(),
			DefaultValue:     defaultIdempotencyRetryWindow,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Failed idempotency key retry window in seconds",
			Group:            "idempotency",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "idempotency.success_ttl_hours",
			Kind:             domain.KindSetting,
			AllowedScopes:    settingScopes(),
			DefaultValue:     defaultIdempotencySuccessTTL,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Completed idempotency key cache TTL in hours",
			Group:            "idempotency",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "idempotency.hmac_secret",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Secret:           true,
			Description:      "HMAC secret for signing idempotency keys before storage",
			Group:            "idempotency",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactFull,
		},
	}
}

func matcherKeyDefsCallbackRateLimit() []domain.KeyDef {
	return []domain.KeyDef{
		// --- Callback Rate Limit. ---
		{
			Key:              "callback_rate_limit.per_minute",
			Kind:             domain.KindSetting,
			AllowedScopes:    settingScopes(),
			DefaultValue:     defaultCallbackPerMinute,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Maximum callbacks per external system per minute",
			Group:            "callback_rate_limit",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsFetcherCore() []domain.KeyDef {
	return []domain.KeyDef{
		// --- Fetcher. ---
		{
			Key:              "fetcher.enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultFetcherEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Enable Fetcher-backed source discovery module",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.url",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultFetcherURL,
			ValueType:        domain.ValueTypeString,
			Validator:        validateAbsoluteHTTPURL,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Fetcher service base URL",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.allow_private_ips",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultFetcherAllowPrivateIPs,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Allow Fetcher to connect to private IP addresses",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.health_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultKeyFetcherHealthTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Fetcher health check timeout in seconds",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsFetcherRuntime() []domain.KeyDef {
	return []domain.KeyDef{
		{
			Key:              "fetcher.request_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultKeyFetcherRequestTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Fetcher HTTP request timeout in seconds",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.discovery_interval_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultFetcherDiscoveryInt,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Fetcher source discovery polling interval in seconds",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.schema_cache_ttl_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultKeyFetcherSchemaCacheTTL,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Fetcher schema cache TTL in seconds",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.extraction_poll_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultFetcherExtractionPoll,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Fetcher extraction job polling interval in seconds",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.extraction_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultFetcherExtractionTO,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Fetcher extraction job timeout in seconds",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			// app_enc_key is the raw master key shared with Fetcher. Matcher
			// derives HMAC-SHA256 and AES-256-GCM keys locally via HKDF
			// (contexts "fetcher-external-hmac-v1" / "fetcher-external-aes-v1")
			// to verify completed-artifact integrity in the Fetcher bridge
			// (T-002). Bootstrap-only because the derived keys are cached
			// for the process lifetime; rotation requires a restart. Empty
			// soft-disables verified-artifact retrieval (the T-001 intake
			// path still works).
			Key:              "fetcher.app_enc_key",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Secret:           true,
			Description:      "Master key shared with Fetcher (base64); HKDF-derived HMAC/AES keys verify artifact integrity",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactFull,
		},
		{
			// max_extraction_bytes caps the size FlattenFetcherJSON will
			// materialise. T-003 P2-T001 hardening: guards against
			// OOM-by-giant-payload when the bridge worker wires a live
			// Fetcher HTTP body into the flattener. Bootstrap-only because
			// changing it mid-flight while a long-running flatten is in
			// progress would create ambiguous behaviour.
			//
			// Bounded validator (Fix 9): plain validatePositiveInt accepted
			// MaxInt64 and defeated the DoS guard. Bounds chosen so the
			// floor (1 MiB) keeps the guard meaningful and the ceiling
			// (16 GiB) caps the worst-case malicious payload at well under
			// any realistic pod memory budget.
			Key:              "fetcher.max_extraction_bytes",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     int64(2 << 30),
			ValueType:        domain.ValueTypeInt,
			Validator:        validateFetcherMaxExtractionBytes,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Maximum Fetcher extraction payload size in bytes (DoS guard, bounds [1 MiB, 16 GiB])",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.bridge_interval_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     30,
			ValueType:        domain.ValueTypeInt,
			Validator:        validateBridgeIntervalSec,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Bridge worker poll interval in seconds (bounds [5, 3600])",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.bridge_batch_size",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     50,
			ValueType:        domain.ValueTypeInt,
			Validator:        validateBridgeBatchSize,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Bridge worker per-tenant batch size (bounds [1, 10000])",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			// T-004 read-model partitioning threshold for the operational
			// dashboard's bridge readiness counts. Read at query time;
			// the bridge worker does not consume this value, so live-read
			// is appropriate (no worker reconcile required).
			Key:              "fetcher.bridge_stale_threshold_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     3600,
			ValueType:        domain.ValueTypeInt,
			Validator:        validateBridgeStaleThresholdSec,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Stale threshold (seconds) for bridge readiness dashboard partition (bounds [60, 86400])",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			// T-005 retry policy: max attempts before terminal escalation.
			// ApplyWorkerReconcile because the bridge worker reads it on
			// startup and the reconciler restarts the worker on change.
			//
			// Polish Fix 2: the sibling `bridge_retry_initial_backoff_sec`
			// and `bridge_retry_max_backoff_sec` keys were deleted along
			// with the dead exponential-backoff helpers. The worker enforces
			// backoff passively via FindEligibleForBridge ordering by
			// updated_at; the tick cadence (bridge_interval_sec) IS the
			// retry cadence.
			Key:              "fetcher.bridge_retry_max_attempts",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     5,
			ValueType:        domain.ValueTypeInt,
			Validator:        validateBridgeRetryMaxAttempts,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Bridge worker retry max attempts before terminal escalation (bounds [1, 100])",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

// matcherKeyDefsFetcherCustodyRetention holds the T-006 custody retention
// sweep worker knobs. Split from matcherKeyDefsFetcherRuntime to keep the
// fetcher runtime defs under the funlen ceiling; functionally equivalent
// (matcherKeyDefsInfrastructure concatenates both).
func matcherKeyDefsFetcherCustodyRetention() []domain.KeyDef {
	return []domain.KeyDef{
		{
			// T-006 custody retention sweep cadence. ApplyWorkerReconcile
			// because the worker reads the interval at construction and
			// the reconciler restarts the worker on change.
			Key:              "fetcher.custody_retention_sweep_interval_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCustodyRetentionSweepIntervalSec,
			ValueType:        domain.ValueTypeInt,
			Validator:        validateCustodyRetentionSweepIntervalSec,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Custody retention sweep worker tick interval in seconds (bounds [60, 86400])",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			// T-006 grace period for LATE-LINKED retention candidates.
			// ApplyWorkerReconcile because the worker reads the grace
			// period at construction and the reconciler restarts the
			// worker on change.
			Key:              "fetcher.custody_retention_grace_period_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCustodyRetentionGracePeriodSec,
			ValueType:        domain.ValueTypeInt,
			Validator:        validateCustodyRetentionGracePeriodSec,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Custody retention grace period in seconds for LATE-LINKED candidates (bounds [0, 604800])",
			Group:            "fetcher",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsM2M() []domain.KeyDef {
	return []domain.KeyDef{
		// --- M2M (Machine-to-Machine). ---
		{
			Key:              "m2m.m2m_target_service",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultM2MTargetService,
			ValueType:        domain.ValueTypeString,
			Validator:        validateNonEmptyString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Target service name for M2M credential path in Secrets Manager",
			Group:            "m2m",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "m2m.m2m_credential_cache_ttl_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultM2MCredentialCacheTTL,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "M2M credential L2 (Redis) cache TTL in seconds",
			Group:            "m2m",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "m2m.aws_region",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "AWS region for Secrets Manager API calls (empty uses SDK default chain)",
			Group:            "m2m",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}
