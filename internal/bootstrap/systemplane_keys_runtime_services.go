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
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
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
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
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
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
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
