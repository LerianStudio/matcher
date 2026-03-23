// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"

func matcherKeyDefsTenancy() []domain.KeyDef {
	return concatKeyDefs(
		matcherKeyDefsTenancyDefaults(),
		matcherKeyDefsTenancyConnectivity(),
		matcherKeyDefsTenancyResilience(),
	)
}

func matcherKeyDefsTenancyDefaults() []domain.KeyDef {
	return []domain.KeyDef{
		// --- Tenancy. ---
		{
			Key:              "tenancy.default_tenant_id",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTenantID,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Default tenant UUID for single-tenant mode",
			Group:            "tenancy",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.default_tenant_slug",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTenantSlug,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Default tenant slug for single-tenant mode",
			Group:            "tenancy",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsTenancyConnectivity() []domain.KeyDef {
	return []domain.KeyDef{
		{
			Key:              "tenancy.multi_tenant_enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultMultiTenantEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Enable multi-tenant infrastructure (tenant manager, per-tenant pools)",
			Group:            "tenancy",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_url",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Tenant management service URL",
			Group:            "tenancy",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_environment",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Tenant management environment identifier",
			Group:            "tenancy",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_max_tenant_pools",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultMultiTenantMaxTenantPools,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Maximum number of concurrent per-tenant connection pools",
			Group:            "tenancy",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_idle_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultMultiTenantIdleTimeoutSec,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Idle timeout (seconds) before evicting a tenant connection pool",
			Group:            "tenancy",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsTenancyResilience() []domain.KeyDef {
	return []domain.KeyDef{
		{
			Key:              "tenancy.multi_tenant_circuit_breaker_threshold",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultMultiTenantCircuitBreakerThresh,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Failure threshold before circuit breaker opens for a tenant pool",
			Group:            "tenancy",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_circuit_breaker_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultMultiTenantCircuitBreakerSec,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Circuit breaker open duration (seconds) before half-open retry",
			Group:            "tenancy",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_service_api_key",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Secret:           true,
			Description:      "API key for authenticating with the tenant management service",
			Group:            "tenancy",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactFull,
		},
	}
}
