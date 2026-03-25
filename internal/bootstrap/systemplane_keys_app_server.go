// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"

func matcherKeyDefsAppServer() []domain.KeyDef {
	return concatKeyDefs(
		matcherKeyDefsApp(),
		matcherKeyDefsServerHTTP(),
		matcherKeyDefsServerTLS(),
	)
}

func matcherKeyDefsApp() []domain.KeyDef {
	return []domain.KeyDef{
		// --- App. ---
		{
			Key:              "app.env_name",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultEnvName,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Application environment name (e.g., development, staging, production)",
			Group:            "app",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "app.log_level",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultLogLevel,
			ValueType:        domain.ValueTypeString,
			Validator:        validateLogLevel,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Application log level (debug, info, warn, error)",
			Group:            "app",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsServerHTTP() []domain.KeyDef {
	return []domain.KeyDef{
		// --- Server. ---
		{
			Key:              "server.address",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultServerAddress,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "HTTP server listen address (e.g., :4018)",
			Group:            "server",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "server.body_limit_bytes",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultKeyBodyLimitBytes,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Maximum HTTP request body size in bytes",
			Group:            "server",
			Component:        "http",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "cors.allowed_origins",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCORSAllowedOrigins,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Comma-separated list of allowed CORS origins",
			Group:            "cors",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "cors.allowed_methods",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCORSAllowedMethods,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Comma-separated list of allowed CORS methods",
			Group:            "cors",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "cors.allowed_headers",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCORSAllowedHeaders,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Comma-separated list of allowed CORS headers",
			Group:            "cors",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}

func matcherKeyDefsServerTLS() []domain.KeyDef {
	return []domain.KeyDef{
		{
			Key:              "server.tls_cert_file",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultServerTLSCertFile,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Path to TLS certificate file",
			Group:            "server",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "server.tls_key_file",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultServerTLSKeyFile,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Path to TLS private key file",
			Group:            "server",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "server.tls_terminated_upstream",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTLSTerminatedUpstream,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Whether TLS is terminated by an upstream proxy",
			Group:            "server",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "server.trusted_proxies",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultServerTrustedProxies,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Comma-separated list of trusted proxy CIDRs",
			Group:            "server",
			Component:        domain.ComponentNone,
			RedactPolicy:     domain.RedactNone,
		},
	}
}
