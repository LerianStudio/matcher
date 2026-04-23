// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

// Explicit per-stack TLS enforcement. Each infra stack (Postgres primary,
// Postgres replica, Redis, RabbitMQ, Object Storage) exposes a boolean
// X_TLS_REQUIRED flag on its Config struct. When the flag is set, this module
// validates that the stack's connection configuration declares TLS; when
// unset (default false) the stack is unenforced — the operator's call.
//
// ValidateRequiredTLS is called once from bootstrap, BEFORE any infrastructure
// connection opens, so a stack flagged TLS_REQUIRED but configured with a
// plaintext DSN fails closed with ErrTLSRequiredButNotDeclared instead of
// opening a plaintext connection. Callers add new infra deps by extending the
// slice below and adding a new boolean flag to the corresponding Config
// sub-struct — not by replicating the guard at call sites.
//
// This module intentionally does NOT consult cfg.App.Mode (DEPLOYMENT_MODE).
// TLS enforcement is orthogonal to deployment mode: an operator can run in
// "local" mode with every stack enforced, or in "saas" mode with selective
// enforcement. Coupling the two led to a previous foot-gun where an operator
// running ENV_NAME=production with DEPLOYMENT_MODE unset (defaulting to
// "local") got plaintext connections silently.

import (
	"errors"
	"fmt"
	"strings"
)

// ErrTLSRequiredButNotDeclared is returned when a stack flagged TLS_REQUIRED
// has a connection configuration that does not declare TLS. Errors.Is-friendly.
var ErrTLSRequiredButNotDeclared = errors.New("tls required but not declared")

// ErrTLSMalformedDependencyConfig is returned when a stack flagged
// TLS_REQUIRED has a malformed connection configuration (e.g., Redis host
// with an invalid IPv6 literal). Fails closed rather than silently opening a
// plaintext connection.
var ErrTLSMalformedDependencyConfig = errors.New("malformed dependency configuration under tls_required")

// ValidateRequiredTLS enforces that every infra stack flagged TLS_REQUIRED
// has a connection configuration that declares TLS. Called once from
// bootstrap, BEFORE any connection opens, so a plaintext start with a stack
// flagged TLS_REQUIRED is impossible.
//
// Contract:
//   - cfg == nil: noop, returns nil.
//   - For each stack (postgres, postgres_replica, redis, rabbitmq,
//     object_storage): when the stack's TLSRequired flag is false, the check
//     is skipped. When true, the stack's connection config is inspected.
//   - Postgres replica is only checked when a distinct replica host is
//     configured (cfg.Postgres.ReplicaHost != ""). Without a distinct replica,
//     ReplicaDSN() falls back to PrimaryDSN and the replica flag is a no-op.
//   - Empty conn strings (unconfigured stacks) are treated as not-present and
//     skipped with no error — TLS_REQUIRED=true against a non-configured
//     stack does not force configuration.
//   - A stack with TLS_REQUIRED=true and a non-TLS configuration produces an
//     error wrapping ErrTLSRequiredButNotDeclared and naming the offending
//     stack (one of: postgres, postgres_replica, redis, rabbitmq,
//     object_storage).
//   - A stack with TLS_REQUIRED=true and a malformed configuration fails
//     closed with ErrTLSMalformedDependencyConfig naming the offending
//     stack. Raw parser details are intentionally not propagated in the
//     returned error text.
func ValidateRequiredTLS(cfg *Config) error {
	if cfg == nil {
		return nil
	}

	type depCheck struct {
		name     string
		required bool
		present  bool
		detect   func() (bool, error)
	}

	// Postgres replica is "present" only when a distinct replica host is
	// configured. When empty, ReplicaDSN() falls back to the primary DSN, so
	// an independent replica TLS check would just re-check the primary.
	replicaConfigured := cfg.Postgres.ReplicaHost != ""

	checks := []depCheck{
		{
			name:     "postgres",
			required: cfg.Postgres.TLSRequired,
			present:  cfg.PrimaryDSN() != "",
			detect:   func() (bool, error) { return detectPostgresTLS(cfg.PrimaryDSN()) },
		},
		{
			name:     "postgres_replica",
			required: cfg.Postgres.ReplicaTLSRequired,
			present:  replicaConfigured,
			detect:   func() (bool, error) { return detectPostgresTLS(cfg.ReplicaDSN()) },
		},
		{
			name:     "redis",
			required: cfg.Redis.TLSRequired,
			present:  cfg.Redis.Host != "",
			detect:   func() (bool, error) { return detectRedisTLS(buildRedisURLForTLSCheck(cfg)) },
		},
		{
			name:     "rabbitmq",
			required: cfg.RabbitMQ.TLSRequired,
			present:  cfg.RabbitMQDSN() != "",
			detect:   func() (bool, error) { return detectAMQPTLS(cfg.RabbitMQDSN()) },
		},
		{
			// Empty object-storage endpoint is AWS default (HTTPS), which
			// detectS3TLS reports as TLS. So "present=true" with an empty
			// endpoint is safe — detectS3TLS returns true.
			name:     "object_storage",
			required: cfg.ObjectStorage.TLSRequired,
			present:  true,
			detect:   func() (bool, error) { return detectS3TLS(cfg.ObjectStorage.Endpoint) },
		},
	}

	for _, check := range checks {
		if !check.required || !check.present {
			continue
		}

		tls, err := check.detect()
		if err != nil {
			return fmt.Errorf("%w: %s", ErrTLSMalformedDependencyConfig, check.name)
		}

		if !tls {
			return fmt.Errorf("%w: %s (TLS_REQUIRED=true but configuration does not declare TLS)",
				ErrTLSRequiredButNotDeclared, check.name)
		}
	}

	return nil
}

// buildRedisURLForTLSCheck constructs a minimal Redis URL reflecting the
// configured TLS posture for detection purposes. It is NOT a connection
// string — it does not carry credentials, database selector, or topology
// detail — and must not be logged or used to dial.
//
// The matcher Redis config (RedisConfig) flags TLS as a boolean
// (cfg.Redis.TLS) rather than as a URL scheme. The detection layer expects
// a URL-shaped input so we synthesise one. Empty Host yields an empty
// output so callers can decide to skip the check.
func buildRedisURLForTLSCheck(cfg *Config) string {
	if cfg == nil || cfg.Redis.Host == "" {
		return ""
	}

	scheme := "redis"
	if cfg.Redis.TLS {
		scheme = "rediss"
	}

	// Host may be a comma-separated list for cluster/sentinel modes. Use the
	// first non-empty segment for scheme detection.
	addr := firstNonEmptyCommaSegment(cfg.Redis.Host)

	if addr == "" {
		return ""
	}

	return scheme + "://" + addr
}

func firstNonEmptyCommaSegment(value string) string {
	for _, segment := range strings.Split(value, ",") {
		candidate := strings.TrimSpace(segment)
		if candidate == "" {
			continue
		}

		return candidate
	}

	return ""
}
