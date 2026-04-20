// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"strings"
	"syscall"
)

// categoriseProbeError maps an error from a per-dependency readiness probe to
// one of a bounded set of opaque tokens safe to expose in the /readyz response
// body. The full error (including any DSN fragments) is written to server logs
// via logger.Log at the call site; the HTTP body only ever carries one of the
// tokens below.
//
// Why opaque tokens rather than a sanitised message:
//   - /readyz is pre-auth — K8s probes are unauthenticated and probe logs
//     ingest response bodies verbatim.
//   - pgx/libpq errors under some failure modes include the DSN verbatim
//     (host=..., user=..., password=...). A length cap still leaves the first
//     N bytes exposed, which is usually where the credentials live.
//   - Operators who need the full detail read the server logs, which are
//     authenticated and audited.
//
// Typed-error checks (errors.Is / errors.As) are preferred where Go's net
// stack surfaces a typed error; substring fallback only applies to cases the
// stdlib does not type (TLS handshake). The default category is "check
// failed" — the truthful "something broke, see server logs".
// probeErrorTokenTLSHandshake is the bounded token for any TLS-handshake
// failure surfaced via /readyz. Kept in one place so typed and substring
// detection paths agree.
const probeErrorTokenTLSHandshake = "tls handshake failed" // #nosec G101 -- bounded error token, not a credential

// Category priority (DNS → timeout → econnrefused → tls → default) is
// intentional. DNS surfaces first because it is the most actionable for
// operators (wrong host vs. wrong network). Deadline second so a wrapped error
// carrying both DNS and deadline categorises as "dns failure". TLS fourth —
// typed errors first, substring fallback last — so legitimate handshake
// failures do not accidentally match on error messages that mention "tls" in
// unrelated contexts.
func categoriseProbeError(err error) string {
	if err == nil {
		return ""
	}

	// Order matters: DNS errors wrap a syscall error in some paths, so match
	// the more-specific DNS case first.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns failure"
	}

	if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
		return "timeout"
	}

	if errors.Is(err, syscall.ECONNREFUSED) {
		return "connection refused"
	}

	// Prefer typed TLS error detection: crypto/tls exposes alert errors and
	// record-header errors. These catch the most common handshake failure
	// modes without relying on message formatting.
	var alertErr tls.AlertError
	if errors.As(err, &alertErr) {
		return probeErrorTokenTLSHandshake
	}

	var recordErr *tls.RecordHeaderError
	if errors.As(err, &recordErr) {
		return probeErrorTokenTLSHandshake
	}

	msg := strings.ToLower(err.Error())

	// Substring fallback for TLS errors that go through net.OpError wrapping
	// or are surfaced as plain errors.New() strings by drivers.
	if strings.Contains(msg, "tls") && strings.Contains(msg, "handshake") {
		return probeErrorTokenTLSHandshake
	}

	return "check failed"
}

// postgresTLSPosture returns a pointer to the primary DSN TLS posture and
// an error-reason string (empty on success) suitable for response Error field.
// Nil TLS means "unknown"/"parse error"; callers render that as field absent.
func postgresTLSPosture(cfg *Config) (*bool, string) {
	if cfg == nil {
		return nil, ""
	}

	tlsOn, err := detectPostgresTLS(cfg.PrimaryDSN())
	if err != nil {
		return nil, "invalid postgres connection configuration"
	}

	return &tlsOn, ""
}

// postgresReplicaTLSPosture: when no replica host is configured, ReplicaDSN
// falls back to the primary, so the TLS posture matches the primary. We
// detect on ReplicaDSN() unconditionally.
func postgresReplicaTLSPosture(cfg *Config) (*bool, string) {
	if cfg == nil {
		return nil, ""
	}

	tlsOn, err := detectPostgresTLS(cfg.ReplicaDSN())
	if err != nil {
		return nil, "invalid postgres replica connection configuration"
	}

	return &tlsOn, ""
}

func redisTLSPosture(cfg *Config) (*bool, string) {
	if cfg == nil {
		return nil, ""
	}

	tlsOn, err := detectRedisTLS(buildRedisURLForTLSCheck(cfg))
	if err != nil {
		return nil, "invalid redis connection configuration"
	}

	return &tlsOn, ""
}

func rabbitMQTLSPosture(cfg *Config) (*bool, string) {
	if cfg == nil {
		return nil, ""
	}

	tlsOn, err := detectAMQPTLS(cfg.RabbitMQDSN())
	if err != nil {
		return nil, "invalid rabbitmq connection configuration"
	}

	return &tlsOn, ""
}

func objectStorageTLSPosture(cfg *Config) (*bool, string) {
	if cfg == nil {
		return nil, ""
	}

	tlsOn, err := detectS3TLS(cfg.ObjectStorage.Endpoint)
	if err != nil {
		return nil, "invalid object storage endpoint configuration"
	}

	return &tlsOn, ""
}
