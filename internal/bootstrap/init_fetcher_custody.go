// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Custody crypto + retention for the Fetcher bridge. Split from
// init_fetcher_bridge.go so APP_ENC_KEY parsing, the cgroup-backed memory
// budget guard, GOMEMLIMIT application, and the custody retention worker
// live in one place. Everything here is concerned with the life of
// custody-held artifacts (protecting them with crypto, making sure the pod
// has enough RAM to verify them, sweeping them once extractions settle).

package bootstrap

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	discoveryExtractionRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	discoveryWorker "github.com/LerianStudio/matcher/internal/discovery/services/worker"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ErrFetcherBridgeMasterKeyRequired indicates verified-artifact retrieval
// was requested but APP_ENC_KEY is empty. Exported so bootstrap callers
// can distinguish the "crypto disabled" policy choice from a generic init
// failure. The bridge worker in T-003 will also check this.
var ErrFetcherBridgeMasterKeyRequired = errors.New(
	"fetcher bridge requires APP_ENC_KEY to verify artifacts",
)

// ErrFetcherBridgeMasterKeyInvalid indicates APP_ENC_KEY was provided
// but could not be decoded as base64 or was shorter than the Fetcher
// contract minimum. The underlying cause is wrapped so operators can
// see whether it was a decode failure or a length failure without the
// log pipeline leaking key bytes.
var ErrFetcherBridgeMasterKeyInvalid = errors.New(
	"fetcher bridge APP_ENC_KEY is invalid",
)

// decodeMasterKey parses the base64-encoded master key and validates
// that it meets the Fetcher contract length (at least 32 bytes). Empty
// input returns ErrFetcherBridgeMasterKeyRequired so the caller can
// distinguish "disabled" from "misconfigured".
//
// Only standard base64 (RFC 4648 §4) is accepted. URL-safe base64 is
// rejected so operators cannot end up with two environments silently
// holding different derived keys because the distribution channel
// re-encoded `+`/`/` into `-`/`_`. If a URL-safe value is provided,
// the error points the operator at the distribution pipeline rather
// than at the service config.
func decodeMasterKey(raw string) ([]byte, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, ErrFetcherBridgeMasterKeyRequired
	}

	decoded, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		// Detect the common foot-gun: the value is URL-safe base64. If so,
		// tell operators explicitly to re-encode as standard base64 at the
		// distribution source rather than silently accepting both encodings
		// and risking divergent derived keys across environments.
		if _, urlErr := base64.URLEncoding.DecodeString(trimmed); urlErr == nil {
			return nil, fmt.Errorf(
				"%w: value is URL-safe base64; re-encode as standard base64 (RFC 4648 §4) at the distribution source so every environment shares the same canonical bytes",
				ErrFetcherBridgeMasterKeyInvalid,
			)
		}

		return nil, fmt.Errorf(
			"%w: not standard base64",
			ErrFetcherBridgeMasterKeyInvalid,
		)
	}

	// 32-byte minimum matches fetcher.minMasterKeyLen. We re-check here
	// so the bootstrap error is produced before the verifier constructor
	// reports the same violation — keeps the failure signal close to the
	// config source.
	const minLen = 32
	if len(decoded) < minLen {
		return nil, fmt.Errorf(
			"%w: decoded length %d is shorter than %d",
			ErrFetcherBridgeMasterKeyInvalid,
			len(decoded),
			minLen,
		)
	}

	return decoded, nil
}

// bridgeMinMemoryBytes is the minimum pod memory budget we require before
// letting the Fetcher bridge come up. The verified-artifact verifier
// currently materializes plaintext in memory at roughly 512 MiB per
// concurrent artifact; with MaxIdleConnsPerHost=10 the transient worst
// case lands in multi-GiB territory. 2 GiB is the floor below which
// OOMKill becomes the dominant failure mode.
//
// This is an interim mitigation while the streaming verification path
// (T-002 backlog P1) is in flight. Once verifiers stream, this guard and
// the cgroup probing can be removed.
const bridgeMinMemoryBytes int64 = 2 << 30 // 2 GiB

// gomemlimitHeadroomPct is the fraction of the detected cgroup limit we
// hand to GOMEMLIMIT. 85% leaves the remaining 15% for cgo/mmap/stacks
// and the kernel's own accounting slack, which in practice keeps the
// runtime soft limit well clear of OOMKill at the cgroup ceiling.
const gomemlimitHeadroomPct = 0.85

// gomemlimitHeadroomPctScale converts the headroom fraction (0..1) into a
// percent value for log messages only. Factored out as a named constant so
// the literal 100 does not appear inline (mnd).
const gomemlimitHeadroomPctScale = 100.0

// memoryLimitReader reads the effective memory limit for the current
// process. Returns (bytesOrZero, source, err):
//   - bytes  > 0: explicit limit detected.
//   - bytes == 0, err == nil: cgroup advertised "no limit" (e.g., "max").
//   - err != nil: the probe could not read cgroup files (dev/macOS, bare
//     metal, unsupported kernel) — caller should treat as "unknown".
//
// Kept as a function type so tests can substitute a deterministic reader.
type memoryLimitReader func() (int64, string, error)

// defaultMemoryLimitReader is the production implementation: cgroup v2
// first, cgroup v1 fallback. Any filesystem error is returned so the
// caller can decide the policy ("unknown = do not block").
func defaultMemoryLimitReader() (int64, string, error) {
	return detectMemoryLimit()
}

// EnsureBridgeMemoryBudget enforces a minimum pod memory budget when
// Fetcher is enabled. The artifact verifier currently materializes
// plaintext in memory (~512 MiB peak per concurrent artifact); this
// guard hard-fails at boot if the container appears to have less than
// the minimum safe budget.
//
// Removed once the streaming verification path lands (project memory P1).
func EnsureBridgeMemoryBudget(cfg *Config) error {
	return enforceMemoryBudget(cfg, defaultMemoryLimitReader)
}

// enforceMemoryBudget is the testable core of EnsureBridgeMemoryBudget.
// The reader is injected so unit tests can exercise the below/at/above
// branches without touching /sys/fs/cgroup.
func enforceMemoryBudget(cfg *Config, reader memoryLimitReader) error {
	if cfg == nil || !cfg.Fetcher.Enabled {
		return nil
	}

	if reader == nil {
		return nil
	}

	limit, source, err := reader()
	if err != nil {
		// Non-cgroup systems (dev/macOS) surface an error here. We do
		// not block bootstrap — the guard is meaningless outside the
		// cgroup-enforced environments it is designed to protect.
		// Intentional: reader error == "unknown environment", which is
		// the documented "skip enforcement" case.
		return nil //nolint:nilerr // intentional degraded-mode: no cgroup host => skip guard
	}

	// A zero limit means the cgroup advertised "no limit" (e.g., "max"
	// on v2). The container is allowed to use whatever the host grants;
	// nothing to enforce at boot. If operators want to cap memory on a
	// hostless runtime they can set GOMEMLIMIT themselves.
	if limit <= 0 {
		return nil
	}

	if limit < bridgeMinMemoryBytes {
		return fmt.Errorf(
			"%w: pod memory limit %d bytes (from %s) < minimum %d bytes when FETCHER_ENABLED=true",
			ErrFetcherBridgeNotOperational, limit, source, bridgeMinMemoryBytes,
		)
	}

	return nil
}

// applyGOMEMLIMIT sets the Go runtime soft memory limit to a fraction of
// the detected cgroup limit when Fetcher is enabled and the operator has
// not already provided GOMEMLIMIT explicitly. Returns the limit set (or 0
// if no action was taken) so the caller can log it once.
//
// Respects the operator override: if GOMEMLIMIT is non-empty in the
// environment we leave it alone, even if we disagree with the value. The
// Go runtime parses GOMEMLIMIT on startup; this call is only relevant
// when the runtime defaulted to math.MaxInt64.
func applyGOMEMLIMIT(ctx context.Context, cfg *Config, logger libLog.Logger, reader memoryLimitReader) int64 {
	if cfg == nil || !cfg.Fetcher.Enabled {
		return 0
	}

	if strings.TrimSpace(os.Getenv("GOMEMLIMIT")) != "" {
		// Operator has an explicit opinion. Respect it.
		return 0
	}

	if reader == nil {
		return 0
	}

	limit, source, err := reader()
	if err != nil || limit <= 0 {
		return 0
	}

	soft := int64(gomemlimitHeadroomPct * float64(limit))
	if soft <= 0 {
		return 0
	}

	previous := debug.SetMemoryLimit(soft)

	if logger != nil {
		logger.Log(ctx, libLog.LevelInfo,
			fmt.Sprintf(
				"GOMEMLIMIT set to %d bytes (%.0f%% of %d from %s); previous soft limit %d",
				soft, gomemlimitHeadroomPctScale*gomemlimitHeadroomPct, limit, source, previous,
			),
		)
	}

	return soft
}

// detectMemoryLimit probes the standard cgroup memory control files to
// discover the effective container memory limit. Tries cgroup v2 first
// (the unified hierarchy used by modern runtimes) and falls back to
// cgroup v1. Returns (bytesOrZero, source, err):
//
//   - bytes > 0: explicit numeric limit.
//   - bytes == 0 + err == nil: cgroup advertised "max" (no limit set).
//   - err != nil: no readable cgroup file found — we cannot decide and
//     the caller should skip enforcement.
//
// This is best-effort. Newer runtimes with nested cgroup namespaces, or
// rootless containers that rewrite these paths, may produce misleading
// results. For those cases operators can set GOMEMLIMIT explicitly.
func detectMemoryLimit() (int64, string, error) {
	// cgroup v2 unified hierarchy. "max" means no explicit limit.
	const cgroupV2Path = "/sys/fs/cgroup/memory.max"

	if raw, err := os.ReadFile(cgroupV2Path); err == nil {
		return parseCgroupMemoryLimit(raw, cgroupV2Path)
	}

	// cgroup v1 legacy path.
	const cgroupV1Path = "/sys/fs/cgroup/memory/memory.limit_in_bytes"

	if raw, err := os.ReadFile(cgroupV1Path); err == nil {
		return parseCgroupMemoryLimit(raw, cgroupV1Path)
	}

	return 0, "", errCgroupMemoryUnavailable
}

// errCgroupMemoryUnavailable is returned when neither cgroup v2 nor v1
// memory control files are readable. Expected on dev/macOS hosts.
var errCgroupMemoryUnavailable = errors.New("cgroup memory controller not available")

// parseCgroupMemoryLimit decodes a cgroup memory limit value. cgroup v2
// uses the literal string "max" to indicate no limit; cgroup v1 uses a
// very large sentinel (commonly 9223372036854771712) that we treat as
// "no meaningful limit" by mapping anything above the cgroup v1 unlimited
// threshold back to zero.
func parseCgroupMemoryLimit(raw []byte, source string) (int64, string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "max" {
		return 0, source, nil
	}

	limit, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, source, fmt.Errorf("parse %s: %w", source, err)
	}

	// cgroup v1 unlimited sentinel: values at or above this threshold
	// mean "no effective limit" (kernel uses a large bitmask). We treat
	// them as zero so callers do not accidentally interpret them as
	// multi-exabyte budgets.
	const cgroupV1UnlimitedThreshold = int64(1) << 62
	if limit >= cgroupV1UnlimitedThreshold {
		return 0, source, nil
	}

	return limit, source, nil
}

// initCustodyRetentionWorker constructs the T-006 custody retention sweep
// worker when all preconditions are satisfied. Returns (nil, nil) only
// when Fetcher is disabled (cfg.Fetcher.Enabled=false). When Fetcher is
// enabled but any upstream dependency (bundle/custody, extraction repo,
// tenant lister, provider) is nil, this function returns
// ErrFetcherBridgeNotOperational so operators see the integration bug at
// startup instead of silently skipping retention.
//
// T-003 P4/P5 hardening: Fetcher-enabled deployments MUST fail loudly
// when wiring is incomplete. Orphan custody objects would otherwise
// accumulate indefinitely without the operator noticing.
func initCustodyRetentionWorker(
	ctx context.Context,
	cfg *Config,
	extractionRepo *discoveryExtractionRepo.Repository,
	tenantLister sharedPorts.TenantLister,
	provider sharedPorts.InfrastructureProvider,
	bundle *FetcherBridgeAdapters,
	logger libLog.Logger,
) (*discoveryWorker.CustodyRetentionWorker, error) {
	if cfg == nil || !cfg.Fetcher.Enabled {
		return nil, nil
	}

	if bundle == nil || bundle.ArtifactCustody == nil {
		return nil, fmt.Errorf("%w: artifact custody store is not wired", ErrFetcherBridgeNotOperational)
	}

	if extractionRepo == nil {
		return nil, fmt.Errorf("%w: extraction repository is nil", ErrFetcherBridgeNotOperational)
	}

	if tenantLister == nil {
		return nil, fmt.Errorf("%w: tenant lister is nil", ErrFetcherBridgeNotOperational)
	}

	if provider == nil {
		return nil, fmt.Errorf("%w: infrastructure provider is nil", ErrFetcherBridgeNotOperational)
	}

	// The custody store wired into bundle.ArtifactCustody already satisfies
	// CustodyKeyBuilder (compile-time checked in the custody adapter). Pass
	// it via the dedicated port parameter so the worker never imports the
	// custody adapter package directly — see the worker-no-adapters
	// depguard rule.
	keyBuilder, ok := bundle.ArtifactCustody.(sharedPorts.CustodyKeyBuilder)
	if !ok {
		return nil, fmt.Errorf(
			"%w: artifact custody store does not implement CustodyKeyBuilder",
			ErrFetcherBridgeNotOperational,
		)
	}

	worker, err := discoveryWorker.NewCustodyRetentionWorker(
		extractionRepo,
		bundle.ArtifactCustody,
		keyBuilder,
		tenantLister,
		provider,
		discoveryWorker.CustodyRetentionWorkerConfig{
			Interval:    cfg.FetcherCustodyRetentionSweepInterval(),
			GracePeriod: cfg.FetcherCustodyRetentionGracePeriod(),
			BatchSize:   discoveryWorker.CustodyRetentionDefaultBatchSize,
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("create custody retention worker: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo,
		fmt.Sprintf("custody retention worker wired (interval=%s grace=%s)",
			cfg.FetcherCustodyRetentionSweepInterval(),
			cfg.FetcherCustodyRetentionGracePeriod()))

	return worker, nil
}
