// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
	discoveryExtractionRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	discoveryRedis "github.com/LerianStudio/matcher/internal/discovery/adapters/redis"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	discoveryWorker "github.com/LerianStudio/matcher/internal/discovery/services/worker"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	"github.com/LerianStudio/matcher/internal/shared/adapters/custody"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// errFetcherBridgeMissingLogger indicates the bridge was wired without a
// logger dependency. It is a package-private sentinel because the function
// is only called from bootstrap where logger nil-ness reflects a bootstrap
// wiring bug, not a runtime condition callers should distinguish.
var errFetcherBridgeMissingLogger = errors.New(
	"fetcher bridge requires a logger",
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

// ErrFetcherBridgeNotOperational indicates the bridge was requested
// (FETCHER_ENABLED=true and bridge worker slot registered) but one of
// the preconditions for live operation failed: verified-artifact
// orchestrator is nil (APP_ENC_KEY misconfigured), or object storage
// is not reachable.
//
// T-003 P4 hardening: soft-disable at T-002 preserved the non-bridge
// path, but T-003 bridge worker MUST refuse to start without crypto
// and object storage. This sentinel lets bootstrap return a clear
// error instead of logging a warning and silently continuing.
var ErrFetcherBridgeNotOperational = errors.New(
	"fetcher bridge cannot start: verified-artifact pipeline unavailable",
)

// artifactRetrievalTimeoutPadSec is the additional allowance we apply on
// top of the extraction request timeout when sizing the artifact
// download HTTP client. Downloading a completed artifact is I/O bound;
// allow enough headroom for a slow S3 round-trip on top of whatever the
// operator has configured for extraction polling.
const artifactRetrievalTimeoutPadSec = 60

// FetcherBridgeAdapters bundles the adapters that form the
// Fetcher-to-ingestion trusted bridge. They live behind shared-kernel ports
// so discovery-side callers (in a later task) can depend on them without
// importing the ingestion or discovery adapter implementations directly.
//
// The T-001 set of two (Intake + LinkWriter) is extended by T-002 with
// three more that together form the verified-artifact pipeline:
// Retrieval → Verification → Custody, composed by a single orchestrator.
type FetcherBridgeAdapters struct {
	// T-001 intake path.
	Intake    sharedPorts.FetcherBridgeIntake
	LinkWrite sharedPorts.ExtractionLifecycleLinkWriter

	// T-002 verified-artifact pipeline.
	// The retrieval gateway and trust verifier are held internally by the
	// orchestrator; only custody is surfaced here because downstream worker
	// wiring needs it independently.
	ArtifactCustody sharedPorts.ArtifactCustodyStore

	// VerifiedArtifactOrchestrator is the single entry point the bridge
	// worker (T-003) will drive. nil when the verified-artifact pipeline
	// is disabled (APP_ENC_KEY empty or custody storage unavailable).
	VerifiedArtifactOrchestrator *discoveryCommand.VerifiedArtifactRetrievalOrchestrator
}

// FetcherBridgeDeps carries the bootstrap dependencies required to wire
// the bridge. Passing them as a struct keeps the init signature stable as
// new adapters are added.
type FetcherBridgeDeps struct {
	// Config is the loaded application configuration. APP_ENC_KEY and the
	// fetcher request timeout are read from it.
	Config *Config
	// IngestionUseCase is the ingestion command use case. Required for the
	// T-001 trusted stream intake adapter.
	IngestionUseCase *ingestionCommand.UseCase
	// ExtractionRepo is the discovery extraction repository. Required for
	// the T-001 lifecycle link writer.
	ExtractionRepo *discoveryExtractionRepo.Repository
	// ObjectStorage is the shared object storage client used to persist
	// custody copies. When nil, the verified-artifact pipeline is
	// disabled (artifacts cannot be stored anywhere).
	ObjectStorage sharedPorts.ObjectStorageClient
	// Logger is used for bootstrap warnings. Required.
	Logger libLog.Logger
}

// initFetcherBridgeAdapters constructs the adapters that form the
// Fetcher trusted-stream bridge. T-001 proves intake + lifecycle link
// are reachable; T-002 extends the bundle with the verified-artifact
// pipeline (retrieval + verify + custody + orchestrator). The T-003
// worker task will consume these adapters from discovery.
//
// The function returns nil (and logs a warning) when any T-001
// prerequisite is missing so bootstrap stays tolerant of
// fetcher-disabled deployments. T-002 adapters are optional: when
// APP_ENC_KEY is empty or object storage is not configured, they are
// left nil and the orchestrator is skipped. The caller decides whether
// to treat that as fatal or as a feature flag.
func initFetcherBridgeAdapters(
	ctx context.Context,
	deps FetcherBridgeDeps,
) (*FetcherBridgeAdapters, error) {
	logger := deps.Logger
	if logger == nil {
		return nil, fmt.Errorf(
			"init fetcher bridge adapters: %w",
			errFetcherBridgeMissingLogger,
		)
	}

	if deps.IngestionUseCase == nil {
		logger.Log(
			ctx,
			libLog.LevelWarn,
			"fetcher bridge not wired: ingestion command use case unavailable",
		)

		return nil, nil
	}

	if deps.ExtractionRepo == nil {
		logger.Log(
			ctx,
			libLog.LevelWarn,
			"fetcher bridge not wired: discovery extraction repository unavailable",
		)

		return nil, nil
	}

	intake, err := crossAdapters.NewFetcherBridgeIntakeAdapter(deps.IngestionUseCase)
	if err != nil {
		return nil, fmt.Errorf("create fetcher bridge intake adapter: %w", err)
	}

	linkWriter, err := crossAdapters.NewExtractionLifecycleLinkWriterAdapter(deps.ExtractionRepo)
	if err != nil {
		return nil, fmt.Errorf("create extraction lifecycle link writer adapter: %w", err)
	}

	bundle := &FetcherBridgeAdapters{
		Intake:    intake,
		LinkWrite: linkWriter,
	}

	if err := wireVerifiedArtifactPipeline(ctx, bundle, deps); err != nil {
		return nil, err
	}

	logger.Log(
		ctx,
		libLog.LevelInfo,
		describeBridgeWiring(bundle),
	)

	return bundle, nil
}

// wireVerifiedArtifactPipeline attaches the T-002 adapters to the bundle
// when all prerequisites are satisfied. A missing prerequisite produces
// a warning log and leaves the T-002 fields nil; callers that depend on
// verified-artifact retrieval must check bundle.VerifiedArtifactOrchestrator.
//
// Prerequisites:
//   - APP_ENC_KEY set and base64-decodable to at least 32 bytes.
//   - ObjectStorage configured (nil means we have nowhere to write the
//     custody copy).
func wireVerifiedArtifactPipeline(
	ctx context.Context,
	bundle *FetcherBridgeAdapters,
	deps FetcherBridgeDeps,
) error {
	if deps.Config == nil {
		deps.Logger.Log(
			ctx,
			libLog.LevelWarn,
			"fetcher bridge verified-artifact pipeline not wired: nil config",
		)

		return nil
	}

	masterKey, keyErr := decodeMasterKey(deps.Config.Fetcher.AppEncKey)
	if keyErr != nil {
		// Invalid key is a hard error: we cannot verify anything. Empty
		// is a soft disable handled in the empty branch below.
		if errors.Is(keyErr, ErrFetcherBridgeMasterKeyRequired) {
			deps.Logger.Log(
				ctx,
				libLog.LevelWarn,
				"fetcher bridge verified-artifact pipeline disabled: APP_ENC_KEY is empty",
			)

			return nil
		}

		return fmt.Errorf("fetcher bridge: %w", keyErr)
	}

	if deps.ObjectStorage == nil || !objectStorageAvailable(ctx, deps.ObjectStorage) {
		deps.Logger.Log(
			ctx,
			libLog.LevelWarn,
			"fetcher bridge verified-artifact pipeline disabled: object storage unavailable",
		)

		return nil
	}

	verifier, err := fetcher.NewArtifactVerifier(masterKey)
	if err != nil {
		return fmt.Errorf("fetcher bridge: create artifact verifier: %w", err)
	}

	retrievalClient, err := fetcher.NewArtifactRetrievalClient(
		newArtifactHTTPClient(deps.Config),
	)
	if err != nil {
		return fmt.Errorf("fetcher bridge: create artifact retrieval client: %w", err)
	}

	custodyStore, err := custody.NewArtifactCustodyStore(deps.ObjectStorage)
	if err != nil {
		return fmt.Errorf("fetcher bridge: create artifact custody store: %w", err)
	}

	orchestrator, err := discoveryCommand.NewVerifiedArtifactRetrievalOrchestrator(
		retrievalClient,
		verifier,
		custodyStore,
	)
	if err != nil {
		return fmt.Errorf("fetcher bridge: create verified artifact orchestrator: %w", err)
	}

	bundle.ArtifactCustody = custodyStore
	bundle.VerifiedArtifactOrchestrator = orchestrator

	return nil
}

// decodeMasterKey parses the base64-encoded master key and validates
// that it meets the Fetcher contract length (at least 32 bytes). Empty
// input returns ErrFetcherBridgeMasterKeyRequired so the caller can
// distinguish "disabled" from "misconfigured".
func decodeMasterKey(raw string) ([]byte, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, ErrFetcherBridgeMasterKeyRequired
	}

	decoded, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		// Also try URL-safe encoding so operators are not punished for
		// sharing a key through a URL-compatible channel.
		decoded, err = base64.URLEncoding.DecodeString(trimmed)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: not base64 (std or url)",
				ErrFetcherBridgeMasterKeyInvalid,
			)
		}
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

// newArtifactHTTPClient builds an http.Client suited for artifact
// downloads. We explicitly disable redirect following so any Fetcher
// misconfiguration that emits a 3xx surfaces as a retrieval failure
// rather than silently following a redirect to an attacker-controlled
// host.
//
// T-003 P2 hardening: the transport reuses the SSRF-guarded DialContext
// from the shared fetcher HTTP client config so artifact downloads
// cannot bypass the private-IP guard. Without this, Fetcher could
// redirect matcher into pulling from 169.254.169.254/latest/meta-data/
// or any internal service. We also bump MaxIdleConnsPerHost so bursty
// concurrent bridge work doesn't starve the connection pool.
func newArtifactHTTPClient(cfg *Config) *http.Client {
	timeout := time.Duration(cfg.Fetcher.RequestTimeoutSec+artifactRetrievalTimeoutPadSec) * time.Second

	// Reuse the SSRF-guarded transport from the fetcher HTTP client so
	// artifact downloads inherit the same private-IP protection.
	clientCfg := fetcher.DefaultConfig()
	clientCfg.BaseURL = cfg.Fetcher.URL
	clientCfg.AllowPrivateIPs = cfg.Fetcher.AllowPrivateIPs
	clientCfg.RequestTimeout = timeout

	transport := fetcher.BuildArtifactTransport(clientCfg)

	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// objectStorageAvailable runs a trial Exists call against a sentinel key to
// verify that the dynamic object storage wrapper has a usable delegate at
// runtime. The dynamicObjectStorageClient always returns a non-nil pointer,
// so the bare `deps.ObjectStorage == nil` check never fires. This probe
// surfaces the "configured but unreachable" state at bootstrap time
// instead of deferring the discovery to the first real custody write.
//
// A timeout bounds the probe so a transient storage outage at startup does
// not block the whole service from coming up.
//
// T-003 P5 hardening.
func objectStorageAvailable(ctx context.Context, client sharedPorts.ObjectStorageClient) bool {
	if client == nil {
		return false
	}

	probeCtx, cancel := context.WithTimeout(ctx, objectStorageProbeTimeout)
	defer cancel()

	// A non-existent sentinel key is a cheap probe: storage backends
	// respond with a quick "does not exist" and do NOT fail unless
	// credentials/connectivity are broken.
	_, err := client.Exists(probeCtx, objectStorageProbeKey)

	return err == nil
}

const (
	objectStorageProbeKey     = "matcher/bootstrap/probe.keep"
	objectStorageProbeTimeout = 5 * time.Second
)

// EnsureBridgeOperational asserts that all prerequisites for running the
// bridge worker are satisfied. Called from bootstrap when FETCHER_ENABLED is
// true and the bridge worker slot is going to be registered. Returns
// ErrFetcherBridgeNotOperational wrapped when any precondition is missing so
// the operator sees a specific, actionable error instead of a generic
// bootstrap failure.
//
// T-003 P4 hardening.
func EnsureBridgeOperational(bundle *FetcherBridgeAdapters) error {
	if bundle == nil {
		return fmt.Errorf("%w: bridge adapter bundle is nil", ErrFetcherBridgeNotOperational)
	}

	if bundle.Intake == nil {
		return fmt.Errorf("%w: intake adapter is not wired", ErrFetcherBridgeNotOperational)
	}

	if bundle.LinkWrite == nil {
		return fmt.Errorf("%w: lifecycle link writer is not wired", ErrFetcherBridgeNotOperational)
	}

	if bundle.VerifiedArtifactOrchestrator == nil {
		return fmt.Errorf(
			"%w: verified-artifact orchestrator is nil (APP_ENC_KEY unset or invalid, or object storage unavailable)",
			ErrFetcherBridgeNotOperational,
		)
	}

	if bundle.ArtifactCustody == nil {
		return fmt.Errorf("%w: artifact custody store is not wired", ErrFetcherBridgeNotOperational)
	}

	return nil
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
		return nil
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
func applyGOMEMLIMIT(cfg *Config, logger libLog.Logger, reader memoryLimitReader) int64 {
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
		logger.Log(context.Background(), libLog.LevelInfo,
			fmt.Sprintf(
				"GOMEMLIMIT set to %d bytes (%.0f%% of %d from %s); previous soft limit %d",
				soft, gomemlimitHeadroomPct*100, limit, source, previous,
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

// initFetcherBridgeWorker constructs the T-003 bridge worker when all
// preconditions are satisfied. Returns (nil, nil) only when Fetcher is
// disabled (cfg.Fetcher.Enabled=false). When Fetcher is enabled but any
// upstream dependency (bundle, provider, extraction repo) is nil, this
// function returns ErrFetcherBridgeNotOperational so operators see the
// integration bug at startup instead of silently running without a bridge.
//
// T-003 P4/P5 hardening: Fetcher-enabled deployments MUST fail loudly
// when wiring is incomplete. The caller propagates the error as a
// bootstrap failure.
func initFetcherBridgeWorker(
	ctx context.Context,
	cfg *Config,
	configGetter func() *Config,
	provider sharedPorts.InfrastructureProvider,
	extractionRepo *discoveryExtractionRepo.Repository,
	tenantLister sharedPorts.TenantLister,
	bundle *FetcherBridgeAdapters,
	logger libLog.Logger,
) (*discoveryWorker.BridgeWorker, error) {
	if cfg == nil || !cfg.Fetcher.Enabled {
		return nil, nil
	}

	if bundle == nil {
		return nil, fmt.Errorf("%w: bridge adapter bundle is nil", ErrFetcherBridgeNotOperational)
	}

	if err := EnsureBridgeOperational(bundle); err != nil {
		// Fetcher is enabled but bundle is incomplete — this is a hard
		// failure so operators see the misconfiguration at startup.
		return nil, err
	}

	if provider == nil {
		return nil, fmt.Errorf("%w: infrastructure provider is nil", ErrFetcherBridgeNotOperational)
	}

	if extractionRepo == nil {
		return nil, fmt.Errorf("%w: extraction repository is nil", ErrFetcherBridgeNotOperational)
	}

	sourceResolver, err := crossAdapters.NewBridgeSourceResolverAdapter(provider)
	if err != nil {
		return nil, fmt.Errorf("create bridge source resolver: %w", err)
	}

	orchestratorCfg := discoveryCommand.BridgeOrchestratorConfig{
		FetcherBaseURLGetter: func() string {
			if configGetter == nil {
				return cfg.Fetcher.URL
			}

			if currentCfg := configGetter(); currentCfg != nil {
				return currentCfg.Fetcher.URL
			}

			return cfg.Fetcher.URL
		},
		MaxExtractionBytes: cfg.FetcherMaxExtractionBytes(),
		Flatten:            fetcher.FlattenFetcherJSON,
	}

	orchestrator, err := discoveryCommand.NewBridgeExtractionOrchestrator(
		extractionRepo,
		bundle.VerifiedArtifactOrchestrator,
		bundle.ArtifactCustody,
		bundle.Intake,
		bundle.LinkWrite,
		sourceResolver,
		orchestratorCfg,
	)
	if err != nil {
		return nil, fmt.Errorf("create bridge orchestrator: %w", err)
	}

	worker, err := discoveryWorker.NewBridgeWorker(
		orchestrator,
		extractionRepo,
		tenantLister,
		provider,
		discoveryWorker.BridgeWorkerConfig{
			Interval:  cfg.FetcherBridgeInterval(),
			BatchSize: cfg.FetcherBridgeBatchSize(),
			Retry: discoveryWorker.BridgeRetryBackoff{
				MaxAttempts: cfg.FetcherBridgeRetryMaxAttempts(),
			},
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("create bridge worker: %w", err)
	}

	wireBridgeHeartbeatWriter(ctx, provider, worker, logger)

	logger.Log(ctx, libLog.LevelInfo,
		fmt.Sprintf("fetcher bridge worker wired (interval=%s batch=%d)",
			cfg.FetcherBridgeInterval(), cfg.FetcherBridgeBatchSize()))

	return worker, nil
}

// wireBridgeHeartbeatWriter resolves the Redis client from the shared
// infrastructure provider and plumbs a BridgeHeartbeatWriter into the
// bridge worker. Non-fatal on failure — the bridge must still run when
// Redis is momentarily unavailable at boot; it will simply not emit
// heartbeats until the operator addresses the underlying issue. C15.
func wireBridgeHeartbeatWriter(
	ctx context.Context,
	provider sharedPorts.InfrastructureProvider,
	worker *discoveryWorker.BridgeWorker,
	logger libLog.Logger,
) {
	if worker == nil || provider == nil {
		return
	}

	writer, err := resolveBridgeHeartbeat(ctx, provider)
	if err != nil {
		logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("bridge heartbeat writer not wired: %v", err))

		return
	}

	worker.WithHeartbeatWriter(writer)

	logger.Log(ctx, libLog.LevelInfo, "bridge heartbeat writer wired")
}

// resolveBridgeHeartbeat constructs the Redis-backed heartbeat adapter.
// Exported at package level so the query-side wiring (init_discovery.go)
// can reuse the exact same construction site and key contract.
//
// Returns the concrete *discoveryRedis.BridgeHeartbeat which satisfies
// both BridgeHeartbeatWriter and BridgeHeartbeatReader — callers pick
// whichever port their dependency needs. Keeping one construction site
// guarantees the two sides agree on the Redis key and TTL format.
func resolveBridgeHeartbeat(
	ctx context.Context,
	provider sharedPorts.InfrastructureProvider,
) (*discoveryRedis.BridgeHeartbeat, error) {
	lease, leaseErr := provider.GetRedisConnection(ctx)
	if leaseErr != nil {
		return nil, fmt.Errorf("get redis connection: %w", leaseErr)
	}

	if lease == nil {
		return nil, errors.New("redis connection lease is nil")
	}

	defer lease.Release()

	client, clientErr := lease.GetClient(ctx)
	if clientErr != nil {
		return nil, fmt.Errorf("get redis client: %w", clientErr)
	}

	hb, hbErr := discoveryRedis.NewBridgeHeartbeat(client)
	if hbErr != nil {
		return nil, fmt.Errorf("construct bridge heartbeat adapter: %w", hbErr)
	}

	return hb, nil
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

// describeBridgeWiring produces a single log line summarising which
// adapters were wired. Kept compact: operators reading bootstrap logs
// should see at a glance whether the verified-artifact pipeline is
// active.
//
// A nil bundle is treated as "not wired" rather than panicking — callers
// guard against this today, but a defensive guard here keeps bootstrap
// logs readable if a future code path forgets the caller-side check.
func describeBridgeWiring(bundle *FetcherBridgeAdapters) string {
	if bundle == nil {
		return "fetcher bridge adapters not wired (bundle is nil)"
	}

	components := []string{"intake", "lifecycle link writer"}

	if bundle.VerifiedArtifactOrchestrator != nil {
		components = append(components, "verified-artifact pipeline")
	}

	return "fetcher bridge adapters wired (" + strings.Join(components, " + ") + ")"
}
