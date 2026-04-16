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
	"strings"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
	discoveryExtractionRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
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
	ArtifactRetrieval sharedPorts.ArtifactRetrievalGateway
	ArtifactVerifier  sharedPorts.ArtifactTrustVerifier
	ArtifactCustody   sharedPorts.ArtifactCustodyStore

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

	bundle.ArtifactRetrieval = retrievalClient
	bundle.ArtifactVerifier = verifier
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

	return nil
}

// initFetcherBridgeWorker constructs the T-003 bridge worker when all
// preconditions are satisfied. Returns (nil, nil) when the bridge should
// not run (Fetcher disabled, bundle incomplete, no source resolver). The
// soft-disabled branch logs a warning so operators can see why the bridge
// is idle.
//
// T-003 P4/P5 hardening: when Fetcher is explicitly enabled but the
// verified-artifact orchestrator is nil, this function returns an error
// to refuse starting the bridge worker without crypto. The caller
// propagates that as a bootstrap failure.
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
		logger.Log(ctx, libLog.LevelWarn,
			"fetcher bridge worker not wired: bridge adapter bundle is nil")

		return nil, nil
	}

	if err := EnsureBridgeOperational(bundle); err != nil {
		// Fetcher is enabled but bundle is incomplete — this is a hard
		// failure so operators see the misconfiguration at startup.
		return nil, err
	}

	if provider == nil {
		logger.Log(ctx, libLog.LevelWarn,
			"fetcher bridge worker not wired: infrastructure provider unavailable")

		return nil, nil
	}

	if extractionRepo == nil {
		logger.Log(ctx, libLog.LevelWarn,
			"fetcher bridge worker not wired: extraction repository unavailable")

		return nil, nil
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
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("create bridge worker: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo,
		fmt.Sprintf("fetcher bridge worker wired (interval=%s batch=%d)",
			cfg.FetcherBridgeInterval(), cfg.FetcherBridgeBatchSize()))

	return worker, nil
}

// describeBridgeWiring produces a single log line summarising which
// adapters were wired. Kept compact: operators reading bootstrap logs
// should see at a glance whether the verified-artifact pipeline is
// active.
func describeBridgeWiring(bundle *FetcherBridgeAdapters) string {
	components := []string{"intake", "lifecycle link writer"}

	if bundle.VerifiedArtifactOrchestrator != nil {
		components = append(components, "verified-artifact pipeline")
	}

	return "fetcher bridge adapters wired (" + strings.Join(components, " + ") + ")"
}
