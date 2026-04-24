// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Fetcher bridge adapter bundle + operational preconditions. Split from
// init_fetcher_bridge.go so the bundle types, their constructor, and the
// operational assertion that the rest of bootstrap uses to gate the bridge
// worker live in one place. The bundle wiring is pure composition of
// discovery + shared-kernel ports; the heavy lifting (HTTP transport,
// crypto, retention) lives in the sibling init_fetcher_*.go files.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
	discoveryExtractionRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	"github.com/LerianStudio/matcher/internal/shared/adapters/custody"
	"github.com/LerianStudio/matcher/internal/shared/objectstorage"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// errFetcherBridgeMissingLogger indicates the bridge was wired without a
// logger dependency. It is a package-private sentinel because the function
// is only called from bootstrap where logger nil-ness reflects a bootstrap
// wiring bug, not a runtime condition callers should distinguish.
var errFetcherBridgeMissingLogger = errors.New(
	"fetcher bridge requires a logger",
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
	ObjectStorage objectstorage.Backend
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

	linkWriter, err := crossAdapters.NewExtractionLifecycleLinkWriterAdapter(deps.ExtractionRepo)
	if err != nil {
		return nil, fmt.Errorf("create extraction lifecycle link writer adapter: %w", err)
	}

	// T-004 (K-06f): deps.IngestionUseCase satisfies sharedPorts.FetcherBridge
	// Intake directly via its IngestTrustedContent method. The former
	// FetcherBridgeIntakeAdapter wrapped the UseCase solely to rename
	// methods + project a shape-identical outcome struct; both concerns now
	// live on the UseCase itself.
	bundle := &FetcherBridgeAdapters{
		Intake:    deps.IngestionUseCase,
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
