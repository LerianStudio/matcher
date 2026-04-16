// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// BridgeContentFlattener transforms the raw Fetcher-shape extraction
// output into the flat JSON array shape Matcher's generic ingestion
// parser expects. Lives as a port so the bridge orchestrator (service
// layer) does not import the fetcher adapter directly — see depguard rule
// `service-no-adapters`.
type BridgeContentFlattener func(in io.Reader, maxBytes int64) (io.Reader, error)

// defaultMaxExtractionBytes is the fallback ceiling when callers pass
// MaxExtractionBytes <= 0 to the orchestrator config. Mirrors
// fetcher.DefaultMaxExtractionBytes so the bridge orchestrator can use
// the same DoS guard without a direct adapter import.
const defaultMaxExtractionBytes int64 = 2 << 30

// Sentinel errors for bridge-orchestrator construction and input validation.
var (
	ErrNilBridgeExtractionRepo           = errors.New("bridge orchestrator requires extraction repository")
	ErrNilBridgeVerifiedArtifactOrchestr = errors.New("bridge orchestrator requires verified artifact orchestrator")
	ErrNilBridgeCustody                  = errors.New("bridge orchestrator requires custody store")
	ErrNilBridgeIntake                   = errors.New("bridge orchestrator requires intake port")
	ErrNilBridgeLinkWriter               = errors.New("bridge orchestrator requires link writer port")
	ErrNilBridgeSourceResolver           = errors.New("bridge orchestrator requires source resolver")
	ErrNilBridgeFetcherBaseURL           = errors.New("bridge orchestrator requires fetcher base URL")
	ErrNilBridgeContentFlattener         = errors.New("bridge orchestrator requires content flattener")
)

// BridgeOrchestratorConfig bundles the knobs the orchestrator needs to build
// retrieval descriptors. Passed as a struct so new fields do not churn the
// constructor signature.
type BridgeOrchestratorConfig struct {
	// FetcherBaseURLGetter returns the current Fetcher base URL. A function
	// (not a string) so runtime config reloads are reflected without
	// reconstructing the orchestrator. Required.
	FetcherBaseURLGetter func() string

	// MaxExtractionBytes is the per-extraction size cap for
	// FlattenFetcherJSON. Falls back to the package default when <= 0.
	MaxExtractionBytes int64

	// Flatten is the Fetcher-shape→flat-array transform. Injected as a
	// port so the service layer does not import the fetcher adapter
	// directly. When nil, the orchestrator treats it as a wiring bug.
	Flatten BridgeContentFlattener
}

// BridgeExtractionOrchestrator wires together the full bridge pipeline for
// a single extraction. It lives in the discovery command layer because the
// retrieval descriptor construction + FlattenFetcherJSON transform are
// discovery-specific concerns; the adapter ports and source resolver
// abstract the cross-context pieces.
type BridgeExtractionOrchestrator struct {
	extractionRepo   repositories.ExtractionRepository
	verifiedOrchestr *VerifiedArtifactRetrievalOrchestrator
	custody          sharedPorts.ArtifactCustodyStore
	intake           sharedPorts.FetcherBridgeIntake
	linkWriter       sharedPorts.ExtractionLifecycleLinkWriter
	sourceResolver   sharedPorts.BridgeSourceResolver
	cfg              BridgeOrchestratorConfig
}

// Compile-time interface check.
var _ sharedPorts.BridgeOrchestrator = (*BridgeExtractionOrchestrator)(nil)

// NewBridgeExtractionOrchestrator validates every dependency at construction
// time. All deps are required — there is no meaningful degraded mode for the
// bridge pipeline. Returning errors here surfaces wiring bugs at boot time.
func NewBridgeExtractionOrchestrator(
	extractionRepo repositories.ExtractionRepository,
	verifiedOrchestr *VerifiedArtifactRetrievalOrchestrator,
	custody sharedPorts.ArtifactCustodyStore,
	intake sharedPorts.FetcherBridgeIntake,
	linkWriter sharedPorts.ExtractionLifecycleLinkWriter,
	sourceResolver sharedPorts.BridgeSourceResolver,
	cfg BridgeOrchestratorConfig,
) (*BridgeExtractionOrchestrator, error) {
	if extractionRepo == nil {
		return nil, ErrNilBridgeExtractionRepo
	}

	if verifiedOrchestr == nil {
		return nil, ErrNilBridgeVerifiedArtifactOrchestr
	}

	if custody == nil {
		return nil, ErrNilBridgeCustody
	}

	if intake == nil {
		return nil, ErrNilBridgeIntake
	}

	if linkWriter == nil {
		return nil, ErrNilBridgeLinkWriter
	}

	if sourceResolver == nil {
		return nil, ErrNilBridgeSourceResolver
	}

	if cfg.FetcherBaseURLGetter == nil {
		return nil, ErrNilBridgeFetcherBaseURL
	}

	if cfg.MaxExtractionBytes <= 0 {
		cfg.MaxExtractionBytes = defaultMaxExtractionBytes
	}

	if cfg.Flatten == nil {
		return nil, ErrNilBridgeContentFlattener
	}

	return &BridgeExtractionOrchestrator{
		extractionRepo:   extractionRepo,
		verifiedOrchestr: verifiedOrchestr,
		custody:          custody,
		intake:           intake,
		linkWriter:       linkWriter,
		sourceResolver:   sourceResolver,
		cfg:              cfg,
	}, nil
}

// BridgeExtraction runs one extraction through the full pipeline. Each stage
// has its own child span so operators can isolate where a given run failed.
// Any terminal failure leaves the extraction unlinked so the next poll cycle
// (or a future T-005 retry) can attempt it again safely.
func (orch *BridgeExtractionOrchestrator) BridgeExtraction(
	ctx context.Context,
	input sharedPorts.BridgeExtractionInput,
) (*sharedPorts.BridgeExtractionOutcome, error) {
	// Nil-receiver guard MUST run before tracking extraction. NewTracking
	// FromContext is safe to call but returns a nil tracer in degenerate
	// contexts; calling tracer.Start on nil would panic before we reach the
	// nil check. Additionally, we surface the canonical shared sentinel so
	// callers can errors.Is on sharedPorts.ErrNilBridgeOrchestrator without
	// needing to know about the discovery-package-local error identity.
	if orch == nil {
		return nil, sharedPorts.ErrNilBridgeOrchestrator
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.discovery.bridge_extraction")
	defer span.End()

	if input.ExtractionID == uuid.Nil {
		err := sharedPorts.ErrBridgeExtractionIDRequired
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "missing extraction id", err)

		return nil, err
	}

	if input.TenantID == "" {
		err := sharedPorts.ErrBridgeTenantIDRequired
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "missing tenant id", err)

		return nil, err
	}

	extraction, err := orch.loadEligibleExtraction(ctx, input.ExtractionID)
	if err != nil {
		return nil, err
	}

	target, err := orch.sourceResolver.ResolveSourceForConnection(ctx, extraction.ConnectionID)
	if err != nil {
		if errors.Is(err, sharedPorts.ErrBridgeSourceUnresolvable) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "no source wired for connection", err)

			return nil, err
		}

		wrapped := fmt.Errorf("resolve source for connection: %w", err)
		libOpentelemetry.HandleSpanError(span, "source resolver failed", wrapped)

		return nil, wrapped
	}

	custodyRef, err := orch.retrieveAndCustody(ctx, extraction, input.TenantID)
	if err != nil {
		return nil, err
	}

	outcome, err := orch.ingestAndLink(ctx, extraction, target, custodyRef)
	if err != nil {
		// ErrExtractionAlreadyLinked is an idempotent signal: the outcome
		// is still meaningful for audit/trace purposes (it carries the
		// ingestion job id the worker created just before the link
		// conflict). Propagate both so callers see the error AND the data.
		if errors.Is(err, sharedPorts.ErrExtractionAlreadyLinked) && outcome != nil {
			return outcome, err
		}

		return nil, err
	}

	orch.cleanupCustody(ctx, logger, custodyRef, outcome)

	return outcome, nil
}

// loadEligibleExtraction loads the extraction and re-validates its eligibility
// inside the orchestrator transaction scope. This guards against the worker
// having seen a stale snapshot: if the extraction was linked or re-marked
// by another process between discovery and here, we surface
// ErrBridgeExtractionIneligible and bail without side effects.
func (orch *BridgeExtractionOrchestrator) loadEligibleExtraction(
	ctx context.Context,
	id uuid.UUID,
) (*entities.ExtractionRequest, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "orchestrator.load_eligible_extraction")
	defer span.End()

	extraction, err := orch.extractionRepo.FindByID(ctx, id)
	if err != nil {
		if errors.Is(err, repositories.ErrExtractionNotFound) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "extraction not found", err)

			return nil, err
		}

		wrapped := fmt.Errorf("load extraction: %w", err)
		libOpentelemetry.HandleSpanError(span, "load extraction failed", wrapped)

		return nil, wrapped
	}

	if extraction == nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(
			span,
			"extraction not found",
			repositories.ErrExtractionNotFound,
		)

		return nil, repositories.ErrExtractionNotFound
	}

	if extraction.Status != vo.ExtractionStatusComplete {
		libOpentelemetry.HandleSpanBusinessErrorEvent(
			span,
			"extraction no longer complete",
			sharedPorts.ErrBridgeExtractionIneligible,
		)

		return nil, sharedPorts.ErrBridgeExtractionIneligible
	}

	if extraction.IngestionJobID != uuid.Nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(
			span,
			"extraction already linked",
			sharedPorts.ErrBridgeExtractionIneligible,
		)

		return nil, sharedPorts.ErrBridgeExtractionIneligible
	}

	return extraction, nil
}

// retrieveAndCustody runs the verified-artifact orchestrator for the given
// extraction and returns the resulting custody reference. Transient
// retrieval/custody failures are passed through unwrapped so the worker can
// classify them.
func (orch *BridgeExtractionOrchestrator) retrieveAndCustody(
	ctx context.Context,
	extraction *entities.ExtractionRequest,
	tenantID string,
) (*sharedPorts.ArtifactCustodyReference, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "orchestrator.retrieve_and_custody")
	defer span.End()

	baseURL := orch.cfg.FetcherBaseURLGetter()
	if baseURL == "" {
		err := fmt.Errorf("%w: fetcher base url is empty", sharedPorts.ErrArtifactRetrievalFailed)
		libOpentelemetry.HandleSpanError(span, "fetcher base url missing", err)

		return nil, err
	}

	url := baseURL + extraction.ResultPath

	result, err := orch.verifiedOrchestr.RetrieveAndCustodyVerifiedArtifact(
		ctx,
		VerifiedArtifactRetrievalInput{
			Descriptor: sharedPorts.ArtifactRetrievalDescriptor{
				ExtractionID: extraction.ID,
				TenantID:     tenantID,
				URL:          url,
			},
		},
	)
	if err != nil {
		// Orchestrator already annotated the span and wrapped the error.
		return nil, err
	}

	if result == nil || result.Custody == nil {
		wrapped := fmt.Errorf("%w: verified artifact orchestrator returned nil custody", sharedPorts.ErrCustodyStoreFailed)
		libOpentelemetry.HandleSpanError(span, "nil custody reference", wrapped)

		return nil, wrapped
	}

	return result.Custody, nil
}

// ingestAndLink opens the custody copy, flattens it into the ingestion-shape
// JSON array, hands it to the ingestion intake, and writes the extraction→
// ingestion link atomically. The custody reader is closed before the link
// write so any Close failure doesn't mask link-write errors.
func (orch *BridgeExtractionOrchestrator) ingestAndLink(
	ctx context.Context,
	extraction *entities.ExtractionRequest,
	target sharedPorts.BridgeSourceTarget,
	ref *sharedPorts.ArtifactCustodyReference,
) (*sharedPorts.BridgeExtractionOutcome, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "orchestrator.ingest_and_link")
	defer span.End()

	custodyReader, err := orch.custody.Open(ctx, *ref)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "custody open failed", err)

		return nil, err
	}

	defer func() {
		if closeErr := custodyReader.Close(); closeErr != nil && logger != nil {
			logger.With(libLog.Any("error", closeErr.Error())).
				Log(ctx, libLog.LevelWarn, "bridge orchestrator: custody reader close failed")
		}
	}()

	flatReader, err := orch.cfg.Flatten(custodyReader, orch.cfg.MaxExtractionBytes)
	if err != nil {
		wrapped := fmt.Errorf("flatten fetcher json: %w", err)
		libOpentelemetry.HandleSpanError(span, "flatten failed", wrapped)

		return nil, wrapped
	}

	outcome, err := orch.intake.IngestTrustedContent(ctx, sharedPorts.TrustedContentInput{
		ContextID: target.ContextID,
		SourceID:  target.SourceID,
		Format:    target.Format,
		Content:   flatReader,
		SourceMetadata: map[string]string{
			"extraction_id":  extraction.ID.String(),
			"fetcher_job_id": extraction.FetcherJobID,
			"fetcher_result": extraction.ResultPath,
			"custody_key":    ref.Key,
			"custody_sha256": ref.SHA256,
			"filename":       "fetcher-extraction-" + extraction.ID.String() + ".json",
		},
	})
	if err != nil {
		wrapped := fmt.Errorf("ingest trusted content: %w", err)
		libOpentelemetry.HandleSpanError(span, "ingestion failed", wrapped)

		return nil, wrapped
	}

	if outcome.IngestionJobID == uuid.Nil {
		wrapped := fmt.Errorf("%w: ingestion returned nil job id", sharedPorts.ErrArtifactRetrievalFailed)
		libOpentelemetry.HandleSpanError(span, "nil ingestion job id", wrapped)

		return nil, wrapped
	}

	if err := orch.linkWriter.LinkExtractionToIngestion(
		ctx,
		extraction.ID,
		outcome.IngestionJobID,
	); err != nil {
		// ErrExtractionAlreadyLinked is idempotent: another worker beat us
		// to the link write. Treat the outcome as successful because the
		// extraction IS linked — to the other worker's ingestion job, not
		// ours. The "duplicate ingestion job" is an acknowledged cost of
		// the no-distributed-transactions design. T-004 will surface both
		// jobs in the lifecycle view; T-005 may add a second-writer abort.
		if errors.Is(err, sharedPorts.ErrExtractionAlreadyLinked) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(
				span,
				"extraction linked by concurrent worker",
				err,
			)

			return &sharedPorts.BridgeExtractionOutcome{
				IngestionJobID:   outcome.IngestionJobID,
				TransactionCount: outcome.TransactionCount,
			}, sharedPorts.ErrExtractionAlreadyLinked
		}

		wrapped := fmt.Errorf("link extraction to ingestion: %w", err)
		libOpentelemetry.HandleSpanError(span, "link failed", wrapped)

		return nil, wrapped
	}

	return &sharedPorts.BridgeExtractionOutcome{
		IngestionJobID:   outcome.IngestionJobID,
		TransactionCount: outcome.TransactionCount,
	}, nil
}

// cleanupCustody removes the custody copy after successful ingestion. Failure
// is logged but non-fatal — a background retention sweep (T-006) picks up
// orphaned custody objects.
func (orch *BridgeExtractionOrchestrator) cleanupCustody(
	ctx context.Context,
	logger libLog.Logger,
	ref *sharedPorts.ArtifactCustodyReference,
	outcome *sharedPorts.BridgeExtractionOutcome,
) {
	if ref == nil {
		return
	}

	err := orch.custody.Delete(ctx, *ref)
	if err != nil {
		if logger != nil {
			logger.With(
				libLog.String("custody_key", ref.Key),
				libLog.Any("error", err.Error()),
			).Log(ctx, libLog.LevelWarn, "bridge orchestrator: custody delete failed (non-fatal)")
		}

		return
	}

	if outcome != nil {
		outcome.CustodyDeleted = true
	}
}
