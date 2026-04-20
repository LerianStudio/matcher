// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// VerifiedArtifactRetrievalInput is the per-call input to the verified
// artifact orchestrator. Wrapping the descriptor in its own struct keeps
// the public method signature stable if we later add knobs (e.g. an
// explicit content-type override or per-call retry preferences).
type VerifiedArtifactRetrievalInput struct {
	Descriptor sharedPorts.ArtifactRetrievalDescriptor
}

// VerifiedArtifactRetrievalOutput carries the terminal state of a
// successful verify-and-custody run. Only Custody is meaningful to
// callers today; TransactionCount and other downstream signals belong to
// T-003's ingestion handoff.
type VerifiedArtifactRetrievalOutput struct {
	Custody *sharedPorts.ArtifactCustodyReference
}

// VerifiedArtifactRetrievalOrchestrator stitches the three stages of
// T-002 together:
//
//  1. Retrieval: pull ciphertext + metadata from Fetcher (transient
//     failures are transient — retry is the caller's job).
//  2. Verification: HMAC-check + AES-GCM decrypt (terminal failures are
//     terminal — never retry).
//  3. Custody: write the verified plaintext to Matcher-owned storage
//     (transient failures are transient — safe to retry because we still
//     hold the plaintext in-memory at the call site).
//
// The orchestrator deliberately keeps no state between calls; concurrent
// invocations on different extractions are safe.
type VerifiedArtifactRetrievalOrchestrator struct {
	gateway  sharedPorts.ArtifactRetrievalGateway
	verifier sharedPorts.ArtifactTrustVerifier
	custody  sharedPorts.ArtifactCustodyStore
}

// NewVerifiedArtifactRetrievalOrchestrator validates every dependency up
// front so bootstrap misconfiguration surfaces at init time, not on the
// first request. Nil guards use simple `== nil` checks (not pkg/assert)
// because these are infrastructure dependencies, not domain invariants —
// see CLAUDE.md "Nil Checks vs Asserters".
func NewVerifiedArtifactRetrievalOrchestrator(
	gateway sharedPorts.ArtifactRetrievalGateway,
	verifier sharedPorts.ArtifactTrustVerifier,
	custody sharedPorts.ArtifactCustodyStore,
) (*VerifiedArtifactRetrievalOrchestrator, error) {
	if gateway == nil {
		return nil, sharedPorts.ErrNilArtifactRetrievalGateway
	}

	if verifier == nil {
		return nil, sharedPorts.ErrNilArtifactTrustVerifier
	}

	if custody == nil {
		return nil, sharedPorts.ErrNilArtifactCustodyStore
	}

	return &VerifiedArtifactRetrievalOrchestrator{
		gateway:  gateway,
		verifier: verifier,
		custody:  custody,
	}, nil
}

// RetrieveAndCustodyVerifiedArtifact executes the three-stage pipeline
// for a single extraction. Each stage has its own child span so operators
// can isolate where a given run failed. The outer span carries the
// extraction id as an attribute so traces remain correlated with the
// originating extraction lifecycle.
//
// Error contract:
//   - ErrArtifactDescriptorRequired / ErrArtifactExtractionIDRequired /
//     ErrArtifactTenantIDRequired for input mistakes.
//   - ErrArtifactRetrievalFailed wrapped for transient retrieval issues.
//   - ErrIntegrityVerificationFailed for both HMAC and AES-GCM failures
//     (terminal — never retry).
//   - ErrCustodyStoreFailed wrapped for transient custody write issues.
//   - ErrFetcherResourceNotFound for 404 from Fetcher (terminal, but
//     distinguishable from integrity failures).
func (orch *VerifiedArtifactRetrievalOrchestrator) RetrieveAndCustodyVerifiedArtifact(
	ctx context.Context,
	input VerifiedArtifactRetrievalInput,
) (*VerifiedArtifactRetrievalOutput, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // orchestrator only needs tracer

	ctx, span := tracer.Start(ctx, "command.discovery.retrieve_and_custody_verified_artifact")
	defer span.End()

	if err := validateVerifiedArtifactInput(input); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "validate verified artifact input", err)

		return nil, err
	}

	plaintext, err := orch.retrieveAndVerify(ctx, input.Descriptor)
	if err != nil {
		return nil, err
	}

	ref, err := orch.persistCustody(ctx, input.Descriptor, plaintext)
	if err != nil {
		return nil, err
	}

	return &VerifiedArtifactRetrievalOutput{Custody: ref}, nil
}

// retrieveAndVerify executes stages 1 and 2 of the pipeline. Keeping them
// together lets us close the retrieval body exactly once at the boundary
// between the two stages without leaking file-descriptor style resources.
func (orch *VerifiedArtifactRetrievalOrchestrator) retrieveAndVerify(
	ctx context.Context,
	descriptor sharedPorts.ArtifactRetrievalDescriptor,
) (io.Reader, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "orchestrator.retrieve_and_verify")
	defer span.End()

	result, err := orch.gateway.Retrieve(ctx, descriptor)
	if err != nil {
		wrapped := wrapRetrievalError(err)
		libOpentelemetry.HandleSpanError(span, "artifact retrieval failed", wrapped)

		return nil, wrapped
	}

	if result == nil || result.Content == nil {
		wrapped := fmt.Errorf("%w: gateway returned nil body", sharedPorts.ErrArtifactRetrievalFailed)
		libOpentelemetry.HandleSpanError(span, "artifact retrieval returned nil body", wrapped)

		return nil, wrapped
	}

	defer func() {
		_ = result.Content.Close()
	}()

	plaintext, err := orch.verifier.VerifyAndDecrypt(ctx, result.Content, result.HMAC, result.IV)
	if err != nil {
		// Integrity failures are terminal: no wrapping context — the sentinel
		// itself is the signal. Any other error means the verifier's HKDF /
		// crypto init blew up, which we surface as a wrapped verification
		// failure so callers still see a single terminal signal.
		if errors.Is(err, sharedPorts.ErrIntegrityVerificationFailed) {
			libOpentelemetry.HandleSpanBusinessErrorEvent(span, "artifact integrity verification failed", err)

			return nil, err
		}

		wrapped := fmt.Errorf("verify artifact: %w", err)
		libOpentelemetry.HandleSpanError(span, "verifier returned non-integrity error", wrapped)

		return nil, wrapped
	}

	// The verifier already materialised plaintext into a bytes.Reader (it
	// had to, because AES-GCM requires the full ciphertext+tag before
	// decryption completes). Passing the returned reader directly to
	// custody avoids a redundant io.ReadAll + newBytesReader roundtrip
	// that would triple-buffer the payload in memory for a 256 MiB
	// artifact (ciphertext + plaintext + orchestrator rebuffer).
	//
	// T-003 P1 hardening: removed the orchestrator-level ReadAll; the
	// retrieval body has already been drained by VerifyAndDecrypt, so the
	// deferred Close above fires with no outstanding reads against the
	// HTTP body.
	return plaintext, nil
}

// persistCustody executes stage 3. Any failure is wrapped with
// ErrCustodyStoreFailed unless the custody store already emitted that
// sentinel, in which case we preserve the caller's wrapping.
func (orch *VerifiedArtifactRetrievalOrchestrator) persistCustody(
	ctx context.Context,
	descriptor sharedPorts.ArtifactRetrievalDescriptor,
	plaintext io.Reader,
) (*sharedPorts.ArtifactCustodyReference, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "orchestrator.persist_custody")
	defer span.End()

	ref, err := orch.custody.Store(ctx, sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: descriptor.ExtractionID,
		TenantID:     descriptor.TenantID,
		Content:      plaintext,
	})
	if err != nil {
		wrapped := wrapCustodyError(err)
		libOpentelemetry.HandleSpanError(span, "custody write failed", wrapped)

		return nil, wrapped
	}

	if ref == nil {
		wrapped := fmt.Errorf("%w: custody store returned nil reference", sharedPorts.ErrCustodyStoreFailed)
		libOpentelemetry.HandleSpanError(span, "custody store returned nil reference", wrapped)

		return nil, wrapped
	}

	return ref, nil
}

func validateVerifiedArtifactInput(input VerifiedArtifactRetrievalInput) error {
	if input.Descriptor.ExtractionID == uuid.Nil {
		return sharedPorts.ErrArtifactExtractionIDRequired
	}

	if strings.TrimSpace(input.Descriptor.TenantID) == "" {
		return sharedPorts.ErrArtifactTenantIDRequired
	}

	if strings.TrimSpace(input.Descriptor.URL) == "" {
		return sharedPorts.ErrArtifactDescriptorRequired
	}

	return nil
}

func wrapRetrievalError(err error) error {
	if err == nil {
		return nil
	}

	// Already wrapped — preserve caller's chain so errors.Is continues to
	// match downstream specifics like ErrFetcherResourceNotFound.
	if errors.Is(err, sharedPorts.ErrArtifactRetrievalFailed) ||
		errors.Is(err, sharedPorts.ErrFetcherResourceNotFound) {
		return err
	}

	return fmt.Errorf("%w: %w", sharedPorts.ErrArtifactRetrievalFailed, err)
}

func wrapCustodyError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, sharedPorts.ErrCustodyStoreFailed) {
		return err
	}

	return fmt.Errorf("%w: %w", sharedPorts.ErrCustodyStoreFailed, err)
}
