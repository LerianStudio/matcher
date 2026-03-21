// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

var (
	errHTTPReconcilerUnexpectedType = errors.New("http policy reconciler: unexpected bundle type")
	errHTTPReconcilerBodyLimit      = errors.New("http policy reconciler: body limit bytes must be non-negative")
	errHTTPReconcilerCORSOrigins    = errors.New("http policy reconciler: CORS allowed origins must not be empty")
)

// Compile-time interface check.
var _ ports.BundleReconciler = (*HTTPPolicyReconciler)(nil)

// HTTPPolicyReconciler validates HTTP policy changes during bundle swaps.
// The actual application of HTTP policies is handled by dynamic middleware
// that reads from the active MatcherBundle's HTTPPolicyBundle field. This
// reconciler acts as a validation gate: if the candidate bundle contains
// an invalid HTTP policy, reconciliation fails and the supervisor does not
// swap the bundle.
//
// Validation rules:
//   - BodyLimitBytes must be non-negative (zero means "use Fiber default").
//   - CORSAllowedOrigins must not be empty when an HTTPPolicyBundle is present.
type HTTPPolicyReconciler struct{}

// NewHTTPPolicyReconciler creates a new HTTPPolicyReconciler.
func NewHTTPPolicyReconciler() *HTTPPolicyReconciler {
	return &HTTPPolicyReconciler{}
}

// Name returns the reconciler's identifier for logging and metrics.
func (r *HTTPPolicyReconciler) Name() string {
	return "http-policy-reconciler"
}

// Phase returns PhaseValidation because the HTTP policy reconciler acts as a
// gate — it rejects structurally invalid HTTP configurations before any side
// effects run.
func (r *HTTPPolicyReconciler) Phase() domain.ReconcilerPhase {
	return domain.PhaseValidation
}

// Reconcile validates the candidate bundle's HTTP policy. If the candidate
// is not a *MatcherBundle (unexpected type), an error is returned to prevent
// the swap. If the HTTPPolicyBundle is nil, validation is skipped — a nil
// policy means no HTTP-layer overrides were configured.
func (r *HTTPPolicyReconciler) Reconcile(_ context.Context, _, candidate domain.RuntimeBundle, _ domain.Snapshot) error {
	bundle, ok := candidate.(*MatcherBundle)
	if !ok {
		return fmt.Errorf("%w: %T", errHTTPReconcilerUnexpectedType, candidate)
	}

	if bundle.HTTP == nil {
		return nil // No HTTP policy to validate.
	}

	if bundle.HTTP.BodyLimitBytes < 0 {
		return fmt.Errorf("%w: got %d", errHTTPReconcilerBodyLimit, bundle.HTTP.BodyLimitBytes)
	}

	if bundle.HTTP.CORSAllowedOrigins == "" {
		return errHTTPReconcilerCORSOrigins
	}

	return nil
}
