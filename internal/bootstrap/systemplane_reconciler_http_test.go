// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
)

// Compile-time interface satisfaction check.
var _ ports.BundleReconciler = (*HTTPPolicyReconciler)(nil)

func TestHTTPPolicyReconciler_ImplementsBundleReconciler(t *testing.T) {
	t.Parallel()

	var rec ports.BundleReconciler = &HTTPPolicyReconciler{}
	assert.NotNil(t, rec)
}

func TestHTTPPolicyReconciler_Name(t *testing.T) {
	t.Parallel()

	rec := NewHTTPPolicyReconciler()

	assert.Equal(t, "http-policy-reconciler", rec.Name())
}

func TestHTTPPolicyReconciler_Phase(t *testing.T) {
	t.Parallel()

	rec := NewHTTPPolicyReconciler()

	assert.Equal(t, domain.PhaseValidation, rec.Phase(), "http policy must run in validation phase")
}

func TestHTTPPolicyReconciler_NilHTTPBundle(t *testing.T) {
	t.Parallel()

	rec := NewHTTPPolicyReconciler()
	candidate := &MatcherBundle{HTTP: nil}

	err := rec.Reconcile(context.Background(), nil, candidate, domain.Snapshot{})

	assert.NoError(t, err)
}

func TestHTTPPolicyReconciler_ValidPolicy(t *testing.T) {
	t.Parallel()

	rec := NewHTTPPolicyReconciler()
	candidate := &MatcherBundle{
		HTTP: &HTTPPolicyBundle{
			BodyLimitBytes:     104857600,
			CORSAllowedOrigins: "http://localhost:3000",
			CORSAllowedMethods: "GET,POST",
			CORSAllowedHeaders: "Content-Type",
		},
	}

	err := rec.Reconcile(context.Background(), nil, candidate, domain.Snapshot{})

	assert.NoError(t, err)
}

func TestHTTPPolicyReconciler_NegativeBodyLimit(t *testing.T) {
	t.Parallel()

	rec := NewHTTPPolicyReconciler()
	candidate := &MatcherBundle{
		HTTP: &HTTPPolicyBundle{
			BodyLimitBytes:     -1,
			CORSAllowedOrigins: "http://localhost:3000",
		},
	}

	err := rec.Reconcile(context.Background(), nil, candidate, domain.Snapshot{})

	require.Error(t, err)
	assert.ErrorContains(t, err, "body limit bytes must be non-negative")
	assert.ErrorContains(t, err, "-1")
}

func TestHTTPPolicyReconciler_EmptyCORSOrigins(t *testing.T) {
	t.Parallel()

	rec := NewHTTPPolicyReconciler()
	candidate := &MatcherBundle{
		HTTP: &HTTPPolicyBundle{
			BodyLimitBytes:     1024,
			CORSAllowedOrigins: "",
		},
	}

	err := rec.Reconcile(context.Background(), nil, candidate, domain.Snapshot{})

	require.Error(t, err)
	assert.ErrorContains(t, err, "CORS allowed origins must not be empty")
}

func TestHTTPPolicyReconciler_WrongBundleType(t *testing.T) {
	t.Parallel()

	rec := NewHTTPPolicyReconciler()

	// Use a non-MatcherBundle RuntimeBundle implementation.
	candidate := &wrongBundleType{}

	err := rec.Reconcile(context.Background(), nil, candidate, domain.Snapshot{})

	require.Error(t, err)
	assert.ErrorContains(t, err, "unexpected bundle type")
}

func TestHTTPPolicyReconciler_ZeroBodyLimit(t *testing.T) {
	t.Parallel()

	rec := NewHTTPPolicyReconciler()

	// Zero is valid — Fiber applies its own default when BodyLimitBytes is 0.
	candidate := &MatcherBundle{
		HTTP: &HTTPPolicyBundle{
			BodyLimitBytes:     0,
			CORSAllowedOrigins: "http://localhost:3000",
		},
	}

	err := rec.Reconcile(context.Background(), nil, candidate, domain.Snapshot{})

	assert.NoError(t, err)
}

func TestHTTPPolicyReconciler_LargeBodyLimit(t *testing.T) {
	t.Parallel()

	rec := NewHTTPPolicyReconciler()

	// Very large body limit should still be valid (no upper bound enforced).
	candidate := &MatcherBundle{
		HTTP: &HTTPPolicyBundle{
			BodyLimitBytes:     1073741824, // 1 GiB
			CORSAllowedOrigins: "*",
		},
	}

	err := rec.Reconcile(context.Background(), nil, candidate, domain.Snapshot{})

	assert.NoError(t, err)
}

func TestHTTPPolicyReconciler_MultipleValidationErrors(t *testing.T) {
	t.Parallel()

	rec := NewHTTPPolicyReconciler()

	// Negative body limit AND empty CORS — first check fails, so we only
	// see the body limit error (fail-fast validation).
	candidate := &MatcherBundle{
		HTTP: &HTTPPolicyBundle{
			BodyLimitBytes:     -100,
			CORSAllowedOrigins: "",
		},
	}

	err := rec.Reconcile(context.Background(), nil, candidate, domain.Snapshot{})

	require.Error(t, err)
	assert.ErrorContains(t, err, "body limit bytes must be non-negative")
}

// wrongBundleType is a test double that satisfies domain.RuntimeBundle
// but is not a *MatcherBundle.
type wrongBundleType struct{}

func (w *wrongBundleType) Close(_ context.Context) error {
	return nil
}
