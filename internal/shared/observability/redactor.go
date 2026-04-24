// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package observability provides matcher-specific observability helpers
// layered on top of lib-commons opentelemetry primitives. Right now this
// package only exposes the span-attribute redactor; future helpers that are
// genuinely cross-cutting (metrics wrappers, custom exporters) can land here
// without pulling every caller into a bigger import surface.
package observability

import (
	"sync"

	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
)

// matcherRedactor and matcherRedactorOnce back the lazily-initialized
// process-wide redactor singleton. A singleton is the right shape here for
// two reasons: (1) lib-commons generates an HMAC key inside NewRedactor, and
// generating a fresh key per call site would make any future hash-based rule
// emit different digests for the same input across spans, defeating
// correlation; (2) the redactor is read-only after construction, so a shared
// instance is safe to hand out to every caller without coordination.
var (
	matcherRedactor     *libOpentelemetry.Redactor
	matcherRedactorOnce sync.Once
)

// NewMatcherRedactor returns the matcher-wide *libOpentelemetry.Redactor used
// as the 4th argument to libOpentelemetry.SetSpanAttributesFromValue.
//
// The redactor is intentionally constructed with an empty rule set. It is NOT
// a no-op — it forwards through the lib-commons redactor plumbing so that
// when a field is later identified as PII or secret (e.g. external connector
// credentials, raw PAN, resolution notes containing customer data), the
// matcher can add one RedactionRule here and every span-attribute call site
// starts masking that field without any further code changes.
//
// Callers MUST NOT pass nil as the 4th argument to
// SetSpanAttributesFromValue. Passing the matcher redactor keeps the hook
// point reserved for future redaction growth; callers that reach for nil
// will silently skip any redactor-driven masking once rules are added.
//
// The returned pointer is a process-wide singleton. Callers must not mutate
// it; the lib-commons Redactor is designed to be read-only after
// construction and treat mutation as undefined behaviour.
func NewMatcherRedactor() *libOpentelemetry.Redactor {
	matcherRedactorOnce.Do(func() {
		redactor, err := libOpentelemetry.NewRedactor(nil, "")
		if err != nil {
			// NewRedactor only fails on rule compilation; with an empty rule
			// set it cannot fail. Fall back to the always-mask redactor
			// anyway so we fail safe (over-redact) rather than fail open
			// (leak) if lib-commons ever changes the contract.
			matcherRedactor = libOpentelemetry.NewAlwaysMaskRedactor()

			return
		}

		matcherRedactor = redactor
	})

	return matcherRedactor
}
