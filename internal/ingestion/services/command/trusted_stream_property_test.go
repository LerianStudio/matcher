// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"errors"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// propInput is the generator payload for validator properties. It models
// the four validator-relevant fields explicitly so quick.Check can shrink
// meaningfully: a mix of nil/non-nil Content, zero/non-zero UUIDs, and
// an assortment of Format strings.
type propInput struct {
	HasContent bool
	ZeroSource bool
	ZeroCtx    bool
	Format     string
}

// Generate produces the four-field shape uniformly. Format is drawn from
// a small palette that exercises all documented validator branches: the
// accepted formats of the stub registry, whitespace-padded variants, the
// empty/whitespace cases, and "obviously unsupported" payloads. Using a
// palette (not arbitrary strings) keeps iterations tight and predictable.
func (propInput) Generate(rand *rand.Rand, _ int) reflect.Value {
	palette := []string{
		"csv", "json", "xml",
		"CSV", "  Json  ", "\tXML\n",
		"", " ", "\t\n",
		"avro", "../../etc/passwd", "\x00\x01",
	}

	return reflect.ValueOf(propInput{
		HasContent: rand.Intn(2) == 1,
		ZeroSource: rand.Intn(2) == 1,
		ZeroCtx:    rand.Intn(2) == 1,
		Format:     palette[rand.Intn(len(palette))],
	})
}

// buildInput assembles a concrete IngestFromTrustedStreamInput from the
// generated abstract shape. Keeping this separate from Generate means the
// reproduction payload visible in shrink output is the cheap four-field
// struct, not a heavyweight IngestFromTrustedStreamInput with a reader.
func buildInput(g propInput) IngestFromTrustedStreamInput {
	var content io.Reader
	if g.HasContent {
		content = strings.NewReader("trusted-stream-body")
	}

	ctxID := uuid.New()
	if g.ZeroCtx {
		ctxID = uuid.Nil
	}

	srcID := uuid.New()
	if g.ZeroSource {
		srcID = uuid.Nil
	}

	return IngestFromTrustedStreamInput{
		ContextID: ctxID,
		SourceID:  srcID,
		Format:    g.Format,
		Content:   content,
	}
}

// newPropUseCase returns a UseCase skeleton wired only with the parser
// registry — validateTrustedStreamInput touches nothing else.
func newPropUseCase() *UseCase {
	// Reuse the fuzz test's stub registry (same package, same build tag)
	// so both test types agree on what "supported" means.
	return &UseCase{parsers: fuzzStubParser{}}
}

// TestProperty_ValidateTrustedStreamInput_Idempotent asserts Prop A:
// calling validateTrustedStreamInput twice on the same input returns
// the same result — either both nil, or both wrap the same sentinel.
// Validation is pure, so idempotence follows from referential integrity.
func TestProperty_ValidateTrustedStreamInput_Idempotent(t *testing.T) {
	t.Parallel()

	uc := newPropUseCase()

	prop := func(g propInput) bool {
		input := buildInput(g)

		a := validateTrustedStreamInput(uc, input)
		b := validateTrustedStreamInput(uc, input)

		switch {
		case a == nil && b == nil:
			return true
		case a == nil || b == nil:
			return false
		}

		// Both non-nil. Sentinel equality is the contract (messages may
		// differ only if a wrapped error inner text differs, which does
		// not happen for pure validation).
		return errors.Is(a, b) || errors.Is(b, a) || a.Error() == b.Error()
	}

	require.NoError(t, quick.Check(prop, nil))
}

// TestProperty_ValidateTrustedStreamInput_RejectionClassification asserts
// Prop B: if a specific sentinel is returned, the corresponding field was
// in its ill-formed state. This is the contrapositive direction of the
// validator's branch logic — it catches reordering bugs that would let
// a nil Content slip through once another field was fixed.
func TestProperty_ValidateTrustedStreamInput_RejectionClassification(t *testing.T) {
	t.Parallel()

	uc := newPropUseCase()

	prop := func(g propInput) bool {
		input := buildInput(g)

		err := validateTrustedStreamInput(uc, input)
		if err == nil {
			return true // no classification to verify
		}

		// ContentRequired => Content is nil.
		if errors.Is(err, ErrIngestFromTrustedStreamContentRequired) {
			return input.Content == nil
		}

		// SourceRequired => SourceID is zero.
		if errors.Is(err, ErrIngestFromTrustedStreamSourceRequired) {
			return input.SourceID == uuid.Nil
		}

		// ContextRequired => ContextID is zero.
		if errors.Is(err, ErrIngestFromTrustedStreamContextRequired) {
			return input.ContextID == uuid.Nil
		}

		// FormatRequired => Format trims to empty.
		if errors.Is(err, ErrIngestFromTrustedStreamFormatRequired) {
			return strings.TrimSpace(input.Format) == ""
		}

		// FormatUnsupported => trimmed/lowercased format is unknown.
		if errors.Is(err, ErrIngestFromTrustedStreamFormatUnsupported) {
			normalized := strings.ToLower(strings.TrimSpace(input.Format))
			switch normalized {
			case "csv", "json", "xml":
				return false // a known format must not trigger Unsupported
			default:
				return true
			}
		}

		// Any other error shape is a contract break.
		return false
	}

	require.NoError(t, quick.Check(prop, nil))
}
