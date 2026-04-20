// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestTerminalFailureMessage_NilErr_ReturnsLoudBugMarker asserts the S6-3
// contract: a nil err reaching terminalFailureMessage is a wiring bug, and
// the returned message must be loud enough that operators grepping audit
// rows will spot the bug rather than the masked symptom. Pre-S6-3 the helper
// returned the generic "unknown failure"; post-S6-3 it returns a self-
// identifying marker that names the helper so grep hits the right symbol.
func TestTerminalFailureMessage_NilErr_ReturnsLoudBugMarker(t *testing.T) {
	t.Parallel()

	got := terminalFailureMessage(nil)

	assert.NotEqual(t, "unknown failure", got,
		"pre-S6-3 silent-fallback message must not leak back in")
	assert.Contains(t, got, "wiring bug",
		"the returned message must explicitly name this as a wiring bug")
	assert.Contains(t, got, "terminalFailureMessage",
		"the marker must reference the helper name so audit greps find it")
	assert.True(t, strings.HasPrefix(got, "internal:"),
		"prefix must make the persisted row visibly different from operator-authored messages")
}

// TestTerminalFailureMessage_NonNilErr_UnwrapsErrorString asserts the
// common-path behavior is unchanged: a non-nil error flows through
// untouched so the persisted bridge_last_error_message matches whatever
// the classifier / bridgeOne surfaced.
func TestTerminalFailureMessage_NonNilErr_UnwrapsErrorString(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("custody store put failed")
	wrapped := errors.Join(sentinel, errors.New("redis unreachable"))

	assert.Equal(t, sentinel.Error(), terminalFailureMessage(sentinel))
	assert.Equal(t, wrapped.Error(), terminalFailureMessage(wrapped))
}
