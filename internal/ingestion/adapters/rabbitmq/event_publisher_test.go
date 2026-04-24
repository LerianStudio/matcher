// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package rabbitmq

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewEventPublisherFromChannel_NilReturnsError(t *testing.T) {
	t.Parallel()

	_, err := NewEventPublisherFromChannel(nil)
	require.Error(t, err)
}
