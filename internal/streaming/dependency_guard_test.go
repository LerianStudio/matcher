// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package streaming_test

import (
	"testing"

	streaming "github.com/LerianStudio/lib-streaming/v2"
	"github.com/LerianStudio/lib-streaming/v2/streamingtest"
)

func TestLibStreamingV2DependencyGuard(t *testing.T) {
	noopEmitter := streaming.NewNoopEmitter()
	if noopEmitter == nil {
		t.Fatal("expected lib-streaming v2 noop emitter constructor to be available")
	}

	mockEmitter := streamingtest.NewMockEmitter()
	if mockEmitter == nil {
		t.Fatal("expected lib-streaming v2 mock emitter constructor to be available")
	}
}
