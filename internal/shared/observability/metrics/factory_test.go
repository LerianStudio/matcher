// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package metrics_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedMetrics "github.com/LerianStudio/matcher/internal/shared/observability/metrics"
)

func TestMeter_ReturnsNonNil(t *testing.T) {
	t.Parallel()

	m := sharedMetrics.Meter("matcher.test.scope")
	assert.NotNil(t, m)
}

func TestInt64Counter_ValidName_Succeeds(t *testing.T) {
	t.Parallel()

	m := sharedMetrics.Meter("matcher.test.int64counter")

	counter, err := sharedMetrics.Int64Counter(m, "matcher.test.counter", "test counter")

	require.NoError(t, err)
	require.NotNil(t, counter)
}

func TestFloat64Histogram_ValidName_Succeeds(t *testing.T) {
	t.Parallel()

	m := sharedMetrics.Meter("matcher.test.histogram")

	h, err := sharedMetrics.Float64Histogram(m, "matcher.test.histogram_value", "test histogram", "ms")

	require.NoError(t, err)
	require.NotNil(t, h)
}

func TestInt64UpDownCounter_ValidName_Succeeds(t *testing.T) {
	t.Parallel()

	m := sharedMetrics.Meter("matcher.test.updown")

	c, err := sharedMetrics.Int64UpDownCounter(m, "matcher.test.updown_value", "test updown")

	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestAttr_ReturnsStringKV(t *testing.T) {
	t.Parallel()

	kv := sharedMetrics.Attr("outcome", "confirmed")

	assert.Equal(t, "outcome", string(kv.Key))
	assert.Equal(t, "confirmed", kv.Value.AsString())
}

func TestBuildAttrs_ReturnsMeasurementOption(t *testing.T) {
	t.Parallel()

	opt := sharedMetrics.BuildAttrs(
		sharedMetrics.Attr("outcome", "failed"),
		sharedMetrics.Attr("context_id", "abc"),
	)

	assert.NotNil(t, opt)
}
