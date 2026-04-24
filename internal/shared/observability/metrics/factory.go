// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package metrics

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Meter returns the OTel Meter for the given scope. A thin wrapper over
// otel.Meter so per-context packages have a single entry point if the
// global meter provider ever needs to be swapped in tests.
func Meter(scope string) metric.Meter {
	return otel.Meter(scope)
}

// Int64Counter creates a counter named matcher.<context>.<metric>.
// On construction failure the returned counter is a no-op so business
// logic is never blocked on observability init — the error is returned
// for the caller to log at package init if desired.
func Int64Counter(meter metric.Meter, name, description string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	base := make([]metric.Int64CounterOption, 0, 1+len(opts))
	base = append(base, metric.WithDescription(description))
	base = append(base, opts...)

	counter, err := meter.Int64Counter(name, base...)
	if err != nil {
		fallback, ferr := noop.NewMeterProvider().Meter("noop").Int64Counter(name)
		if ferr != nil {
			// The noop meter cannot fail; this branch exists only as a
			// defensive guard against a future OTel SDK contract change.
			return nil, fmt.Errorf("create %s counter: %w", name, err)
		}

		return fallback, fmt.Errorf("create %s counter: %w", name, err)
	}

	return counter, nil
}

// histogramBaseOptions is the count of description + unit options prepended
// to every histogram constructor call. Lifted out of the slice-allocation
// site so the capacity hint is readable without a magic number.
const histogramBaseOptions = 2

// Float64Histogram creates a histogram with the given unit. Same nop-fallback
// contract as Int64Counter.
func Float64Histogram(meter metric.Meter, name, description, unit string, opts ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	base := make([]metric.Float64HistogramOption, 0, histogramBaseOptions+len(opts))
	base = append(base,
		metric.WithDescription(description),
		metric.WithUnit(unit),
	)
	base = append(base, opts...)

	histogram, err := meter.Float64Histogram(name, base...)
	if err != nil {
		fallback, ferr := noop.NewMeterProvider().Meter("noop").Float64Histogram(name)
		if ferr != nil {
			// noop meter cannot actually fail; this branch is defensive.
			return nil, fmt.Errorf("create %s histogram: %w", name, err)
		}

		return fallback, fmt.Errorf("create %s histogram: %w", name, err)
	}

	return histogram, nil
}

// Int64UpDownCounter creates an up-down counter, used where a value can
// decrease (in-flight counters, queue depth).
func Int64UpDownCounter(meter metric.Meter, name, description string, opts ...metric.Int64UpDownCounterOption) (metric.Int64UpDownCounter, error) {
	base := make([]metric.Int64UpDownCounterOption, 0, 1+len(opts))
	base = append(base, metric.WithDescription(description))
	base = append(base, opts...)

	counter, err := meter.Int64UpDownCounter(name, base...)
	if err != nil {
		fallback, ferr := noop.NewMeterProvider().Meter("noop").Int64UpDownCounter(name)
		if ferr != nil {
			return nil, fmt.Errorf("create %s up-down counter: %w", name, err)
		}

		return fallback, fmt.Errorf("create %s up-down counter: %w", name, err)
	}

	return counter, nil
}

// Attr is a compact constructor for attribute.KeyValue used by per-context
// metric packages. Emitted separately from attribute.String et al. so
// call-sites don't need to import go.opentelemetry.io/otel/attribute
// directly for the common string-label case.
func Attr(key, value string) attribute.KeyValue {
	return attribute.String(key, value)
}

// BuildAttrs returns a WithAttributes option for the given key/value pairs.
// The variadic shape keeps call-sites terse:
//
//	counter.Add(ctx, 1, metrics.BuildAttrs(metrics.Attr("outcome", "confirmed")))
func BuildAttrs(attrs ...attribute.KeyValue) metric.MeasurementOption {
	return metric.WithAttributes(attrs...)
}
