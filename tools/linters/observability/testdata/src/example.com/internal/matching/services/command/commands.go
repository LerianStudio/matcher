// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package command is a fixture for the observability analyzer test. The
// package path intentionally contains /services/command/ so
// isServicePackage() matches.
//
// Every exported *UseCase method below (except BrokenForceMatch, which omits
// tracer.Start) must pass all three observability checks.
//
// BrokenForceMatch is annotated with an analysistest expectation directive
// so we prove the analyzer is still live and reporting — a clean fixture
// would not distinguish "linter works" from "linter silently skips
// everything".
package command

import "context"

// stubTracking is a minimal stand-in for libCommons.NewTrackingFromContext.
// The analyzer only matches by selector name (.NewTrackingFromContext,
// .Start, .End), not by import path or full type, so a local stub keeps the
// fixture hermetic.
type tracker struct{}

func (tracker) NewTrackingFromContext(ctx context.Context) (logger, tracerObj, string, metricFactory) {
	return logger{}, tracerObj{}, "", metricFactory{}
}

type logger struct{}
type tracerObj struct{}
type metricFactory struct{}
type spanObj struct{}

func (tracerObj) Start(ctx context.Context, name string) (context.Context, spanObj) {
	return ctx, spanObj{}
}
func (spanObj) End() {}

var libCommons tracker

// UseCase is the service aggregate the analyzer looks for.
type UseCase struct{}

// RunMatch exercises the canonical compliant pattern.
func (uc *UseCase) RunMatch(ctx context.Context) error {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "matching.run_match")
	defer span.End()

	_ = ctx

	return nil
}

// ManualMatch exercises the second domain verb in the task list.
func (uc *UseCase) ManualMatch(ctx context.Context) error {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "matching.manual_match")
	defer span.End()

	_ = ctx

	return nil
}

// OpenDispute covers the exception-context verb shape.
func (uc *UseCase) OpenDispute(ctx context.Context) error {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "exception.open_dispute")
	defer span.End()

	_ = ctx

	return nil
}

// CreateAdjustment covers the create/write verb shape.
func (uc *UseCase) CreateAdjustment(ctx context.Context) error {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "matching.create_adjustment")
	defer span.End()

	_ = ctx

	return nil
}

// AdjustEntry covers the adjust verb shape.
func (uc *UseCase) AdjustEntry(ctx context.Context) error {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "matching.adjust_entry")
	defer span.End()

	_ = ctx

	return nil
}

// BrokenForceMatch deliberately omits tracer.Start and defer span.End so the
// analyzer must flag both — plus the missing tracking extraction if that were
// also absent. Here tracking IS extracted, leaving two diagnostics only, so
// the test demonstrates each check fires independently.
func (uc *UseCase) BrokenForceMatch(ctx context.Context) error { // want `service method BrokenForceMatch: missing tracer\.Start\(\) span creation` `service method BrokenForceMatch: missing defer span\.End\(\) for proper span cleanup`
	_, _, _, _ = libCommons.NewTrackingFromContext(ctx)

	return nil
}

// prepareMatchRun is an unexported helper — must NOT be flagged even though
// it lives in the same package and has no tracing.
func (uc *UseCase) prepareMatchRun(ctx context.Context) error {
	_ = ctx

	return nil
}
