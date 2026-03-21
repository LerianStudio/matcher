// Copyright 2025 Lerian Studio.

package domain

import (
	"errors"
	"fmt"
	"strings"
)

// ApplyBehavior describes how a configuration change propagates at runtime.
type ApplyBehavior string

// Supported ApplyBehavior values.
const (
	ApplyLiveRead                  ApplyBehavior = "live-read"
	ApplyWorkerReconcile           ApplyBehavior = "worker-reconcile"
	ApplyBundleRebuild             ApplyBehavior = "bundle-rebuild"
	ApplyBundleRebuildAndReconcile ApplyBehavior = "bundle-rebuild+worker-reconcile"
	ApplyBootstrapOnly             ApplyBehavior = "bootstrap-only"
)

// ErrInvalidApplyBehavior indicates an invalid apply behavior value.
var ErrInvalidApplyBehavior = errors.New("invalid apply behavior")

const (
	applyBehaviorStrengthBootstrapOnly = iota
	applyBehaviorStrengthLiveRead
	applyBehaviorStrengthWorkerReconcile
	applyBehaviorStrengthBundleRebuild
	applyBehaviorStrengthBundleRebuildAndReconcile
	applyBehaviorStrengthUnknown = -1
)

// IsValid reports whether the apply behavior is supported.
func (ab ApplyBehavior) IsValid() bool {
	switch ab {
	case ApplyLiveRead, ApplyWorkerReconcile, ApplyBundleRebuild,
		ApplyBundleRebuildAndReconcile, ApplyBootstrapOnly:
		return true
	}

	return false
}

// String returns the string representation of the apply behavior.
func (ab ApplyBehavior) String() string {
	return string(ab)
}

// Strength returns an integer rank indicating how disruptive the apply
// behavior is. Higher values imply heavier runtime impact.
//
//	bootstrap-only                  = 0
//	live-read                       = 1
//	worker-reconcile                = 2
//	bundle-rebuild                  = 3
//	bundle-rebuild+worker-reconcile = 4
func (ab ApplyBehavior) Strength() int {
	switch ab {
	case ApplyBootstrapOnly:
		return applyBehaviorStrengthBootstrapOnly
	case ApplyLiveRead:
		return applyBehaviorStrengthLiveRead
	case ApplyWorkerReconcile:
		return applyBehaviorStrengthWorkerReconcile
	case ApplyBundleRebuild:
		return applyBehaviorStrengthBundleRebuild
	case ApplyBundleRebuildAndReconcile:
		return applyBehaviorStrengthBundleRebuildAndReconcile
	default:
		return applyBehaviorStrengthUnknown
	}
}

// ParseApplyBehavior parses a string into an ApplyBehavior (case-insensitive).
func ParseApplyBehavior(s string) (ApplyBehavior, error) {
	ab := ApplyBehavior(strings.ToLower(strings.TrimSpace(s)))
	if !ab.IsValid() {
		return "", fmt.Errorf("parse %s: %w", s, ErrInvalidApplyBehavior)
	}

	return ab, nil
}
