// Copyright 2025 Lerian Studio.

package domain

import "errors"

// Sentinel errors for the systemplane domain.
var (
	ErrKeyUnknown          = errors.New("unknown configuration key")
	ErrValueInvalid        = errors.New("invalid configuration value")
	ErrRevisionMismatch    = errors.New("revision mismatch")
	ErrScopeInvalid        = errors.New("scope not allowed for this key")
	ErrPermissionDenied    = errors.New("permission denied")
	ErrReloadFailed        = errors.New("configuration reload failed")
	ErrKeyNotMutable       = errors.New("key is not mutable at runtime")
	ErrSnapshotBuildFailed = errors.New("snapshot build failed")
	ErrBundleBuildFailed   = errors.New("runtime bundle build failed")
	ErrBundleSwapFailed    = errors.New("runtime bundle swap failed")
	ErrReconcileFailed     = errors.New("bundle reconciliation failed")
	ErrNoCurrentBundle     = errors.New("no current runtime bundle")
	ErrSupervisorStopped   = errors.New("supervisor has been stopped")
	ErrRegistryRequired    = errors.New("registry is required")
)
