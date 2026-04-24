// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package errors defines sentinel errors for the governance domain.
package errors

import "errors"

// Sentinel errors for governance domain operations.
var (
	// ErrAuditLogNotFound is returned when an audit log entry is not found.
	ErrAuditLogNotFound = errors.New("audit log not found")

	// ErrActorMappingNotFound is returned when an actor mapping is not found.
	ErrActorMappingNotFound = errors.New("actor mapping not found")

	// ErrMetadataNotFound is returned when archive metadata is not found.
	ErrMetadataNotFound = errors.New("archive metadata not found")
)
