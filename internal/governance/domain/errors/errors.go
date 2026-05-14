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

	// ErrActorMappingImmutable is returned when a caller attempts to mutate
	// the identity fields (display_name, email) of an existing actor mapping.
	// Actor identity is append-only post-creation to prevent the
	// pseudonymization-bypass vulnerability flagged by Taura Security
	// (28/04/2026). The HTTP layer maps this sentinel to 409 Conflict.
	ErrActorMappingImmutable = errors.New("actor mapping identity fields are immutable post-creation")

	// ErrMetadataNotFound is returned when archive metadata is not found.
	ErrMetadataNotFound = errors.New("archive metadata not found")
)
