// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package actormapping provides the PostgreSQL repository for actor mappings.
package actormapping

import (
	"errors"

	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
)

// Repository-specific sentinel errors.
var (
	ErrRepositoryNotInitialized = errors.New("actor mapping repository not initialized")
	ErrActorMappingRequired     = errors.New("actor mapping is required")
	ErrActorIDRequired          = errors.New("actor id is required")
	ErrNilScanner               = errors.New("nil scanner")

	// ErrActorMappingNotFound is returned when an actor mapping is not found.
	// Re-exported from domain/errors for adapter-layer consumers.
	ErrActorMappingNotFound = governanceErrors.ErrActorMappingNotFound
)
