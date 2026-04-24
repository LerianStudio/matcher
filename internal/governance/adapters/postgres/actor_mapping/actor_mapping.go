// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package actormapping provides the PostgreSQL repository for actor mappings.
package actormapping

import (
	"fmt"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

func scanActorMapping(scanner interface{ Scan(dest ...any) error }) (*entities.ActorMapping, error) {
	if scanner == nil {
		return nil, fmt.Errorf("scanning actor mapping: %w", ErrNilScanner)
	}

	var mapping entities.ActorMapping

	if err := scanner.Scan(
		&mapping.ActorID,
		&mapping.DisplayName,
		&mapping.Email,
		&mapping.CreatedAt,
		&mapping.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("scanning actor mapping: %w", err)
	}

	return &mapping, nil
}
