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
