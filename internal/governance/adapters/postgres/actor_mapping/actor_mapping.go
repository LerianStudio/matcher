// Package actormapping provides the PostgreSQL repository for actor mappings.
package actormapping

import (
	"errors"
	"fmt"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// Repository-specific sentinel errors.
var (
	ErrRepositoryNotInitialized = errors.New("actor mapping repository not initialized")
	ErrActorMappingRequired     = errors.New("actor mapping is required")
	ErrActorIDRequired          = errors.New("actor id is required")
	ErrActorMappingNotFound     = errors.New("actor mapping not found")
	ErrNilScanner               = errors.New("nil scanner")
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
