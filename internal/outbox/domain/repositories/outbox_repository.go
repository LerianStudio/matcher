// Package repositories provides outbox persistence contracts.
// The canonical interface definition lives in the shared kernel (internal/shared/ports)
// and is re-exported here as a type alias for backward compatibility.
package repositories

//go:generate mockgen -destination=mocks/outbox_repository_mock.go -package=mocks . OutboxRepository

import (
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// OutboxRepository defines persistence operations for outbox events.
// Re-exported from the shared kernel (internal/shared/ports.OutboxRepository).
//
// All bounded contexts that need this interface should use the shared kernel directly:
//
//	import sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
//
// This alias exists for backward compatibility with code that already imports
// this package. No new code should import outbox/domain/repositories from outside
// the outbox bounded context.
type OutboxRepository = sharedPorts.OutboxRepository
