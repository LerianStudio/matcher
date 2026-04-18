// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import "github.com/LerianStudio/lib-commons/v5/commons/outbox"

// OutboxRepository is a type alias for the canonical lib-commons outbox repository.
// All bounded contexts reference this alias, so the migration from the bespoke
// outbox to the canonical package is transparent to callers.
type OutboxRepository = outbox.OutboxRepository
