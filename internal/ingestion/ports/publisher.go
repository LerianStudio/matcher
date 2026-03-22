package ports

import (
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// EventPublisher publishes ingestion events to message broker.
// The canonical IngestionEventPublisher interface lives in the shared kernel
// (internal/shared/ports.IngestionEventPublisher) and is re-exported here
// as a type alias for backward compatibility.
//
// All bounded contexts that need this interface should use the shared kernel directly:
//
//	import sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
//
// This alias exists for backward compatibility with code that already imports
// this package. No new code outside the ingestion bounded context should import
// ingestion/ports for the EventPublisher.
type EventPublisher = sharedPorts.IngestionEventPublisher
