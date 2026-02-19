package ports

import (
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// MatchTrigger is an alias for the shared MatchTrigger port interface.
// The canonical definition lives in the shared ports package to avoid
// cross-context import violations (e.g., configuration importing ingestion).
type MatchTrigger = sharedPorts.MatchTrigger

// ContextProvider is an alias for the shared ContextProvider port interface.
// The canonical definition lives in the shared ports package.
type ContextProvider = sharedPorts.ContextProvider
