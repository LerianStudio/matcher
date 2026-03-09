// Package repositories provides governance persistence contracts.
// The canonical AuditLogRepository interface lives in the shared kernel (internal/shared/ports)
// and is re-exported here as a type alias for backward compatibility.
package repositories

//go:generate mockgen -destination=mocks/audit_repository_mock.go -package=mocks github.com/LerianStudio/matcher/internal/shared/ports AuditLogRepository

import (
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Tx is the transaction alias used for audit log repository operations.
type Tx = sharedPorts.Tx

// AuditLogRepository defines persistence operations for audit logs.
// Re-exported from the shared kernel (internal/shared/ports.AuditLogRepository).
//
// All bounded contexts that need this interface should use the shared kernel directly:
//
//	import sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
//
// This alias exists for backward compatibility with code that already imports
// this package. No new code should import governance/domain/repositories from outside
// the governance bounded context.
//
// This is an append-only repository: no Update or Delete methods are defined.
type AuditLogRepository = sharedPorts.AuditLogRepository
