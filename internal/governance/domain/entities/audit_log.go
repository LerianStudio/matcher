// Package entities defines governance domain types and validation logic.
// The canonical AuditLog type definition lives in the shared kernel (internal/shared/domain)
// and is re-exported here as a type alias for backward compatibility.
package entities

import (
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Field length limits matching database constraints.
// Re-exported from the shared kernel.
const (
	MaxEntityTypeLength = sharedDomain.MaxEntityTypeLength
	MaxActionLength     = sharedDomain.MaxActionLength
	MaxActorIDLength    = sharedDomain.MaxActorIDLength
)

// Sentinel errors for audit log validation.
// Re-exported from the shared kernel.
var (
	ErrTenantIDRequired   = sharedDomain.ErrAuditTenantIDRequired
	ErrEntityTypeRequired = sharedDomain.ErrAuditEntityTypeRequired
	ErrEntityTypeTooLong  = sharedDomain.ErrAuditEntityTypeTooLong
	ErrEntityIDRequired   = sharedDomain.ErrAuditEntityIDRequired
	ErrActionRequired     = sharedDomain.ErrAuditActionRequired
	ErrActionTooLong      = sharedDomain.ErrAuditActionTooLong
	ErrActorIDTooLong     = sharedDomain.ErrAuditActorIDTooLong
	ErrChangesRequired    = sharedDomain.ErrAuditChangesRequired
	ErrChangesInvalidJSON = sharedDomain.ErrAuditChangesInvalidJSON
)

// AuditLogFilter defines optional filters for listing audit logs.
// Type alias for the shared kernel AuditLogFilter.
type AuditLogFilter = sharedDomain.AuditLogFilter

// AuditLog is a type alias for the shared kernel AuditLog.
// All bounded contexts that need audit log types should use the shared kernel directly:
//
//	import sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
//
// This alias exists for backward compatibility with code that already imports
// this package. No new code should import governance/domain/entities from outside
// the governance bounded context.
type AuditLog = sharedDomain.AuditLog

// NewAuditLog creates a new AuditLog. Delegates to the shared kernel constructor.
var NewAuditLog = sharedDomain.NewAuditLog
