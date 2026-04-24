// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package auth

// RBAC resource names for the Matcher service.
// Use module-level naming: {module} not {module}-{entity}
// Actions follow pattern: {entity}:{operation} for granular control
// or standard verbs: read, write, delete, admin.
const (
	// ResourceConfiguration is the RBAC resource for configuration management.
	ResourceConfiguration = "configuration"

	// ResourceIngestion is the RBAC resource for ingestion operations.
	ResourceIngestion = "ingestion"

	// ResourceMatching is the RBAC resource for matching/reconciliation operations.
	ResourceMatching = "matching"

	// ResourceGovernance is the RBAC resource for audit and governance.
	ResourceGovernance = "governance"

	// ResourceReporting is the RBAC resource for reporting and analytics.
	ResourceReporting = "reporting"

	// ResourceException is the RBAC resource for exception management.
	ResourceException = "exception"

	// ResourceDiscovery is the RBAC resource for schema discovery operations.
	ResourceDiscovery = "discovery"

	// ResourceSystem is the RBAC resource for system-level operations.
	ResourceSystem = "system"
)

// Standard RBAC actions.
const (
	// ActionRead is the standard read action.
	ActionRead = "read"

	// ActionWrite is the standard write action (create/update).
	ActionWrite = "write"

	// ActionDelete is the standard delete action.
	ActionDelete = "delete"

	// ActionAdmin is for administrative operations.
	ActionAdmin = "admin"
)

// Configuration module actions (entity:operation pattern).
const (
	ActionContextCreate = "context:create"
	ActionContextRead   = "context:read"
	ActionContextUpdate = "context:update"
	ActionContextDelete = "context:delete"

	ActionSourceCreate = "source:create"
	ActionSourceRead   = "source:read"
	ActionSourceUpdate = "source:update"
	ActionSourceDelete = "source:delete"

	ActionFieldMapCreate = "field-map:create"
	ActionFieldMapRead   = "field-map:read"
	ActionFieldMapUpdate = "field-map:update"
	ActionFieldMapDelete = "field-map:delete"

	ActionRuleCreate = "rule:create"
	ActionRuleRead   = "rule:read"
	ActionRuleUpdate = "rule:update"
	ActionRuleDelete = "rule:delete"

	ActionFeeScheduleCreate = "fee-schedule:create"
	ActionFeeScheduleRead   = "fee-schedule:read"
	ActionFeeScheduleUpdate = "fee-schedule:update"
	ActionFeeScheduleDelete = "fee-schedule:delete"

	ActionScheduleCreate = "schedule:create"
	ActionScheduleRead   = "schedule:read"
	ActionScheduleUpdate = "schedule:update"
	ActionScheduleDelete = "schedule:delete"

	ActionFeeRuleCreate = "fee-rule:create"
	ActionFeeRuleRead   = "fee-rule:read"
	ActionFeeRuleUpdate = "fee-rule:update"
	ActionFeeRuleDelete = "fee-rule:delete"
)

// Ingestion module actions.
const (
	ActionImportCreate      = "import:create"
	ActionJobRead           = "job:read"
	ActionTransactionIgnore = "transaction:ignore"
	ActionTransactionSearch = "transaction:search"
)

// Matching module actions.
const (
	ActionMatchRun         = "job:run"
	ActionMatchRead        = "job:read"
	ActionMatchDelete      = "job:delete"
	ActionManualMatch      = "manual:create"
	ActionAdjustmentCreate = "adjustment:create"
)

// Governance module actions.
const (
	ActionAuditRead   = "audit:read"
	ActionArchiveRead = "archive:read"

	// ActionActorMappingRead gates metadata reads that do not expose
	// PII. Retained for future list/index endpoints. The single cleartext-
	// returning read is now gated by ActionActorMappingDeanonymize so that
	// reviewing an actor roster and resolving a specific actor's identity
	// are separately auditable operations.
	ActionActorMappingRead   = "actor-mapping:read"
	ActionActorMappingWrite  = "actor-mapping:write"
	ActionActorMappingDelete = "actor-mapping:delete"

	// ActionActorMappingDeanonymize gates endpoints that resolve a hashed
	// actor ID back to its cleartext display name / email. Intentionally
	// distinct from ActionActorMappingRead: operators investigating a
	// specific audit entry can be granted this narrow permission without
	// receiving broader mapping access, and the audit log records which
	// individual exercised a de-anonymization step.
	ActionActorMappingDeanonymize = "actor-mapping:deanonymize"
)

// Reporting module actions.
const (
	ActionDashboardRead  = "dashboard:read"
	ActionExportRead     = "export:read"
	ActionExportJobWrite = "export-job:write"
	ActionExportJobRead  = "export-job:read"
)

// Discovery module actions.
const (
	ActionDiscoveryRead  = "discovery:read"
	ActionDiscoveryWrite = "discovery:write"
)

// Exception module actions.
const (
	ActionExceptionRead     = "exception:read"
	ActionExceptionResolve  = "exception:resolve"
	ActionExceptionDispatch = "exception:dispatch"
	ActionCallbackProcess   = "callback:process"
	ActionDisputeRead       = "dispute:read"
	ActionDisputeWrite      = "dispute:write"
	ActionCommentWrite      = "comment:write"
)
