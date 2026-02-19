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
	ActionAuditRead          = "audit:read"
	ActionArchiveRead        = "archive:read"
	ActionActorMappingRead   = "actor-mapping:read"
	ActionActorMappingWrite  = "actor-mapping:write"
	ActionActorMappingDelete = "actor-mapping:delete"
)

// Reporting module actions.
const (
	ActionDashboardRead  = "dashboard:read"
	ActionExportRead     = "export:read"
	ActionExportJobWrite = "export-job:write"
	ActionExportJobRead  = "export-job:read"
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
