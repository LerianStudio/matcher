//go:build e2e

package client

import "time"

// Context represents a reconciliation context.
type Context struct {
	ID               string    `json:"id"`
	TenantID         string    `json:"tenantId"`
	Name             string    `json:"name"`
	Type             string    `json:"type"`
	Interval         string    `json:"interval"`
	Status           string    `json:"status,omitempty"`
	Description      string    `json:"description,omitempty"`
	FeeNormalization string    `json:"feeNormalization,omitempty"`
	CreatedAt        time.Time `json:"createdAt"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

// CreateContextRequest is the payload for creating a context.
type CreateContextRequest struct {
	Name             string                             `json:"name"`
	Type             string                             `json:"type"`
	Interval         string                             `json:"interval"`
	Description      string                             `json:"description,omitempty"`
	FeeNormalization string                             `json:"feeNormalization,omitempty"`
	Sources          []CreateContextInlineSourceRequest `json:"sources,omitempty"`
	Rules            []CreateContextInlineRuleRequest   `json:"rules,omitempty"`
}

// CreateContextInlineSourceRequest is an inline source payload for context creation.
type CreateContextInlineSourceRequest struct {
	Name    string         `json:"name"`
	Type    string         `json:"type"`
	Side    string         `json:"side"`
	Config  map[string]any `json:"config,omitempty"`
	Mapping map[string]any `json:"mapping,omitempty"`
}

// CreateContextInlineRuleRequest is an inline rule payload for context creation.
type CreateContextInlineRuleRequest struct {
	Priority int            `json:"priority"`
	Type     string         `json:"type"`
	Config   map[string]any `json:"config,omitempty"`
}

// UpdateContextRequest is the payload for updating a context.
type UpdateContextRequest struct {
	Name        *string `json:"name,omitempty"`
	Interval    *string `json:"interval,omitempty"`
	Description *string `json:"description,omitempty"`
	Status      *string `json:"status,omitempty"`
}

// Source represents a reconciliation source.
type Source struct {
	ID        string         `json:"id"`
	ContextID string         `json:"contextId"`
	Name      string         `json:"name"`
	Type      string         `json:"type"`
	Side      string         `json:"side"`
	Config    map[string]any `json:"config,omitempty"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

// CreateSourceRequest is the payload for creating a source.
type CreateSourceRequest struct {
	Name   string         `json:"name"`
	Type   string         `json:"type"`
	Side   string         `json:"side"`
	Config map[string]any `json:"config,omitempty"`
}

// UpdateSourceRequest is the payload for updating a source.
type UpdateSourceRequest struct {
	Name   *string        `json:"name,omitempty"`
	Side   *string        `json:"side,omitempty"`
	Config map[string]any `json:"config,omitempty"`
}

// FieldMap represents a field mapping configuration.
type FieldMap struct {
	ID        string         `json:"id"`
	ContextID string         `json:"contextId"`
	SourceID  string         `json:"sourceId"`
	Mapping   map[string]any `json:"mapping"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

// CreateFieldMapRequest is the payload for creating a field map.
type CreateFieldMapRequest struct {
	Mapping map[string]any `json:"mapping"`
}

// UpdateFieldMapRequest is the payload for updating a field map.
type UpdateFieldMapRequest struct {
	Mapping map[string]any `json:"mapping"`
}

// FeeRuleResponse represents a fee rule.
type FeeRuleResponse struct {
	ID            string                     `json:"id"`
	ContextID     string                     `json:"contextId"`
	Side          string                     `json:"side"`
	FeeScheduleID string                     `json:"feeScheduleId"`
	Name          string                     `json:"name"`
	Priority      int                        `json:"priority"`
	Predicates    []FeeRulePredicateResponse `json:"predicates"`
	CreatedAt     time.Time                  `json:"createdAt"`
	UpdatedAt     time.Time                  `json:"updatedAt"`
}

// FeeRulePredicateResponse represents a fee rule predicate.
type FeeRulePredicateResponse struct {
	Field    string   `json:"field"`
	Operator string   `json:"operator"`
	Value    string   `json:"value,omitempty"`
	Values   []string `json:"values,omitempty"`
}

// CreateFeeRuleRequest is the payload for creating a fee rule.
type CreateFeeRuleRequest struct {
	Side          string                          `json:"side"`
	FeeScheduleID string                          `json:"feeScheduleId"`
	Name          string                          `json:"name"`
	Priority      int                             `json:"priority"`
	Predicates    []CreateFeeRulePredicateRequest `json:"predicates,omitempty"`
}

// CreateFeeRulePredicateRequest is a single predicate in a fee rule create request.
type CreateFeeRulePredicateRequest struct {
	Field    string   `json:"field"`
	Operator string   `json:"operator"`
	Value    string   `json:"value,omitempty"`
	Values   []string `json:"values,omitempty"`
}

// UpdateFeeRuleRequest is the payload for updating a fee rule.
type UpdateFeeRuleRequest struct {
	Side          *string                          `json:"side,omitempty"`
	FeeScheduleID *string                          `json:"feeScheduleId,omitempty"`
	Name          *string                          `json:"name,omitempty"`
	Priority      *int                             `json:"priority,omitempty"`
	Predicates    *[]CreateFeeRulePredicateRequest `json:"predicates,omitempty"`
}

// MatchRule represents a matching rule.
type MatchRule struct {
	ID        string         `json:"id"`
	ContextID string         `json:"contextId"`
	Priority  int            `json:"priority"`
	Type      string         `json:"type"`
	Config    map[string]any `json:"config,omitempty"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

// CreateMatchRuleRequest is the payload for creating a match rule.
type CreateMatchRuleRequest struct {
	Priority int            `json:"priority"`
	Type     string         `json:"type"`
	Config   map[string]any `json:"config,omitempty"`
}

// UpdateMatchRuleRequest is the payload for updating a match rule.
type UpdateMatchRuleRequest struct {
	Priority *int           `json:"priority,omitempty"`
	Config   map[string]any `json:"config,omitempty"`
}

// ReorderMatchRulesRequest is the payload for reordering match rules.
type ReorderMatchRulesRequest struct {
	RuleIDs []string `json:"ruleIds"`
}

// IngestionJob represents an ingestion job.
type IngestionJob struct {
	ID          string    `json:"id"`
	ContextID   string    `json:"contextId"`
	SourceID    string    `json:"sourceId"`
	FileName    string    `json:"fileName"`
	Status      string    `json:"status"`
	TotalRows   int       `json:"totalRows"`
	FailedRows  int       `json:"failedRows"`
	StartedAt   time.Time `json:"startedAt,omitempty"`
	CompletedAt time.Time `json:"completedAt,omitempty"`
	CreatedAt   time.Time `json:"createdAt"`
}

// Transaction represents an ingested transaction.
type Transaction struct {
	ID               string         `json:"id"`
	JobID            string         `json:"jobId"`
	SourceID         string         `json:"sourceId"`
	ContextID        string         `json:"contextId"`
	ExternalID       string         `json:"externalId"`
	Amount           string         `json:"amount"`
	Currency         string         `json:"currency"`
	Date             time.Time      `json:"date"`
	Description      string         `json:"description,omitempty"`
	Status           string         `json:"status"`
	ExtractionStatus string         `json:"extractionStatus"`
	Metadata         map[string]any `json:"metadata,omitempty"`
	CreatedAt        time.Time      `json:"createdAt"`
	UpdatedAt        time.Time      `json:"updatedAt"`
}

// MatchRun represents a matching run.
type MatchRun struct {
	ID          string         `json:"id"`
	ContextID   string         `json:"contextId"`
	Mode        string         `json:"mode"`
	Status      string         `json:"status"`
	Stats       map[string]int `json:"stats,omitempty"`
	StartedAt   time.Time      `json:"startedAt,omitempty"`
	CompletedAt time.Time      `json:"completedAt,omitempty"`
	CreatedAt   time.Time      `json:"createdAt"`
	UpdatedAt   time.Time      `json:"updatedAt"`
}

// RunMatchRequest is the payload for triggering a match run.
type RunMatchRequest struct {
	Mode string `json:"mode"`
}

// RunMatchResponse is the response from triggering a match run.
type RunMatchResponse struct {
	RunID  string `json:"runId"`
	Status string `json:"status"`
}

// MatchGroup represents a group of matched transactions.
type MatchGroup struct {
	ID         string      `json:"id"`
	ContextID  string      `json:"contextId"`
	RunID      string      `json:"runId"`
	RuleID     string      `json:"ruleId"`
	Confidence float64     `json:"confidence"`
	Items      []MatchItem `json:"items,omitempty"`
	CreatedAt  time.Time   `json:"createdAt"`
}

// MatchItem represents an item within a match group.
type MatchItem struct {
	ID            string    `json:"id"`
	MatchGroupID  string    `json:"matchGroupId"`
	TransactionID string    `json:"transactionId"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	Contribution  string    `json:"contribution"`
	CreatedAt     time.Time `json:"createdAt"`
}

// VolumeStatsResponse represents volume statistics.
type VolumeStatsResponse struct {
	TotalTransactions   int    `json:"totalTransactions"`
	MatchedTransactions int    `json:"matchedTransactions"`
	UnmatchedCount      int    `json:"unmatchedCount"`
	TotalAmount         string `json:"totalAmount"`
	MatchedAmount       string `json:"matchedAmount"`
	UnmatchedAmount     string `json:"unmatchedAmount"`
	PeriodStart         string `json:"periodStart"`
	PeriodEnd           string `json:"periodEnd"`
}

// MatchRateStatsResponse represents match rate statistics.
type MatchRateStatsResponse struct {
	MatchRate       float64 `json:"matchRate"`
	MatchRateAmount float64 `json:"matchRateAmount"`
	TotalCount      int     `json:"totalCount"`
	MatchedCount    int     `json:"matchedCount"`
	UnmatchedCount  int     `json:"unmatchedCount"`
}

// SLAStatsResponse represents SLA statistics.
type SLAStatsResponse struct {
	TotalExceptions     int     `json:"totalExceptions"`
	ResolvedOnTime      int     `json:"resolvedOnTime"`
	ResolvedLate        int     `json:"resolvedLate"`
	PendingWithinSLA    int     `json:"pendingWithinSla"`
	PendingOverdue      int     `json:"pendingOverdue"`
	SLAComplianceRate   float64 `json:"slaComplianceRate"`
	AverageResolutionMs int64   `json:"averageResolutionMs"`
}

// DashboardAggregates represents dashboard summary data.
type DashboardAggregates struct {
	Volume    *VolumeStatsResponse    `json:"volume"`
	MatchRate *MatchRateStatsResponse `json:"matchRate"`
	SLA       *SLAStatsResponse       `json:"sla"`
	UpdatedAt string                  `json:"updatedAt"`
}

// VolumeStats represents volume statistics.
type VolumeStats struct {
	Period        string `json:"period"`
	TotalVolume   string `json:"totalVolume"`
	MatchedVolume string `json:"matchedVolume"`
}

// MatchRateStats represents match rate statistics.
type MatchRateStats struct {
	Period    string  `json:"period"`
	MatchRate float64 `json:"matchRate"`
}

// SLAStats represents SLA statistics.
type SLAStats struct {
	AverageMatchTime  string  `json:"averageMatchTime"`
	SLAComplianceRate float64 `json:"slaComplianceRate"`
}

// CreateExportJobResponse represents the response from creating an export job.
type CreateExportJobResponse struct {
	JobID     string `json:"jobId"`
	Status    string `json:"status"`
	StatusURL string `json:"statusUrl"`
}

// ExportJob represents an export job (for GetExportJob responses).
type ExportJob struct {
	ID             string  `json:"id"`
	ReportType     string  `json:"reportType"`
	Format         string  `json:"format"`
	Status         string  `json:"status"`
	RecordsWritten int64   `json:"recordsWritten"`
	BytesWritten   int64   `json:"bytesWritten"`
	FileName       *string `json:"fileName,omitempty"`
	Error          *string `json:"error,omitempty"`
	CreatedAt      string  `json:"createdAt"`
	StartedAt      *string `json:"startedAt,omitempty"`
	FinishedAt     *string `json:"finishedAt,omitempty"`
	ExpiresAt      string  `json:"expiresAt"`
	DownloadURL    *string `json:"downloadUrl,omitempty"`
}

// CreateExportJobRequest is the payload for creating an export job.
type CreateExportJobRequest struct {
	ReportType string  `json:"reportType"`
	Format     string  `json:"format"`
	DateFrom   string  `json:"dateFrom"`
	DateTo     string  `json:"dateTo"`
	SourceID   *string `json:"sourceId,omitempty"`
}

// AuditLog represents an audit log entry.
type AuditLog struct {
	ID         string         `json:"id"`
	EntityType string         `json:"entityType"`
	EntityID   string         `json:"entityId"`
	Action     string         `json:"action"`
	ActorID    string         `json:"actorId,omitempty"`
	Changes    map[string]any `json:"changes,omitempty"`
	CreatedAt  time.Time      `json:"createdAt"`
}

// ListResponse is a generic paginated list response.
type ListResponse[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"nextCursor,omitempty"`
	HasMore    bool   `json:"hasMore"`
}

// ErrorResponse represents an API error response.
type ErrorResponse struct {
	Code    int    `json:"code"`
	Title   string `json:"title,omitempty"`
	Message string `json:"message"`
	Error   string `json:"error,omitempty"`
	Details any    `json:"details,omitempty"`
}

// FeeScheduleResponse represents a fee schedule.
type FeeScheduleResponse struct {
	ID               string                    `json:"id"`
	TenantID         string                    `json:"tenantId"`
	Name             string                    `json:"name"`
	Currency         string                    `json:"currency"`
	ApplicationOrder string                    `json:"applicationOrder"`
	RoundingScale    int                       `json:"roundingScale"`
	RoundingMode     string                    `json:"roundingMode"`
	Items            []FeeScheduleItemResponse `json:"items"`
	CreatedAt        time.Time                 `json:"createdAt"`
	UpdatedAt        time.Time                 `json:"updatedAt"`
}

// FeeScheduleItemResponse represents a fee schedule item.
type FeeScheduleItemResponse struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Priority      int       `json:"priority"`
	StructureType string    `json:"structureType"`
	Structure     any       `json:"structure"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

// CreateFeeScheduleRequest is the payload for creating a fee schedule.
type CreateFeeScheduleRequest struct {
	Name             string                         `json:"name"`
	Currency         string                         `json:"currency"`
	ApplicationOrder string                         `json:"applicationOrder"`
	RoundingScale    int                            `json:"roundingScale"`
	RoundingMode     string                         `json:"roundingMode"`
	Items            []CreateFeeScheduleItemRequest `json:"items"`
}

// CreateFeeScheduleItemRequest is a single item in the create request.
type CreateFeeScheduleItemRequest struct {
	Name          string         `json:"name"`
	Priority      int            `json:"priority"`
	StructureType string         `json:"structureType"`
	Structure     map[string]any `json:"structure"`
}

// UpdateFeeScheduleRequest is the payload for updating a fee schedule.
type UpdateFeeScheduleRequest struct {
	Name             *string `json:"name,omitempty"`
	ApplicationOrder *string `json:"applicationOrder,omitempty"`
	RoundingScale    *int    `json:"roundingScale,omitempty"`
	RoundingMode     *string `json:"roundingMode,omitempty"`
}

// SimulateFeeRequest is the payload for simulating fee calculation.
type SimulateFeeRequest struct {
	GrossAmount string `json:"grossAmount"`
	Currency    string `json:"currency"`
}

// SimulateFeeResponse is the response for fee simulation.
type SimulateFeeResponse struct {
	GrossAmount string            `json:"grossAmount"`
	NetAmount   string            `json:"netAmount"`
	TotalFee    string            `json:"totalFee"`
	Currency    string            `json:"currency"`
	Items       []SimulateFeeItem `json:"items"`
}

// SimulateFeeItem represents a single fee item in simulation response.
type SimulateFeeItem struct {
	Name     string `json:"name"`
	Fee      string `json:"fee"`
	BaseUsed string `json:"baseUsed"`
}

// DisputeResponse represents a dispute.
type DisputeResponse struct {
	ID           string             `json:"id"`
	ExceptionID  string             `json:"exceptionId"`
	State        string             `json:"state"`
	Category     string             `json:"category"`
	Description  string             `json:"description"`
	OpenedBy     string             `json:"openedBy,omitempty"`
	Resolution   string             `json:"resolution,omitempty"`
	ReopenReason string             `json:"reopenReason,omitempty"`
	Evidence     []EvidenceResponse `json:"evidence,omitempty"`
	CreatedAt    time.Time          `json:"createdAt"`
	UpdatedAt    time.Time          `json:"updatedAt"`
}

// EvidenceResponse represents evidence attached to a dispute.
type EvidenceResponse struct {
	ID        string    `json:"id"`
	Comment   string    `json:"comment"`
	FileURL   string    `json:"fileUrl,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

// OpenDisputeRequest is the payload for opening a dispute on an exception.
type OpenDisputeRequest struct {
	Category    string `json:"category"`
	Description string `json:"description"`
}

// CloseDisputeRequest is the payload for closing a dispute.
type CloseDisputeRequest struct {
	Resolution string `json:"resolution"`
	Won        *bool  `json:"won,omitempty"`
}

// SubmitEvidenceRequest is the payload for submitting evidence to a dispute.
type SubmitEvidenceRequest struct {
	Comment string `json:"comment"`
	FileURL string `json:"fileUrl,omitempty"`
}

// IgnoreTransactionRequest is the payload for ignoring a transaction.
type IgnoreTransactionRequest struct {
	Reason string `json:"reason"`
}

// IgnoreTransactionResponse represents the response from ignoring a transaction.
type IgnoreTransactionResponse struct {
	ID               string    `json:"id"`
	JobID            string    `json:"jobId"`
	SourceID         string    `json:"sourceId"`
	ContextID        string    `json:"contextId"`
	ExternalID       string    `json:"externalId"`
	Amount           string    `json:"amount"`
	Currency         string    `json:"currency"`
	Date             time.Time `json:"date"`
	Description      string    `json:"description,omitempty"`
	Status           string    `json:"status"`
	ExtractionStatus string    `json:"extractionStatus"`
	CreatedAt        time.Time `json:"createdAt"`
}

// ManualMatchRequest is the payload for creating a manual match.
type ManualMatchRequest struct {
	TransactionIDs []string `json:"transactionIds"`
	Notes          string   `json:"notes,omitempty"`
}

// ManualMatchResponse is the response from creating a manual match.
type ManualMatchResponse struct {
	MatchGroup MatchGroup `json:"matchGroup"`
}

// CreateAdjustmentRequest is the payload for creating an adjustment.
type CreateAdjustmentRequest struct {
	Type          string `json:"type"`
	Amount        string `json:"amount"`
	Currency      string `json:"currency"`
	Direction     string `json:"direction"`
	Reason        string `json:"reason"`
	Description   string `json:"description"`
	TransactionID string `json:"transactionId,omitempty"`
	MatchGroupID  string `json:"matchGroupId,omitempty"`
}

// AdjustmentResponse is the response from creating an adjustment.
type AdjustmentResponse struct {
	Adjustment map[string]any `json:"adjustment"`
}

// UnmatchRequest is the payload for breaking a match group.
type UnmatchRequest struct {
	Reason string `json:"reason"`
}

// ArchiveMetadata represents archive metadata in list responses.
type ArchiveMetadata struct {
	ID        string `json:"id"`
	StartDate string `json:"startDate,omitempty"`
	EndDate   string `json:"endDate,omitempty"`
	Status    string `json:"status,omitempty"`
	CreatedAt string `json:"createdAt,omitempty"`
}

// ArchiveDownloadResponse represents the download URL for an archive.
// NOTE: snake_case JSON tags match the governance handler's ArchiveDownloadResponse
// (internal/governance/adapters/http/handlers_archive.go) which uses snake_case.
type ArchiveDownloadResponse struct {
	DownloadURL string `json:"download_url"`
	ExpiresAt   string `json:"expires_at"`
	Checksum    string `json:"checksum"`
}

// DashboardMetricsResponse represents comprehensive dashboard metrics.
type DashboardMetricsResponse struct {
	Summary    map[string]any `json:"summary"`
	Trends     map[string]any `json:"trends"`
	Breakdowns map[string]any `json:"breakdowns"`
	UpdatedAt  string         `json:"updatedAt"`
}

// CloneContextRequest is the payload for cloning a context.
type CloneContextRequest struct {
	Name           string `json:"name"`
	IncludeSources *bool  `json:"includeSources,omitempty"`
	IncludeRules   *bool  `json:"includeRules,omitempty"`
}

// CloneContextResponse is the response from cloning a context.
type CloneContextResponse struct {
	Context         Context `json:"context"`
	SourcesCloned   int     `json:"sourcesCloned"`
	RulesCloned     int     `json:"rulesCloned"`
	FeeRulesCloned  int     `json:"feeRulesCloned"`
	FieldMapsCloned int     `json:"fieldMapsCloned"`
}

// ScheduleResponse represents a reconciliation schedule.
type ScheduleResponse struct {
	ID             string     `json:"id"`
	ContextID      string     `json:"contextId"`
	CronExpression string     `json:"cronExpression"`
	Enabled        bool       `json:"enabled"`
	LastRunAt      *time.Time `json:"lastRunAt,omitempty"`
	NextRunAt      *time.Time `json:"nextRunAt,omitempty"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
}

// CreateScheduleRequest is the payload for creating a schedule.
type CreateScheduleRequest struct {
	CronExpression string `json:"cronExpression"`
	Enabled        bool   `json:"enabled"`
}

// UpdateScheduleRequest is the payload for updating a schedule.
type UpdateScheduleRequest struct {
	CronExpression *string `json:"cronExpression,omitempty"`
	Enabled        *bool   `json:"enabled,omitempty"`
}

// FilePreviewResponse represents a file preview result.
type FilePreviewResponse struct {
	Columns    []string   `json:"columns"`
	SampleRows [][]string `json:"sampleRows"`
	RowCount   int        `json:"rowCount"`
	Format     string     `json:"format"`
}

// SearchTransactionsParams contains query parameters for transaction search.
type SearchTransactionsParams struct {
	Query     string
	AmountMin string
	AmountMax string
	DateFrom  string
	DateTo    string
	Reference string
	Currency  string
	SourceID  string
	Status    string
	Limit     int
	Offset    int
}

// SearchTransactionsResponse represents the transaction search response.
type SearchTransactionsResponse struct {
	Items  []Transaction `json:"items"`
	Total  int           `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

// SourceBreakdownResponse represents source-level reconciliation metrics.
type SourceBreakdownResponse struct {
	Items []SourceBreakdownItem `json:"items"`
}

// SourceBreakdownItem represents metrics for a single source.
type SourceBreakdownItem struct {
	SourceID       string `json:"sourceId"`
	SourceName     string `json:"sourceName"`
	TotalCount     int    `json:"totalCount"`
	MatchedCount   int    `json:"matchedCount"`
	UnmatchedCount int    `json:"unmatchedCount"`
	PendingCount   int    `json:"pendingCount"`
}

// CashImpactResponse represents unreconciled financial exposure.
type CashImpactResponse struct {
	TotalUnreconciledAmount string             `json:"totalUnreconciledAmount"`
	CurrencyExposures       []CurrencyExposure `json:"currencyExposures"`
	AgeExposures            []AgeExposure      `json:"ageExposures"`
}

// CurrencyExposure represents exposure for a single currency.
type CurrencyExposure struct {
	Currency string `json:"currency"`
	Amount   string `json:"amount"`
	Count    int    `json:"count"`
}

// AgeExposure represents exposure by age bucket.
type AgeExposure struct {
	Bucket string `json:"bucket"`
	Amount string `json:"amount"`
	Count  int    `json:"count"`
}

// ExportCountResponse represents a record count for export decision.
type ExportCountResponse struct {
	Count int `json:"count"`
}

// BulkAssignRequest is the payload for bulk assigning exceptions.
type BulkAssignRequest struct {
	ExceptionIDs []string `json:"exception_ids"`
	Assignee     string   `json:"assignee"`
}

// BulkResolveRequest is the payload for bulk resolving exceptions.
type BulkResolveRequest struct {
	ExceptionIDs []string `json:"exception_ids"`
	Resolution   string   `json:"resolution"`
	Reason       string   `json:"reason,omitempty"`
}

// BulkDispatchRequest is the payload for bulk dispatching exceptions.
type BulkDispatchRequest struct {
	ExceptionIDs []string `json:"exception_ids"`
	TargetSystem string   `json:"target_system"`
	Queue        string   `json:"queue,omitempty"`
}

// BulkActionResponse is the response from a bulk exception operation.
type BulkActionResponse struct {
	Succeeded []string      `json:"succeeded"`
	Failed    []BulkFailure `json:"failed"`
	Total     int           `json:"total"`
}

// BulkFailure represents a single failed item in a bulk operation.
type BulkFailure struct {
	ExceptionID string `json:"exception_id"`
	Error       string `json:"error"`
}

// AddCommentRequest is the payload for adding a comment to an exception.
type AddCommentRequest struct {
	Content string `json:"content"`
}

// CommentResponse represents an exception comment.
type CommentResponse struct {
	ID          string    `json:"id"`
	ExceptionID string    `json:"exceptionId"`
	Author      string    `json:"author"`
	Content     string    `json:"content"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// ListCommentsResponse represents the response for listing comments.
type ListCommentsResponse struct {
	Items []CommentResponse `json:"items"`
}

// ListDisputesResponse represents the response for listing disputes.
type ListDisputesResponse struct {
	Items      []DisputeResponse `json:"items"`
	NextCursor string            `json:"nextCursor,omitempty"`
	HasMore    bool              `json:"hasMore"`
}
