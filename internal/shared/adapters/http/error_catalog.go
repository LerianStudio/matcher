package http

import (
	"net/http"

	matchererrors "github.com/LerianStudio/matcher/pkg"
	"github.com/LerianStudio/matcher/pkg/constant"
)

var (
	defaultInternalErrorMessage = "an unexpected error occurred"

	defInvalidRequest           = matchererrors.Definition{Code: constant.CodeInvalidRequest, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defInternalServerError      = matchererrors.Definition{Code: constant.CodeInternalServerError, Title: http.StatusText(http.StatusInternalServerError), HTTPStatus: http.StatusInternalServerError}
	defUnauthorized             = matchererrors.Definition{Code: constant.CodeUnauthorized, Title: http.StatusText(http.StatusUnauthorized), HTTPStatus: http.StatusUnauthorized}
	defForbidden                = matchererrors.Definition{Code: constant.CodeForbidden, Title: http.StatusText(http.StatusForbidden), HTTPStatus: http.StatusForbidden}
	defNotFound                 = matchererrors.Definition{Code: constant.CodeNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defUnprocessableEntity      = matchererrors.Definition{Code: constant.CodeUnprocessableEntity, Title: http.StatusText(http.StatusUnprocessableEntity), HTTPStatus: http.StatusUnprocessableEntity}
	defConflict                 = matchererrors.Definition{Code: constant.CodeConflict, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defServiceUnavailable       = matchererrors.Definition{Code: constant.CodeServiceUnavailable, Title: http.StatusText(http.StatusServiceUnavailable), HTTPStatus: http.StatusServiceUnavailable}
	defRateLimitExceeded        = matchererrors.Definition{Code: constant.CodeRateLimitExceeded, Title: http.StatusText(http.StatusTooManyRequests), HTTPStatus: http.StatusTooManyRequests}
	defRequestEntityTooLarge    = matchererrors.Definition{Code: constant.CodeRequestEntityTooLarge, Title: http.StatusText(http.StatusRequestEntityTooLarge), HTTPStatus: http.StatusRequestEntityTooLarge}
	defRequestFailed            = matchererrors.Definition{Code: constant.CodeRequestFailed, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defContextNotActive         = matchererrors.Definition{Code: constant.CodeContextNotActive, Title: http.StatusText(http.StatusForbidden), HTTPStatus: http.StatusForbidden}
	defInvalidContextID         = matchererrors.Definition{Code: constant.CodeInvalidContextID, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defInvalidIdempotencyKey    = matchererrors.Definition{Code: constant.CodeInvalidIdempotencyKey, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defIdempotencyConfiguration = matchererrors.Definition{Code: constant.CodeIdempotencyConfiguration, Title: http.StatusText(http.StatusInternalServerError), HTTPStatus: http.StatusInternalServerError}
	defIdempotencyError         = matchererrors.Definition{Code: constant.CodeIdempotencyError, Title: http.StatusText(http.StatusInternalServerError), HTTPStatus: http.StatusInternalServerError}
	defRequestInProgress        = matchererrors.Definition{Code: constant.CodeRequestInProgress, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defObjectStorageUnavailable = matchererrors.Definition{Code: constant.CodeObjectStorageUnavailable, Title: http.StatusText(http.StatusServiceUnavailable), HTTPStatus: http.StatusServiceUnavailable}

	defConfigurationDuplicateName       = matchererrors.Definition{Code: constant.CodeConfigurationDuplicateName, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defConfigurationPriorityConflict    = matchererrors.Definition{Code: constant.CodeConfigurationPriorityConflict, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defConfigurationInvalidState        = matchererrors.Definition{Code: constant.CodeConfigurationInvalidState, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defConfigurationArchivedContext     = matchererrors.Definition{Code: constant.CodeConfigurationArchivedContext, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defConfigurationHasFieldMap         = matchererrors.Definition{Code: constant.CodeConfigurationHasFieldMap, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defConfigurationHasChildren         = matchererrors.Definition{Code: constant.CodeConfigurationHasChildren, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defConfigurationDuplicatePriority   = matchererrors.Definition{Code: constant.CodeConfigurationDuplicatePriority, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defConfigurationFeeScheduleInUse    = matchererrors.Definition{Code: constant.CodeConfigurationFeeScheduleInUse, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defConfigurationContextNameRequired = matchererrors.Definition{Code: constant.CodeConfigurationContextNameRequired, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defConfigurationContextNotFound     = matchererrors.Definition{Code: constant.CodeConfigurationContextNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defConfigurationSourceNotFound      = matchererrors.Definition{Code: constant.CodeConfigurationSourceNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defConfigurationFieldMapNotFound    = matchererrors.Definition{Code: constant.CodeConfigurationFieldMapNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defConfigurationMatchRuleNotFound   = matchererrors.Definition{Code: constant.CodeConfigurationMatchRuleNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defConfigurationFeeRuleNotFound     = matchererrors.Definition{Code: constant.CodeConfigurationFeeRuleNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defConfigurationFeeScheduleNotFound = matchererrors.Definition{Code: constant.CodeConfigurationFeeScheduleNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defConfigurationScheduleNotFound    = matchererrors.Definition{Code: constant.CodeConfigurationScheduleNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}

	defDiscoveryConnectionNotFound = matchererrors.Definition{Code: constant.CodeDiscoveryConnectionNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defDiscoveryExtractionNotFound = matchererrors.Definition{Code: constant.CodeDiscoveryExtractionNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defDiscoveryInvalidExtraction  = matchererrors.Definition{Code: constant.CodeDiscoveryInvalidExtraction, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defDiscoveryRefreshInProgress  = matchererrors.Definition{Code: constant.CodeDiscoveryRefreshInProgress, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defDiscoveryFetcherUnavailable = matchererrors.Definition{Code: constant.CodeDiscoveryFetcherUnavailable, Title: http.StatusText(http.StatusServiceUnavailable), HTTPStatus: http.StatusServiceUnavailable}

	defIngestionSourceNotFound   = matchererrors.Definition{Code: constant.CodeIngestionSourceNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defIngestionFieldMapNotFound = matchererrors.Definition{Code: constant.CodeIngestionFieldMapNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defIngestionFormatRequired   = matchererrors.Definition{Code: constant.CodeIngestionFormatRequired, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defIngestionEmptyFile        = matchererrors.Definition{Code: constant.CodeIngestionEmptyFile, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defIngestionInvalidState     = matchererrors.Definition{Code: constant.CodeIngestionInvalidState, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defIngestionJobNotFound      = matchererrors.Definition{Code: constant.CodeIngestionJobNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}

	defMatchingNoSourcesConfigured      = matchererrors.Definition{Code: constant.CodeMatchingNoSourcesConfigured, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defMatchingAtLeastTwoSources        = matchererrors.Definition{Code: constant.CodeMatchingAtLeastTwoSources, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defMatchingSourceSideRequired       = matchererrors.Definition{Code: constant.CodeMatchingSourceSideRequired, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defMatchingInvalidOneToOneTopology  = matchererrors.Definition{Code: constant.CodeMatchingInvalidOneToOneTopology, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defMatchingInvalidOneToManyTopology = matchererrors.Definition{Code: constant.CodeMatchingInvalidOneToManyTopology, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}
	defMatchingFeeRulesMisconfigured    = matchererrors.Definition{Code: constant.CodeMatchingFeeRulesMisconfigured, Title: http.StatusText(http.StatusUnprocessableEntity), HTTPStatus: http.StatusUnprocessableEntity}
	defMatchingFeeRulesMissing          = matchererrors.Definition{Code: constant.CodeMatchingFeeRulesMissing, Title: http.StatusText(http.StatusUnprocessableEntity), HTTPStatus: http.StatusUnprocessableEntity}
	defMatchingRunInProgress            = matchererrors.Definition{Code: constant.CodeMatchingRunInProgress, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}

	defExceptionNotFound              = matchererrors.Definition{Code: constant.CodeExceptionNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defDisputeNotFound                = matchererrors.Definition{Code: constant.CodeDisputeNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defExceptionInvalidState          = matchererrors.Definition{Code: constant.CodeExceptionInvalidState, Title: http.StatusText(http.StatusUnprocessableEntity), HTTPStatus: http.StatusUnprocessableEntity}
	defCallbackInProgress             = matchererrors.Definition{Code: constant.CodeCallbackInProgress, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defCallbackRetryable              = matchererrors.Definition{Code: constant.CodeCallbackRetryable, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defCallbackRateLimitExceeded      = matchererrors.Definition{Code: constant.CodeCallbackRateLimitExceeded, Title: http.StatusText(http.StatusTooManyRequests), HTTPStatus: http.StatusTooManyRequests}
	defCommentNotFound                = matchererrors.Definition{Code: constant.CodeCommentNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defDispatchTargetUnsupported      = matchererrors.Definition{Code: constant.CodeDispatchTargetUnsupported, Title: http.StatusText(http.StatusUnprocessableEntity), HTTPStatus: http.StatusUnprocessableEntity}
	defDispatchConnectorNotConfigured = matchererrors.Definition{Code: constant.CodeDispatchConnectorNotConfigured, Title: http.StatusText(http.StatusUnprocessableEntity), HTTPStatus: http.StatusUnprocessableEntity}
	defGovernanceAuditLogNotFound     = matchererrors.Definition{Code: constant.CodeGovernanceAuditLogNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defGovernanceActorMappingNotFound = matchererrors.Definition{Code: constant.CodeGovernanceActorMappingNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defGovernanceArchiveNotFound      = matchererrors.Definition{Code: constant.CodeGovernanceArchiveNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}

	defReportingExportJobNotFound    = matchererrors.Definition{Code: constant.CodeReportingExportJobNotFound, Title: http.StatusText(http.StatusNotFound), HTTPStatus: http.StatusNotFound}
	defReportingExportWorkerDisabled = matchererrors.Definition{Code: constant.CodeReportingExportWorkerDisabled, Title: http.StatusText(http.StatusServiceUnavailable), HTTPStatus: http.StatusServiceUnavailable}
	defReportingExportNotReady       = matchererrors.Definition{Code: constant.CodeReportingExportNotReady, Title: http.StatusText(http.StatusConflict), HTTPStatus: http.StatusConflict}
	defReportingExportExpired        = matchererrors.Definition{Code: constant.CodeReportingExportExpired, Title: http.StatusText(http.StatusGone), HTTPStatus: http.StatusGone}
	defReportingInvalidExportFormat  = matchererrors.Definition{Code: constant.CodeReportingInvalidExportFormat, Title: http.StatusText(http.StatusBadRequest), HTTPStatus: http.StatusBadRequest}

	legacyFallbackDefinitionsByStatus = map[int]matchererrors.Definition{
		http.StatusBadRequest:            defInvalidRequest,
		http.StatusUnauthorized:          defUnauthorized,
		http.StatusForbidden:             defForbidden,
		http.StatusNotFound:              defNotFound,
		http.StatusConflict:              defConflict,
		http.StatusUnprocessableEntity:   defUnprocessableEntity,
		http.StatusTooManyRequests:       defRateLimitExceeded,
		http.StatusRequestEntityTooLarge: defRequestEntityTooLarge,
		http.StatusServiceUnavailable:    defServiceUnavailable,
	}
)

var legacyDefinitionsBySlug = map[string]matchererrors.Definition{
	"invalid_request":                       defInvalidRequest,
	"internal_server_error":                 defInternalServerError,
	"not_found":                             defNotFound,
	"unauthorized":                          defUnauthorized,
	"forbidden":                             defForbidden,
	"context_not_active":                    defContextNotActive,
	"duplicate_name":                        defConfigurationDuplicateName,
	"priority_conflict":                     defConfigurationPriorityConflict,
	"object_storage_unavailable":            defObjectStorageUnavailable,
	"conflict":                              defConflict,
	"unprocessable_entity":                  defUnprocessableEntity,
	"rate_limit_exceeded":                   defCallbackRateLimitExceeded,
	"invalid_state_transition":              defConfigurationInvalidState,
	"has_field_map":                         defConfigurationHasFieldMap,
	"has_children":                          defConfigurationHasChildren,
	"duplicate_priority":                    defConfigurationDuplicatePriority,
	"configuration_context_name_required":   defConfigurationContextNameRequired,
	"configuration_context_not_found":       defConfigurationContextNotFound,
	"configuration_source_not_found":        defConfigurationSourceNotFound,
	"configuration_field_map_not_found":     defConfigurationFieldMapNotFound,
	"configuration_match_rule_not_found":    defConfigurationMatchRuleNotFound,
	"configuration_fee_rule_not_found":      defConfigurationFeeRuleNotFound,
	"configuration_fee_schedule_not_found":  defConfigurationFeeScheduleNotFound,
	"configuration_schedule_not_found":      defConfigurationScheduleNotFound,
	"callback_retryable":                    defCallbackRetryable,
	"callback_in_progress":                  defCallbackInProgress,
	"archived_context":                      defConfigurationArchivedContext,
	"invalid_idempotency_key":               defInvalidIdempotencyKey,
	"idempotency_configuration_error":       defIdempotencyConfiguration,
	"idempotency_error":                     defIdempotencyError,
	"request_in_progress":                   defRequestInProgress,
	"request_entity_too_large":              defRequestEntityTooLarge,
	"request_failed":                        defRequestFailed,
	"fee_schedule_in_use":                   defConfigurationFeeScheduleInUse,
	"export_worker_disabled":                defReportingExportWorkerDisabled,
	"not_ready":                             defReportingExportNotReady,
	"expired":                               defReportingExportExpired,
	"fee_rules_misconfigured":               defMatchingFeeRulesMisconfigured,
	"fee_rules_missing":                     defMatchingFeeRulesMissing,
	"match_run_in_progress":                 defMatchingRunInProgress,
	"invalid_state":                         defIngestionInvalidState,
	"discovery_connection_not_found":        defDiscoveryConnectionNotFound,
	"discovery_extraction_not_found":        defDiscoveryExtractionNotFound,
	"discovery_invalid_extraction":          defDiscoveryInvalidExtraction,
	"discovery_fetcher_unavailable":         defDiscoveryFetcherUnavailable,
	"refresh_in_progress":                   defDiscoveryRefreshInProgress,
	"ingestion_source_not_found":            defIngestionSourceNotFound,
	"ingestion_field_map_not_found":         defIngestionFieldMapNotFound,
	"ingestion_format_required":             defIngestionFormatRequired,
	"ingestion_empty_file":                  defIngestionEmptyFile,
	"ingestion_job_not_found":               defIngestionJobNotFound,
	"matching_no_sources_configured":        defMatchingNoSourcesConfigured,
	"matching_at_least_two_sources":         defMatchingAtLeastTwoSources,
	"matching_source_side_required":         defMatchingSourceSideRequired,
	"matching_invalid_one_to_one_topology":  defMatchingInvalidOneToOneTopology,
	"matching_invalid_one_to_many_topology": defMatchingInvalidOneToManyTopology,
	"exception_not_found":                   defExceptionNotFound,
	"dispute_not_found":                     defDisputeNotFound,
	"exception_invalid_state":               defExceptionInvalidState,
	"comment_not_found":                     defCommentNotFound,
	"dispatch_target_unsupported":           defDispatchTargetUnsupported,
	"dispatch_connector_not_configured":     defDispatchConnectorNotConfigured,
	"governance_audit_log_not_found":        defGovernanceAuditLogNotFound,
	"governance_actor_mapping_not_found":    defGovernanceActorMappingNotFound,
	"governance_archive_not_found":          defGovernanceArchiveNotFound,
	"reporting_export_job_not_found":        defReportingExportJobNotFound,
	"reporting_invalid_export_format":       defReportingInvalidExportFormat,
}

func definitionFromLegacySlug(slug string) (matchererrors.Definition, bool) {
	definition, ok := legacyDefinitionsBySlug[slug]

	return definition, ok
}

func definitionForStatus(status int) matchererrors.Definition {
	if definition, ok := legacyFallbackDefinitionsByStatus[status]; ok {
		return definition
	}

	if status >= http.StatusBadRequest && status < http.StatusInternalServerError {
		title := http.StatusText(status)
		if title == "" {
			title = http.StatusText(http.StatusBadRequest)
		}

		return matchererrors.Definition{
			Code:       defRequestFailed.Code,
			Title:      title,
			HTTPStatus: status,
		}
	}

	return defInternalServerError
}
