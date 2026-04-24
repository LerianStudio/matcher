// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package constant

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatcherCodesUseMTCHPrefix(t *testing.T) {
	t.Parallel()

	for _, code := range allMatcherCodes() {
		require.True(t, strings.HasPrefix(code, "MTCH-"))
	}
}

func TestMatcherCodesAreUniqueAndWellFormed(t *testing.T) {
	t.Parallel()

	matcherCodePattern := regexp.MustCompile(`^MTCH-\d{4}$`)
	seen := make(map[string]struct{})

	for _, code := range allMatcherCodes() {
		require.Regexp(t, matcherCodePattern, code)
		_, exists := seen[code]
		require.Falsef(t, exists, "duplicate matcher code %s", code)
		seen[code] = struct{}{}
	}
}

func allMatcherCodes() []string {
	return []string{
		CodeInvalidRequest,
		CodeInternalServerError,
		CodeUnauthorized,
		CodeForbidden,
		CodeNotFound,
		CodeUnprocessableEntity,
		CodeConflict,
		CodeServiceUnavailable,
		CodeRateLimitExceeded,
		CodeRequestEntityTooLarge,
		CodeRequestFailed,
		CodeContextNotActive,
		CodeInvalidContextID,
		CodeInvalidIdempotencyKey,
		CodeIdempotencyConfiguration,
		CodeIdempotencyError,
		CodeRequestInProgress,
		CodeObjectStorageUnavailable,
		CodeConfigurationDuplicateName,
		CodeConfigurationPriorityConflict,
		CodeConfigurationInvalidState,
		CodeConfigurationArchivedContext,
		CodeConfigurationHasFieldMap,
		CodeConfigurationHasChildren,
		CodeConfigurationDuplicatePriority,
		CodeConfigurationFeeScheduleInUse,
		CodeConfigurationContextNameRequired,
		CodeConfigurationContextNotFound,
		CodeConfigurationSourceNotFound,
		CodeConfigurationFieldMapNotFound,
		CodeConfigurationMatchRuleNotFound,
		CodeConfigurationFeeRuleNotFound,
		CodeConfigurationFeeScheduleNotFound,
		CodeConfigurationScheduleNotFound,
		CodeDiscoveryConnectionNotFound,
		CodeDiscoveryExtractionNotFound,
		CodeDiscoveryFetcherUnavailable,
		CodeDiscoveryInvalidExtraction,
		CodeDiscoveryRefreshInProgress,
		CodeIngestionSourceNotFound,
		CodeIngestionFieldMapNotFound,
		CodeIngestionFormatRequired,
		CodeIngestionEmptyFile,
		CodeIngestionInvalidState,
		CodeIngestionJobNotFound,
		CodeMatchingNoSourcesConfigured,
		CodeMatchingAtLeastTwoSources,
		CodeMatchingSourceSideRequired,
		CodeMatchingInvalidOneToOneTopology,
		CodeMatchingInvalidOneToManyTopology,
		CodeMatchingFeeRulesMissing,
		CodeMatchingFeeRulesMisconfigured,
		CodeMatchingRunInProgress,
		CodeExceptionNotFound,
		CodeDisputeNotFound,
		CodeExceptionInvalidState,
		CodeCallbackInProgress,
		CodeCallbackRetryable,
		CodeCallbackRateLimitExceeded,
		CodeCommentNotFound,
		CodeDispatchTargetUnsupported,
		CodeDispatchConnectorNotConfigured,
		CodeGovernanceAuditLogNotFound,
		CodeGovernanceActorMappingNotFound,
		CodeGovernanceArchiveNotFound,
		CodeReportingExportJobNotFound,
		CodeReportingExportWorkerDisabled,
		CodeReportingExportNotReady,
		CodeReportingExportExpired,
		CodeReportingInvalidExportFormat,
	}
}
