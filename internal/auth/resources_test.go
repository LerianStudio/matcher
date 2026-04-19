//go:build unit

package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResourceConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{
			name:     "ResourceConfiguration",
			constant: ResourceConfiguration,
			expected: "configuration",
		},
		{
			name:     "ResourceIngestion",
			constant: ResourceIngestion,
			expected: "ingestion",
		},
		{
			name:     "ResourceMatching",
			constant: ResourceMatching,
			expected: "matching",
		},
		{
			name:     "ResourceGovernance",
			constant: ResourceGovernance,
			expected: "governance",
		},
		{
			name:     "ResourceReporting",
			constant: ResourceReporting,
			expected: "reporting",
		},
		{
			name:     "ResourceException",
			constant: ResourceException,
			expected: "exception",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.constant)
		})
	}
}

func TestStandardActionConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{
			name:     "ActionRead",
			constant: ActionRead,
			expected: "read",
		},
		{
			name:     "ActionWrite",
			constant: ActionWrite,
			expected: "write",
		},
		{
			name:     "ActionDelete",
			constant: ActionDelete,
			expected: "delete",
		},
		{
			name:     "ActionAdmin",
			constant: ActionAdmin,
			expected: "admin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.constant)
		})
	}
}

func TestConfigurationModuleActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{
			name:     "ActionContextCreate",
			constant: ActionContextCreate,
			expected: "context:create",
		},
		{
			name:     "ActionContextRead",
			constant: ActionContextRead,
			expected: "context:read",
		},
		{
			name:     "ActionContextUpdate",
			constant: ActionContextUpdate,
			expected: "context:update",
		},
		{
			name:     "ActionContextDelete",
			constant: ActionContextDelete,
			expected: "context:delete",
		},
		{
			name:     "ActionSourceCreate",
			constant: ActionSourceCreate,
			expected: "source:create",
		},
		{
			name:     "ActionSourceRead",
			constant: ActionSourceRead,
			expected: "source:read",
		},
		{
			name:     "ActionSourceUpdate",
			constant: ActionSourceUpdate,
			expected: "source:update",
		},
		{
			name:     "ActionSourceDelete",
			constant: ActionSourceDelete,
			expected: "source:delete",
		},
		{
			name:     "ActionFieldMapCreate",
			constant: ActionFieldMapCreate,
			expected: "field-map:create",
		},
		{
			name:     "ActionFieldMapRead",
			constant: ActionFieldMapRead,
			expected: "field-map:read",
		},
		{
			name:     "ActionFieldMapUpdate",
			constant: ActionFieldMapUpdate,
			expected: "field-map:update",
		},
		{
			name:     "ActionFieldMapDelete",
			constant: ActionFieldMapDelete,
			expected: "field-map:delete",
		},
		{
			name:     "ActionRuleCreate",
			constant: ActionRuleCreate,
			expected: "rule:create",
		},
		{
			name:     "ActionRuleRead",
			constant: ActionRuleRead,
			expected: "rule:read",
		},
		{
			name:     "ActionRuleUpdate",
			constant: ActionRuleUpdate,
			expected: "rule:update",
		},
		{
			name:     "ActionRuleDelete",
			constant: ActionRuleDelete,
			expected: "rule:delete",
		},
		{
			name:     "ActionFeeScheduleCreate",
			constant: ActionFeeScheduleCreate,
			expected: "fee-schedule:create",
		},
		{
			name:     "ActionFeeScheduleRead",
			constant: ActionFeeScheduleRead,
			expected: "fee-schedule:read",
		},
		{
			name:     "ActionFeeScheduleUpdate",
			constant: ActionFeeScheduleUpdate,
			expected: "fee-schedule:update",
		},
		{
			name:     "ActionFeeScheduleDelete",
			constant: ActionFeeScheduleDelete,
			expected: "fee-schedule:delete",
		},
		{
			name:     "ActionScheduleCreate",
			constant: ActionScheduleCreate,
			expected: "schedule:create",
		},
		{
			name:     "ActionScheduleRead",
			constant: ActionScheduleRead,
			expected: "schedule:read",
		},
		{
			name:     "ActionScheduleUpdate",
			constant: ActionScheduleUpdate,
			expected: "schedule:update",
		},
		{
			name:     "ActionScheduleDelete",
			constant: ActionScheduleDelete,
			expected: "schedule:delete",
		},
		{
			name:     "ActionFeeRuleCreate",
			constant: ActionFeeRuleCreate,
			expected: "fee-rule:create",
		},
		{
			name:     "ActionFeeRuleRead",
			constant: ActionFeeRuleRead,
			expected: "fee-rule:read",
		},
		{
			name:     "ActionFeeRuleUpdate",
			constant: ActionFeeRuleUpdate,
			expected: "fee-rule:update",
		},
		{
			name:     "ActionFeeRuleDelete",
			constant: ActionFeeRuleDelete,
			expected: "fee-rule:delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.constant)
			assert.Contains(t, tt.constant, ":")
		})
	}
}

func TestIngestionModuleActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{
			name:     "ActionImportCreate",
			constant: ActionImportCreate,
			expected: "import:create",
		},
		{
			name:     "ActionJobRead",
			constant: ActionJobRead,
			expected: "job:read",
		},
		{
			name:     "ActionTransactionIgnore",
			constant: ActionTransactionIgnore,
			expected: "transaction:ignore",
		},
		{
			name:     "ActionTransactionSearch",
			constant: ActionTransactionSearch,
			expected: "transaction:search",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.constant)
			assert.Contains(t, tt.constant, ":")
		})
	}
}

func TestMatchingModuleActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{
			name:     "ActionMatchRun",
			constant: ActionMatchRun,
			expected: "job:run",
		},
		{
			name:     "ActionMatchRead",
			constant: ActionMatchRead,
			expected: "job:read",
		},
		{
			name:     "ActionMatchDelete",
			constant: ActionMatchDelete,
			expected: "job:delete",
		},
		{
			name:     "ActionManualMatch",
			constant: ActionManualMatch,
			expected: "manual:create",
		},
		{
			name:     "ActionAdjustmentCreate",
			constant: ActionAdjustmentCreate,
			expected: "adjustment:create",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.constant)
			assert.Contains(t, tt.constant, ":")
		})
	}
}

func TestGovernanceModuleActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{name: "ActionAuditRead", constant: ActionAuditRead, expected: "audit:read"},
		{name: "ActionArchiveRead", constant: ActionArchiveRead, expected: "archive:read"},
		{name: "ActionActorMappingRead", constant: ActionActorMappingRead, expected: "actor-mapping:read"},
		{name: "ActionActorMappingWrite", constant: ActionActorMappingWrite, expected: "actor-mapping:write"},
		{name: "ActionActorMappingDelete", constant: ActionActorMappingDelete, expected: "actor-mapping:delete"},
		{name: "ActionActorMappingDeanonymize", constant: ActionActorMappingDeanonymize, expected: "actor-mapping:deanonymize"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.constant)
			assert.Contains(t, tt.constant, ":")
		})
	}
}

func TestReportingModuleActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{
			name:     "ActionDashboardRead",
			constant: ActionDashboardRead,
			expected: "dashboard:read",
		},
		{
			name:     "ActionExportRead",
			constant: ActionExportRead,
			expected: "export:read",
		},
		{
			name:     "ActionExportJobWrite",
			constant: ActionExportJobWrite,
			expected: "export-job:write",
		},
		{
			name:     "ActionExportJobRead",
			constant: ActionExportJobRead,
			expected: "export-job:read",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.constant)
			assert.Contains(t, tt.constant, ":")
		})
	}
}

func TestSystemModuleActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{name: "ActionConfigRead", constant: ActionConfigRead, expected: "config:read"},
		{name: "ActionConfigWrite", constant: ActionConfigWrite, expected: "config:write"},
		{name: "ActionConfigSchemaRead", constant: ActionConfigSchemaRead, expected: "config/schema:read"},
		{name: "ActionConfigHistoryRead", constant: ActionConfigHistoryRead, expected: "config/history:read"},
		{name: "ActionConfigReloadWrite", constant: ActionConfigReloadWrite, expected: "config/reload:write"},
		{name: "ActionSettingsRead", constant: ActionSettingsRead, expected: "settings:read"},
		{name: "ActionSettingsWrite", constant: ActionSettingsWrite, expected: "settings:write"},
		{name: "ActionSettingsSchemaRead", constant: ActionSettingsSchemaRead, expected: "settings/schema:read"},
		{name: "ActionSettingsHistoryRead", constant: ActionSettingsHistoryRead, expected: "settings/history:read"},
		{name: "ActionSettingsGlobalRead", constant: ActionSettingsGlobalRead, expected: "settings/global:read"},
		{name: "ActionSettingsGlobalWrite", constant: ActionSettingsGlobalWrite, expected: "settings/global:write"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.constant)
			assert.Contains(t, tt.constant, ":")
		})
	}
}

func TestExceptionModuleActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{
			name:     "ActionExceptionRead",
			constant: ActionExceptionRead,
			expected: "exception:read",
		},
		{
			name:     "ActionExceptionResolve",
			constant: ActionExceptionResolve,
			expected: "exception:resolve",
		},
		{
			name:     "ActionExceptionDispatch",
			constant: ActionExceptionDispatch,
			expected: "exception:dispatch",
		},
		{
			name:     "ActionCallbackProcess",
			constant: ActionCallbackProcess,
			expected: "callback:process",
		},
		{
			name:     "ActionDisputeRead",
			constant: ActionDisputeRead,
			expected: "dispute:read",
		},
		{
			name:     "ActionDisputeWrite",
			constant: ActionDisputeWrite,
			expected: "dispute:write",
		},
		{
			name:     "ActionCommentWrite",
			constant: ActionCommentWrite,
			expected: "comment:write",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.constant)
			assert.Contains(t, tt.constant, ":")
		})
	}
}

func TestActionPatternConsistency(t *testing.T) {
	t.Parallel()

	t.Run("entity:operation actions contain colon", func(t *testing.T) {
		t.Parallel()

		entityActions := []string{
			ActionContextCreate, ActionContextRead, ActionContextUpdate, ActionContextDelete,
			ActionSourceCreate, ActionSourceRead, ActionSourceUpdate, ActionSourceDelete,
			ActionFieldMapCreate, ActionFieldMapRead, ActionFieldMapUpdate, ActionFieldMapDelete,
			ActionRuleCreate, ActionRuleRead, ActionRuleUpdate, ActionRuleDelete,
			ActionFeeScheduleCreate, ActionFeeScheduleRead, ActionFeeScheduleUpdate, ActionFeeScheduleDelete,
			ActionScheduleCreate, ActionScheduleRead, ActionScheduleUpdate, ActionScheduleDelete,
			ActionFeeRuleCreate, ActionFeeRuleRead, ActionFeeRuleUpdate, ActionFeeRuleDelete,
			ActionImportCreate, ActionJobRead, ActionTransactionIgnore, ActionTransactionSearch,
			ActionMatchRun, ActionMatchRead, ActionMatchDelete, ActionManualMatch, ActionAdjustmentCreate,
			ActionAuditRead, ActionArchiveRead,
			ActionActorMappingRead, ActionActorMappingWrite, ActionActorMappingDelete,
			ActionActorMappingDeanonymize,
			ActionDashboardRead, ActionExportRead, ActionExportJobWrite, ActionExportJobRead,
			ActionExceptionRead, ActionExceptionResolve, ActionExceptionDispatch, ActionCallbackProcess, ActionDisputeRead, ActionDisputeWrite,
			ActionCommentWrite,
		}

		for _, action := range entityActions {
			assert.Contains(
				t,
				action,
				":",
				"action %q should follow entity:operation pattern",
				action,
			)
		}
	})

	t.Run("standard actions do not contain colon", func(t *testing.T) {
		t.Parallel()

		standardActions := []string{
			ActionRead, ActionWrite, ActionDelete, ActionAdmin,
		}

		for _, action := range standardActions {
			assert.NotContains(
				t,
				action,
				":",
				"standard action %q should not contain colon",
				action,
			)
		}
	})

	t.Run("resources do not contain colon", func(t *testing.T) {
		t.Parallel()

		resources := []string{
			ResourceConfiguration, ResourceIngestion, ResourceMatching,
			ResourceGovernance, ResourceReporting, ResourceException,
		}

		for _, resource := range resources {
			assert.NotContains(t, resource, ":", "resource %q should not contain colon", resource)
			assert.NotContains(
				t,
				resource,
				"-",
				"resource %q should use module-level naming without hyphens",
				resource,
			)
		}
	})
}

func TestResourcesAreNotEmpty(t *testing.T) {
	t.Parallel()

	resources := map[string]string{
		"ResourceConfiguration": ResourceConfiguration,
		"ResourceIngestion":     ResourceIngestion,
		"ResourceMatching":      ResourceMatching,
		"ResourceGovernance":    ResourceGovernance,
		"ResourceReporting":     ResourceReporting,
		"ResourceException":     ResourceException,
	}

	for name, value := range resources {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.NotEmpty(t, value, "%s should not be empty", name)
		})
	}
}

func TestActionsAreNotEmpty(t *testing.T) {
	t.Parallel()

	actions := map[string]string{
		"ActionRead":                ActionRead,
		"ActionWrite":               ActionWrite,
		"ActionDelete":              ActionDelete,
		"ActionAdmin":               ActionAdmin,
		"ActionContextCreate":       ActionContextCreate,
		"ActionContextRead":         ActionContextRead,
		"ActionContextUpdate":       ActionContextUpdate,
		"ActionContextDelete":       ActionContextDelete,
		"ActionSourceCreate":        ActionSourceCreate,
		"ActionSourceRead":          ActionSourceRead,
		"ActionSourceUpdate":        ActionSourceUpdate,
		"ActionSourceDelete":        ActionSourceDelete,
		"ActionFieldMapCreate":      ActionFieldMapCreate,
		"ActionFieldMapRead":        ActionFieldMapRead,
		"ActionFieldMapUpdate":      ActionFieldMapUpdate,
		"ActionFieldMapDelete":      ActionFieldMapDelete,
		"ActionRuleCreate":          ActionRuleCreate,
		"ActionRuleRead":            ActionRuleRead,
		"ActionRuleUpdate":          ActionRuleUpdate,
		"ActionRuleDelete":          ActionRuleDelete,
		"ActionFeeScheduleCreate":   ActionFeeScheduleCreate,
		"ActionFeeScheduleRead":     ActionFeeScheduleRead,
		"ActionFeeScheduleUpdate":   ActionFeeScheduleUpdate,
		"ActionFeeScheduleDelete":   ActionFeeScheduleDelete,
		"ActionScheduleCreate":      ActionScheduleCreate,
		"ActionScheduleRead":        ActionScheduleRead,
		"ActionScheduleUpdate":      ActionScheduleUpdate,
		"ActionScheduleDelete":      ActionScheduleDelete,
		"ActionFeeRuleCreate":       ActionFeeRuleCreate,
		"ActionFeeRuleRead":         ActionFeeRuleRead,
		"ActionFeeRuleUpdate":       ActionFeeRuleUpdate,
		"ActionFeeRuleDelete":       ActionFeeRuleDelete,
		"ActionImportCreate":        ActionImportCreate,
		"ActionJobRead":             ActionJobRead,
		"ActionTransactionIgnore":   ActionTransactionIgnore,
		"ActionTransactionSearch":   ActionTransactionSearch,
		"ActionMatchRun":            ActionMatchRun,
		"ActionMatchRead":           ActionMatchRead,
		"ActionMatchDelete":         ActionMatchDelete,
		"ActionManualMatch":         ActionManualMatch,
		"ActionAdjustmentCreate":    ActionAdjustmentCreate,
		"ActionAuditRead":           ActionAuditRead,
		"ActionArchiveRead":         ActionArchiveRead,
		"ActionActorMappingRead":        ActionActorMappingRead,
		"ActionActorMappingWrite":       ActionActorMappingWrite,
		"ActionActorMappingDelete":      ActionActorMappingDelete,
		"ActionActorMappingDeanonymize": ActionActorMappingDeanonymize,
		"ActionDashboardRead":       ActionDashboardRead,
		"ActionExportRead":          ActionExportRead,
		"ActionExportJobWrite":      ActionExportJobWrite,
		"ActionExportJobRead":       ActionExportJobRead,
		"ActionExceptionRead":       ActionExceptionRead,
		"ActionExceptionResolve":    ActionExceptionResolve,
		"ActionExceptionDispatch":   ActionExceptionDispatch,
		"ActionCallbackProcess":     ActionCallbackProcess,
		"ActionDisputeRead":         ActionDisputeRead,
		"ActionDisputeWrite":        ActionDisputeWrite,
		"ActionCommentWrite":        ActionCommentWrite,
		"ActionConfigRead":          ActionConfigRead,
		"ActionConfigWrite":         ActionConfigWrite,
		"ActionConfigSchemaRead":    ActionConfigSchemaRead,
		"ActionConfigHistoryRead":   ActionConfigHistoryRead,
		"ActionConfigReloadWrite":   ActionConfigReloadWrite,
		"ActionSettingsRead":        ActionSettingsRead,
		"ActionSettingsWrite":       ActionSettingsWrite,
		"ActionSettingsSchemaRead":  ActionSettingsSchemaRead,
		"ActionSettingsHistoryRead": ActionSettingsHistoryRead,
		"ActionSettingsGlobalRead":  ActionSettingsGlobalRead,
		"ActionSettingsGlobalWrite": ActionSettingsGlobalWrite,
	}

	for name, value := range actions {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.NotEmpty(t, value, "%s should not be empty", name)
		})
	}
}

func TestNoDuplicateResourceValues(t *testing.T) {
	t.Parallel()

	resources := []string{
		ResourceConfiguration,
		ResourceIngestion,
		ResourceMatching,
		ResourceGovernance,
		ResourceReporting,
		ResourceException,
	}

	seen := make(map[string]bool)
	for _, r := range resources {
		assert.False(t, seen[r], "duplicate resource value found: %s", r)
		seen[r] = true
	}
}

func TestNoDuplicateStandardActionValues(t *testing.T) {
	t.Parallel()

	actions := []string{
		ActionRead,
		ActionWrite,
		ActionDelete,
		ActionAdmin,
	}

	seen := make(map[string]bool)
	for _, a := range actions {
		assert.False(t, seen[a], "duplicate standard action value found: %s", a)
		seen[a] = true
	}
}
