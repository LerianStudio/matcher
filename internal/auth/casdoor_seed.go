package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

//go:generate go run ../../cmd/generate-casdoor --output ../../config/casdoor/init_data.json

const (
	casdoorOrganizationName = "lerian"
	casdoorApplicationName  = "matcher"
)

var errCasdoorRoleUnknownPermission = errors.New("casdoor role references unknown permission")

// CasdoorInitData defines the generated Casdoor bootstrap payload.
type CasdoorInitData struct {
	Organizations []CasdoorOrganization `json:"organizations"`
	Applications  []CasdoorApplication  `json:"applications"`
	Permissions   []CasdoorPermission   `json:"permissions"`
	Roles         []CasdoorRole         `json:"roles"`
}

// CasdoorOrganization defines a Casdoor organization seed entry.
type CasdoorOrganization struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
}

// CasdoorApplication defines a Casdoor application seed entry.
type CasdoorApplication struct {
	Name         string `json:"name"`
	DisplayName  string `json:"displayName"`
	Organization string `json:"organization"`
}

// CasdoorPermission defines a Casdoor permission seed entry.
type CasdoorPermission struct {
	Name        string   `json:"name"`
	DisplayName string   `json:"displayName"`
	Description string   `json:"description"`
	Resources   []string `json:"resources"`
	Actions     []string `json:"actions"`
	Effect      string   `json:"effect"`
	IsEnabled   bool     `json:"isEnabled"`
}

// CasdoorRole defines a Casdoor role seed entry.
type CasdoorRole struct {
	Name        string   `json:"name"`
	DisplayName string   `json:"displayName"`
	Description string   `json:"description"`
	IsEnabled   bool     `json:"isEnabled"`
	Permissions []string `json:"permissions"`
}

type casdoorPermissionSpec struct {
	Resource string
	Action   string
}

type casdoorRoleSpec struct {
	Name        string
	DisplayName string
	Description string
	Permissions []casdoorPermissionSpec
}

// MarshalCasdoorInitData renders the Casdoor seed payload as stable JSON.
func MarshalCasdoorInitData() ([]byte, error) {
	data, err := BuildCasdoorInitData()
	if err != nil {
		return nil, err
	}

	encoded, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal casdoor init data: %w", err)
	}

	return append(encoded, '\n'), nil
}

// BuildCasdoorInitData builds the Casdoor seed payload from Matcher's RBAC catalog.
func BuildCasdoorInitData() (CasdoorInitData, error) {
	permissionSpecs := matcherCasdoorPermissionSpecs()
	permissions := make([]CasdoorPermission, 0, len(permissionSpecs))
	permissionNames := make(map[string]struct{}, len(permissionSpecs))

	for _, spec := range permissionSpecs {
		permission := CasdoorPermission{
			Name:        casdoorPermissionName(spec.Resource, spec.Action),
			DisplayName: casdoorPermissionDisplayName(spec.Resource, spec.Action),
			Description: fmt.Sprintf("Allows %s on %s.", spec.Action, spec.Resource),
			Resources:   []string{spec.Resource},
			Actions:     []string{spec.Action},
			Effect:      "Allow",
			IsEnabled:   true,
		}

		permissions = append(permissions, permission)
		permissionNames[permission.Name] = struct{}{}
	}

	roles := make([]CasdoorRole, 0, len(matcherCasdoorRoleSpecs()))
	for _, spec := range matcherCasdoorRoleSpecs() {
		rolePermissions := make([]string, 0, len(spec.Permissions))
		for _, permissionSpec := range spec.Permissions {
			permissionName := casdoorPermissionName(permissionSpec.Resource, permissionSpec.Action)
			if _, ok := permissionNames[permissionName]; !ok {
				return CasdoorInitData{}, fmt.Errorf("%w: role %q permission %q", errCasdoorRoleUnknownPermission, spec.Name, permissionName)
			}

			rolePermissions = append(rolePermissions, permissionName)
		}

		roles = append(roles, CasdoorRole{
			Name:        spec.Name,
			DisplayName: spec.DisplayName,
			Description: spec.Description,
			IsEnabled:   true,
			Permissions: rolePermissions,
		})
	}

	return CasdoorInitData{
		Organizations: []CasdoorOrganization{{
			Name:        casdoorOrganizationName,
			DisplayName: "Lerian",
		}},
		Applications: []CasdoorApplication{{
			Name:         casdoorApplicationName,
			DisplayName:  "Matcher",
			Organization: casdoorOrganizationName,
		}},
		Permissions: permissions,
		Roles:       roles,
	}, nil
}

func matcherCasdoorPermissionSpecs() []casdoorPermissionSpec {
	return appendAllPermissionSpecs(
		permissionSpecs(ResourceConfiguration,
			ActionContextCreate, ActionContextRead, ActionContextUpdate, ActionContextDelete,
			ActionSourceCreate, ActionSourceRead, ActionSourceUpdate, ActionSourceDelete,
			ActionFieldMapCreate, ActionFieldMapRead, ActionFieldMapUpdate, ActionFieldMapDelete,
			ActionRuleCreate, ActionRuleRead, ActionRuleUpdate, ActionRuleDelete,
			ActionFeeScheduleCreate, ActionFeeScheduleRead, ActionFeeScheduleUpdate, ActionFeeScheduleDelete,
			ActionScheduleCreate, ActionScheduleRead, ActionScheduleUpdate, ActionScheduleDelete,
			ActionFeeRuleCreate, ActionFeeRuleRead, ActionFeeRuleUpdate, ActionFeeRuleDelete,
		),
		permissionSpecs(ResourceIngestion,
			ActionImportCreate, ActionJobRead, ActionTransactionIgnore, ActionTransactionSearch,
		),
		permissionSpecs(ResourceMatching,
			ActionMatchRun, ActionMatchRead, ActionMatchDelete, ActionManualMatch, ActionAdjustmentCreate,
		),
		permissionSpecs(ResourceGovernance,
			ActionAuditRead, ActionArchiveRead,
			ActionActorMappingRead, ActionActorMappingWrite, ActionActorMappingDelete,
			// ActorMappingDeanonymize is registered as an available permission
			// so Casdoor admins can grant it to specific users/roles, but it
			// is intentionally omitted from every default role. Resolving a
			// hashed actor back to cleartext PII should be an explicit,
			// audited assignment — never an inherited capability.
			ActionActorMappingDeanonymize,
		),
		permissionSpecs(ResourceReporting,
			ActionDashboardRead, ActionExportRead, ActionExportJobWrite, ActionExportJobRead,
		),
		permissionSpecs(ResourceException,
			ActionExceptionRead, ActionExceptionResolve, ActionExceptionDispatch, ActionCallbackProcess,
			ActionDisputeRead, ActionDisputeWrite, ActionCommentWrite,
		),
		permissionSpecs(ResourceDiscovery,
			ActionDiscoveryRead, ActionDiscoveryWrite,
		),
		permissionSpecs(ResourceSystem,
			ActionConfigRead, ActionConfigWrite, ActionConfigSchemaRead, ActionConfigHistoryRead, ActionConfigReloadWrite,
			ActionSettingsRead, ActionSettingsWrite, ActionSettingsSchemaRead, ActionSettingsHistoryRead,
			ActionSettingsGlobalRead, ActionSettingsGlobalWrite,
		),
	)
}

func matcherCasdoorRoleSpecs() []casdoorRoleSpec {
	configurationRead := permissionSpecs(ResourceConfiguration,
		ActionContextRead, ActionSourceRead, ActionFieldMapRead, ActionRuleRead,
		ActionFeeScheduleRead, ActionScheduleRead, ActionFeeRuleRead,
	)
	governanceRead := permissionSpecs(ResourceGovernance,
		ActionAuditRead, ActionArchiveRead, ActionActorMappingRead,
	)
	reportingRead := permissionSpecs(ResourceReporting,
		ActionDashboardRead, ActionExportRead, ActionExportJobRead,
	)

	return []casdoorRoleSpec{
		{
			Name:        "matcher-admin-role",
			DisplayName: "Matcher Admin",
			Description: "Full access to all Matcher resources and actions.",
			Permissions: matcherCasdoorPermissionSpecs(),
		},
		{
			Name:        "matcher-operator-role",
			DisplayName: "Matcher Operator",
			Description: "Operational access to configuration, ingestion, matching, reporting, exceptions, discovery, and governance read actions.",
			Permissions: appendAllPermissionSpecs(
				permissionSpecs(ResourceConfiguration,
					ActionContextCreate, ActionContextRead, ActionContextUpdate, ActionContextDelete,
					ActionSourceCreate, ActionSourceRead, ActionSourceUpdate, ActionSourceDelete,
					ActionFieldMapCreate, ActionFieldMapRead, ActionFieldMapUpdate, ActionFieldMapDelete,
					ActionRuleCreate, ActionRuleRead, ActionRuleUpdate, ActionRuleDelete,
					ActionFeeScheduleCreate, ActionFeeScheduleRead, ActionFeeScheduleUpdate, ActionFeeScheduleDelete,
					ActionScheduleCreate, ActionScheduleRead, ActionScheduleUpdate, ActionScheduleDelete,
					ActionFeeRuleCreate, ActionFeeRuleRead, ActionFeeRuleUpdate, ActionFeeRuleDelete,
				),
				permissionSpecs(ResourceIngestion,
					ActionImportCreate, ActionJobRead, ActionTransactionIgnore, ActionTransactionSearch,
				),
				permissionSpecs(ResourceMatching,
					ActionMatchRun, ActionMatchRead, ActionMatchDelete, ActionManualMatch, ActionAdjustmentCreate,
				),
				governanceRead,
				permissionSpecs(ResourceReporting,
					ActionDashboardRead, ActionExportRead, ActionExportJobWrite, ActionExportJobRead,
				),
				permissionSpecs(ResourceException,
					ActionExceptionRead, ActionExceptionResolve, ActionExceptionDispatch, ActionCallbackProcess,
					ActionDisputeRead, ActionDisputeWrite, ActionCommentWrite,
				),
				permissionSpecs(ResourceDiscovery,
					ActionDiscoveryRead, ActionDiscoveryWrite,
				),
			),
		},
		{
			Name:        "matcher-analyst-role",
			DisplayName: "Matcher Analyst",
			Description: "Analytical access to reporting and exception workflows with read-only access to operational configuration and governance data.",
			Permissions: appendAllPermissionSpecs(
				configurationRead,
				permissionSpecs(ResourceIngestion,
					ActionJobRead, ActionTransactionSearch,
				),
				permissionSpecs(ResourceMatching,
					ActionMatchRead, ActionAdjustmentCreate,
				),
				governanceRead,
				permissionSpecs(ResourceReporting,
					ActionDashboardRead, ActionExportRead, ActionExportJobWrite, ActionExportJobRead,
				),
				permissionSpecs(ResourceException,
					ActionExceptionRead, ActionExceptionResolve, ActionExceptionDispatch,
					ActionDisputeRead, ActionDisputeWrite, ActionCommentWrite,
				),
				permissionSpecs(ResourceDiscovery, ActionDiscoveryRead),
			),
		},
		{
			Name:        "matcher-viewer-role",
			DisplayName: "Matcher Viewer",
			Description: "Read-only access to Matcher resources, reports, and system metadata.",
			Permissions: appendAllPermissionSpecs(
				configurationRead,
				permissionSpecs(ResourceIngestion,
					ActionJobRead, ActionTransactionSearch,
				),
				permissionSpecs(ResourceMatching, ActionMatchRead),
				governanceRead,
				reportingRead,
				permissionSpecs(ResourceException,
					ActionExceptionRead, ActionDisputeRead,
				),
				permissionSpecs(ResourceDiscovery, ActionDiscoveryRead),
				permissionSpecs(ResourceSystem,
					ActionConfigSchemaRead, ActionSettingsSchemaRead,
				),
			),
		},
		{
			Name:        "matcher-system-role",
			DisplayName: "Matcher System",
			Description: "System-level access to runtime configuration, governance administration, configuration reads, and reporting reads.",
			Permissions: appendAllPermissionSpecs(
				configurationRead,
				permissionSpecs(ResourceGovernance,
					ActionAuditRead, ActionArchiveRead, ActionActorMappingRead, ActionActorMappingWrite, ActionActorMappingDelete,
				),
				reportingRead,
				permissionSpecs(ResourceSystem,
					ActionConfigRead, ActionConfigWrite, ActionConfigSchemaRead, ActionConfigHistoryRead, ActionConfigReloadWrite,
					ActionSettingsRead, ActionSettingsWrite, ActionSettingsSchemaRead, ActionSettingsHistoryRead,
					ActionSettingsGlobalRead, ActionSettingsGlobalWrite,
				),
			),
		},
	}
}

func permissionSpecs(resource string, actions ...string) []casdoorPermissionSpec {
	specs := make([]casdoorPermissionSpec, 0, len(actions))
	for _, action := range actions {
		specs = append(specs, casdoorPermissionSpec{Resource: resource, Action: action})
	}

	return specs
}

func appendAllPermissionSpecs(groups ...[]casdoorPermissionSpec) []casdoorPermissionSpec {
	total := 0
	for _, group := range groups {
		total += len(group)
	}

	combined := make([]casdoorPermissionSpec, 0, total)
	for _, group := range groups {
		combined = append(combined, group...)
	}

	return combined
}

func casdoorPermissionName(resource, action string) string {
	replacer := strings.NewReplacer(":", "-", "/", "-")

	return resource + "-" + replacer.Replace(action)
}

func casdoorPermissionDisplayName(resource, action string) string {
	return humanizeRBACPart(resource) + " " + humanizeRBACPart(action)
}

func humanizeRBACPart(value string) string {
	replacer := strings.NewReplacer("-", " ", "/", " ", ":", " ")

	parts := strings.Fields(replacer.Replace(value))
	for idx, part := range parts {
		parts[idx] = strings.ToUpper(part[:1]) + part[1:]
	}

	return strings.Join(parts, " ")
}
