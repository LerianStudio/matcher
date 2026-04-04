//go:build unit

package auth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildCasdoorInitData_ContainsKnownRolesAndPermissions(t *testing.T) {
	t.Parallel()

	data, err := BuildCasdoorInitData()
	require.NoError(t, err)

	require.NotEmpty(t, data.Permissions)
	require.Len(t, data.Roles, 5)
	assert.Equal(t, "matcher-admin-role", data.Roles[0].Name)
	permissionNames := make([]string, 0, len(data.Permissions))
	for _, permission := range data.Permissions {
		permissionNames = append(permissionNames, permission.Name)
	}
	assert.Contains(t, permissionNames, "system-config-read")
	assert.Contains(t, permissionNames, "system-settings-global-write")
	assert.Contains(t, data.Roles[0].Permissions, "system-settings-global-write")
	assert.Contains(t, data.Roles[3].Permissions, "system-config-schema-read")
	assert.NotContains(t, data.Roles[3].Permissions, "system-config-write")
}

func TestBuildCasdoorInitData_RolePermissionsMustExist(t *testing.T) {
	t.Parallel()

	data, err := BuildCasdoorInitData()
	require.NoError(t, err)

	known := make(map[string]struct{}, len(data.Permissions))
	for _, permission := range data.Permissions {
		known[permission.Name] = struct{}{}
	}

	for _, role := range data.Roles {
		for _, permission := range role.Permissions {
			_, ok := known[permission]
			assert.Truef(t, ok, "role %s references unknown permission %s", role.Name, permission)
		}
	}
}

func TestGeneratedCasdoorInitDataFileMatchesSourceOfTruth(t *testing.T) {
	t.Parallel()

	generatedBytes, err := MarshalCasdoorInitData()
	require.NoError(t, err)

	filePath := filepath.Join("..", "..", "config", "casdoor", "init_data.json")
	fileBytes, err := os.ReadFile(filePath)
	require.NoError(t, err)

	var generated CasdoorInitData
	var fromFile CasdoorInitData
	require.NoError(t, json.Unmarshal(generatedBytes, &generated))
	require.NoError(t, json.Unmarshal(fileBytes, &fromFile))
	assert.Equal(t, generated, fromFile)
}
