//go:build unit

package repositories_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	exportjob "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/export_job"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
)

var _ repositories.ExportJobRepository = (*exportjob.Repository)(nil)

func TestExportJobRepository_InterfaceNotNil(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.ExportJobRepository)(nil)).Elem()
	assert.NotNil(t, repoType)
}

func TestExportJobRepository_RequiredMethods(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.ExportJobRepository)(nil)).Elem()

	requiredMethods := []string{
		"Create",
		"GetByID",
		"Update",
		"UpdateStatus",
		"UpdateProgress",
		"List",
		"ListByContext",
		"ListExpired",
		"ClaimNextQueued",
		"Delete",
	}

	for _, methodName := range requiredMethods {
		t.Run(methodName+"_exists", func(t *testing.T) {
			t.Parallel()

			_, exists := repoType.MethodByName(methodName)
			assert.True(t, exists, "method %s must exist in ExportJobRepository", methodName)
		})
	}
}

func TestExportJobRepository_IsInterface(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.ExportJobRepository)(nil)).Elem()
	assert.Equal(t, reflect.Interface, repoType.Kind())
}

func TestExportJobRepository_CRUDOperations(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.ExportJobRepository)(nil)).Elem()

	crudMethods := map[string]bool{
		"Create":         true,
		"GetByID":        true,
		"Update":         true,
		"UpdateStatus":   true,
		"UpdateProgress": true,
		"Delete":         true,
	}

	for methodName := range crudMethods {
		_, exists := repoType.MethodByName(methodName)
		assert.True(t, exists, "CRUD method %s must exist", methodName)
	}
}

func TestExportJobRepository_ListOperations(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.ExportJobRepository)(nil)).Elem()

	listMethods := []string{
		"List",
		"ListByContext",
		"ListExpired",
	}

	for _, methodName := range listMethods {
		t.Run(methodName, func(t *testing.T) {
			t.Parallel()

			method, exists := repoType.MethodByName(methodName)
			assert.True(t, exists, "list method %s must exist", methodName)

			numOut := method.Type.NumOut()
			if methodName == "List" {
				assert.Equal(t, 3, numOut, "list method %s should return slice, pagination, and error", methodName)
			} else {
				assert.Equal(t, 2, numOut, "list method %s should return slice and error", methodName)
			}
		})
	}
}

func TestExportJobRepository_ClaimNextQueued_Signature(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*repositories.ExportJobRepository)(nil)).Elem()

	method, exists := repoType.MethodByName("ClaimNextQueued")
	assert.True(t, exists, "ClaimNextQueued must exist for queued job claiming")

	numIn := method.Type.NumIn()
	assert.Equal(t, 1, numIn, "ClaimNextQueued should only accept context")

	numOut := method.Type.NumOut()
	assert.Equal(t, 2, numOut, "ClaimNextQueued should return (*ExportJob, error)")
	assert.Equal(t, reflect.TypeOf((*entities.ExportJob)(nil)), method.Type.Out(0))
	assert.Equal(t, reflect.TypeOf((*error)(nil)).Elem(), method.Type.Out(1))
}
