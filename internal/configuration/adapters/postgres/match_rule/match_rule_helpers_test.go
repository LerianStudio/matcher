//go:build unit

package match_rule

import (
	"database/sql"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func newPriorityRows(priority int) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"priority"}).AddRow(priority)
}

func newSqlMockDB() (*sql.DB, sqlmock.Sqlmock, error) {
	return sqlmock.New()
}
