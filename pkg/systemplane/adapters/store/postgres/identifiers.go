// Copyright 2025 Lerian Studio.

package postgres

// qualify returns the schema-qualified table name for use in SQL statements.
//
// SECURITY: All schema and table names passed to this function MUST be
// validated at bootstrap time via bootstrap.ValidatePostgresObjectNames
// (regex: ^[a-z_][a-z0-9_]{0,62}$). They are operator-controlled, never
// user input. Raw SQL concatenation is safe under these constraints.
func qualify(schema, table string) string {
	return schema + "." + table
}
