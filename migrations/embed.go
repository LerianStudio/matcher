// Package migrations provides embedded SQL migration files for the Matcher service.
// Using embedded migrations eliminates filesystem access requirements, enabling
// the container to run with readOnlyRootFilesystem: true for enhanced security.
package migrations

import "embed"

// FS contains all SQL migration files embedded at compile time.
// This allows the application to run in containers with read-only root filesystems
// without requiring volume mounts or filesystem access for migrations.
//
//go:embed *.sql
var FS embed.FS
