// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"
)

// Sentinel errors for systemplane initialization.
var (
	errSystemplaneSecretMasterKey      = errors.New("validate systemplane config: SYSTEMPLANE_SECRET_MASTER_KEY is required")
	errSystemplaneDevMasterKeyInNonDev = errors.New("validate systemplane config: SYSTEMPLANE_SECRET_MASTER_KEY must not use the well-known development default outside development or test environments")
	errSystemplanePostgresDBRequired   = errors.New("init systemplane: postgres db is required")
)

// wellKnownDevMasterKey is the development-mode default for the systemplane
// secret master key. It is committed in docker-compose.yml for local
// convenience, but MUST be rejected in production environments.
const wellKnownDevMasterKey = "+PnwgNy8bL3HGT1rOXp47PqyGcPywXH/epgmSVwPkL0="

// InitSystemplane creates and starts a v5 systemplane Client backed by
// Postgres (LISTEN/NOTIFY). The Client is the single runtime-config handle
// for the entire service: typed getters, OnChange callbacks, and admin
// HTTP routes all hang off it.
//
// Lifecycle: Register keys -> Start (hydrate + subscribe) -> return Client.
// On any error, partially-created resources are cleaned up.
func InitSystemplane(
	ctx context.Context,
	cfg *Config,
	db *sql.DB,
	logger libLog.Logger,
	telemetry *libOpentelemetry.Telemetry,
) (*systemplane.Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("init systemplane: %w", ErrConfigNil)
	}

	if err := ValidateSystemplaneSecrets(cfg.App.EnvName); err != nil {
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	if db == nil {
		return nil, errSystemplanePostgresDBRequired
	}

	listenDSN := buildSystemplaneDSN(cfg)

	opts := []systemplane.Option{
		systemplane.WithLogger(logger),
	}

	if telemetry != nil {
		opts = append(opts, systemplane.WithTelemetry(telemetry))
	}

	client, err := systemplane.NewPostgres(db, listenDSN, opts...)
	if err != nil {
		return nil, fmt.Errorf("init systemplane: create client: %w", err)
	}

	if err := RegisterMatcherKeys(client, cfg); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("init systemplane: %w", err)
	}

	if err := client.Start(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("init systemplane: start: %w", err)
	}

	warnOnReclassifiedOrphanKeys(ctx, db, logger)

	return client, nil
}

// reclassifiedBootstrapOnlyKeys lists matcher systemplane keys that moved
// from runtime-mutable to bootstrap-only in the v5 migration. Migration
// 000030 drops these from systemplane_entries on upgrade; this list is
// used at startup to WARN about any late-arriving rows that slipped past
// the migration (e.g. manual SQL, restored backup, competing service).
//
// Keep in sync with matcherKeyDefs — any NEW bootstrap-only reclassification
// should be appended here so ops keep a single source of drift detection.
var reclassifiedBootstrapOnlyKeys = []string{
	"app.log_level",
	"tenancy.default_tenant_id",
	"tenancy.default_tenant_slug",
	"auth.enabled",
	"auth.host",
	"auth.token_secret",
	"outbox.retry_window_sec",
	"outbox.dispatch_interval_sec",
}

// warnOnReclassifiedOrphanKeys emits a WARN log for each reclassified
// bootstrap-only key that still has a persisted value in systemplane_entries.
// The log is best-effort — transient query failures are ignored so a
// degraded systemplane never blocks startup. Operators see a single line
// per orphan with enough context to either drop the row manually or
// re-run migration 000030.
func warnOnReclassifiedOrphanKeys(ctx context.Context, db *sql.DB, logger libLog.Logger) {
	if db == nil || logger == nil {
		return
	}

	// Table is created lazily by systemplane.NewPostgres -> ensureSchema at
	// Start time. By the time this runs it should exist, but guard anyway
	// to avoid a hard failure in degraded setups.
	var exists bool

	err := db.QueryRowContext(ctx,
		`SELECT EXISTS (
		    SELECT 1
		    FROM   information_schema.tables
		    WHERE  table_schema = 'public'
		      AND  table_name   = 'systemplane_entries'
		)`,
	).Scan(&exists)
	if err != nil || !exists {
		return
	}

	// Keys are compile-time constants, so it is safe — and avoids the
	// driver.Value friction of []string arrays — to materialise them into
	// positional placeholders rather than a single ANY($1) array argument.
	placeholders := make([]string, 0, len(reclassifiedBootstrapOnlyKeys))
	args := make([]any, 0, len(reclassifiedBootstrapOnlyKeys))

	for i, key := range reclassifiedBootstrapOnlyKeys {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		args = append(args, key)
	}

	// #nosec G202 -- placeholders are parameterized positional refs ($1,$2,...), values are bound via QueryContext args
	query := `SELECT key FROM public.systemplane_entries
		 WHERE namespace = 'matcher'
		   AND key IN (` + strings.Join(placeholders, ", ") + `)`

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return
	}

	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return
		}

		logger.Log(ctx, libLog.LevelWarn,
			"systemplane: orphan value for reclassified bootstrap-only key; runtime ignores it",
			libLog.String("namespace", systemplaneNamespace),
			libLog.String("key", key),
			libLog.String("action", "drop via SQL or re-run migration 000030"),
		)
	}

	_ = rows.Err()
}

// buildSystemplaneDSN constructs a Postgres DSN from the application config
// for the systemplane LISTEN connection. Falls back to env var
// SYSTEMPLANE_POSTGRES_DSN if set.
func buildSystemplaneDSN(cfg *Config) string {
	if dsn := strings.TrimSpace(os.Getenv("SYSTEMPLANE_POSTGRES_DSN")); dsn != "" {
		return dsn
	}

	hostPort := net.JoinHostPort(cfg.Postgres.PrimaryHost, cfg.Postgres.PrimaryPort)
	query := url.Values{}
	query.Set("sslmode", cfg.Postgres.PrimarySSLMode)

	return (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.Postgres.PrimaryUser, cfg.Postgres.PrimaryPassword),
		Host:     hostPort,
		Path:     "/" + cfg.Postgres.PrimaryDB,
		RawQuery: query.Encode(),
	}).String()
}

// ValidateSystemplaneSecrets checks systemplane secret configuration.
//
// The master key must always be set. The well-known development default
// (committed in docker-compose.yml) is ONLY accepted in explicit
// development or test environments — staging, UAT, QA, preview, and any
// unknown environment all reject it. This prevents a publicly-known key
// from guarding any environment that may hold real data.
func ValidateSystemplaneSecrets(envName string) error {
	masterKey := strings.TrimSpace(os.Getenv("SYSTEMPLANE_SECRET_MASTER_KEY"))

	if masterKey == "" {
		return errSystemplaneSecretMasterKey
	}

	if masterKey == wellKnownDevMasterKey && !IsDevelopmentOrTestEnvironment(envName) {
		return errSystemplaneDevMasterKeyInNonDev
	}

	return nil
}

// SystemplaneGetString reads a string from the systemplane Client with the
// Matcher namespace. Returns the fallback if the client is nil or the key
// is not found.
func SystemplaneGetString(client *systemplane.Client, key, fallback string) string {
	if client == nil {
		return fallback
	}

	value, ok := client.Get(systemplaneNamespace, key)
	if !ok {
		return fallback
	}

	s, isString := value.(string)
	if !isString {
		return fallback
	}

	return s
}

// SystemplaneGetInt reads an int from the systemplane Client with the
// Matcher namespace. Returns the fallback if the client is nil or the key
// is not found.
func SystemplaneGetInt(client *systemplane.Client, key string, fallback int) int {
	if client == nil {
		return fallback
	}

	value, ok := client.Get(systemplaneNamespace, key)
	if !ok {
		return fallback
	}

	switch n := value.(type) {
	case int:
		return n
	case float64:
		return int(n)
	default:
		return fallback
	}
}

// SystemplaneGetInt64 reads an int64 from the systemplane Client with the
// Matcher namespace. Returns the fallback if the client is nil or the key
// is not found. Accepts int, int64, and float64 representations so callers
// storing large byte counts (e.g., FETCHER_MAX_EXTRACTION_BYTES) get the
// same JSON-number tolerance as SystemplaneGetInt.
func SystemplaneGetInt64(client *systemplane.Client, key string, fallback int64) int64 {
	if client == nil {
		return fallback
	}

	value, ok := client.Get(systemplaneNamespace, key)
	if !ok {
		return fallback
	}

	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	default:
		return fallback
	}
}

// SystemplaneGetBool reads a bool from the systemplane Client with the
// Matcher namespace. Returns the fallback if the client is nil or the key
// is not found.
func SystemplaneGetBool(client *systemplane.Client, key string, fallback bool) bool {
	if client == nil {
		return fallback
	}

	value, ok := client.Get(systemplaneNamespace, key)
	if !ok {
		return fallback
	}

	b, isBool := value.(bool)
	if !isBool {
		return fallback
	}

	return b
}
