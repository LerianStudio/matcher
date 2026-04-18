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
	errSystemplaneSecretMasterKey    = errors.New("validate systemplane config: SYSTEMPLANE_SECRET_MASTER_KEY is required")
	errSystemplaneDevMasterKeyInProd = errors.New("validate systemplane config: SYSTEMPLANE_SECRET_MASTER_KEY must not use the well-known development default in production")
	errSystemplanePostgresDBRequired = errors.New("init systemplane: postgres db is required")
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

	return client, nil
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
// In production, the master key must be set and must not be the well-known
// development default.
func ValidateSystemplaneSecrets(envName string) error {
	masterKey := strings.TrimSpace(os.Getenv("SYSTEMPLANE_SECRET_MASTER_KEY"))

	if masterKey == "" {
		return errSystemplaneSecretMasterKey
	}

	if IsProductionEnvironment(envName) && masterKey == wellKnownDevMasterKey {
		return errSystemplaneDevMasterKeyInProd
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

	switch n := value.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
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
