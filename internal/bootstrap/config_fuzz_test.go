//go:build unit

package bootstrap

import (
	"strings"
	"testing"
)

func FuzzConfigEnvOverride(f *testing.F) {
	f.Add("localhost", "5432", "matcher", "secret", "matcher_db", "disable")
	f.Add("db.example.com", "5433", "admin", "p@ssw0rd!", "prod_db", "require")
	f.Add("192.168.1.1", "15432", "user_name", "", "db-name", "verify-full")
	f.Add("", "", "", "", "", "")

	f.Fuzz(func(t *testing.T, host, port, user, password, dbname, sslmode string) {
		if containsNul(host, port, user, password, dbname, sslmode) {
			return
		}

		t.Setenv("INFRA_CONNECT_TIMEOUT_SEC", "30")
		t.Setenv("LOG_LEVEL", "info")
		t.Setenv("EXPORT_RATE_LIMIT_MAX", "10")
		t.Setenv("EXPORT_RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("POSTGRES_HOST", host)
		t.Setenv("POSTGRES_PORT", port)
		t.Setenv("POSTGRES_USER", user)
		t.Setenv("POSTGRES_PASSWORD", password)
		t.Setenv("POSTGRES_DB", dbname)
		t.Setenv("POSTGRES_SSLMODE", sslmode)
		t.Setenv("DEFAULT_TENANT_ID", "11111111-1111-1111-1111-111111111111")
		t.Setenv("DEFAULT_TENANT_SLUG", "default")

		cfg, err := LoadConfigWithLogger(nil)
		if err != nil {
			return
		}

		dsn := cfg.PrimaryDSN()
		if dsn == "" && (host != "" || port != "" || user != "" || dbname != "") {
			t.Errorf("PrimaryDSN returned empty string with non-empty inputs")
		}
	})
}

func containsNul(values ...string) bool {
	for _, value := range values {
		if strings.ContainsRune(value, 0) {
			return true
		}
	}

	return false
}

func FuzzConfigValidation(f *testing.F) {
	f.Add("development", false, "", "devpass", "*", "", "")
	f.Add(
		"production",
		true,
		"http://auth:8080",
		"secure-pass",
		"https://example.com",
		"/tls/cert.pem",
		"/tls/key.pem",
	)
	f.Add(
		"production",
		true,
		"http://auth:8080",
		"",
		"https://example.com",
		"/tls/cert.pem",
		"/tls/key.pem",
	)
	f.Add(
		"production",
		true,
		"http://auth:8080",
		"secure-pass",
		"*",
		"/tls/cert.pem",
		"/tls/key.pem",
	)
	f.Add("staging", true, "", "staging-pass", "*", "", "")
	f.Add("", false, "", "", "", "", "")

	f.Fuzz(
		func(t *testing.T, envName string, authEnabled bool, authHost string, password string, corsOrigins string, tlsCert string, tlsKey string) {
			cfg := buildFuzzConfig(
				envName,
				authEnabled,
				authHost,
				password,
				corsOrigins,
				tlsCert,
				tlsKey,
			)
			err := cfg.Validate()

			validateFuzzResult(
				t,
				cfg,
				err,
				envName,
				authEnabled,
				authHost,
				corsOrigins,
				tlsCert,
				tlsKey,
			)
		},
	)
}

func buildFuzzConfig(
	envName string,
	authEnabled bool,
	authHost, password, corsOrigins, tlsCert, tlsKey string,
) *Config {
	cfg := &Config{
		App: AppConfig{
			EnvName:  envName,
			LogLevel: "info",
		},
		Server: ServerConfig{
			BodyLimitBytes:     10 * 1024 * 1024,
			CORSAllowedOrigins: corsOrigins,
			TLSCertFile:        tlsCert,
			TLSKeyFile:         tlsKey,
		},
		Tenancy: TenancyConfig{
			DefaultTenantID: "11111111-1111-1111-1111-111111111111",
		},
		Auth: AuthConfig{
			Enabled:     authEnabled,
			Host:        authHost,
			TokenSecret: "secret",
		},
		Postgres: PostgresConfig{
			PrimaryPassword: password,
		},
		RateLimit: RateLimitConfig{
			Max:             100,
			ExpirySec:       60,
			ExportMax:       10,
			ExportExpirySec: 60,
		},
		Infrastructure: InfrastructureConfig{
			ConnectTimeoutSec: 30,
		},
	}

	if envName == "production" {
		cfg.Postgres.PrimarySSLMode = "require"
		cfg.Redis.TLS = true
		cfg.RabbitMQ.URI = "amqps"
		cfg.RabbitMQ.User = "matcher"
		cfg.RabbitMQ.Password = "secure-pass"
	}

	return cfg
}

func validateFuzzResult(
	t *testing.T,
	cfg *Config,
	err error,
	envName string,
	authEnabled bool,
	authHost, corsOrigins, tlsCert, tlsKey string,
) {
	t.Helper()

	validateProductionRequirements(t, cfg, err, envName, authEnabled, corsOrigins)
	validateTLSRequirements(t, err, tlsCert, tlsKey)
	validateAuthRequirements(t, err, authEnabled, authHost)
}

func validateProductionRequirements(
	t *testing.T,
	cfg *Config,
	err error,
	envName string,
	authEnabled bool,
	corsOrigins string,
) {
	t.Helper()

	if envName != "production" {
		return
	}

	validateProductionCredentials(t, cfg, err, authEnabled)
	validateProductionCORSIfNeeded(t, cfg, corsOrigins)
	validateProductionTLS(t, cfg, err)
}

func validateProductionCredentials(t *testing.T, cfg *Config, err error, authEnabled bool) {
	t.Helper()

	if cfg.Postgres.PrimaryPassword == "" && err == nil {
		t.Errorf("expected error for production without password")
	}

	if !authEnabled && err == nil {
		t.Errorf("expected error for production without auth enabled")
	}
}

func validateProductionCORSIfNeeded(t *testing.T, cfg *Config, corsOrigins string) {
	t.Helper()

	if strings.TrimSpace(corsOrigins) == "" || strings.Contains(corsOrigins, "*") {
		validateProductionCORS(t, cfg)
	}
}

func validateProductionTLS(t *testing.T, cfg *Config, err error) {
	t.Helper()

	if strings.EqualFold(cfg.Postgres.PrimarySSLMode, "disable") && err == nil {
		t.Errorf("expected error for production without database tls")
	}

	if !cfg.Redis.TLS && err == nil {
		t.Errorf("expected error for production without redis tls")
	}

	if strings.EqualFold(cfg.RabbitMQ.URI, "amqp") && err == nil {
		t.Errorf("expected error for production without amqps")
	}
}

func validateTLSRequirements(t *testing.T, err error, tlsCert, tlsKey string) {
	t.Helper()

	certEmpty := strings.TrimSpace(tlsCert) == ""
	keyEmpty := strings.TrimSpace(tlsKey) == ""

	if certEmpty != keyEmpty && err == nil {
		t.Errorf("expected error for mismatched tls cert/key")
	}
}

func validateAuthRequirements(t *testing.T, err error, authEnabled bool, authHost string) {
	t.Helper()

	if authEnabled && authHost == "" && err == nil {
		t.Errorf("expected error for auth enabled without host")
	}
}

// validateProductionCORS validates CORS settings in production by creating a sanitized copy
// of the config with all other production requirements satisfied, then checking CORS validation.
func validateProductionCORS(t *testing.T, cfg *Config) {
	t.Helper()

	// Avoid false positives due to earlier validation rules failing first
	// (e.g., missing DB password / auth disabled). Validate a sanitized copy
	// so we specifically exercise the CORS restriction in production.
	corsCfg := *cfg

	if strings.TrimSpace(corsCfg.Postgres.PrimaryPassword) == "" {
		corsCfg.Postgres.PrimaryPassword = "non-empty"
	}

	corsCfg.Auth.Enabled = true

	if strings.TrimSpace(corsCfg.Auth.Host) == "" {
		corsCfg.Auth.Host = "http://auth:8080"
	}

	if strings.TrimSpace(corsCfg.Auth.TokenSecret) == "" {
		corsCfg.Auth.TokenSecret = "secret"
	}

	corsCfg.Postgres.PrimarySSLMode = "require"
	corsCfg.Redis.TLS = true
	corsCfg.RabbitMQ.URI = "amqps"
	corsCfg.RabbitMQ.User = "matcher"
	corsCfg.RabbitMQ.Password = "secure-pass"

	corsErr := corsCfg.Validate()
	if corsErr == nil {
		t.Errorf("expected error for production with wildcard cors")
	} else if !strings.Contains(corsErr.Error(), "CORS_ALLOWED_ORIGINS") {
		t.Errorf("expected CORS validation error, got: %v", corsErr)
	}
}
