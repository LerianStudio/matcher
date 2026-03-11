// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"os"
	"reflect"
	"strconv"
	"strings"
)

// configFieldDef is the static definition of a config field.
// CurrentValue is populated at runtime from the live config.
type configFieldDef struct {
	Key           string
	Label         string
	Type          string // "string", "int", "bool"
	DefaultValue  any
	HotReloadable bool
	EnvVar        string
	Constraints   []string
	Description   string
	Section       string
	Secret        bool // if true, value is redacted in API responses
}

// redactedValue is the placeholder shown for secret fields.
const redactedValue = "********"

// fieldDescriptions maps mapstructure keys to human-readable descriptions.
// This is the single place to maintain descriptions — reflection derives everything else.
//
//nolint:gochecknoglobals // package-level lookup table, read-only after init.
var fieldDescriptions = map[string]string{ // #nosec G101 -- UI description labels, not credentials
	// app
	"app.env_name":  "Deployment environment (development, staging, production)",
	"app.log_level": "Application log verbosity level",

	// server
	"server.address":                 "HTTP server bind address",
	"server.body_limit_bytes":        "Maximum HTTP request body size in bytes",
	"server.cors_allowed_origins":    "Comma-separated list of allowed CORS origins",
	"server.cors_allowed_methods":    "Comma-separated list of allowed HTTP methods",
	"server.cors_allowed_headers":    "Comma-separated list of allowed request headers",
	"server.tls_cert_file":           "Path to TLS certificate file",
	"server.tls_key_file":            "Path to TLS private key file",
	"server.tls_terminated_upstream": "Whether TLS is terminated by a reverse proxy upstream",
	"server.trusted_proxies":         "Comma-separated list of trusted proxy addresses",

	// tenancy
	"tenancy.default_tenant_id":          "UUID of the default tenant",
	"tenancy.default_tenant_slug":        "URL slug for the default tenant",
	"tenancy.multi_tenant_infra_enabled": "Enable multi-tenant infrastructure isolation",

	// postgres
	"postgres.primary_host":            "PostgreSQL primary server hostname",
	"postgres.primary_port":            "PostgreSQL primary server port",
	"postgres.primary_user":            "PostgreSQL primary username",
	"postgres.primary_password":        "PostgreSQL primary password",
	"postgres.primary_db":              "PostgreSQL primary database name",
	"postgres.primary_ssl_mode":        "PostgreSQL primary SSL mode",
	"postgres.replica_host":            "PostgreSQL replica server hostname",
	"postgres.replica_port":            "PostgreSQL replica server port",
	"postgres.replica_user":            "PostgreSQL replica username",
	"postgres.replica_password":        "PostgreSQL replica password",
	"postgres.replica_db":              "PostgreSQL replica database name",
	"postgres.replica_ssl_mode":        "PostgreSQL replica SSL mode",
	"postgres.max_open_connections":    "Maximum open database connections",
	"postgres.max_idle_connections":    "Maximum idle database connections",
	"postgres.conn_max_lifetime_mins":  "Maximum connection lifetime in minutes",
	"postgres.conn_max_idle_time_mins": "Maximum idle time per connection in minutes",
	"postgres.connect_timeout_sec":     "Database connection timeout in seconds",
	"postgres.query_timeout_sec":       "Database query timeout in seconds",
	"postgres.migrations_path":         "Path to database migration files",

	// redis
	"redis.host":             "Redis server address",
	"redis.master_name":      "Redis Sentinel master name",
	"redis.password":         "Redis password",
	"redis.db":               "Redis database number",
	"redis.protocol":         "Redis protocol version",
	"redis.tls":              "Enable TLS for Redis connections",
	"redis.ca_cert":          "Redis CA certificate for TLS",
	"redis.pool_size":        "Redis connection pool size",
	"redis.min_idle_conn":    "Minimum idle Redis connections",
	"redis.read_timeout_ms":  "Redis read timeout in milliseconds",
	"redis.write_timeout_ms": "Redis write timeout in milliseconds",
	"redis.dial_timeout_ms":  "Redis dial timeout in milliseconds",

	// rabbitmq
	"rabbitmq.uri":                         "RabbitMQ connection URI scheme",
	"rabbitmq.host":                        "RabbitMQ server hostname",
	"rabbitmq.port":                        "RabbitMQ server port",
	"rabbitmq.user":                        "RabbitMQ username",
	"rabbitmq.password":                    "RabbitMQ password",
	"rabbitmq.vhost":                       "RabbitMQ virtual host",
	"rabbitmq.health_url":                  "RabbitMQ management health URL",
	"rabbitmq.allow_insecure_health_check": "Allow insecure TLS for health checks",

	// auth
	"auth.enabled":      "Enable JWT authentication",
	"auth.host":         "Auth service address",
	"auth.token_secret": "JWT signing secret",

	// swagger
	"swagger.enabled": "Enable Swagger UI (non-production only)",
	"swagger.host":    "Swagger spec host override",
	"swagger.schemes": "Swagger spec schemes (comma-separated)",

	// telemetry
	"telemetry.enabled":                 "Enable OpenTelemetry tracing and metrics",
	"telemetry.service_name":            "OpenTelemetry service name",
	"telemetry.library_name":            "OpenTelemetry instrumentation library name",
	"telemetry.service_version":         "OpenTelemetry service version",
	"telemetry.deployment_env":          "OpenTelemetry deployment environment",
	"telemetry.collector_endpoint":      "OpenTelemetry collector OTLP endpoint",
	"telemetry.db_metrics_interval_sec": "Database metrics collection interval in seconds",

	// rate_limit
	"rate_limit.enabled":             "Enable global rate limiting",
	"rate_limit.max":                 "Maximum requests per window",
	"rate_limit.expiry_sec":          "Rate limit window duration in seconds",
	"rate_limit.export_max":          "Maximum export requests per window",
	"rate_limit.export_expiry_sec":   "Export rate limit window in seconds",
	"rate_limit.dispatch_max":        "Maximum dispatch requests per window",
	"rate_limit.dispatch_expiry_sec": "Dispatch rate limit window in seconds",

	// infrastructure
	"infrastructure.connect_timeout_sec":      "Infrastructure connection timeout in seconds",
	"infrastructure.health_check_timeout_sec": "Health check timeout in seconds",

	// idempotency
	"idempotency.retry_window_sec":  "Failed idempotency key retry window in seconds",
	"idempotency.success_ttl_hours": "Completed idempotency key cache duration in hours",
	"idempotency.hmac_secret":       "Server-side HMAC secret for idempotency key signing",

	// callback_rate_limit
	"callback_rate_limit.per_minute": "Max callbacks per external system per minute",

	// deduplication
	"deduplication.ttl_sec": "Deduplication key TTL in seconds",

	// object_storage
	"object_storage.endpoint":          "Object storage endpoint URL",
	"object_storage.region":            "Object storage region",
	"object_storage.bucket":            "Object storage bucket name",
	"object_storage.access_key_id":     "Object storage access key ID",
	"object_storage.secret_access_key": "Object storage secret access key",
	"object_storage.use_path_style":    "Use path-style addressing for S3",

	// export_worker
	"export_worker.enabled":            "Enable background export worker",
	"export_worker.poll_interval_sec":  "Export worker poll interval in seconds",
	"export_worker.page_size":          "Number of records per export page",
	"export_worker.presign_expiry_sec": "Presigned URL expiry in seconds",

	// cleanup_worker
	"cleanup_worker.enabled":          "Enable background cleanup worker",
	"cleanup_worker.interval_sec":     "Cleanup worker interval in seconds",
	"cleanup_worker.batch_size":       "Number of records per cleanup batch",
	"cleanup_worker.grace_period_sec": "Grace period before cleanup in seconds",

	// scheduler
	"scheduler.interval_sec": "Scheduler poll interval in seconds",

	// archival
	"archival.enabled":               "Enable audit log archival worker",
	"archival.interval_hours":        "Archival worker run interval in hours",
	"archival.hot_retention_days":    "Days to retain hot audit log data",
	"archival.warm_retention_months": "Months to retain warm archived data",
	"archival.cold_retention_months": "Months to retain cold archived data",
	"archival.batch_size":            "Records per archival batch",
	"archival.storage_bucket":        "Object storage bucket for archives",
	"archival.storage_prefix":        "Object key prefix for archived files",
	"archival.storage_class":         "Storage class for archived objects",
	"archival.partition_lookahead":   "Number of future partitions to pre-create",
	"archival.presign_expiry_sec":    "Presigned URL expiry for archive downloads",

	// webhook
	"webhook.timeout_sec": "HTTP timeout for webhook dispatches in seconds",

	// fetcher
	"fetcher.enabled":                "Enable Fetcher service integration",
	"fetcher.url":                    "Fetcher service base URL",
	"fetcher.allow_private_ips":      "Allow Fetcher to connect to private IPs",
	"fetcher.health_timeout_sec":     "Fetcher health check timeout in seconds",
	"fetcher.request_timeout_sec":    "Fetcher HTTP request timeout in seconds",
	"fetcher.discovery_interval_sec": "Fetcher schema discovery interval in seconds",
	"fetcher.schema_cache_ttl_sec":   "Fetcher schema cache TTL in seconds",
	"fetcher.extraction_poll_sec":    "Fetcher extraction poll interval in seconds",
	"fetcher.extraction_timeout_sec": "Fetcher extraction timeout in seconds",
}

// fieldLabels maps mapstructure keys to human-readable labels.
// When absent, a label is auto-generated from the field name.
//
//nolint:gochecknoglobals // package-level lookup table, read-only after init.
var fieldLabels = map[string]string{
	"app.env_name":                            "Environment Name",
	"app.log_level":                           "Log Level",
	"server.address":                          "Listen Address",
	"server.body_limit_bytes":                 "Body Limit (bytes)",
	"server.cors_allowed_origins":             "CORS Allowed Origins",
	"server.cors_allowed_methods":             "CORS Allowed Methods",
	"server.cors_allowed_headers":             "CORS Allowed Headers",
	"server.tls_cert_file":                    "TLS Certificate File",
	"server.tls_key_file":                     "TLS Key File",
	"server.tls_terminated_upstream":          "TLS Terminated Upstream",
	"server.trusted_proxies":                  "Trusted Proxies",
	"tenancy.default_tenant_id":               "Default Tenant ID",
	"tenancy.default_tenant_slug":             "Default Tenant Slug",
	"tenancy.multi_tenant_infra_enabled":      "Multi-Tenant Infra Enabled",
	"postgres.primary_host":                   "Primary Host",
	"postgres.primary_port":                   "Primary Port",
	"postgres.primary_user":                   "Primary User",
	"postgres.primary_password":               "Primary Password",
	"postgres.primary_db":                     "Primary Database",
	"postgres.primary_ssl_mode":               "Primary SSL Mode",
	"postgres.replica_host":                   "Replica Host",
	"postgres.replica_port":                   "Replica Port",
	"postgres.replica_user":                   "Replica User",
	"postgres.replica_password":               "Replica Password",
	"postgres.replica_db":                     "Replica Database",
	"postgres.replica_ssl_mode":               "Replica SSL Mode",
	"postgres.max_open_connections":           "Max Open Connections",
	"postgres.max_idle_connections":           "Max Idle Connections",
	"postgres.conn_max_lifetime_mins":         "Conn Max Lifetime (min)",
	"postgres.conn_max_idle_time_mins":        "Conn Max Idle Time (min)",
	"postgres.connect_timeout_sec":            "Connect Timeout (sec)",
	"postgres.query_timeout_sec":              "Query Timeout (sec)",
	"postgres.migrations_path":                "Migrations Path",
	"redis.host":                              "Host",
	"redis.master_name":                       "Master Name",
	"redis.password":                          "Password",
	"redis.db":                                "Database",
	"redis.protocol":                          "Protocol",
	"redis.tls":                               "TLS Enabled",
	"redis.ca_cert":                           "CA Certificate",
	"redis.pool_size":                         "Pool Size",
	"redis.min_idle_conn":                     "Min Idle Connections",
	"redis.read_timeout_ms":                   "Read Timeout (ms)",
	"redis.write_timeout_ms":                  "Write Timeout (ms)",
	"redis.dial_timeout_ms":                   "Dial Timeout (ms)",
	"rabbitmq.uri":                            "URI",
	"rabbitmq.host":                           "Host",
	"rabbitmq.port":                           "Port",
	"rabbitmq.user":                           "User",
	"rabbitmq.password":                       "Password",
	"rabbitmq.vhost":                          "VHost",
	"rabbitmq.health_url":                     "Health URL",
	"rabbitmq.allow_insecure_health_check":    "Allow Insecure Health Check",
	"auth.enabled":                            "Auth Enabled",
	"auth.host":                               "Auth Host",
	"auth.token_secret":                       "JWT Secret",
	"swagger.enabled":                         "Swagger Enabled",
	"swagger.host":                            "Swagger Host",
	"swagger.schemes":                         "Swagger Schemes",
	"telemetry.enabled":                       "Telemetry Enabled",
	"telemetry.service_name":                  "Service Name",
	"telemetry.library_name":                  "Library Name",
	"telemetry.service_version":               "Service Version",
	"telemetry.deployment_env":                "Deployment Environment",
	"telemetry.collector_endpoint":            "Collector Endpoint",
	"telemetry.db_metrics_interval_sec":       "DB Metrics Interval (sec)",
	"rate_limit.enabled":                      "Rate Limit Enabled",
	"rate_limit.max":                          "Max Requests",
	"rate_limit.expiry_sec":                   "Window (sec)",
	"rate_limit.export_max":                   "Export Max",
	"rate_limit.export_expiry_sec":            "Export Window (sec)",
	"rate_limit.dispatch_max":                 "Dispatch Max",
	"rate_limit.dispatch_expiry_sec":          "Dispatch Window (sec)",
	"infrastructure.connect_timeout_sec":      "Connect Timeout (sec)",
	"infrastructure.health_check_timeout_sec": "Health Check Timeout (sec)",
	"idempotency.retry_window_sec":            "Retry Window (sec)",
	"idempotency.success_ttl_hours":           "Success TTL (hours)",
	"idempotency.hmac_secret":                 "HMAC Secret",
	"callback_rate_limit.per_minute":          "Callbacks Per Minute",
	"deduplication.ttl_sec":                   "Dedupe TTL (sec)",
	"object_storage.endpoint":                 "Endpoint",
	"object_storage.region":                   "Region",
	"object_storage.bucket":                   "Bucket",
	"object_storage.access_key_id":            "Access Key ID",
	"object_storage.secret_access_key":        "Secret Access Key",
	"object_storage.use_path_style":           "Use Path Style",
	"export_worker.enabled":                   "Export Worker Enabled",
	"export_worker.poll_interval_sec":         "Poll Interval (sec)",
	"export_worker.page_size":                 "Page Size",
	"export_worker.presign_expiry_sec":        "Presign Expiry (sec)",
	"cleanup_worker.enabled":                  "Cleanup Worker Enabled",
	"cleanup_worker.interval_sec":             "Interval (sec)",
	"cleanup_worker.batch_size":               "Batch Size",
	"cleanup_worker.grace_period_sec":         "Grace Period (sec)",
	"scheduler.interval_sec":                  "Scheduler Interval (sec)",
	"archival.enabled":                        "Archival Enabled",
	"archival.interval_hours":                 "Interval (hours)",
	"archival.hot_retention_days":             "Hot Retention (days)",
	"archival.warm_retention_months":          "Warm Retention (months)",
	"archival.cold_retention_months":          "Cold Retention (months)",
	"archival.batch_size":                     "Batch Size",
	"archival.storage_bucket":                 "Storage Bucket",
	"archival.storage_prefix":                 "Storage Prefix",
	"archival.storage_class":                  "Storage Class",
	"archival.partition_lookahead":            "Partition Lookahead",
	"archival.presign_expiry_sec":             "Presign Expiry (sec)",
	"webhook.timeout_sec":                     "Webhook Timeout (sec)",
	"fetcher.enabled":                         "Fetcher Enabled",
	"fetcher.url":                             "Fetcher URL",
	"fetcher.allow_private_ips":               "Allow Private IPs",
	"fetcher.health_timeout_sec":              "Health Timeout (sec)",
	"fetcher.request_timeout_sec":             "Request Timeout (sec)",
	"fetcher.discovery_interval_sec":          "Discovery Interval (sec)",
	"fetcher.schema_cache_ttl_sec":            "Schema Cache TTL (sec)",
	"fetcher.extraction_poll_sec":             "Extraction Poll (sec)",
	"fetcher.extraction_timeout_sec":          "Extraction Timeout (sec)",
}

// fieldConstraints maps mapstructure keys to validation constraints.
// Domain-specific constraints cannot be derived from struct tags.
//
//nolint:gochecknoglobals // package-level lookup table, read-only after init.
var fieldConstraints = map[string][]string{
	"app.log_level":                    {"enum:debug,info,warn,error"},
	"server.body_limit_bytes":          {"min:1"},
	"postgres.primary_ssl_mode":        {"enum:disable,require,verify-ca,verify-full"},
	"postgres.max_open_connections":    {"min:1"},
	"postgres.max_idle_connections":    {"min:0"},
	"postgres.query_timeout_sec":       {"min:0"},
	"redis.db":                         {"min:0"},
	"redis.pool_size":                  {"min:1"},
	"rate_limit.max":                   {"min:1", "max:1000000"},
	"rate_limit.expiry_sec":            {"min:1"},
	"rate_limit.export_max":            {"min:1"},
	"rate_limit.export_expiry_sec":     {"min:1"},
	"rate_limit.dispatch_max":          {"min:1"},
	"rate_limit.dispatch_expiry_sec":   {"min:1"},
	"export_worker.poll_interval_sec":  {"min:1"},
	"export_worker.page_size":          {"min:1"},
	"export_worker.presign_expiry_sec": {"min:60"},
	"cleanup_worker.interval_sec":      {"min:60"},
	"cleanup_worker.batch_size":        {"min:1"},
	"cleanup_worker.grace_period_sec":  {"min:60"},
	"scheduler.interval_sec":           {"min:1"},
	"webhook.timeout_sec":              {"min:1"},
	"callback_rate_limit.per_minute":   {"min:1"},
	"deduplication.ttl_sec":            {"min:1"},
	"idempotency.retry_window_sec":     {"min:1"},
	"idempotency.success_ttl_hours":    {"min:1"},
	"fetcher.health_timeout_sec":       {"min:1"},
	"fetcher.request_timeout_sec":      {"min:1"},
	"fetcher.discovery_interval_sec":   {"min:1"},
	"fetcher.schema_cache_ttl_sec":     {"min:1"},
	"fetcher.extraction_poll_sec":      {"min:1"},
	"fetcher.extraction_timeout_sec":   {"min:1"},
	"archival.interval_hours":          {"min:1"},
	"archival.batch_size":              {"min:1"},
}

// buildConfigSchema returns the schema definitions for all config fields,
// derived via reflection from the Config struct's tags. Descriptions,
// labels, and constraints come from the companion maps above.
func buildConfigSchema() []configFieldDef {
	var defs []configFieldDef

	t := reflect.TypeOf(Config{})

	for i := range t.NumField() {
		sectionField := t.Field(i)
		if !sectionField.IsExported() {
			continue
		}

		sectionTag := sectionField.Tag.Get("mapstructure")
		if sectionTag == "-" || sectionTag == "" {
			continue
		}

		// Only recurse into struct types (sub-configs).
		ft := sectionField.Type
		if ft.Kind() != reflect.Struct {
			continue
		}

		collectSchemaFields(&defs, ft, sectionTag)
	}

	return defs
}

// collectSchemaFields appends a configFieldDef for each leaf field in the given
// struct type, using the section prefix for dotted key construction.
func collectSchemaFields(defs *[]configFieldDef, t reflect.Type, section string) {
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		tag := field.Tag.Get("mapstructure")
		if tag == "-" || tag == "" {
			continue
		}

		key := section + "." + tag

		def := configFieldDef{
			Key:           key,
			Type:          goKindToSchemaType(field.Type.Kind()),
			DefaultValue:  parseDefaultValue(field),
			EnvVar:        extractEnvVar(field),
			Section:       section,
			Secret:        isSensitiveKey(key),
			HotReloadable: mutableConfigKeys[key],
			Description:   fieldDescriptions[key],
			Label:         fieldLabels[key],
			Constraints:   fieldConstraints[key],
		}

		// Fallback: generate label from the mapstructure tag if none is registered.
		if def.Label == "" {
			def.Label = labelFromTag(tag)
		}

		// Fallback: generate description from the label if none is registered.
		if def.Description == "" {
			def.Description = def.Label
		}

		*defs = append(*defs, def)
	}
}

// goKindToSchemaType maps a reflect.Kind to the schema type string.
func goKindToSchemaType(k reflect.Kind) string {
	switch k {
	case reflect.Bool:
		return "bool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int"
	default:
		return "string"
	}
}

// extractEnvVar reads the first token from the `env` struct tag.
func extractEnvVar(field reflect.StructField) string {
	env := field.Tag.Get("env")
	if env == "" {
		return ""
	}

	if idx := strings.IndexByte(env, ','); idx >= 0 {
		return env[:idx]
	}

	return env
}

// parseDefaultValue extracts the envDefault tag value and coerces it to the
// correct Go type (int/bool/string) to match the existing schema contract.
func parseDefaultValue(field reflect.StructField) any {
	raw := field.Tag.Get("envDefault")

	switch field.Type.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if raw == "" {
			return 0
		}

		v, err := strconv.Atoi(raw)
		if err != nil {
			return 0
		}

		return v
	case reflect.Bool:
		return raw == "true"
	default:
		return raw
	}
}

// labelFromTag converts a snake_case mapstructure tag to a Title Case label.
// e.g. "primary_ssl_mode" → "Primary Ssl Mode".
func labelFromTag(tag string) string {
	parts := strings.Split(tag, "_")
	for i, p := range parts {
		if p == "" {
			continue
		}

		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}

	return strings.Join(parts, " ")
}

// isEnvOverridden returns true if the given env var is set in the process environment.
// It checks both legacy keys (e.g. LOG_LEVEL) and MATCHER-prefixed keys
// (e.g. MATCHER_APP_LOG_LEVEL) used by Viper.
func isEnvOverridden(envVar, key string) bool {
	if envVar == "" && key == "" {
		return false
	}

	if envVar != "" {
		if _, exists := os.LookupEnv(envVar); exists {
			return true
		}
	}

	if key == "" {
		return false
	}

	prefixedKey := "MATCHER_" + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
	_, exists := os.LookupEnv(prefixedKey)

	return exists
}

// buildSchemaResponse builds a ConfigSchemaResponse from the static schema
// and the current effective config snapshot (via viper for value resolution).
func buildSchemaResponse(cm *ConfigManager) ConfigSchemaResponse {
	defs := buildConfigSchema()
	sections := make(map[string][]ConfigFieldSchema, len(defs))
	visibleFields := 0

	for _, def := range defs {
		if def.Secret {
			continue
		}

		currentVal := resolveCurrentValue(cm, def)

		field := ConfigFieldSchema{
			Key:           def.Key,
			Label:         def.Label,
			Type:          def.Type,
			DefaultValue:  def.DefaultValue,
			CurrentValue:  currentVal,
			HotReloadable: def.HotReloadable,
			EnvOverride:   isEnvOverridden(def.EnvVar, def.Key),
			EnvVar:        def.EnvVar,
			Constraints:   def.Constraints,
			Description:   def.Description,
			Section:       def.Section,
		}

		sections[def.Section] = append(sections[def.Section], field)
		visibleFields++
	}

	return ConfigSchemaResponse{
		Sections:    sections,
		TotalFields: visibleFields,
	}
}

// resolveCurrentValue reads the current value from viper (which merges YAML + env).
// Secret fields are redacted.
func resolveCurrentValue(cm *ConfigManager, def configFieldDef) any {
	if def.Secret {
		return redactedValue
	}

	if val, ok := resolveCurrentConfigValue(cm, def.Key); ok {
		return val
	}

	return def.DefaultValue
}

// buildRedactedConfig builds a map representation of the current config
// with secrets replaced by redactedValue.
func buildRedactedConfig(cm *ConfigManager) map[string]any {
	defs := buildConfigSchema()
	result := make(map[string]any, len(defs))

	for _, def := range defs {
		if def.Secret {
			result[def.Key] = redactedValue

			continue
		}

		if val, ok := resolveCurrentConfigValue(cm, def.Key); ok {
			result[def.Key] = val

			continue
		}

		result[def.Key] = def.DefaultValue
	}

	return result
}

// buildEnvOverridesList returns the list of config keys currently overridden by env vars.
func buildEnvOverridesList() []string {
	defs := buildConfigSchema()
	overrides := make([]string, 0)

	for _, def := range defs {
		if isEnvOverridden(def.EnvVar, def.Key) {
			overrides = append(overrides, def.Key)
		}
	}

	return overrides
}

func resolveConfigValue(cfg *Config, key string) (any, bool) {
	if cfg == nil || strings.TrimSpace(key) == "" {
		return nil, false
	}

	parts := strings.Split(key, ".")
	current, ok := derefPointerValue(reflect.ValueOf(cfg))

	if !ok {
		return nil, false
	}

	for idx, part := range parts {
		if current.Kind() != reflect.Struct {
			return nil, false
		}

		next, found := findMapstructureField(current, part)
		if !found {
			return nil, false
		}

		if idx == len(parts)-1 {
			return next.Interface(), true
		}

		current, ok = derefPointerValue(next)
		if !ok {
			return nil, false
		}
	}

	return nil, false
}

func resolveCurrentConfigValue(cm *ConfigManager, key string) (any, bool) {
	if cm == nil {
		return nil, false
	}

	if cfg := cm.Get(); cfg != nil {
		if val, ok := resolveConfigValue(cfg, key); ok {
			return val, true
		}
	}

	if cm.viper == nil {
		return nil, false
	}

	val := cm.viper.Get(key)
	if val == nil {
		return nil, false
	}

	return val, true
}

func derefPointerValue(value reflect.Value) (reflect.Value, bool) {
	if value.Kind() != reflect.Pointer {
		return value, true
	}

	if value.IsNil() {
		return reflect.Value{}, false
	}

	return value.Elem(), true
}

func findMapstructureField(current reflect.Value, part string) (reflect.Value, bool) {
	currentType := current.Type()

	for i := range currentType.NumField() {
		field := currentType.Field(i)
		if !field.IsExported() {
			continue
		}

		if field.Tag.Get("mapstructure") != part {
			continue
		}

		return current.Field(i), true
	}

	return reflect.Value{}, false
}
