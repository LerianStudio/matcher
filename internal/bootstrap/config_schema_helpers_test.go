// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

// secretFields lists YAML keys whose values must be redacted.
// This is a test-only helper used to verify redaction behavior.
var secretFields = map[string]bool{
	"postgres.primary_password":        true,
	"postgres.replica_password":        true,
	"redis.password":                   true,
	"rabbitmq.password":                true,
	"auth.token_secret":                true,
	"idempotency.hmac_secret":          true,
	"object_storage.access_key_id":     true,
	"object_storage.secret_access_key": true,
	"redis.ca_cert":                    true,
}

// sectionNames returns the ordered list of unique section names.
// This is a test-only helper used to verify schema section coverage.
func sectionNames() []string {
	seen := make(map[string]bool)

	var names []string

	for _, def := range buildConfigSchema() {
		if !seen[def.Section] {
			seen[def.Section] = true
			names = append(names, def.Section)
		}
	}

	return names
}

// schemaKeySet returns a set of all schema keys for validation.
// This is a test-only helper used to verify schema key completeness.
func schemaKeySet() map[string]bool {
	defs := buildConfigSchema()
	keys := make(map[string]bool, len(defs))

	for _, def := range defs {
		keys[def.Key] = true
	}

	return keys
}
