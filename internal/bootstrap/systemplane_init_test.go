// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

// TestValidateSystemplaneSecrets_MissingMasterKeyInProd asserts that a production
// deployment without SYSTEMPLANE_SECRET_MASTER_KEY is rejected. This is a critical
// security guardrail: running without a master key would leave systemplane secret
// payloads unencrypted.
func TestValidateSystemplaneSecrets_MissingMasterKeyInProd(t *testing.T) {
	// Cannot be parallel: t.Setenv mutates process env.
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "")

	err := ValidateSystemplaneSecrets("production")

	require.Error(t, err)
	assert.True(t, errors.Is(err, errSystemplaneSecretMasterKey),
		"expected errSystemplaneSecretMasterKey, got: %v", err)
}

// TestValidateSystemplaneSecrets_DevDefaultInProd asserts the well-known
// development default key (committed in docker-compose.yml) is rejected in
// production to prevent accidental deployment with a publicly-known key.
func TestValidateSystemplaneSecrets_DevDefaultInProd(t *testing.T) {
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", wellKnownDevMasterKey)

	err := ValidateSystemplaneSecrets("production")

	require.Error(t, err)
	assert.True(t, errors.Is(err, errSystemplaneDevMasterKeyInNonDev),
		"expected errSystemplaneDevMasterKeyInNonDev, got: %v", err)
}

// TestValidateSystemplaneSecrets_DevDefaultRejectedOutsideDev asserts the
// well-known dev key is rejected in every environment that is not explicitly
// "development" or "test". Staging, UAT, QA, preview, and any unknown value
// must all reject — they can hold real data, so the publicly-known key would
// be a credential leak.
func TestValidateSystemplaneSecrets_DevDefaultRejectedOutsideDev(t *testing.T) {
	for _, envName := range []string{"staging", "uat", "qa", "preview", "sandbox", "", "Production"} {
		t.Run(envName, func(t *testing.T) {
			t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", wellKnownDevMasterKey)

			err := ValidateSystemplaneSecrets(envName)

			require.Error(t, err, "env %q must reject the dev default key", envName)
			assert.True(t, errors.Is(err, errSystemplaneDevMasterKeyInNonDev),
				"env %q: expected errSystemplaneDevMasterKeyInNonDev, got: %v", envName, err)
		})
	}
}

// TestValidateSystemplaneSecrets_ValidKeyInProd asserts a non-default key in
// production passes validation.
func TestValidateSystemplaneSecrets_ValidKeyInProd(t *testing.T) {
	// Random 32-byte base64 key (not the dev default).
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")

	err := ValidateSystemplaneSecrets("production")

	assert.NoError(t, err)
}

// TestValidateSystemplaneSecrets_DevDefaultAllowedInDevAndTest asserts the
// dev default is allowed in exactly two environments: "development" and
// "test" (case-insensitive). Local developers and unit-test harnesses need
// this path; everything else goes through the rejection branch above.
func TestValidateSystemplaneSecrets_DevDefaultAllowedInDevAndTest(t *testing.T) {
	for _, envName := range []string{"development", "test", "DEVELOPMENT", "Test"} {
		t.Run(envName, func(t *testing.T) {
			t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", wellKnownDevMasterKey)

			err := ValidateSystemplaneSecrets(envName)

			assert.NoError(t, err, "env %q must accept the dev default key", envName)
		})
	}
}

// TestValidateSystemplaneSecrets_NonProdMissingKey asserts that non-production
// still requires *some* master key — the empty-key check is universal.
func TestValidateSystemplaneSecrets_NonProdMissingKey(t *testing.T) {
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "")

	err := ValidateSystemplaneSecrets("development")

	require.Error(t, err)
	assert.True(t, errors.Is(err, errSystemplaneSecretMasterKey))
}

// TestSystemplaneGetString_NilClient asserts nil-client returns the fallback
// without panicking.
func TestSystemplaneGetString_NilClient(t *testing.T) {
	t.Parallel()

	got := SystemplaneGetString(nil, "some.key", "fallback-value")

	assert.Equal(t, "fallback-value", got)
}

// TestSystemplaneGetInt_NilClient asserts nil-client returns the fallback
// without panicking.
func TestSystemplaneGetInt_NilClient(t *testing.T) {
	t.Parallel()

	got := SystemplaneGetInt(nil, "some.key", 42)

	assert.Equal(t, 42, got)
}

// TestSystemplaneGetBool_NilClient asserts nil-client returns the fallback
// without panicking.
func TestSystemplaneGetBool_NilClient(t *testing.T) {
	t.Parallel()

	got := SystemplaneGetBool(nil, "some.key", true)

	assert.True(t, got)
}

// recordingLogger records every Log call so tests can assert on WARN
// emission without depending on a real log sink.
type recordingLogger struct {
	libLog.Logger
	records []recordedLog
}

type recordedLog struct {
	level  libLog.Level
	msg    string
	fields []libLog.Field
}

func (rl *recordingLogger) Log(_ context.Context, level libLog.Level, msg string, fields ...libLog.Field) {
	rl.records = append(rl.records, recordedLog{level: level, msg: msg, fields: fields})
}

func (rl *recordingLogger) With(_ ...libLog.Field) libLog.Logger { return rl }
func (rl *recordingLogger) WithGroup(_ string) libLog.Logger     { return rl }
func (rl *recordingLogger) Enabled(_ libLog.Level) bool          { return true }
func (rl *recordingLogger) Sync(_ context.Context) error         { return nil }

// TestWarnOnReclassifiedOrphanKeys_NilArgsAreNoOps asserts the guard never
// panics when the caller omits either dependency.
func TestWarnOnReclassifiedOrphanKeys_NilArgsAreNoOps(t *testing.T) {
	t.Parallel()

	// nil db → early return.
	warnOnReclassifiedOrphanKeys(context.Background(), nil, &recordingLogger{})

	// nil logger → early return (also covers the zero-value case).
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	warnOnReclassifiedOrphanKeys(context.Background(), db, nil)

	// No expectations were set, so no queries should have fired.
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestWarnOnReclassifiedOrphanKeys_NoTable_SilentlyReturns asserts the
// guard does not emit warnings when systemplane_entries has not yet been
// created (first-time deploys) — this is the expected state before v5's
// Client.Start runs ensureSchema.
func TestWarnOnReclassifiedOrphanKeys_NoTable_SilentlyReturns(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT EXISTS").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	logger := &recordingLogger{}

	warnOnReclassifiedOrphanKeys(context.Background(), db, logger)

	require.NoError(t, mock.ExpectationsWereMet())
	assert.Empty(t, logger.records, "no warn should fire when the table is missing")
}

// anyKeyArgs returns a slice of sqlmock.AnyArg matchers matching one
// positional placeholder per entry in reclassifiedBootstrapOnlyKeys.
func anyKeyArgs() []driver.Value {
	matchers := make([]driver.Value, 0, len(reclassifiedBootstrapOnlyKeys))
	for range reclassifiedBootstrapOnlyKeys {
		matchers = append(matchers, sqlmock.AnyArg())
	}

	return matchers
}

// TestWarnOnReclassifiedOrphanKeys_EmptyTable_NoWarns asserts a clean
// deployment (table exists, zero orphan rows) stays silent so healthy
// startup logs don't become noisy.
func TestWarnOnReclassifiedOrphanKeys_EmptyTable_NoWarns(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT EXISTS").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT key FROM public\.systemplane_entries`).
		WithArgs(anyKeyArgs()...).
		WillReturnRows(sqlmock.NewRows([]string{"key"}))

	logger := &recordingLogger{}

	warnOnReclassifiedOrphanKeys(context.Background(), db, logger)

	require.NoError(t, mock.ExpectationsWereMet())
	assert.Empty(t, logger.records, "no warn should fire when there are no orphan rows")
}

// TestWarnOnReclassifiedOrphanKeys_EmitsWarnPerOrphan asserts each surviving
// orphan key produces one WARN log entry. Covers the primary operational
// signal that migration 000030 under-cleaned or a peer process wrote a row
// post-migration.
func TestWarnOnReclassifiedOrphanKeys_EmitsWarnPerOrphan(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT EXISTS").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT key FROM public\.systemplane_entries`).
		WithArgs(anyKeyArgs()...).
		WillReturnRows(sqlmock.NewRows([]string{"key"}).
			AddRow("auth.token_secret").
			AddRow("outbox.retry_window_sec"))

	logger := &recordingLogger{}

	warnOnReclassifiedOrphanKeys(context.Background(), db, logger)

	require.NoError(t, mock.ExpectationsWereMet())
	require.Len(t, logger.records, 2, "one WARN per observed orphan row")

	for _, rec := range logger.records {
		assert.Equal(t, libLog.LevelWarn, rec.level)
		assert.Contains(t, rec.msg, "reclassified bootstrap-only key")
	}
}

// TestWarnOnReclassifiedOrphanKeys_CoversMatcherReclassifications asserts
// the hard-coded key list stays in sync with the documented reclassification
// set so the drift-detector never misses a class of orphan rows.
func TestWarnOnReclassifiedOrphanKeys_CoversMatcherReclassifications(t *testing.T) {
	t.Parallel()

	expected := []string{
		"app.log_level",
		"tenancy.default_tenant_id",
		"tenancy.default_tenant_slug",
		"auth.enabled",
		"auth.host",
		"auth.token_secret",
		"outbox.retry_window_sec",
		"outbox.dispatch_interval_sec",
	}

	assert.ElementsMatch(t, expected, reclassifiedBootstrapOnlyKeys,
		"reclassifiedBootstrapOnlyKeys must mirror the v5 migration docs")
}
