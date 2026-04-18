//go:build chaos

// Package chaos provides infrastructure for chaos/resilience testing of the Matcher service.
// It extends the integration test harness with Toxiproxy-based fault injection,
// enabling programmatic simulation of network failures, latency spikes, and connection drops
// on PostgreSQL, Redis, and RabbitMQ dependencies.
//
// Build tag: chaos (run with `make test-chaos` or `go test -tags chaos ./tests/chaos/...`)
package chaos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/golang-migrate/migrate/v4"
	migratePostgres "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/LerianStudio/matcher/internal/auth"
	configContextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/internal/shared/ports"
	embeddedmigrations "github.com/LerianStudio/matcher/migrations"
)

// Proxy port constants — Toxiproxy listens on these inside the container.
// Each proxied service gets a dedicated port.
const (
	toxiControlPort = "8474" // Toxiproxy HTTP API
	pgProxyPort     = "18001"
	redisProxyPort  = "18002"
	rabbitProxyPort = "18003"

	// Container network aliases.
	pgNetworkAlias     = "chaos-postgres"
	redisNetworkAlias  = "chaos-redis"
	rabbitNetworkAlias = "chaos-rabbitmq"
	toxiNetworkAlias   = "chaos-toxiproxy"
)

// InitSharedChaos initializes the shared chaos infrastructure (containers + proxies).
// Call from TestMain. Safe to call multiple times via sync.Once.
func InitSharedChaos(ctx context.Context) (*ChaosHarness, error) {
	sharedChaosOnce.Do(func() {
		sharedChaos, sharedChaosErr = newChaosHarness(ctx)
	})

	return sharedChaos, sharedChaosErr
}

// newChaosHarness creates the complete chaos infrastructure:
// 1. Docker network for container-to-container communication
// 2. PostgreSQL, Redis, RabbitMQ containers
// 3. Toxiproxy container with proxies for each service
// 4. Database migrations and seed data
func newChaosHarness(ctx context.Context) (*ChaosHarness, error) {
	h := &ChaosHarness{}

	startupCtx, startupCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer startupCancel()

	// 1. Create Docker network — all containers must share it for Toxiproxy to route.
	nw, err := network.New(startupCtx)
	if err != nil {
		return nil, fmt.Errorf("create docker network: %w", err)
	}

	h.Network = nw

	// Cleanup helper for partial failure during setup.
	cleanupOnError := func(cause error) (*ChaosHarness, error) {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()

		if cleanupErr := h.Cleanup(cleanupCtx); cleanupErr != nil {
			return nil, fmt.Errorf("%w (cleanup: %v)", cause, cleanupErr)
		}

		return nil, cause
	}

	// 2. Start PostgreSQL on the shared network.
	pgContainer, err := postgres.Run(startupCtx,
		"postgres:17-alpine",
		postgres.WithDatabase("matcher_chaos"),
		postgres.WithUsername("matcher"),
		postgres.WithPassword("matcher_chaos"),
		network.WithNetwork([]string{pgNetworkAlias}, nw),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(60*time.Second),
				wait.ForListeningPort("5432/tcp"),
			).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return cleanupOnError(fmt.Errorf("start postgres: %w", err))
	}

	h.PostgresContainer = pgContainer

	// 3. Start Redis on the shared network.
	redisContainer, err := redis.Run(startupCtx,
		"redis:7-alpine",
		network.WithNetwork([]string{redisNetworkAlias}, nw),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForListeningPort("6379/tcp"),
				wait.ForLog("Ready to accept connections"),
			).WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		return cleanupOnError(fmt.Errorf("start redis: %w", err))
	}

	h.RedisContainer = redisContainer

	// 4. Start RabbitMQ on the shared network.
	rabbitContainer, err := testcontainers.GenericContainer(
		startupCtx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "rabbitmq:4.1.3-management-alpine",
				ExposedPorts: []string{"5672/tcp", "15672/tcp"},
				Networks:     []string{nw.Name},
				NetworkAliases: map[string][]string{
					nw.Name: {rabbitNetworkAlias},
				},
				WaitingFor: wait.ForAll(
					wait.ForListeningPort("5672/tcp"),
					wait.ForLog("Server startup complete").WithStartupTimeout(120*time.Second),
				).WithStartupTimeout(120 * time.Second),
			},
			Started: true,
		},
	)
	if err != nil {
		return cleanupOnError(fmt.Errorf("start rabbitmq: %w", err))
	}

	h.RabbitMQContainer = rabbitContainer

	// Capture direct addresses (bypassing proxy) for health verification.
	if err := h.captureDirectAddresses(startupCtx); err != nil {
		return cleanupOnError(fmt.Errorf("capture direct addresses: %w", err))
	}

	// 5. Start Toxiproxy on the shared network with proxy ports exposed.
	// We use GenericContainer instead of tctoxiproxy.Run for full control over
	// exposed ports — the module's WithExposedPorts doesn't reliably map
	// custom proxy ports on all platforms.
	toxiContainer, err := testcontainers.GenericContainer(
		startupCtx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image: "ghcr.io/shopify/toxiproxy:2.12.0",
				ExposedPorts: []string{
					toxiControlPort + "/tcp",
					pgProxyPort + "/tcp",
					redisProxyPort + "/tcp",
					rabbitProxyPort + "/tcp",
				},
				Networks: []string{nw.Name},
				NetworkAliases: map[string][]string{
					nw.Name: {toxiNetworkAlias},
				},
				WaitingFor: wait.ForListeningPort(toxiControlPort + "/tcp").
					WithStartupTimeout(30 * time.Second),
			},
			Started: true,
		},
	)
	if err != nil {
		return cleanupOnError(fmt.Errorf("start toxiproxy: %w", err))
	}

	h.ToxiContainer = toxiContainer

	// 6. Create Toxiproxy client and configure proxies.
	toxiHost, err := toxiContainer.Host(startupCtx)
	if err != nil {
		return cleanupOnError(fmt.Errorf("get toxiproxy host: %w", err))
	}

	toxiControlMapped, err := toxiContainer.MappedPort(startupCtx, toxiControlPort+"/tcp")
	if err != nil {
		return cleanupOnError(fmt.Errorf("get toxiproxy control port: %w", err))
	}

	toxiURI := fmt.Sprintf("http://%s:%s", toxiHost, toxiControlMapped.Port())
	h.ToxiClient = toxiproxy.NewClient(toxiURI)

	if err := h.createProxies(); err != nil {
		return cleanupOnError(fmt.Errorf("create proxies: %w", err))
	}

	// 7. Capture proxied addresses (through Toxiproxy).
	if err := h.captureProxiedAddresses(startupCtx); err != nil {
		return cleanupOnError(fmt.Errorf("capture proxied addresses: %w", err))
	}

	// 8. Initialize database through the proxy.
	if err := h.initDatabase(); err != nil {
		return cleanupOnError(fmt.Errorf("init database: %w", err))
	}

	return h, nil
}

// createProxies sets up Toxiproxy proxy instances for each infrastructure service.
// Proxies route via Docker network aliases (container-to-container).
func (h *ChaosHarness) createProxies() error {
	var err error

	h.PGProxy, err = h.ToxiClient.CreateProxy(
		"postgres",
		"0.0.0.0:"+pgProxyPort,
		pgNetworkAlias+":5432",
	)
	if err != nil {
		return fmt.Errorf("create postgres proxy: %w", err)
	}

	h.RedisProxy, err = h.ToxiClient.CreateProxy(
		"redis",
		"0.0.0.0:"+redisProxyPort,
		redisNetworkAlias+":6379",
	)
	if err != nil {
		return fmt.Errorf("create redis proxy: %w", err)
	}

	h.RabbitProxy, err = h.ToxiClient.CreateProxy(
		"rabbitmq",
		"0.0.0.0:"+rabbitProxyPort,
		rabbitNetworkAlias+":5672",
	)
	if err != nil {
		return fmt.Errorf("create rabbitmq proxy: %w", err)
	}

	return nil
}

// captureDirectAddresses stores direct container addresses (bypassing proxy).
func (h *ChaosHarness) captureDirectAddresses(ctx context.Context) error {
	pgDSN, err := h.PostgresContainer.(interface {
		ConnectionString(ctx context.Context, args ...string) (string, error)
	}).ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("postgres DSN: %w", err)
	}

	h.DirectPostgresDSN = pgDSN

	redisAddr, err := h.RedisContainer.(interface {
		ConnectionString(ctx context.Context) (string, error)
	}).ConnectionString(ctx)
	if err != nil {
		return fmt.Errorf("redis addr: %w", err)
	}

	h.DirectRedisAddr = redisAddr

	rabbitHost, err := h.RabbitMQContainer.Host(ctx)
	if err != nil {
		return fmt.Errorf("rabbitmq host: %w", err)
	}

	rabbitPort, err := h.RabbitMQContainer.MappedPort(ctx, "5672/tcp")
	if err != nil {
		return fmt.Errorf("rabbitmq port: %w", err)
	}

	rabbitHealthPort, err := h.RabbitMQContainer.MappedPort(ctx, "15672/tcp")
	if err != nil {
		return fmt.Errorf("rabbitmq health port: %w", err)
	}

	h.DirectRabbitHost = rabbitHost
	h.DirectRabbitPort = rabbitPort.Port()
	h.RabbitMQHealthURL = fmt.Sprintf("http://%s:%s", rabbitHost, rabbitHealthPort.Port())

	return nil
}

// captureProxiedAddresses stores addresses routed through Toxiproxy.
func (h *ChaosHarness) captureProxiedAddresses(ctx context.Context) error {
	toxiHost, err := h.ToxiContainer.Host(ctx)
	if err != nil {
		return fmt.Errorf("toxiproxy host: %w", err)
	}

	pgPort, err := h.ToxiContainer.MappedPort(ctx, pgProxyPort+"/tcp")
	if err != nil {
		return fmt.Errorf("pg proxy port: %w", err)
	}

	h.ProxiedPostgresDSN = fmt.Sprintf(
		"postgres://matcher:matcher_chaos@%s:%s/matcher_chaos?sslmode=disable",
		toxiHost, pgPort.Port(),
	)

	redisPort, err := h.ToxiContainer.MappedPort(ctx, redisProxyPort+"/tcp")
	if err != nil {
		return fmt.Errorf("redis proxy port: %w", err)
	}

	h.ProxiedRedisAddr = fmt.Sprintf("redis://%s:%s", toxiHost, redisPort.Port())

	rabbitMappedPort, err := h.ToxiContainer.MappedPort(ctx, rabbitProxyPort+"/tcp")
	if err != nil {
		return fmt.Errorf("rabbit proxy port: %w", err)
	}

	h.ProxiedRabbitHost = toxiHost
	h.ProxiedRabbitPort = rabbitMappedPort.Port()

	return nil
}

// initDatabase connects through the proxy, runs migrations, and seeds test data.
func (h *ChaosHarness) initDatabase() error {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("failed to get current file path")
	}

	migrationsPath := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../../migrations"))

	connection, closeDBs, err := initChaosDBConnection(h.ProxiedPostgresDSN, migrationsPath)
	if err != nil {
		return fmt.Errorf("init db connection: %w", err)
	}

	h.Connection = connection
	h.closeDBs = closeDBs

	seed, err := setupChaosSeedData(connection)
	if err != nil {
		if closeErr := closeDBs(); closeErr != nil {
			return fmt.Errorf("seed data: %w (close: %v)", err, closeErr)
		}

		return fmt.Errorf("seed data: %w", err)
	}

	h.Seed = seed

	return nil
}

// Ctx returns a context with the tenant ID set for repository operations.
func (h *ChaosHarness) Ctx() context.Context {
	return context.WithValue(context.Background(), auth.TenantIDKey, h.Seed.TenantID.String())
}

// Provider returns an InfrastructureProvider wrapping the proxied connections.
func (h *ChaosHarness) Provider() ports.InfrastructureProvider {
	return infraTestutil.NewSingleTenantInfrastructureProvider(h.Connection, nil)
}

// ResetDatabase truncates all tables and re-seeds for test isolation.
func (h *ChaosHarness) ResetDatabase(t *testing.T) {
	t.Helper()
	h.testMu.Lock()

	t.Cleanup(func() {
		h.testMu.Unlock()
	})

	require.NoError(t, resetChaosDatabase(h.Connection), "reset database")

	seed, err := setupChaosSeedData(h.Connection)
	require.NoError(t, err, "re-seed database")

	h.Seed = seed
}

// DirectDB returns a direct database connection that bypasses Toxiproxy.
// Useful for state assertions that must work even when the proxy is poisoned.
func (h *ChaosHarness) DirectDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("pgx", h.DirectPostgresDSN)
	require.NoError(t, err, "open direct DB connection")

	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)

	t.Cleanup(func() {
		_ = db.Close()
	})

	return db
}

// --------------------------------------------------------------------------
// Internal helpers
// --------------------------------------------------------------------------

func initChaosDBConnection(
	connectionString, _ string,
) (*libPostgres.Client, func() error, error) {
	primaryDB, err := sql.Open("pgx", connectionString)
	if err != nil {
		return nil, nil, fmt.Errorf("open primary db: %w", err)
	}

	replicaDB, err := sql.Open("pgx", connectionString)
	if err != nil {
		_ = primaryDB.Close()
		return nil, nil, fmt.Errorf("open replica db: %w", err)
	}

	waitForDB(primaryDB)
	waitForDB(replicaDB)

	primaryDB.SetMaxOpenConns(10)
	primaryDB.SetMaxIdleConns(5)
	primaryDB.SetConnMaxLifetime(5 * time.Minute)

	replicaDB.SetMaxOpenConns(10)
	replicaDB.SetMaxIdleConns(5)
	replicaDB.SetConnMaxLifetime(5 * time.Minute)

	connectionDB := dbresolver.New(
		dbresolver.WithPrimaryDBs(primaryDB),
		dbresolver.WithReplicaDBs(replicaDB),
		dbresolver.WithLoadBalancer(dbresolver.RoundRobinLB),
	)

	source, err := iofs.New(embeddedmigrations.FS, ".")
	if err != nil {
		_ = primaryDB.Close()
		_ = replicaDB.Close()

		return nil, nil, fmt.Errorf("create embedded migration source: %w", err)
	}

	driver, err := migratePostgres.WithInstance(primaryDB, &migratePostgres.Config{
		MultiStatementEnabled: true,
		DatabaseName:          "matcher_chaos",
		SchemaName:            "public",
	})
	if err != nil {
		_ = primaryDB.Close()
		_ = replicaDB.Close()

		return nil, nil, fmt.Errorf("create migration driver: %w", err)
	}

	migrator, err := migrate.NewWithInstance("iofs", source, "matcher_chaos", driver)
	if err != nil {
		_ = primaryDB.Close()
		_ = replicaDB.Close()

		return nil, nil, fmt.Errorf("create migrator: %w", err)
	}

	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		_ = primaryDB.Close()
		_ = replicaDB.Close()

		return nil, nil, fmt.Errorf("run migrations: %w", err)
	}

	connection := infraTestutil.NewClientWithResolver(connectionDB)

	cleanup := func() error {
		var errs []error
		if err := primaryDB.Close(); err != nil {
			errs = append(errs, err)
		}

		if err := replicaDB.Close(); err != nil {
			errs = append(errs, err)
		}

		return errors.Join(errs...)
	}

	return connection, cleanup, nil
}

func waitForDB(db *sql.DB) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		if db.PingContext(ctx) == nil {
			cancel()
			return
		}

		cancel()
		time.Sleep(200 * time.Millisecond)
	}
}

func setupChaosSeedData(connection *libPostgres.Client) (SeedData, error) {
	provider := infraTestutil.NewSingleTenantInfrastructureProvider(connection, nil)
	contextRepo := configContextRepo.NewRepository(provider)

	sourceRepo, err := configSourceRepo.NewRepository(provider)
	if err != nil {
		return SeedData{}, fmt.Errorf("create source repository: %w", err)
	}

	tenantID, err := uuid.Parse(auth.DefaultTenantID)
	if err != nil {
		return SeedData{}, fmt.Errorf("parse default tenant ID: %w", err)
	}

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	contextName := fmt.Sprintf("Chaos Test Context %s", uuid.New().String()[:8])

	contextEntity, err := configEntities.NewReconciliationContext(
		ctx,
		tenantID,
		configEntities.CreateReconciliationContextInput{
			Name:     contextName,
			Type:     configVO.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	if err != nil {
		return SeedData{}, fmt.Errorf("create context entity: %w", err)
	}

	if err := contextEntity.Activate(ctx); err != nil {
		return SeedData{}, fmt.Errorf("activate context entity: %w", err)
	}

	createdContext, err := contextRepo.Create(ctx, contextEntity)
	if err != nil {
		return SeedData{}, fmt.Errorf("create context: %w", err)
	}

	sourceEntity, err := configEntities.NewReconciliationSource(
		ctx,
		createdContext.ID,
		configEntities.CreateReconciliationSourceInput{
			Name:   "Chaos Test Source",
			Type:   configVO.SourceTypeLedger,
			Config: map[string]any{},
		},
	)
	if err != nil {
		return SeedData{}, fmt.Errorf("create source entity: %w", err)
	}

	createdSource, err := sourceRepo.Create(ctx, sourceEntity)
	if err != nil {
		return SeedData{}, fmt.Errorf("create source: %w", err)
	}

	return SeedData{
		TenantID:  tenantID,
		ContextID: createdContext.ID,
		SourceID:  createdSource.ID,
	}, nil
}

func resetChaosDatabase(connection *libPostgres.Client) error {
	resolverCtx, resolverCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer resolverCancel()

	dbResolver, err := connection.Resolver(resolverCtx)
	if err != nil {
		return fmt.Errorf("get db resolver: %w", err)
	}

	primaryDBs := dbResolver.PrimaryDBs()
	if len(primaryDBs) == 0 || primaryDBs[0] == nil {
		return fmt.Errorf("primary database unavailable")
	}

	truncateCtx, truncateCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer truncateCancel()

	tx, err := primaryDBs[0].BeginTx(truncateCtx, nil)
	if err != nil {
		return fmt.Errorf("begin reset transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.QueryContext(truncateCtx, `
SELECT tablename FROM pg_tables
WHERE schemaname = 'public' AND tablename <> 'schema_migrations'
ORDER BY tablename ASC
`)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan table name: %w", err)
		}

		tables = append(tables, `public."`+name+`"`)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate tables: %w", err)
	}

	if len(tables) > 0 {
		query := "TRUNCATE TABLE "
		// Table names are sourced from pg_tables (system catalog), not user input.
		// If table-name source changes in the future, revalidate concatenation safety
		// or switch to parameterized identifier handling.
		for i, t := range tables {
			if i > 0 {
				query += ", "
			}

			query += t
		}

		query += " RESTART IDENTITY CASCADE"

		if _, err := tx.ExecContext(truncateCtx, query); err != nil {
			return fmt.Errorf("truncate tables: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reset: %w", err)
	}

	committed = true

	return nil
}

func ensureTenantTxWorks(connection *libPostgres.Client) error {
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)

	_, err := pgcommon.WithTenantTx(ctx, connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, "SELECT 1")
		return struct{}{}, execErr
	})

	return err
}

// EnvVarsForBootstrap returns environment variables pointing the application
// at the proxied infrastructure addresses. Used when booting a full
// bootstrap.Service inside a chaos test.
func (h *ChaosHarness) EnvVarsForBootstrap() (map[string]string, error) {
	pgURL, err := url.Parse(h.ProxiedPostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("parse proxied postgres DSN: %w", err)
	}

	if pgURL == nil || pgURL.Hostname() == "" || pgURL.User == nil || len(pgURL.Path) <= 1 {
		return nil, fmt.Errorf("invalid proxied postgres DSN: %q", h.ProxiedPostgresDSN)
	}

	pgHost, pgPort := pgURL.Hostname(), pgURL.Port()

	if pgPort == "" {
		pgPort = "5432"
	}

	pgUser := pgURL.User.Username()
	pgPass, _ := pgURL.User.Password()
	pgDB := pgURL.Path[1:] // strip leading /
	if pgUser == "" || pgDB == "" {
		return nil, fmt.Errorf("invalid proxied postgres DSN credentials/database: %q", h.ProxiedPostgresDSN)
	}

	_, currentFile, _, _ := runtime.Caller(0)
	migrationsPath := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../../migrations"))

	redisURL, err := url.Parse(h.ProxiedRedisAddr)
	if err != nil {
		return nil, fmt.Errorf("parse proxied redis address: %w", err)
	}

	if redisURL == nil || redisURL.Host == "" {
		return nil, fmt.Errorf("invalid proxied redis address: %q", h.ProxiedRedisAddr)
	}

	return map[string]string{
		"POSTGRES_HOST":                        pgHost,
		"POSTGRES_PORT":                        pgPort,
		"POSTGRES_USER":                        pgUser,
		"POSTGRES_PASSWORD":                    pgPass,
		"POSTGRES_DB":                          pgDB,
		"POSTGRES_SSLMODE":                     "disable",
		"POSTGRES_MAX_OPEN_CONNS":              "5",
		"POSTGRES_MAX_IDLE_CONNS":              "2",
		"MIGRATIONS_PATH":                      migrationsPath,
		"REDIS_HOST":                           redisURL.Host,
		"REDIS_PASSWORD":                       "",
		"REDIS_TLS":                            "false",
		"RABBITMQ_HOST":                        h.ProxiedRabbitHost,
		"RABBITMQ_PORT":                        h.ProxiedRabbitPort,
		"RABBITMQ_USER":                        "guest",
		"RABBITMQ_PASSWORD":                    "guest",
		"RABBITMQ_VHOST":                       "/",
		"RABBITMQ_URI":                         "amqp",
		"RABBITMQ_HEALTH_URL":                  h.RabbitMQHealthURL,
		"RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK": "true",
		"AUTH_ENABLED":                         "false",
		"PLUGIN_AUTH_ENABLED":                  "false",
		"HTTP_BODY_LIMIT_BYTES":                "115343360",
		"DEFAULT_TENANT_ID":                    h.Seed.TenantID.String(),
		"DEFAULT_TENANT_SLUG":                  "default",
		"ENABLE_TELEMETRY":                     "false",
		"ENV_NAME":                             "development",
		"LOG_LEVEL":                            "debug",
		"RATE_LIMIT_MAX":                       "10000",
		"RATE_LIMIT_EXPIRY_SEC":                "60",
		"EXPORT_RATE_LIMIT_MAX":                "1000",
		"EXPORT_RATE_LIMIT_EXPIRY_SEC":         "300",
		"DISPATCH_RATE_LIMIT_MAX":              "100",
		"DISPATCH_RATE_LIMIT_EXPIRY_SEC":       "60",
		"INFRA_CONNECT_TIMEOUT_SEC":            "30",
		"ARCHIVAL_WORKER_ENABLED":              "false",
	}, nil
}

// SetEnvForBootstrap sets all environment variables needed for bootstrap.InitServersWithOptions
// to connect through the proxied addresses.
func (h *ChaosHarness) SetEnvForBootstrap(t *testing.T) {
	t.Helper()

	envVars, err := h.EnvVarsForBootstrap()
	require.NoError(t, err, "build env vars for bootstrap")

	for k, v := range envVars {
		t.Setenv(k, v)
	}
}

// SetEnvForBootstrapGlobal sets environment variables globally (for TestMain context).
// Use SetEnvForBootstrap(t) in tests when possible for automatic cleanup.
func (h *ChaosHarness) SetEnvForBootstrapGlobal() {
	envVars, err := h.EnvVarsForBootstrap()
	if err != nil {
		panic(fmt.Sprintf("failed to build chaos bootstrap env vars: %v", err))
	}

	for k, v := range envVars {
		os.Setenv(k, v)
	}
}
