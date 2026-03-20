//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
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
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/LerianStudio/matcher/internal/auth"
	configContextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	tenantAdapters "github.com/LerianStudio/matcher/internal/shared/infrastructure/tenant/adapters"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	embeddedmigrations "github.com/LerianStudio/matcher/migrations"
)

// SharedInfra holds containers that are shared across all tests in a package.
// Initialize via InitSharedInfra in TestMain, cleanup via CleanupSharedInfra.
type SharedInfra struct {
	PostgresContainer testcontainers.Container
	RedisContainer    testcontainers.Container
	RabbitMQContainer testcontainers.Container
	PostgresDSN       string
	RedisAddr         string
	RabbitMQHost      string
	RabbitMQPort      string
	RabbitMQHealthURL string

	// Shared database connection (reused across tests)
	sharedConnection *libPostgres.Client
	sharedCloseDBs   func() error
	connectionMu     sync.Mutex
	testMu           sync.Mutex
	migrationsRan    bool
}

var (
	sharedInfra     *SharedInfra
	sharedInfraOnce sync.Once
	sharedInfraErr  error
)

// InitSharedInfra initializes shared container infrastructure.
// Call this from TestMain. It's safe to call multiple times.
func InitSharedInfra(ctx context.Context) (*SharedInfra, error) {
	sharedInfraOnce.Do(func() {
		sharedInfra, sharedInfraErr = createSharedInfra(ctx)
	})
	return sharedInfra, sharedInfraErr
}

// GetSharedInfra returns the initialized shared infrastructure.
// Returns nil if InitSharedInfra was not called or failed.
func GetSharedInfra() *SharedInfra {
	return sharedInfra
}

// CleanupSharedInfra cleans up all shared containers.
// Call this from TestMain after m.Run().
func CleanupSharedInfra(ctx context.Context) error {
	if sharedInfra == nil {
		return nil
	}
	return sharedInfra.Cleanup(ctx)
}

func createSharedInfra(ctx context.Context) (*SharedInfra, error) {
	infra := &SharedInfra{}

	startupCtx, startupCancel := context.WithTimeout(ctx, 3*time.Minute)
	defer startupCancel()

	// Start PostgreSQL
	pgContainer, err := postgres.Run(startupCtx,
		"postgres:17-alpine",
		postgres.WithDatabase("matcher_test"),
		postgres.WithUsername("matcher"),
		postgres.WithPassword("matcher_test"),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(60*time.Second),
				wait.ForListeningPort("5432/tcp"),
			).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres: %w", err)
	}
	infra.PostgresContainer = pgContainer

	pgDSN, err := pgContainer.ConnectionString(startupCtx, "sslmode=disable")
	if err != nil {
		_ = pgContainer.Terminate(ctx)
		return nil, fmt.Errorf("failed to get postgres DSN: %w", err)
	}
	infra.PostgresDSN = pgDSN

	// Start Redis
	redisContainer, err := redis.Run(startupCtx,
		"redis:7-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForListeningPort("6379/tcp"),
				wait.ForLog("Ready to accept connections"),
			).WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		_ = pgContainer.Terminate(ctx)
		return nil, fmt.Errorf("failed to start redis: %w", err)
	}
	infra.RedisContainer = redisContainer

	redisAddr, err := redisContainer.ConnectionString(startupCtx)
	if err != nil {
		_ = pgContainer.Terminate(ctx)
		_ = redisContainer.Terminate(ctx)
		return nil, fmt.Errorf("failed to get redis address: %w", err)
	}
	infra.RedisAddr = redisAddr

	// Start RabbitMQ
	rabbitContainer, err := startRabbitMQContainer(startupCtx)
	if err != nil {
		_ = pgContainer.Terminate(ctx)
		_ = redisContainer.Terminate(ctx)
		return nil, fmt.Errorf("failed to start rabbitmq: %w", err)
	}
	infra.RabbitMQContainer = rabbitContainer

	rabbitHost, err := containerHostWithRetry(startupCtx, rabbitContainer)
	if err != nil {
		_ = infra.Cleanup(ctx)
		return nil, fmt.Errorf("failed to get rabbitmq host: %w", err)
	}
	rabbitPort, err := mappedPortWithRetry(startupCtx, rabbitContainer, "5672/tcp")
	if err != nil {
		_ = infra.Cleanup(ctx)
		return nil, fmt.Errorf("failed to get rabbitmq port: %w", err)
	}
	rabbitHealthPort, err := mappedPortWithRetry(startupCtx, rabbitContainer, "15672/tcp")
	if err != nil {
		_ = infra.Cleanup(ctx)
		return nil, fmt.Errorf("failed to get rabbitmq health port: %w", err)
	}

	infra.RabbitMQHost = rabbitHost
	infra.RabbitMQPort = rabbitPort
	infra.RabbitMQHealthURL = fmt.Sprintf("http://%s:%s", rabbitHost, rabbitHealthPort)

	return infra, nil
}

// Cleanup terminates all containers.
func (si *SharedInfra) Cleanup(ctx context.Context) error {
	var errs []error

	// Close shared database connection first
	if si.sharedCloseDBs != nil {
		if err := si.sharedCloseDBs(); err != nil {
			errs = append(errs, fmt.Errorf("database: %w", err))
		}
	}

	if si.RabbitMQContainer != nil {
		if err := si.RabbitMQContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("rabbitmq: %w", err))
		}
	}
	if si.RedisContainer != nil {
		if err := si.RedisContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("redis: %w", err))
		}
	}
	if si.PostgresContainer != nil {
		if err := si.PostgresContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("postgres: %w", err))
		}
	}

	return errors.Join(errs...)
}

// GetOrCreateConnection returns a shared database connection, creating it if needed.
// This connection is shared across all tests in the package.
func (si *SharedInfra) GetOrCreateConnection(
	t *testing.T,
) (*libPostgres.Client, error) {
	si.connectionMu.Lock()
	defer si.connectionMu.Unlock()

	if si.sharedConnection != nil {
		return si.sharedConnection, nil
	}

	connection, closeDBs, err := initSharedDBConnection(t, si.PostgresDSN, "migrations")
	if err != nil {
		return nil, err
	}

	si.sharedConnection = connection
	si.sharedCloseDBs = closeDBs
	si.migrationsRan = true

	return connection, nil
}

// SharedTestHarness wraps SharedInfra with per-test database connection and seed data.
type SharedTestHarness struct {
	*SharedInfra
	Connection *libPostgres.Client
	Seed       SeedData
	closeDBs   func() error
	unlockTest func()
}

// NewSharedTestHarness creates a test harness using shared infrastructure.
// Each test gets its own database connection but reuses containers.
func NewSharedTestHarness(t *testing.T) (*SharedTestHarness, error) {
	t.Helper()

	infra := GetSharedInfra()
	if infra == nil {
		return nil, fmt.Errorf(
			"shared infrastructure not initialized - call InitSharedInfra in TestMain",
		)
	}

	// Shared DB requires serialization to guarantee per-test isolation.
	infra.testMu.Lock()

	harness := &SharedTestHarness{
		SharedInfra: infra,
		unlockTest:  infra.testMu.Unlock,
	}

	// Initialize database connection for this test
	if err := harness.initDatabase(t); err != nil {
		if harness.unlockTest != nil {
			harness.unlockTest()
			harness.unlockTest = nil
		}

		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return harness, nil
}

func (h *SharedTestHarness) initDatabase(t *testing.T) error {
	t.Helper()

	connection, err := h.SharedInfra.GetOrCreateConnection(t)
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}

	h.Connection = connection
	h.closeDBs = nil

	if err := resetSharedDatabase(t, connection); err != nil {
		return fmt.Errorf("failed to reset shared database: %w", err)
	}

	seed, err := setupSharedSeedData(t, connection)
	if err != nil {
		return fmt.Errorf("failed to setup seed data: %w", err)
	}
	h.Seed = seed

	if err := ensureSharedTenantTxWorks(t, connection); err != nil {
		return fmt.Errorf("tenant tx verification failed: %w", err)
	}

	return nil
}

// Cleanup closes the database connection (containers remain for next test).
func (h *SharedTestHarness) Cleanup() error {
	var errs []error

	if h.closeDBs != nil {
		if err := h.closeDBs(); err != nil {
			errs = append(errs, err)
		}
	}

	if h.unlockTest != nil {
		h.unlockTest()
		h.unlockTest = nil
	}

	return errors.Join(errs...)
}

const schemaMigrationsTable = "schema_migrations"

func resetSharedDatabase(t *testing.T, connection *libPostgres.Client) error {
	t.Helper()

	resolverCtx, resolverCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer resolverCancel()

	dbResolver, err := connection.Resolver(resolverCtx)
	if err != nil {
		return fmt.Errorf("failed to get db resolver: %w", err)
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

	rolledBack := false
	defer func() {
		if !rolledBack {
			if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				t.Logf("warning: rollback error during database reset cleanup: %v", rbErr)
			}
		}
	}()

	tables, err := listPublicTables(truncateCtx, tx)
	if err != nil {
		return fmt.Errorf("listing public tables: %w", err)
	}

	if len(tables) > 0 {
		qualifiedTables := make([]string, 0, len(tables))

		for _, table := range tables {
			qualifiedTables = append(qualifiedTables, "public."+quoteIdentifier(table))
		}

		truncateQuery := fmt.Sprintf(
			"TRUNCATE TABLE %s RESTART IDENTITY CASCADE",
			strings.Join(qualifiedTables, ", "),
		)
		if _, err := tx.ExecContext(truncateCtx, truncateQuery); err != nil {
			return fmt.Errorf("truncating public tables: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reset transaction: %w", err)
	}
	rolledBack = true

	return nil
}

func listPublicTables(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename <> $1
ORDER BY tablename ASC
`, schemaMigrationsTable)
	if err != nil {
		return nil, fmt.Errorf("querying table names: %w", err)
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}

		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating table names: %w", err)
	}

	return tables, nil
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// Ctx returns a context with the tenant ID set for repository operations.
func (h *SharedTestHarness) Ctx() context.Context {
	return context.WithValue(context.Background(), auth.TenantIDKey, h.Seed.TenantID.String())
}

// ToLegacyHarness converts SharedTestHarness to the legacy TestHarness format
// for compatibility with existing test code.
func (h *SharedTestHarness) ToLegacyHarness() *TestHarness {
	return &TestHarness{
		PostgresContainer: h.PostgresContainer,
		RedisContainer:    h.RedisContainer,
		RabbitMQContainer: h.RabbitMQContainer,
		PostgresDSN:       h.PostgresDSN,
		RedisAddr:         h.RedisAddr,
		RabbitMQHost:      h.RabbitMQHost,
		RabbitMQPort:      h.RabbitMQPort,
		RabbitMQHealthURL: h.RabbitMQHealthURL,
		Connection:        h.Connection,
		Seed:              h.Seed,
		closeDBs:          h.closeDBs,
	}
}

// RunWithSharedHarness runs a test using shared infrastructure.
func RunWithSharedHarness(t *testing.T, testFn func(t *testing.T, h *SharedTestHarness)) {
	harness, err := NewSharedTestHarness(t)
	if err != nil {
		t.Fatalf("failed to create shared test harness: %v", err)
	}

	t.Cleanup(func() {
		if err := harness.Cleanup(); err != nil {
			t.Logf("failed to cleanup harness: %v", err)
		}
	})

	testFn(t, harness)
}

// RunWithSharedDatabase is a compatibility wrapper that uses shared infrastructure.
func RunWithSharedDatabase(t *testing.T, testFn func(t *testing.T, h *TestHarness)) {
	RunWithSharedHarness(t, func(t *testing.T, sh *SharedTestHarness) {
		testFn(t, sh.ToLegacyHarness())
	})
}

// initSharedDBConnection creates a database connection that will be shared across all tests.
// Uses larger pool sizes since it's shared.
func initSharedDBConnection(
	t *testing.T,
	connectionString, _ string,
) (*libPostgres.Client, func() error, error) {
	t.Helper()

	primaryDB, err := sql.Open("pgx", connectionString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open primary db: %w", err)
	}

	replicaDB, err := sql.Open("pgx", connectionString)
	if err != nil {
		_ = primaryDB.Close()
		return nil, nil, fmt.Errorf("failed to open replica db: %w", err)
	}

	waitForSharedPostgres(t, primaryDB)
	waitForSharedPostgres(t, replicaDB)

	if err := dropAllPublicTables(t, primaryDB); err != nil {
		_ = primaryDB.Close()
		_ = replicaDB.Close()
		return nil, nil, fmt.Errorf("failed to reset shared database before migrations: %w", err)
	}

	if err := dropAllPublicEnumTypes(t, primaryDB); err != nil {
		_ = primaryDB.Close()
		_ = replicaDB.Close()
		return nil, nil, fmt.Errorf("failed to reset shared enum types before migrations: %w", err)
	}

	// Conservative pool sizes to prevent connection exhaustion with parallel tests.
	// Each test's bootstrap.Service also creates its own pool, so we keep this low.
	primaryDB.SetMaxOpenConns(10)
	primaryDB.SetMaxIdleConns(5)
	primaryDB.SetConnMaxLifetime(time.Minute * 5)
	primaryDB.SetConnMaxIdleTime(time.Minute * 1)

	replicaDB.SetMaxOpenConns(10)
	replicaDB.SetMaxIdleConns(5)
	replicaDB.SetConnMaxLifetime(time.Minute * 5)
	replicaDB.SetConnMaxIdleTime(time.Minute * 1)

	connectionDB := dbresolver.New(
		dbresolver.WithPrimaryDBs(primaryDB),
		dbresolver.WithReplicaDBs(replicaDB),
		dbresolver.WithLoadBalancer(dbresolver.RoundRobinLB),
	)

	source, err := iofs.New(embeddedmigrations.FS, ".")
	if err != nil {
		_ = primaryDB.Close()
		_ = replicaDB.Close()
		return nil, nil, fmt.Errorf("failed to create embedded migration source: %w", err)
	}

	driver, err := migratePostgres.WithInstance(primaryDB, &migratePostgres.Config{
		MultiStatementEnabled: true,
		DatabaseName:          "matcher_test",
		SchemaName:            "public",
	})
	if err != nil {
		_ = primaryDB.Close()
		_ = replicaDB.Close()
		return nil, nil, fmt.Errorf("failed to create migration driver: %w", err)
	}

	migrator, err := migrate.NewWithInstance("iofs", source, "matcher_test", driver)
	if err != nil {
		_ = primaryDB.Close()
		_ = replicaDB.Close()
		return nil, nil, fmt.Errorf("failed to create migrator: %w", err)
	}

	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		_ = primaryDB.Close()
		_ = replicaDB.Close()
		return nil, nil, fmt.Errorf("failed to run migrations: %w", err)
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

func dropAllPublicTables(t *testing.T, db *sql.DB) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename ASC
`)
	if err != nil {
		return fmt.Errorf("querying public tables: %w", err)
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("scanning public table name: %w", err)
		}

		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating public tables: %w", err)
	}

	if len(tables) == 0 {
		return nil
	}

	qualifiedTables := make([]string, 0, len(tables))
	for _, table := range tables {
		qualifiedTables = append(qualifiedTables, "public."+quoteIdentifier(table))
	}

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", strings.Join(qualifiedTables, ", "))
	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("dropping public tables: %w", err)
	}

	return nil
}

func dropAllPublicEnumTypes(t *testing.T, db *sql.DB) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `
SELECT t.typname
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = 'public'
  AND t.typtype = 'e'
ORDER BY t.typname ASC
`)
	if err != nil {
		return fmt.Errorf("querying public enum types: %w", err)
	}
	defer rows.Close()

	types := make([]string, 0)
	for rows.Next() {
		var typeName string
		if err := rows.Scan(&typeName); err != nil {
			return fmt.Errorf("scanning public enum type: %w", err)
		}

		types = append(types, typeName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating public enum types: %w", err)
	}

	for _, typeName := range types {
		query := fmt.Sprintf("DROP TYPE IF EXISTS public.%s CASCADE", quoteIdentifier(typeName))
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("dropping public enum type %s: %w", typeName, err)
		}
	}

	return nil
}

func setupSharedSeedData(
	t *testing.T,
	connection *libPostgres.Client,
) (SeedData, error) {
	t.Helper()

	provider := tenantAdapters.NewSingleTenantInfrastructureProvider(connection, nil)
	contextRepo := configContextRepo.NewRepository(provider)
	sourceRepo, err := configSourceRepo.NewRepository(provider)
	if err != nil {
		return SeedData{}, fmt.Errorf("failed to create source repository: %w", err)
	}

	tenantID, err := uuid.Parse(auth.DefaultTenantID)
	if err != nil {
		return SeedData{}, fmt.Errorf("failed to parse default tenant ID: %w", err)
	}

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	// Use unique name to avoid conflicts across tests
	contextName := fmt.Sprintf("Integration Test Context %s", uuid.New().String()[:8])
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
		return SeedData{}, fmt.Errorf("failed to create context entity: %w", err)
	}

	// Activate the context so ingestion/matching/reporting verifiers accept it
	if err := contextEntity.Activate(ctx); err != nil {
		return SeedData{}, fmt.Errorf("failed to activate context entity: %w", err)
	}

	createdContext, err := contextRepo.Create(ctx, contextEntity)
	if err != nil {
		return SeedData{}, fmt.Errorf("failed to create context: %w", err)
	}

	sourceEntity, err := configEntities.NewReconciliationSource(
		ctx,
		createdContext.ID,
		configEntities.CreateReconciliationSourceInput{
			Name:   "Integration Test Source",
			Type:   configVO.SourceTypeLedger,
			Side:   sharedfee.MatchingSideLeft,
			Config: map[string]any{},
		},
	)
	if err != nil {
		return SeedData{}, fmt.Errorf("failed to create source entity: %w", err)
	}

	createdSource, err := sourceRepo.Create(ctx, sourceEntity)
	if err != nil {
		return SeedData{}, fmt.Errorf("failed to create source: %w", err)
	}

	return SeedData{
		TenantID:  tenantID,
		ContextID: createdContext.ID,
		SourceID:  createdSource.ID,
	}, nil
}

func ensureSharedTenantTxWorks(t *testing.T, connection *libPostgres.Client) error {
	t.Helper()

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)

	_, err := pgcommon.WithTenantTx(ctx, connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, "SELECT 1")
		return struct{}{}, execErr
	})

	return err
}

func waitForSharedPostgres(t *testing.T, db *sql.DB) {
	t.Helper()

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil
	}, 30*time.Second, 200*time.Millisecond, "postgres did not become ready")
}
