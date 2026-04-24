//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
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
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/internal/shared/ports"
	embeddedmigrations "github.com/LerianStudio/matcher/migrations"
)

// SeedData contains pre-created entities for integration tests.
type SeedData struct {
	TenantID  uuid.UUID
	ContextID uuid.UUID
	SourceID  uuid.UUID
}

// TestHarness provides shared infrastructure for integration tests.
type TestHarness struct {
	PostgresContainer testcontainers.Container
	RedisContainer    testcontainers.Container
	RabbitMQContainer testcontainers.Container
	PostgresDSN       string
	RedisAddr         string
	RabbitMQHost      string
	RabbitMQPort      string
	RabbitMQHealthURL string

	// Database connection for repository tests
	Connection *libPostgres.Client
	Seed       SeedData
	closeDBs   func() error
}

func NewTestHarness(ctx context.Context, t *testing.T) (*TestHarness, error) {
	t.Helper()

	harness := &TestHarness{}

	startupCtx, startupCancel := context.WithTimeout(ctx, 3*time.Minute)
	defer startupCancel()

	pgContainer, err := postgres.Run(startupCtx,
		"postgres:17-alpine",
		postgres.WithDatabase("matcher_test"),
		postgres.WithUsername("matcher"),
		postgres.WithPassword("matcher_test"),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(120*time.Second),
				wait.ForListeningPort("5432/tcp"),
			).WithStartupTimeout(120*time.Second)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres: %w", err)
	}

	harness.PostgresContainer = pgContainer

	pgDSN, err := pgContainer.ConnectionString(startupCtx, "sslmode=disable")
	if err != nil {
		if terminateErr := terminateContainer(ctx, pgContainer); terminateErr != nil {
			return nil, fmt.Errorf(
				"failed to get postgres DSN: %w (cleanup error: %v)",
				err,
				terminateErr,
			)
		}

		return nil, fmt.Errorf("failed to get postgres DSN: %w", err)
	}

	harness.PostgresDSN = pgDSN

	redisContainer, err := redis.Run(startupCtx,
		"valkey/valkey:8",
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForListeningPort("6379/tcp"),
				wait.ForLog("Ready to accept connections"),
			).WithStartupTimeout(120*time.Second)),
	)
	if err != nil {
		if terminateErr := terminateContainer(ctx, pgContainer); terminateErr != nil {
			return nil, fmt.Errorf(
				"failed to start redis: %w (cleanup error: %v)",
				err,
				terminateErr,
			)
		}

		return nil, fmt.Errorf("failed to start redis: %w", err)
	}

	harness.RedisContainer = redisContainer

	redisAddr, err := redisContainer.ConnectionString(startupCtx)
	if err != nil {
		if terminateErr := terminateContainer(ctx, pgContainer); terminateErr != nil {
			return nil, fmt.Errorf(
				"failed to get redis address: %w (cleanup error: %v)",
				err,
				terminateErr,
			)
		}

		if terminateErr := terminateContainer(ctx, redisContainer); terminateErr != nil {
			return nil, fmt.Errorf(
				"failed to get redis address: %w (cleanup error: %v)",
				err,
				terminateErr,
			)
		}

		return nil, fmt.Errorf("failed to get redis address: %w", err)
	}

	harness.RedisAddr = redisAddr

	rabbitContainer, err := startRabbitMQContainer(startupCtx)
	if err != nil {
		if terminateErr := terminateContainer(ctx, pgContainer); terminateErr != nil {
			return nil, fmt.Errorf(
				"failed to start rabbitmq: %w (cleanup error: %v)",
				err,
				terminateErr,
			)
		}

		if terminateErr := terminateContainer(ctx, redisContainer); terminateErr != nil {
			return nil, fmt.Errorf(
				"failed to start rabbitmq: %w (cleanup error: %v)",
				err,
				terminateErr,
			)
		}

		return nil, fmt.Errorf("failed to start rabbitmq: %w", err)
	}

	harness.RabbitMQContainer = rabbitContainer

	rabbitHost, err := containerHostWithRetry(startupCtx, rabbitContainer)
	if err != nil {
		if cleanupErr := harness.Cleanup(ctx); cleanupErr != nil {
			return nil, fmt.Errorf(
				"failed to get rabbitmq host: %w (cleanup error: %v)",
				err,
				cleanupErr,
			)
		}

		return nil, fmt.Errorf("failed to get rabbitmq host: %w", err)
	}

	rabbitPort, err := mappedPortWithRetry(startupCtx, rabbitContainer, "5672/tcp")
	if err != nil {
		if cleanupErr := harness.Cleanup(ctx); cleanupErr != nil {
			return nil, fmt.Errorf(
				"failed to get rabbitmq port: %w (cleanup error: %v)",
				err,
				cleanupErr,
			)
		}

		return nil, fmt.Errorf("failed to get rabbitmq port: %w", err)
	}

	rabbitHealthPort, err := mappedPortWithRetry(startupCtx, rabbitContainer, "15672/tcp")
	if err != nil {
		if cleanupErr := harness.Cleanup(ctx); cleanupErr != nil {
			return nil, fmt.Errorf(
				"failed to get rabbitmq health port: %w (cleanup error: %v)",
				err,
				cleanupErr,
			)
		}

		return nil, fmt.Errorf("failed to get rabbitmq health port: %w", err)
	}

	harness.RabbitMQHost = rabbitHost
	harness.RabbitMQPort = rabbitPort
	harness.RabbitMQHealthURL = fmt.Sprintf("http://%s:%s", rabbitHost, rabbitHealthPort)

	return harness, nil
}

// InitDatabase initializes the database connection, runs migrations, and seeds test data.
// This is automatically called by RunWithDatabase.
func (h *TestHarness) InitDatabase(t *testing.T) error {
	t.Helper()

	connection, closeDBs, err := initializeTestConnection(t, h.PostgresDSN, "migrations")
	if err != nil {
		return fmt.Errorf("failed to initialize database connection: %w", err)
	}

	h.Connection = connection
	h.closeDBs = closeDBs

	seed, err := setupSeedData(t, connection)
	if err != nil {
		if closeErr := closeDBs(); closeErr != nil {
			return fmt.Errorf("failed to setup seed data: %w (close error: %v)", err, closeErr)
		}

		return fmt.Errorf("failed to setup seed data: %w", err)
	}

	h.Seed = seed

	if err := ensureTenantTxWorks(t, connection); err != nil {
		if closeErr := closeDBs(); closeErr != nil {
			return fmt.Errorf("tenant tx verification failed: %w (close error: %v)", err, closeErr)
		}

		return fmt.Errorf("tenant tx verification failed: %w", err)
	}

	return nil
}

func (h *TestHarness) Cleanup(ctx context.Context) error {
	var errs []error

	if h.closeDBs != nil {
		if err := h.closeDBs(); err != nil {
			errs = append(errs, fmt.Errorf("database: %w", err))
		}
	}

	if err := terminateContainer(ctx, h.PostgresContainer); err != nil {
		errs = append(errs, fmt.Errorf("postgres: %w", err))
	}

	if err := terminateContainer(ctx, h.RedisContainer); err != nil {
		errs = append(errs, fmt.Errorf("redis: %w", err))
	}

	if err := terminateContainer(ctx, h.RabbitMQContainer); err != nil {
		errs = append(errs, fmt.Errorf("rabbitmq: %w", err))
	}

	return errors.Join(errs...)
}

// RunWithHarness runs a test with the full infrastructure harness (containers only).
// If shared infrastructure is available (initialized via TestMain), it uses that.
// Otherwise, it falls back to creating per-test containers.
func RunWithHarness(t *testing.T, testFn func(t *testing.T, h *TestHarness)) {
	// Check if shared infrastructure is available
	if GetSharedInfra() != nil {
		RunWithSharedDatabase(t, testFn)
		return
	}

	// Fallback to per-test containers (legacy behavior)
	ctx := context.Background()

	harness, err := NewTestHarness(ctx, t)
	if err != nil {
		t.Fatalf("failed to create test harness: %v", err)
	}

	t.Cleanup(func() {
		if err := harness.Cleanup(ctx); err != nil {
			t.Errorf("failed to cleanup harness: %v", err)
		}
	})

	testFn(t, harness)
}

// RunWithDatabase runs a test with full infrastructure + initialized database and seed data.
// This is the preferred harness for repository integration tests.
// If shared infrastructure is available (initialized via TestMain), it uses that.
func RunWithDatabase(t *testing.T, testFn func(t *testing.T, h *TestHarness)) {
	// Check if shared infrastructure is available
	if GetSharedInfra() != nil {
		RunWithSharedDatabase(t, testFn)
		return
	}

	// Fallback to per-test containers (legacy behavior)
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		if err := h.InitDatabase(t); err != nil {
			t.Fatalf("failed to initialize database: %v", err)
		}

		testFn(t, h)
	})
}

// Ctx returns a context with the tenant ID set for repository operations.
func (h *TestHarness) Ctx() context.Context {
	return context.WithValue(context.Background(), auth.TenantIDKey, h.Seed.TenantID.String())
}

// Provider returns an InfrastructureProvider wrapping the test harness connections.
// This should be used to pass to repository constructors instead of h.Connection directly.
func (h *TestHarness) Provider() ports.InfrastructureProvider {
	return infraTestutil.NewSingleTenantInfrastructureProvider(h.Connection, nil)
}

func terminateContainer(ctx context.Context, container testcontainers.Container) error {
	if container == nil {
		return nil
	}

	return container.Terminate(ctx)
}

func initializeTestConnection(
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

	waitForPostgres(t, primaryDB)
	waitForPostgres(t, replicaDB)

	primaryDB.SetMaxOpenConns(5)
	primaryDB.SetMaxIdleConns(2)
	primaryDB.SetConnMaxLifetime(time.Minute * 30)

	replicaDB.SetMaxOpenConns(5)
	replicaDB.SetMaxIdleConns(2)
	replicaDB.SetConnMaxLifetime(time.Minute * 30)

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

func setupSeedData(t *testing.T, connection *libPostgres.Client) (SeedData, error) {
	t.Helper()

	provider := infraTestutil.NewSingleTenantInfrastructureProvider(connection, nil)

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

	contextEntity, err := configEntities.NewReconciliationContext(
		ctx,
		tenantID,
		configEntities.CreateReconciliationContextInput{
			Name:     "Integration Test Context",
			Type:     shared.ContextTypeOneToOne,
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

func ensureTenantTxWorks(t *testing.T, connection *libPostgres.Client) error {
	t.Helper()

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)

	_, err := pgcommon.WithTenantTx(ctx, connection, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, "SELECT 1")
		return struct{}{}, execErr
	})

	return err
}

func waitForPostgres(t *testing.T, db *sql.DB) {
	t.Helper()

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		return db.PingContext(ctx) == nil
	}, 30*time.Second, 200*time.Millisecond, "postgres did not become ready")
}
