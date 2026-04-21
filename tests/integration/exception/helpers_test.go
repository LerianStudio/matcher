//go:build integration

package exception

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	configContextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configFeeRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/fee_rule"
	configFieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	configMatchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"

	ingestionParsers "github.com/LerianStudio/matcher/internal/ingestion/adapters/parsers"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionRedis "github.com/LerianStudio/matcher/internal/ingestion/adapters/redis"
	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"

	governancePostgres "github.com/LerianStudio/matcher/internal/governance/adapters/postgres"
	adjustmentRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/adjustment"
	exceptionCreatorRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/exception_creator"
	feeScheduleRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_schedule"
	feeVarianceRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/fee_variance"
	matchGroupRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_group"
	matchItemRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_item"
	matchRunRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/match_run"
	matchLockManager "github.com/LerianStudio/matcher/internal/matching/adapters/redis"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"

	exceptionAdapters "github.com/LerianStudio/matcher/internal/exception/adapters"
	exceptionAudit "github.com/LerianStudio/matcher/internal/exception/adapters/audit"
	disputeRepoAdapter "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/dispute"
	exceptionRepoAdapter "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/exception"
	exceptionRedis "github.com/LerianStudio/matcher/internal/exception/adapters/redis"
	exceptionEntities "github.com/LerianStudio/matcher/internal/exception/domain/entities"
	exceptionVO "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	exceptionPorts "github.com/LerianStudio/matcher/internal/exception/ports"
	exceptionCommand "github.com/LerianStudio/matcher/internal/exception/services/command"

	sharedCross "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/ports"

	"github.com/LerianStudio/matcher/tests/integration"
)

type wiredServices struct {
	IngestionUC   *ingestionCommand.UseCase
	MatchingUC    *matchingCommand.UseCase
	ExceptionRepo *exceptionRepoAdapter.Repository
	TxRepo        *ingestionTxRepo.Repository
	Provider      ports.InfrastructureProvider
}

type seedConfig struct {
	ContextID         uuid.UUID
	LedgerSourceID    uuid.UUID
	NonLedgerSourceID uuid.UUID
	RuleID            uuid.UUID
}

func testCtx(t *testing.T, h *integration.TestHarness) context.Context {
	t.Helper()

	ctx := h.Ctx()
	ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

	return ctx
}

func mustRedisConn(t *testing.T, redisAddr string) *libRedis.Client {
	t.Helper()

	parsed, err := url.Parse(strings.TrimSpace(redisAddr))
	require.NoError(t, err)
	require.NotEmpty(t, parsed.Host)

	client := redis.NewClient(&redis.Options{Addr: parsed.Host})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("redis cleanup: %v (expected in test teardown)", err)
		}
	})

	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, client.Ping(pingCtx).Err())

	return infraTestutil.NewRedisClientWithMock(client)
}

func seedTestConfig(t *testing.T, h *integration.TestHarness) seedConfig {
	t.Helper()

	ctx := testCtx(t, h)
	provider := h.Provider()

	ledgerSourceID := h.Seed.SourceID

	srcRepo, err := configSourceRepo.NewRepository(provider)
	require.NoError(t, err)
	fmRepo := configFieldMapRepo.NewRepository(provider)
	ruleRepo := configMatchRuleRepo.NewRepository(provider)

	bankSrc, err := configEntities.NewReconciliationSource(
		ctx,
		h.Seed.ContextID,
		configEntities.CreateReconciliationSourceInput{
			Name:   "Exception Test Bank Source",
			Type:   configVO.SourceTypeBank,
			Side:   sharedfee.MatchingSideRight,
			Config: map[string]any{"format": "csv"},
		},
	)
	require.NoError(t, err)

	createdBankSrc, err := srcRepo.Create(ctx, bankSrc)
	require.NoError(t, err)

	mapping := map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}

	ledgerFM, err := configEntities.NewFieldMap(
		ctx,
		h.Seed.ContextID,
		ledgerSourceID,
		configEntities.CreateFieldMapInput{Mapping: mapping},
	)
	require.NoError(t, err)
	_, err = fmRepo.Create(ctx, ledgerFM)
	require.NoError(t, err)

	bankFM, err := configEntities.NewFieldMap(
		ctx,
		h.Seed.ContextID,
		createdBankSrc.ID,
		configEntities.CreateFieldMapInput{Mapping: mapping},
	)
	require.NoError(t, err)
	_, err = fmRepo.Create(ctx, bankFM)
	require.NoError(t, err)

	rule, err := configEntities.NewMatchRule(
		ctx,
		h.Seed.ContextID,
		configEntities.CreateMatchRuleInput{
			Priority: 1,
			Type:     configVO.RuleTypeExact,
			Config: map[string]any{
				"matchAmount":     true,
				"matchCurrency":   true,
				"matchReference":  true,
				"caseInsensitive": true,
				"matchScore":      100,
			},
		},
	)
	require.NoError(t, err)
	createdRule, err := ruleRepo.Create(ctx, rule)
	require.NoError(t, err)

	return seedConfig{
		ContextID:         h.Seed.ContextID,
		LedgerSourceID:    ledgerSourceID,
		NonLedgerSourceID: createdBankSrc.ID,
		RuleID:            createdRule.ID,
	}
}

type noopIngestionPublisher struct{}

func (p *noopIngestionPublisher) PublishIngestionCompleted(
	_ context.Context,
	_ *ingestionEntities.IngestionCompletedEvent,
) error {
	return nil
}

func (p *noopIngestionPublisher) PublishIngestionFailed(
	_ context.Context,
	_ *ingestionEntities.IngestionFailedEvent,
) error {
	return nil
}

func wireServices(t *testing.T, h *integration.TestHarness) wiredServices {
	t.Helper()

	redisConn := mustRedisConn(t, h.RedisAddr)
	provider := infraTestutil.NewSingleTenantInfrastructureProvider(h.Connection, redisConn)

	jobRepo := ingestionJobRepo.NewRepository(provider)
	txRepo := ingestionTxRepo.NewRepository(provider)
	dedupe := ingestionRedis.NewDedupeService(provider)
	outbox := integration.NewTestOutboxRepository(t, h.Connection)

	parserRegistry := ingestionParsers.NewParserRegistry()
	parserRegistry.Register(ingestionParsers.NewCSVParser())

	cfgSourceRepo, err := configSourceRepo.NewRepository(provider)
	require.NoError(t, err)
	cfgFieldMapRepo := configFieldMapRepo.NewRepository(provider)
	fieldMapAdapter, err := sharedCross.NewFieldMapRepositoryAdapter(cfgFieldMapRepo)
	require.NoError(t, err)
	sourceAdapter, err := sharedCross.NewSourceRepositoryAdapter(cfgSourceRepo)
	require.NoError(t, err)

	ingestionUC, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
		JobRepo:         jobRepo,
		TransactionRepo: txRepo,
		Dedupe:          dedupe,
		Publisher:       &noopIngestionPublisher{},
		OutboxRepo:      outbox,
		Parsers:         parserRegistry,
		FieldMapRepo:    fieldMapAdapter,
		SourceRepo:      sourceAdapter,
	})
	require.NoError(t, err)

	configProvider, err := sharedCross.NewMatchingConfigurationProvider(
		configContextRepo.NewRepository(provider),
		cfgSourceRepo,
		configMatchRuleRepo.NewRepository(provider),
		configFeeRuleRepo.NewRepository(provider),
	)
	require.NoError(t, err)
	srcProvider := configProvider.SourceProvider()

	txAdapter, err := sharedCross.NewTransactionRepositoryAdapterFromRepo(provider, txRepo)
	require.NoError(t, err)
	lockManager := matchLockManager.NewLockManager(provider)

	matchRun := matchRunRepo.NewRepository(provider)
	matchGroup := matchGroupRepo.NewRepository(provider)
	matchItem := matchItemRepo.NewRepository(provider)
	exceptionCreator := exceptionCreatorRepo.NewRepository(provider)
	feeVariance := feeVarianceRepo.NewRepository(provider)
	auditLogRepo := governancePostgres.NewRepository(provider)
	adjustment := adjustmentRepo.NewRepository(provider, auditLogRepo)
	feeSchedule := feeScheduleRepo.NewRepository(provider)

	feeRuleProvider := configProvider.FeeRuleProvider()

	matchingUC, err := matchingCommand.New(matchingCommand.UseCaseDeps{
		ContextProvider:  configProvider.ContextProvider(),
		SourceProvider:   srcProvider,
		RuleProvider:     configProvider.MatchRuleProvider(),
		TxRepo:           txAdapter,
		LockManager:      lockManager,
		MatchRunRepo:     matchRun,
		MatchGroupRepo:   matchGroup,
		MatchItemRepo:    matchItem,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outbox,
		FeeVarianceRepo:  feeVariance,
		AdjustmentRepo:   adjustment,
		InfraProvider:    provider,
		AuditLogRepo:     auditLogRepo,
		FeeScheduleRepo:  feeSchedule,
		FeeRuleProvider:  feeRuleProvider,
	})
	require.NoError(t, err)

	exceptionRepo := exceptionRepoAdapter.NewRepository(provider)

	return wiredServices{
		IngestionUC:   ingestionUC,
		MatchingUC:    matchingUC,
		ExceptionRepo: exceptionRepo,
		TxRepo:        txRepo,
		Provider:      provider,
	}
}

func buildCSV(externalID, amount, currency, date, description string) string {
	return "id,amount,currency,date,description\n" +
		fmt.Sprintf("%s,%s,%s,%s,%s\n", externalID, amount, currency, date, description)
}

func countInt(
	t *testing.T,
	ctx context.Context,
	conn *libPostgres.Client,
	query string,
	args ...any,
) int {
	t.Helper()

	n, err := pgcommon.WithTenantTx(ctx, conn, func(tx *sql.Tx) (int, error) {
		var out int
		if err := tx.QueryRowContext(ctx, query, args...).Scan(&out); err != nil {
			return 0, err
		}

		return out, nil
	})
	require.NoError(t, err)

	return n
}

func createTransaction(
	t *testing.T,
	ctx context.Context,
	txRepo *ingestionTxRepo.Repository,
	jobID, sourceID uuid.UUID,
	externalID string,
	amount decimal.Decimal,
	currency string,
) *shared.Transaction {
	t.Helper()

	tenantID, err := uuid.Parse(auth.GetTenantID(ctx))
	require.NoError(t, err, "createTransaction: context must carry a valid tenant ID")

	tx, err := shared.NewTransaction(
		ctx,
		tenantID,
		jobID,
		sourceID,
		externalID,
		amount,
		currency,
		time.Now().UTC(),
		"test tx",
		map[string]any{},
	)
	require.NoError(t, err)
	tx.ExtractionStatus = shared.ExtractionStatusComplete
	tx.Status = shared.TransactionStatusUnmatched

	created, err := txRepo.Create(ctx, tx)
	require.NoError(t, err)

	return created
}

func createIngestionJob(
	t *testing.T,
	ctx context.Context,
	jRepo *ingestionJobRepo.Repository,
	contextID, sourceID uuid.UUID,
	recordCount int,
) *ingestionEntities.IngestionJob {
	t.Helper()

	job, err := ingestionEntities.NewIngestionJob(ctx, contextID, sourceID, "test.csv", 100)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	require.NoError(t, job.Complete(ctx, recordCount, 0))

	createdJob, err := jRepo.Create(ctx, job)
	require.NoError(t, err)

	return createdJob
}

func findExceptionByTransactionID(
	t *testing.T,
	ctx context.Context,
	conn *libPostgres.Client,
	transactionID uuid.UUID,
) (*exceptionEntities.Exception, bool, error) {
	t.Helper()

	exception, err := pgcommon.WithTenantTx(
		ctx,
		conn,
		func(tx *sql.Tx) (*exceptionEntities.Exception, error) {
			var (
				id        string
				txID      string
				severity  string
				status    string
				reason    sql.NullString
				createdAt time.Time
				updatedAt time.Time
			)

			err := tx.QueryRowContext(ctx, `
			SELECT id, transaction_id, severity, status, reason, created_at, updated_at
			FROM exceptions
			WHERE transaction_id = $1
		`, transactionID.String()).Scan(&id, &txID, &severity, &status, &reason, &createdAt, &updatedAt)
			if err != nil {
				return nil, err
			}

			parsedID, err := uuid.Parse(id)
			if err != nil {
				return nil, err
			}

			parsedTxID, err := uuid.Parse(txID)
			if err != nil {
				return nil, err
			}

			parsedSeverity, err := exceptionVO.ParseExceptionSeverity(severity)
			if err != nil {
				return nil, err
			}

			parsedStatus, err := exceptionVO.ParseExceptionStatus(status)
			if err != nil {
				return nil, err
			}

			exc := &exceptionEntities.Exception{
				ID:            parsedID,
				TransactionID: parsedTxID,
				Severity:      parsedSeverity,
				Status:        parsedStatus,
				CreatedAt:     createdAt,
				UpdatedAt:     updatedAt,
			}

			if reason.Valid {
				exc.Reason = &reason.String
			}

			return exc, nil
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("find exception by transaction id %s: %w", transactionID, err)
	}

	return exception, true, nil
}

func wireIdempotencyRepo(
	t *testing.T,
	h *integration.TestHarness,
) *exceptionRedis.IdempotencyRepository {
	t.Helper()

	redisConn := mustRedisConn(t, h.RedisAddr)
	provider := infraTestutil.NewSingleTenantInfrastructureProvider(h.Connection, redisConn)

	repo, err := exceptionRedis.NewIdempotencyRepository(provider)
	require.NoError(t, err)

	return repo
}

// testCtxWithActor returns a context with tenant info and a user ID for actor extraction.
func testCtxWithActor(t *testing.T, h *integration.TestHarness, actorID string) context.Context {
	t.Helper()

	ctx := testCtx(t, h)
	ctx = context.WithValue(ctx, auth.UserIDKey, actorID)

	return ctx
}

// createExceptionForTransaction inserts an exception directly in the DB for a given transaction.
func createExceptionForTransaction(
	t *testing.T,
	ctx context.Context,
	conn *libPostgres.Client,
	transactionID uuid.UUID,
	severity exceptionVO.ExceptionSeverity,
	reason string,
) *exceptionEntities.Exception {
	t.Helper()

	exc, err := exceptionEntities.NewException(ctx, transactionID, severity, &reason)
	require.NoError(t, err)

	_, err = pgcommon.WithTenantTx(ctx, conn, func(tx *sql.Tx) (struct{}, error) {
		_, execErr := tx.ExecContext(ctx, `
			INSERT INTO exceptions (
				id, transaction_id, severity, status, reason, version, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`,
			exc.ID.String(),
			exc.TransactionID.String(),
			exc.Severity.String(),
			exc.Status.String(),
			reason,
			0,
			exc.CreatedAt,
			exc.UpdatedAt,
		)

		return struct{}{}, execErr
	})
	require.NoError(t, err)

	// Re-read to get the persisted state (including version)
	loaded, err := pgcommon.WithTenantTx(ctx, conn, func(tx *sql.Tx) (*exceptionEntities.Exception, error) {
		row := tx.QueryRowContext(ctx, `
			SELECT id, transaction_id, severity, status, reason, version, created_at, updated_at
			FROM exceptions WHERE id = $1
		`, exc.ID.String())

		var (
			id, txID, sev, st string
			reasonVal         sql.NullString
			version           int64
			createdAt, updAt  time.Time
		)

		if scanErr := row.Scan(&id, &txID, &sev, &st, &reasonVal, &version, &createdAt, &updAt); scanErr != nil {
			return nil, scanErr
		}

		parsedID, err := uuid.Parse(id)
		if err != nil {
			return nil, fmt.Errorf("parse exception id: %w", err)
		}

		parsedTxID, err := uuid.Parse(txID)
		if err != nil {
			return nil, fmt.Errorf("parse transaction id: %w", err)
		}

		parsedSev, err := exceptionVO.ParseExceptionSeverity(sev)
		if err != nil {
			return nil, fmt.Errorf("parse severity: %w", err)
		}

		parsedSt, err := exceptionVO.ParseExceptionStatus(st)
		if err != nil {
			return nil, fmt.Errorf("parse status: %w", err)
		}

		result := &exceptionEntities.Exception{
			ID:            parsedID,
			TransactionID: parsedTxID,
			Severity:      parsedSev,
			Status:        parsedSt,
			Version:       version,
			CreatedAt:     createdAt,
			UpdatedAt:     updAt,
		}
		if reasonVal.Valid {
			result.Reason = &reasonVal.String
		}

		return result, nil
	})
	require.NoError(t, err)

	return loaded
}

// wireExceptionUseCase creates a real exception UseCase with real repos and the given
// ResolutionExecutor (can be a stub for tests).
func wireExceptionUseCase(
	t *testing.T,
	h *integration.TestHarness,
	executor exceptionPorts.ResolutionExecutor,
) (*exceptionCommand.UseCase, ports.InfrastructureProvider) {
	t.Helper()

	redisConn := mustRedisConn(t, h.RedisAddr)
	provider := infraTestutil.NewSingleTenantInfrastructureProvider(h.Connection, redisConn)

	exceptionRepo := exceptionRepoAdapter.NewRepository(provider)
	outbox := integration.NewTestOutboxRepository(t, h.Connection)

	auditPub, err := exceptionAudit.NewOutboxPublisher(outbox)
	require.NoError(t, err)

	actorExtractor := exceptionAdapters.NewAuthActorExtractor()

	uc, err := exceptionCommand.NewUseCase(
		exceptionRepo,
		executor,
		auditPub,
		actorExtractor,
		provider,
	)
	require.NoError(t, err)

	return uc, provider
}

// wireDisputeUseCase creates a real DisputeUseCase with real repos.
func wireDisputeUseCase(
	t *testing.T,
	h *integration.TestHarness,
) (*exceptionCommand.DisputeUseCase, ports.InfrastructureProvider) {
	t.Helper()

	redisConn := mustRedisConn(t, h.RedisAddr)
	provider := infraTestutil.NewSingleTenantInfrastructureProvider(h.Connection, redisConn)

	exceptionRepo := exceptionRepoAdapter.NewRepository(provider)
	disputeRepo := disputeRepoAdapter.NewRepository(provider)
	outbox := integration.NewTestOutboxRepository(t, h.Connection)

	auditPub, err := exceptionAudit.NewOutboxPublisher(outbox)
	require.NoError(t, err)

	actorExtractor := exceptionAdapters.NewAuthActorExtractor()

	uc, err := exceptionCommand.NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		auditPub,
		actorExtractor,
		provider,
	)
	require.NoError(t, err)

	return uc, provider
}

// noopResolutionExecutor is a test double that succeeds silently.
type noopResolutionExecutor struct{}

func (e *noopResolutionExecutor) ForceMatch(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ exceptionVO.OverrideReason,
) error {
	return nil
}

func (e *noopResolutionExecutor) AdjustEntry(
	_ context.Context,
	_ uuid.UUID,
	_ exceptionPorts.AdjustmentInput,
) error {
	return nil
}

// failingResolutionExecutor is a test double that always returns an error.
type failingResolutionExecutor struct {
	err error
}

func (e *failingResolutionExecutor) ForceMatch(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ exceptionVO.OverrideReason,
) error {
	return e.err
}

func (e *failingResolutionExecutor) AdjustEntry(
	_ context.Context,
	_ uuid.UUID,
	_ exceptionPorts.AdjustmentInput,
) error {
	return e.err
}

// countOutboxEvents counts outbox events with optional aggregate_id filter.
func countOutboxEvents(
	t *testing.T,
	ctx context.Context,
	conn *libPostgres.Client,
	entityID uuid.UUID,
) int {
	t.Helper()

	return countInt(t, ctx, conn,
		"SELECT count(*) FROM outbox_events WHERE aggregate_id = $1",
		entityID.String(),
	)
}
