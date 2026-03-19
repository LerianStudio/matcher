//go:build integration

package matching

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/LerianStudio/matcher/internal/auth"
	configContextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configFieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	configMatchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	configFeeRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/fee_rule"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
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
	rateRepo "github.com/LerianStudio/matcher/internal/matching/adapters/postgres/rate"
	matchLockManager "github.com/LerianStudio/matcher/internal/matching/adapters/redis"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"

	outboxRepo "github.com/LerianStudio/matcher/internal/outbox/adapters/postgres"
	outboxEntities "github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	outboxServices "github.com/LerianStudio/matcher/internal/outbox/services"
	sharedCross "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	tenantAdapters "github.com/LerianStudio/matcher/internal/shared/infrastructure/tenant/adapters"

	"github.com/LerianStudio/matcher/tests/integration"
)

type e4t9Wired struct {
	IngestionUC *ingestionCommand.UseCase
	MatchingUC  *matchingCommand.UseCase
	TxRepo      *ingestionTxRepo.Repository
}

type e4t9Seed struct {
	ContextID         uuid.UUID
	LedgerSourceID    uuid.UUID
	NonLedgerSourceID uuid.UUID
	RuleID            uuid.UUID
}

func e4t9Ctx(t *testing.T, h *integration.TestHarness) context.Context {
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

func seedE4T9Config(t *testing.T, h *integration.TestHarness) e4t9Seed {
	t.Helper()

	ctx := e4t9Ctx(t, h)
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
			Name:   "Integration Bank Source",
			Type:   configVO.SourceTypeBank,
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
				"matchDate":       true,
				"datePrecision":   "DAY",
				"matchReference":  true,
				"caseInsensitive": true,
				"matchScore":      100,
			},
		},
	)
	require.NoError(t, err)
	createdRule, err := ruleRepo.Create(ctx, rule)
	require.NoError(t, err)

	return e4t9Seed{
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

func wireE4T9UseCases(t *testing.T, h *integration.TestHarness) e4t9Wired {
	t.Helper()

	redisConn := mustRedisConn(t, h.RedisAddr)
	provider := tenantAdapters.NewSingleTenantInfrastructureProvider(h.Connection, redisConn)

	jobRepo := ingestionJobRepo.NewRepository(provider)
	txRepo := ingestionTxRepo.NewRepository(provider)
	dedupe := ingestionRedis.NewDedupeService(provider)
	outbox := outboxRepo.NewRepository(provider)

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

	ctxProvider, err := sharedCross.NewContextProviderAdapter(configContextRepo.NewRepository(provider))
	require.NoError(t, err)
	srcProvider, err := sharedCross.NewSourceProviderAdapter(cfgSourceRepo)
	require.NoError(t, err)
	ruleProvider, err := sharedCross.NewMatchRuleProviderAdapter(
		configMatchRuleRepo.NewRepository(provider),
	)
	require.NoError(t, err)

	txAdapter, err := sharedCross.NewTransactionRepositoryAdapterFromRepo(provider, txRepo)
	require.NoError(t, err)
	lockManager := matchLockManager.NewLockManager(provider)

	matchRun := matchRunRepo.NewRepository(provider)
	matchGroup := matchGroupRepo.NewRepository(provider)
	matchItem := matchItemRepo.NewRepository(provider)
	exceptionCreator := exceptionCreatorRepo.NewRepository(provider)
	rate := rateRepo.NewRepository(provider)
	feeVariance := feeVarianceRepo.NewRepository(provider)
	auditLogRepo := governancePostgres.NewRepository(provider)
	adjustment := adjustmentRepo.NewRepository(provider, auditLogRepo)
	feeSchedule := feeScheduleRepo.NewRepository(provider)

	feeRuleProvider, err := sharedCross.NewFeeRuleProviderAdapter(
		configFeeRuleRepo.NewRepository(provider),
	)
	require.NoError(t, err)

	matchingUC, err := matchingCommand.New(matchingCommand.UseCaseDeps{
		ContextProvider:  ctxProvider,
		SourceProvider:   srcProvider,
		RuleProvider:     ruleProvider,
		TxRepo:           txAdapter,
		LockManager:      lockManager,
		MatchRunRepo:     matchRun,
		MatchGroupRepo:   matchGroup,
		MatchItemRepo:    matchItem,
		ExceptionCreator: exceptionCreator,
		OutboxRepo:       outbox,
		RateRepo:         rate,
		FeeVarianceRepo:  feeVariance,
		AdjustmentRepo:   adjustment,
		InfraProvider:    provider,
		AuditLogRepo:     auditLogRepo,
		FeeScheduleRepo:  feeSchedule,
		FeeRuleProvider:  feeRuleProvider,
	})
	require.NoError(t, err)

	return e4t9Wired{IngestionUC: ingestionUC, MatchingUC: matchingUC, TxRepo: txRepo}
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

type capturePublishers struct {
	matchConfirmed int
	last           *shared.MatchConfirmedEvent
}

func (c *capturePublishers) PublishIngestionCompleted(
	_ context.Context,
	_ *ingestionEntities.IngestionCompletedEvent,
) error {
	return nil
}

func (c *capturePublishers) PublishIngestionFailed(
	_ context.Context,
	_ *ingestionEntities.IngestionFailedEvent,
) error {
	return nil
}

func (c *capturePublishers) PublishMatchConfirmed(
	_ context.Context,
	event *shared.MatchConfirmedEvent,
) error {
	c.matchConfirmed++
	c.last = event
	return nil
}

func (c *capturePublishers) PublishMatchUnmatched(
	_ context.Context,
	_ *shared.MatchUnmatchedEvent,
) error {
	return nil
}

func newDispatcher(
	t *testing.T,
	h *integration.TestHarness,
	cap *capturePublishers,
) *outboxServices.Dispatcher {
	t.Helper()

	provider := h.Provider()
	repo := outboxRepo.NewRepository(provider)
	dispatcher, err := outboxServices.NewDispatcher(
		repo,
		cap,
		cap,
		nil,
		noop.NewTracerProvider().Tracer("tests.integration.outbox"),
	)
	require.NoError(t, err)

	return dispatcher
}

func assertMatchConfirmedPending(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
) int {
	t.Helper()
	return countInt(t, ctx, h.Connection,
		"SELECT count(*) FROM outbox_events WHERE event_type=$1 AND status=$2",
		shared.EventTypeMatchConfirmed,
		outboxEntities.OutboxStatusPending,
	)
}

func buildExceptionInputFromTx(
	t *testing.T,
	txn *shared.Transaction,
	sourceType, reason string,
) matchingPorts.ExceptionTransactionInput {
	t.Helper()
	require.NotNil(t, txn, "buildExceptionInputFromTx: txn must not be nil - indicates test setup bug")

	var amountAbsBase decimal.Decimal
	if txn.AmountBase != nil {
		amountAbsBase = txn.AmountBase.Abs()
	} else {
		amountAbsBase = txn.Amount.Abs()
	}

	fxMissing := txn.AmountBase == nil && txn.BaseCurrency != nil

	return matchingPorts.ExceptionTransactionInput{
		TransactionID:   txn.ID,
		AmountAbsBase:   amountAbsBase,
		TransactionDate: txn.Date,
		SourceType:      sourceType,
		FXMissing:       fxMissing,
		Reason:          reason,
	}
}

// registerFailureDiagnostics registers a t.Cleanup that dumps diagnostic info on test failure.
// A fresh context is created via e4t9Ctx inside the cleanup to ensure database queries succeed.
// runMatchAndGetGroup ingests transactions on both sources, runs matching, and returns the first group.
func runMatchAndGetGroup(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	wired e4t9Wired,
	seed e4t9Seed,
) *matchingEntities.MatchGroup {
	t.Helper()

	ledgerCSV := buildCSV("MATCH-001", "250.00", "USD", "2026-01-15", "payment")
	_, err := wired.IngestionUC.StartIngestion(
		ctx,
		seed.ContextID,
		seed.LedgerSourceID,
		"ledger_helper.csv",
		int64(len(ledgerCSV)),
		"csv",
		strings.NewReader(ledgerCSV),
	)
	require.NoError(t, err)

	bankCSV := buildCSV("match-001", "250.00", "USD", "2026-01-15", "payment")
	_, err = wired.IngestionUC.StartIngestion(
		ctx,
		seed.ContextID,
		seed.NonLedgerSourceID,
		"bank_helper.csv",
		int64(len(bankCSV)),
		"csv",
		strings.NewReader(bankCSV),
	)
	require.NoError(t, err)

	_, groups, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
		TenantID:        h.Seed.TenantID,
		ContextID:       seed.ContextID,
		Mode:            matchingVO.MatchRunModeCommit,
		PrimarySourceID: nil,
	})
	require.NoError(t, err)
	require.NotEmpty(t, groups, "expected at least one match group from RunMatch")

	return groups[0]
}

func registerFailureDiagnostics(t *testing.T, h *integration.TestHarness, contextID uuid.UUID) {
	t.Helper()

	t.Cleanup(func() {
		if !t.Failed() {
			return
		}

		diagCtx := e4t9Ctx(t, h)

		t.Log("--- diagnostics: counts ---")
		t.Logf("context_id=%s", contextID.String())
		t.Logf(
			"match_runs=%d",
			countInt(
				t,
				diagCtx,
				h.Connection,
				"SELECT count(*) FROM match_runs WHERE context_id=$1",
				contextID.String(),
			),
		)
		t.Logf(
			"match_groups=%d",
			countInt(
				t,
				diagCtx,
				h.Connection,
				"SELECT count(*) FROM match_groups WHERE context_id=$1",
				contextID.String(),
			),
		)
		t.Logf(
			"match_items=%d",
			countInt(t, diagCtx, h.Connection, "SELECT count(*) FROM match_items"),
		)
		t.Logf(
			"outbox_pending=%d",
			countInt(
				t,
				diagCtx,
				h.Connection,
				"SELECT count(*) FROM outbox_events WHERE status=$1",
				outboxEntities.OutboxStatusPending,
			),
		)
	})
}
