//go:build integration

package flow

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"

	configContextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configFieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	configMatchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/tests/integration"
	"github.com/LerianStudio/matcher/tests/integration/server"
)

// TestHarnessLike is an interface that both ServerHarness and SharedServerHarness satisfy.
type TestHarnessLike interface {
	ServerCtx() context.Context
	DoJSON(method, path string, payload any) (*http.Response, []byte, error)
	DoMultipart(
		path string,
		fieldName, fileName string,
		fileContent []byte,
		formFields map[string]string,
	) (*http.Response, []byte, error)
	WaitForEventWithTimeout(
		timeout time.Duration,
		match func(routingKey string, body []byte) bool,
	) ([]byte, error)
	WaitForEvent(
		ctx context.Context,
		match func(routingKey string, body []byte) bool,
	) ([]byte, error)
	DispatchOutboxOnce(ctx context.Context) int
	DispatchOutboxUntilEmpty(ctx context.Context, maxIterations int)
	GetConnection() *libPostgres.Client
	GetSeed() integration.SeedData
}

// ServerHarnessAdapter wraps *server.ServerHarness to implement TestHarnessLike.
type ServerHarnessAdapter struct {
	*server.ServerHarness
}

func (a *ServerHarnessAdapter) GetConnection() *libPostgres.Client {
	return a.Connection
}

func (a *ServerHarnessAdapter) GetSeed() integration.SeedData {
	return a.ServerHarness.TestHarness.Seed
}

// SharedServerHarnessAdapter wraps *server.SharedServerHarness to implement TestHarnessLike.
type SharedServerHarnessAdapter struct {
	*server.SharedServerHarness
}

func (a *SharedServerHarnessAdapter) GetConnection() *libPostgres.Client {
	return a.Connection
}

func (a *SharedServerHarnessAdapter) GetSeed() integration.SeedData {
	return a.SharedServerHarness.SharedTestHarness.Seed
}

// WrapHarness converts a ServerHarness to TestHarnessLike.
func WrapHarness(sh *server.ServerHarness) TestHarnessLike {
	return &ServerHarnessAdapter{sh}
}

// WrapSharedHarness converts a SharedServerHarness to TestHarnessLike.
func WrapSharedHarness(sh *server.SharedServerHarness) TestHarnessLike {
	return &SharedServerHarnessAdapter{sh}
}

// FlowTestSeed contains IDs for HTTP flow tests.
type FlowTestSeed struct {
	TenantID          uuid.UUID
	ContextID         uuid.UUID
	LedgerSourceID    uuid.UUID
	NonLedgerSourceID uuid.UUID
	RuleID            uuid.UUID
}

// FlowTestConfigOptions allows customizing the test configuration.
type FlowTestConfigOptions struct {
	CaseInsensitive bool
	DatePrecision   string
}

// DefaultFlowTestConfigOptions returns the default options.
func DefaultFlowTestConfigOptions() FlowTestConfigOptions {
	return FlowTestConfigOptions{
		CaseInsensitive: true,
		DatePrecision:   "DAY",
	}
}

// SetupFlowTestConfig creates the minimum configuration needed for ingestion and matching.
func SetupFlowTestConfig(t *testing.T, sh *server.ServerHarness) FlowTestSeed {
	return SetupFlowTestConfigWithOptions(t, sh, DefaultFlowTestConfigOptions())
}

// SetupFlowTestConfigWithOptions creates configuration with custom options.
func SetupFlowTestConfigWithOptions(
	t *testing.T,
	sh *server.ServerHarness,
	opts FlowTestConfigOptions,
) FlowTestSeed {
	t.Helper()
	return SetupFlowTestConfigWithOptionsGeneric(t, &ServerHarnessAdapter{ServerHarness: sh}, opts)
}

// BuildCSVContent creates a minimal CSV file for testing.
func BuildCSVContent(externalID, amount, currency, date, description string) []byte {
	return []byte("id,amount,currency,date,description\n" +
		externalID + "," + amount + "," + currency + "," + date + "," + description + "\n")
}

// BuildJSONContent creates a minimal JSON array file for testing.
func BuildJSONContent(t *testing.T, externalID, amount, currency, date, description string) []byte {
	t.Helper()

	tx := map[string]string{
		"id":          externalID,
		"amount":      amount,
		"currency":    currency,
		"date":        date,
		"description": description,
	}
	data, err := json.Marshal([]map[string]string{tx})
	require.NoError(t, err, "BuildJSONContent: unexpected marshal error")

	return data
}

// BuildMultiRowJSON creates a JSON array file with multiple transactions.
// Each row is []string{id, amount, currency, date, description}.
func BuildMultiRowJSON(t *testing.T, rows [][]string) []byte {
	t.Helper()

	txs := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		txs = append(txs, map[string]string{
			"id":          row[0],
			"amount":      row[1],
			"currency":    row[2],
			"date":        row[3],
			"description": row[4],
		})
	}
	data, err := json.Marshal(txs)
	require.NoError(t, err, "BuildMultiRowJSON: unexpected marshal error")

	return data
}

// BuildMultiRowCSV creates a CSV file with multiple rows for testing.
// Each row is []string{id, amount, currency, date, description}.
func BuildMultiRowCSV(rows [][]string) []byte {
	var sb strings.Builder
	sb.WriteString("id,amount,currency,date,description\n")
	for _, row := range rows {
		sb.WriteString(strings.Join(row, ",") + "\n")
	}
	return []byte(sb.String())
}

// BuildXMLContent creates a minimal XML file for testing.
func BuildXMLContent(externalID, amount, currency, date, description string) []byte {
	return []byte(`<transactions>
  <transaction>
    <id>` + externalID + `</id>
    <amount>` + amount + `</amount>
    <currency>` + currency + `</currency>
    <date>` + date + `</date>
    <description>` + description + `</description>
  </transaction>
</transactions>`)
}

// BuildMultiRowXML creates an XML file with multiple transactions for testing.
// Each row is []string{id, amount, currency, date, description}.
func BuildMultiRowXML(rows [][]string) []byte {
	var sb strings.Builder
	sb.WriteString("<transactions>\n")
	for _, row := range rows {
		sb.WriteString("  <transaction>\n")
		sb.WriteString("    <id>" + row[0] + "</id>\n")
		sb.WriteString("    <amount>" + row[1] + "</amount>\n")
		sb.WriteString("    <currency>" + row[2] + "</currency>\n")
		sb.WriteString("    <date>" + row[3] + "</date>\n")
		sb.WriteString("    <description>" + row[4] + "</description>\n")
		sb.WriteString("  </transaction>\n")
	}
	sb.WriteString("</transactions>")
	return []byte(sb.String())
}

// UploadPath returns the upload endpoint path.
func UploadPath(contextID, sourceID uuid.UUID) string {
	return "/v1/imports/contexts/" + contextID.String() + "/sources/" + sourceID.String() + "/upload"
}

// RunMatchPath returns the run match endpoint path.
func RunMatchPath(contextID uuid.UUID) string {
	return "/v1/matching/contexts/" + contextID.String() + "/run"
}

// JobResponse represents the HTTP response from upload endpoint.
type JobResponse struct {
	ID     uuid.UUID `json:"id"`
	Status string    `json:"status"`
}

// RunMatchResponse represents the HTTP response from run match endpoint.
type RunMatchResponse struct {
	RunID  uuid.UUID `json:"runId"`
	Status string    `json:"status"`
}

// ParseJobResponse parses upload response body.
func ParseJobResponse(t *testing.T, body []byte) JobResponse {
	t.Helper()
	var resp JobResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	return resp
}

// ParseRunMatchResponse parses run match response body.
func ParseRunMatchResponse(t *testing.T, body []byte) RunMatchResponse {
	t.Helper()
	var resp RunMatchResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	return resp
}

// ErrorResponse represents the standard Matcher API error response.
type ErrorResponse struct {
	Code    string `json:"code"`
	Title   string `json:"title"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

// ParseErrorResponse parses error response body.
func ParseErrorResponse(t *testing.T, body []byte) ErrorResponse {
	t.Helper()

	var resp ErrorResponse
	require.NoError(t, json.Unmarshal(body, &resp))

	return resp
}

// AssertErrorResponse validates the Matcher error-response contract.
func AssertErrorResponse(t *testing.T, body []byte) ErrorResponse {
	t.Helper()
	resp := ParseErrorResponse(t, body)
	require.NotEmpty(t, resp.Code, "error response should have code")
	require.NotEmpty(t, resp.Title, "error response should have title")
	require.NotEmpty(t, resp.Message, "error response should have message")

	return resp
}

// RequireExactErrorResponse validates the full Matcher error-response contract.
func RequireExactErrorResponse(t *testing.T, body []byte, code, title, message string) ErrorResponse {
	t.Helper()

	resp := AssertErrorResponse(t, body)
	require.Equal(t, code, resp.Code)
	require.Equal(t, title, resp.Title)
	require.Equal(t, message, resp.Message)

	return resp
}

// MatchConfirmedEvent represents the match_confirmed event payload.
// This matches the shared domain MatchConfirmedEvent structure.
type MatchConfirmedEvent struct {
	EventType      string      `json:"eventType"`
	TenantID       uuid.UUID   `json:"tenantId"`
	TenantSlug     string      `json:"tenantSlug"`
	ContextID      uuid.UUID   `json:"contextId"`
	RunID          uuid.UUID   `json:"runId"`
	MatchID        uuid.UUID   `json:"matchId"`
	RuleID         uuid.UUID   `json:"ruleId"`
	TransactionIDs []uuid.UUID `json:"transactionIds"`
	Confidence     int         `json:"confidence"`
	ConfirmedAt    time.Time   `json:"confirmedAt"`
	Timestamp      time.Time   `json:"timestamp"`

	// Legacy fields for backward compatibility with older test assertions
	Matches []MatchedPair `json:"matches,omitempty"`
}

// MatchedPair represents a single matched transaction pair (legacy).
type MatchedPair struct {
	LedgerTransactionID    uuid.UUID `json:"ledgerTransactionId"`
	LedgerExternalID       string    `json:"ledgerExternalId"`
	NonLedgerTransactionID uuid.UUID `json:"nonLedgerTransactionId"`
	NonLedgerExternalID    string    `json:"nonLedgerExternalId"`
	Score                  int       `json:"score"`
}

// ParseMatchConfirmedEvent parses a match_confirmed event body.
func ParseMatchConfirmedEvent(t *testing.T, body []byte) MatchConfirmedEvent {
	t.Helper()
	var event MatchConfirmedEvent
	require.NoError(t, json.Unmarshal(body, &event))
	return event
}

// AssertMatchConfirmedEventValid verifies that a MatchConfirmedEvent contains all required fields.
func AssertMatchConfirmedEventValid(
	t *testing.T,
	event MatchConfirmedEvent,
	expectedContextID, expectedRuleID uuid.UUID,
) {
	t.Helper()

	require.Equal(
		t,
		"matching.match_confirmed",
		event.EventType,
		"event_type should be matching.match_confirmed",
	)
	require.NotEqual(t, uuid.Nil, event.TenantID, "tenant_id should not be nil")
	require.Equal(t, expectedContextID, event.ContextID, "context_id should match expected")
	require.NotEqual(t, uuid.Nil, event.RunID, "run_id should not be nil")
	require.NotEqual(t, uuid.Nil, event.MatchID, "match_id should not be nil")
	require.Equal(t, expectedRuleID, event.RuleID, "rule_id should match expected")
	require.NotEmpty(t, event.TransactionIDs, "transaction_ids should not be empty")
	require.GreaterOrEqual(t, event.Confidence, 0, "confidence should be >= 0")
	require.LessOrEqual(t, event.Confidence, 100, "confidence should be <= 100")
	require.False(t, event.ConfirmedAt.IsZero(), "confirmed_at should not be zero")
	require.False(t, event.Timestamp.IsZero(), "timestamp should not be zero")
}

// AssertTransactionIDsContain verifies that the event contains specific transaction IDs.
func AssertTransactionIDsContain(t *testing.T, event MatchConfirmedEvent, expectedCount int) {
	t.Helper()

	require.Len(
		t,
		event.TransactionIDs,
		expectedCount,
		"should have %d transaction IDs",
		expectedCount,
	)
	for i, txID := range event.TransactionIDs {
		require.NotEqual(t, uuid.Nil, txID, "transaction_id[%d] should not be nil", i)
	}
}

// MatchRunDetailsResponse represents the HTTP response from GET /matching/runs/:runId.
type MatchRunDetailsResponse struct {
	ID        uuid.UUID `json:"id"`
	ContextID uuid.UUID `json:"contextId"`
	Status    string    `json:"status"`
	Mode      string    `json:"mode"`
	CreatedAt string    `json:"createdAt"`
	UpdatedAt string    `json:"updatedAt"`
}

// ParseMatchRunDetailsResponse parses match run details response body.
func ParseMatchRunDetailsResponse(t *testing.T, body []byte) MatchRunDetailsResponse {
	t.Helper()
	var resp MatchRunDetailsResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	return resp
}

// MatchGroupItem represents a single match group in the paginated response.
type MatchGroupItem struct {
	ID        uuid.UUID `json:"id"`
	RunID     uuid.UUID `json:"runId"`
	ContextID uuid.UUID `json:"contextId"`
	Status    string    `json:"status"`
	Score     int       `json:"score"`
	CreatedAt string    `json:"createdAt"`
}

// MatchGroupsResponse represents the paginated response from GET /matching/runs/:runId/groups.
type MatchGroupsResponse struct {
	Items []MatchGroupItem `json:"items"`
	CursorResponse
}

// ParseMatchGroupsResponse parses match groups paginated response body.
func ParseMatchGroupsResponse(t *testing.T, body []byte) MatchGroupsResponse {
	t.Helper()
	var resp MatchGroupsResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	return resp
}

// JobListResponse represents the paginated job list HTTP response.
type JobListResponse struct {
	Items []JobResponse `json:"items"`
	CursorResponse
}

// CursorResponse represents pagination cursor in response.
type CursorResponse struct {
	NextCursor string `json:"nextCursor"`
	PrevCursor string `json:"prevCursor"`
	Limit      int    `json:"limit"`
	HasMore    bool   `json:"hasMore"`
}

// ParseJobListResponse parses job list response body.
func ParseJobListResponse(t *testing.T, body []byte) JobListResponse {
	t.Helper()
	var resp JobListResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	return resp
}

// SetupFlowTestConfigWithDatePrecision creates config with a specific date precision for matching rules.
// Deprecated: Use SetupFlowTestConfigWithOptions with DatePrecision option instead.
func SetupFlowTestConfigWithDatePrecision(
	t *testing.T,
	sh *server.ServerHarness,
	datePrecision string,
) FlowTestSeed {
	t.Helper()
	opts := DefaultFlowTestConfigOptions()
	opts.DatePrecision = datePrecision
	return SetupFlowTestConfigWithOptions(t, sh, opts)
}

// TransactionResponse represents a transaction in list responses.
type TransactionResponse struct {
	ID               uuid.UUID      `json:"id"`
	JobID            uuid.UUID      `json:"jobId"`
	SourceID         uuid.UUID      `json:"sourceId"`
	ContextID        uuid.UUID      `json:"contextId"`
	ExternalID       string         `json:"externalId"`
	Amount           string         `json:"amount"`
	Currency         string         `json:"currency"`
	Date             string         `json:"date"`
	Description      string         `json:"description"`
	Status           string         `json:"status"`
	ExtractionStatus string         `json:"extractionStatus"`
	RawData          map[string]any `json:"rawData"`
	CreatedAt        string         `json:"createdAt"`
	UpdatedAt        string         `json:"updatedAt"`
}

// TransactionListResponse represents the transaction list response.
type TransactionListResponse struct {
	Items []TransactionResponse `json:"items"`
	CursorResponse
}

// ParseTransactionListResponse parses a transaction list response body.
func ParseTransactionListResponse(t *testing.T, body []byte) TransactionListResponse {
	t.Helper()
	var resp TransactionListResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	return resp
}

// TransactionsPath returns the transactions list endpoint path.
func TransactionsPath(contextID, jobID uuid.UUID) string {
	return "/v1/imports/contexts/" + contextID.String() + "/jobs/" + jobID.String() + "/transactions"
}

// WaitForIngestionCompleted waits for an ingestion.completed event.
func WaitForIngestionCompleted(
	t *testing.T,
	sh *server.ServerHarness,
	timeout time.Duration,
) []byte {
	t.Helper()

	body, err := sh.WaitForEventWithTimeout(timeout, func(routingKey string, _ []byte) bool {
		return routingKey == server.RoutingKeyIngestionCompleted
	})
	require.NoError(t, err)
	return body
}

// WaitForMatchConfirmed waits for a match.confirmed event.
func WaitForMatchConfirmed(t *testing.T, sh *server.ServerHarness, timeout time.Duration) []byte {
	t.Helper()

	body, err := sh.WaitForEventWithTimeout(timeout, func(routingKey string, _ []byte) bool {
		return routingKey == server.RoutingKeyMatchConfirmed
	})
	require.NoError(t, err)
	return body
}

// WaitForMultipleMatchEvents waits for the specified number of match_confirmed events.
// Each match_confirmed event represents ONE match. When expecting N matches, call this with count=N.
// Returns all received events.
func WaitForMultipleMatchEvents(
	t *testing.T,
	sh *server.ServerHarness,
	count int,
	timeout time.Duration,
) []MatchConfirmedEvent {
	t.Helper()

	if count <= 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	events := make([]MatchConfirmedEvent, 0, count)

	for i := 0; i < count; i++ {
		body, err := sh.WaitForEvent(ctx, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.NoError(
			t,
			err,
			"timed out waiting for match_confirmed events: received %d of %d",
			i,
			count,
		)

		event := ParseMatchConfirmedEvent(t, body)
		events = append(events, event)
		t.Logf("Received match_confirmed event %d/%d: MatchID=%s", i+1, count, event.MatchID)
	}

	return events
}

// CountPendingOutbox returns the count of pending outbox events.
func CountPendingOutbox(t *testing.T, ctx context.Context, sh *server.ServerHarness) int {
	t.Helper()

	resolver, err := sh.Connection.Resolver(ctx)
	require.NoError(t, err, "failed to get db resolver for outbox count")

	primaryDBs := resolver.PrimaryDBs()
	require.NotEmpty(t, primaryDBs, "no primary databases available")

	var count int
	row := primaryDBs[0].QueryRowContext(
		ctx,
		"SELECT count(*) FROM outbox_events WHERE status = 'pending'",
	)
	require.NoError(t, row.Scan(&count))
	return count
}

// EnsureContext verifies the context exists and is accessible.
func EnsureContext(t *testing.T, sh *server.ServerHarness, contextID uuid.UUID) {
	t.Helper()

	ctx := sh.ServerCtx()
	provider := infraTestutil.NewSingleTenantInfrastructureProvider(sh.Connection, nil)
	repo := configContextRepo.NewRepository(provider)
	found, err := repo.FindByID(ctx, contextID)
	require.NoError(t, err)
	require.NotNil(t, found)
}

// MultiRuleFlowTestSeed extends FlowTestSeed with multiple rules for priority testing.
type MultiRuleFlowTestSeed struct {
	FlowTestSeed
	ExactRuleID     uuid.UUID // Priority 1 (highest) - exact matching
	ToleranceRuleID uuid.UUID // Priority 2 - tolerance matching with amount wiggle room
}

type multiRuleFlowTestSeedParts struct {
	tenantID        uuid.UUID
	contextID       uuid.UUID
	ledgerSourceID  uuid.UUID
	bankSourceID    uuid.UUID
	exactRuleID     uuid.UUID
	toleranceRuleID uuid.UUID
}

func createMultiRuleFlowTestConfig(
	t *testing.T,
	ctx context.Context,
	conn *libPostgres.Client,
	seed integration.SeedData,
) multiRuleFlowTestSeedParts {
	t.Helper()

	provider := infraTestutil.NewSingleTenantInfrastructureProvider(conn, nil)
	srcRepo, err := configSourceRepo.NewRepository(provider)
	require.NoError(t, err)
	fmRepo := configFieldMapRepo.NewRepository(provider)
	ruleRepo := configMatchRuleRepo.NewRepository(provider)

	// Create non-ledger source (bank)
	bankSrc, err := configEntities.NewReconciliationSource(
		ctx,
		seed.ContextID,
		configEntities.CreateReconciliationSourceInput{
			Name:   "Multi-Rule Test Bank Source",
			Type:   configVO.SourceTypeBank,
			Side:   sharedfee.MatchingSideRight,
			Config: map[string]any{"format": "csv"},
		},
	)
	require.NoError(t, err)

	createdBankSrc, err := srcRepo.Create(ctx, bankSrc)
	require.NoError(t, err)

	// Create field maps
	mapping := map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}

	ledgerFM, err := configEntities.NewFieldMap(
		ctx,
		seed.ContextID,
		seed.SourceID,
		configEntities.CreateFieldMapInput{Mapping: mapping},
	)
	require.NoError(t, err)
	_, err = fmRepo.Create(ctx, ledgerFM)
	require.NoError(t, err)

	bankFM, err := configEntities.NewFieldMap(
		ctx,
		seed.ContextID,
		createdBankSrc.ID,
		configEntities.CreateFieldMapInput{Mapping: mapping},
	)
	require.NoError(t, err)
	_, err = fmRepo.Create(ctx, bankFM)
	require.NoError(t, err)

	// Create EXACT rule (priority 1 - highest priority)
	// Requires exact amount, currency, date, and reference match
	exactRule, err := configEntities.NewMatchRule(
		ctx,
		seed.ContextID,
		configEntities.CreateMatchRuleInput{
			Priority: 1,
			Type:     shared.RuleTypeExact,
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
	createdExactRule, err := ruleRepo.Create(ctx, exactRule)
	require.NoError(t, err)

	// Create TOLERANCE rule (priority 2 - lower priority)
	// Allows some amount tolerance (e.g., for fees or rounding differences)
	// NOTE: Tolerance rules support optional reference matching via matchReference field.
	// When matchReference=false (default), ReferenceScore=1.0 for 100% confidence.
	// NOTE: matchScore must be >= 90 for auto-confirmation (see match_group.go autoConfirmConfidence)
	toleranceRule, err := configEntities.NewMatchRule(
		ctx,
		seed.ContextID,
		configEntities.CreateMatchRuleInput{
			Priority: 2,
			Type:     shared.RuleTypeTolerance,
			Config: map[string]any{
				"matchCurrency":    true,
				"absTolerance":     "5.00", // Allow up to $5 difference
				"percentTolerance": "0.00", // No percentage tolerance
				"dateWindowDays":   0,      // Same day only (stricter to avoid ambiguous matches)
				"matchScore":       90,     // Minimum score for auto-confirmation
			},
		},
	)
	require.NoError(t, err)
	createdToleranceRule, err := ruleRepo.Create(ctx, toleranceRule)
	require.NoError(t, err)

	return multiRuleFlowTestSeedParts{
		tenantID:        seed.TenantID,
		contextID:       seed.ContextID,
		ledgerSourceID:  seed.SourceID,
		bankSourceID:    createdBankSrc.ID,
		exactRuleID:     createdExactRule.ID,
		toleranceRuleID: createdToleranceRule.ID,
	}
}

// SetupMultiRuleFlowTestConfig creates configuration with multiple rules for priority testing.
// Creates an EXACT rule (priority 1) and a TOLERANCE rule (priority 2).
// Lower priority number = higher priority (checked first).
func SetupMultiRuleFlowTestConfig(t *testing.T, sh *server.ServerHarness) MultiRuleFlowTestSeed {
	t.Helper()

	ctx := sh.ServerCtx()
	configParts := createMultiRuleFlowTestConfig(t, ctx, sh.Connection, sh.TestHarness.Seed)

	return MultiRuleFlowTestSeed{
		FlowTestSeed: FlowTestSeed{
			TenantID:          configParts.tenantID,
			ContextID:         configParts.contextID,
			LedgerSourceID:    configParts.ledgerSourceID,
			NonLedgerSourceID: configParts.bankSourceID,
			RuleID:            configParts.exactRuleID, // Default to exact rule
		},
		ExactRuleID:     configParts.exactRuleID,
		ToleranceRuleID: configParts.toleranceRuleID,
	}
}

// JobStatusPath returns the job status endpoint path.
func JobStatusPath(contextID, jobID uuid.UUID) string {
	return "/v1/imports/contexts/" + contextID.String() + "/jobs/" + jobID.String()
}

// GetJobStatus fetches the current job status via HTTP.
func GetJobStatus(t *testing.T, sh *server.ServerHarness, contextID, jobID uuid.UUID) JobResponse {
	t.Helper()

	resp, body, err := sh.DoJSON("GET", JobStatusPath(contextID, jobID), nil)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, "get job status failed: %s", string(body))

	return ParseJobResponse(t, body)
}

// IsTerminalStatus returns true if the job status is terminal (completed or failed).
func IsTerminalStatus(status string) bool {
	s := strings.ToUpper(status)
	return s == "COMPLETED" || s == "FAILED"
}

// PollJobUntilTerminal polls job status until it reaches a terminal state or timeout.
// Returns the final job response and all observed statuses.
func PollJobUntilTerminal(
	t *testing.T,
	sh *server.ServerHarness,
	contextID, jobID uuid.UUID,
	timeout time.Duration,
) (JobResponse, []string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var observedStatuses []string
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf(
				"timed out waiting for job %s to reach terminal state. Observed statuses: %v",
				jobID,
				observedStatuses,
			)
		case <-ticker.C:
			job := GetJobStatus(t, sh, contextID, jobID)

			// Track unique statuses in order of first observation
			if len(observedStatuses) == 0 ||
				observedStatuses[len(observedStatuses)-1] != job.Status {
				observedStatuses = append(observedStatuses, job.Status)
			}

			if IsTerminalStatus(job.Status) {
				return job, observedStatuses
			}
		}
	}
}

// =====================================================================
// Generic versions of helper functions for TestHarnessLike interface
// =====================================================================

// SetupFlowTestConfigGeneric creates the minimum configuration needed for ingestion and matching.
func SetupFlowTestConfigGeneric(t *testing.T, h TestHarnessLike) FlowTestSeed {
	return SetupFlowTestConfigWithOptionsGeneric(t, h, DefaultFlowTestConfigOptions())
}

// SetupFlowTestConfigWithOptionsGeneric creates configuration with custom options.
func SetupFlowTestConfigWithOptionsGeneric(
	t *testing.T,
	h TestHarnessLike,
	opts FlowTestConfigOptions,
) FlowTestSeed {
	t.Helper()

	ctx := h.ServerCtx()
	conn := h.GetConnection()
	seed := h.GetSeed()
	provider := infraTestutil.NewSingleTenantInfrastructureProvider(conn, nil)

	tenantID := seed.TenantID
	contextID := seed.ContextID
	ledgerSourceID := seed.SourceID

	srcRepo, err := configSourceRepo.NewRepository(provider)
	require.NoError(t, err)
	fmRepo := configFieldMapRepo.NewRepository(provider)
	ruleRepo := configMatchRuleRepo.NewRepository(provider)

	bankSrc, err := configEntities.NewReconciliationSource(
		ctx,
		contextID,
		configEntities.CreateReconciliationSourceInput{
			Name:   "HTTP Flow Test Bank Source",
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
		contextID,
		ledgerSourceID,
		configEntities.CreateFieldMapInput{Mapping: mapping},
	)
	require.NoError(t, err)
	_, err = fmRepo.Create(ctx, ledgerFM)
	require.NoError(t, err)

	bankFM, err := configEntities.NewFieldMap(
		ctx,
		contextID,
		createdBankSrc.ID,
		configEntities.CreateFieldMapInput{Mapping: mapping},
	)
	require.NoError(t, err)
	_, err = fmRepo.Create(ctx, bankFM)
	require.NoError(t, err)

	datePrecision := opts.DatePrecision
	if datePrecision == "" {
		datePrecision = "DAY"
	}

	rule, err := configEntities.NewMatchRule(ctx, contextID, configEntities.CreateMatchRuleInput{
		Priority: 1,
		Type:     shared.RuleTypeExact,
		Config: map[string]any{
			"matchAmount":     true,
			"matchCurrency":   true,
			"matchDate":       true,
			"datePrecision":   datePrecision,
			"matchReference":  true,
			"caseInsensitive": opts.CaseInsensitive,
			"matchScore":      100,
		},
	})
	require.NoError(t, err)
	createdRule, err := ruleRepo.Create(ctx, rule)
	require.NoError(t, err)

	return FlowTestSeed{
		TenantID:          tenantID,
		ContextID:         contextID,
		LedgerSourceID:    ledgerSourceID,
		NonLedgerSourceID: createdBankSrc.ID,
		RuleID:            createdRule.ID,
	}
}

// SetupMultiRuleFlowTestConfigGeneric creates configuration with multiple rules for priority testing.
func SetupMultiRuleFlowTestConfigGeneric(t *testing.T, h TestHarnessLike) MultiRuleFlowTestSeed {
	t.Helper()

	ctx := h.ServerCtx()
	configParts := createMultiRuleFlowTestConfig(t, ctx, h.GetConnection(), h.GetSeed())

	return MultiRuleFlowTestSeed{
		FlowTestSeed: FlowTestSeed{
			TenantID:          configParts.tenantID,
			ContextID:         configParts.contextID,
			LedgerSourceID:    configParts.ledgerSourceID,
			NonLedgerSourceID: configParts.bankSourceID,
			RuleID:            configParts.exactRuleID,
		},
		ExactRuleID:     configParts.exactRuleID,
		ToleranceRuleID: configParts.toleranceRuleID,
	}
}

// WaitForIngestionCompletedGeneric waits for an ingestion.completed event.
func WaitForIngestionCompletedGeneric(
	t *testing.T,
	h TestHarnessLike,
	timeout time.Duration,
) []byte {
	t.Helper()

	body, err := h.WaitForEventWithTimeout(timeout, func(routingKey string, _ []byte) bool {
		return routingKey == server.RoutingKeyIngestionCompleted
	})
	require.NoError(t, err)
	return body
}

// WaitForMatchConfirmedGeneric waits for a match.confirmed event.
func WaitForMatchConfirmedGeneric(t *testing.T, h TestHarnessLike, timeout time.Duration) []byte {
	t.Helper()

	body, err := h.WaitForEventWithTimeout(timeout, func(routingKey string, _ []byte) bool {
		return routingKey == server.RoutingKeyMatchConfirmed
	})
	require.NoError(t, err)
	return body
}

// WaitForMultipleMatchEventsGeneric waits for the specified number of match_confirmed events.
func WaitForMultipleMatchEventsGeneric(
	t *testing.T,
	h TestHarnessLike,
	count int,
	timeout time.Duration,
) []MatchConfirmedEvent {
	t.Helper()

	if count <= 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	events := make([]MatchConfirmedEvent, 0, count)

	for i := 0; i < count; i++ {
		body, err := h.WaitForEvent(ctx, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.NoError(t, err, "timed out waiting for match_confirmed event %d/%d", i+1, count)

		event := ParseMatchConfirmedEvent(t, body)
		events = append(events, event)
		t.Logf("Received match_confirmed event %d/%d: MatchID=%s", i+1, count, event.MatchID)
	}

	return events
}

// GetJobStatusGeneric fetches the current job status via HTTP.
func GetJobStatusGeneric(t *testing.T, h TestHarnessLike, contextID, jobID uuid.UUID) JobResponse {
	t.Helper()

	resp, body, err := h.DoJSON("GET", JobStatusPath(contextID, jobID), nil)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, "get job status failed: %s", string(body))

	return ParseJobResponse(t, body)
}

// PollJobUntilTerminalGeneric polls job status until it reaches a terminal state or timeout.
func PollJobUntilTerminalGeneric(
	t *testing.T,
	h TestHarnessLike,
	contextID, jobID uuid.UUID,
	timeout time.Duration,
) (JobResponse, []string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var observedStatuses []string
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf(
				"timed out waiting for job %s to reach terminal state. Observed statuses: %v",
				jobID,
				observedStatuses,
			)
		case <-ticker.C:
			job := GetJobStatusGeneric(t, h, contextID, jobID)

			if len(observedStatuses) == 0 ||
				observedStatuses[len(observedStatuses)-1] != job.Status {
				observedStatuses = append(observedStatuses, job.Status)
			}

			if IsTerminalStatus(job.Status) {
				return job, observedStatuses
			}
		}
	}
}

// CountPendingOutboxGeneric returns the count of pending outbox events.
func CountPendingOutboxGeneric(t *testing.T, ctx context.Context, h TestHarnessLike) int {
	t.Helper()

	resolver, err := h.GetConnection().Resolver(ctx)
	require.NoError(t, err, "failed to get db resolver for outbox count")

	primaryDBs := resolver.PrimaryDBs()
	require.NotEmpty(t, primaryDBs, "no primary databases available")

	var count int
	row := primaryDBs[0].QueryRowContext(
		ctx,
		"SELECT count(*) FROM outbox_events WHERE status = 'pending'",
	)
	require.NoError(t, row.Scan(&count))
	return count
}

// EnsureContextGeneric verifies the context exists and is accessible.
func EnsureContextGeneric(t *testing.T, h TestHarnessLike, contextID uuid.UUID) {
	t.Helper()

	ctx := h.ServerCtx()
	conn := h.GetConnection()
	provider := infraTestutil.NewSingleTenantInfrastructureProvider(conn, nil)
	repo := configContextRepo.NewRepository(provider)
	found, err := repo.FindByID(ctx, contextID)
	require.NoError(t, err)
	require.NotNil(t, found)
}
