//go:build unit

package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	authMiddleware "github.com/LerianStudio/lib-auth/v3/auth/middleware"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildProtectedAuthChain_NilExtractor(t *testing.T) {
	t.Parallel()

	chain, err := BuildProtectedAuthChain(nil, nil, "resource", []string{"read"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNilTenantExtractor)
	assert.Nil(t, chain)
}

func TestBuildProtectedAuthChain_NoActions(t *testing.T) {
	t.Parallel()

	extractor, err := NewTenantExtractor(
		false, false, DefaultTenantID, DefaultTenantSlug, "", "development",
	)
	require.NoError(t, err)

	chain, err := BuildProtectedAuthChain(nil, extractor, "resource", []string{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoActions)
	assert.Nil(t, chain)

	chain, err = BuildProtectedAuthChain(nil, extractor, "resource", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoActions)
	assert.Nil(t, chain)
}

func TestBuildProtectedAuthChain_BlankAction(t *testing.T) {
	t.Parallel()

	extractor, err := NewTenantExtractor(
		false, false, DefaultTenantID, DefaultTenantSlug, "", "development",
	)
	require.NoError(t, err)

	// First position blank.
	chain, err := BuildProtectedAuthChain(nil, extractor, "resource", []string{"  "})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptyAction)
	assert.Nil(t, chain)

	// Middle position blank.
	chain, err = BuildProtectedAuthChain(nil, extractor, "resource", []string{"read", "", "write"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptyAction)
	assert.Nil(t, chain)

	// Tab/space-only action.
	chain, err = BuildProtectedAuthChain(nil, extractor, "resource", []string{"\t\n  "})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptyAction)
	assert.Nil(t, chain)
}

// TestBuildProtectedAuthChain_AuthDisabled_OmitsValidateTenantClaims verifies
// the contract in BuildProtectedAuthChain's doc comment: when auth is
// disabled, validateTenantClaims MUST NOT appear in the returned chain.
// Concretely, the chain length equals len(actions)+1 (one Authorize per
// action + ExtractTenant) instead of len(actions)+2.
func TestBuildProtectedAuthChain_AuthDisabled_OmitsValidateTenantClaims(t *testing.T) {
	t.Parallel()

	extractor, err := NewTenantExtractor(
		false, // authEnabled=false
		false,
		DefaultTenantID,
		DefaultTenantSlug,
		"",
		"development",
	)
	require.NoError(t, err)

	// Non-nil authClient must not flip the decision — the gating is
	// extractor.authEnabled, not the presence of the client.
	authClient := authMiddleware.NewAuthClient("http://authz.local", false, nil)

	chain, err := BuildProtectedAuthChain(authClient, extractor, "resource", []string{"read", "write"})
	require.NoError(t, err)
	require.NotNil(t, chain)

	// len(actions)=2, +1 for ExtractTenant = 3. validateTenantClaims is omitted.
	assert.Len(t, chain, 3, "auth disabled: chain must omit validateTenantClaims")
}

// TestBuildProtectedAuthChain_AuthEnabled_IncludesValidateTenantClaims verifies
// the symmetric contract: when auth is enabled AND an auth client is present,
// validateTenantClaims is the FIRST handler so a forged/expired/missing JWT
// is rejected BEFORE any Authorize call reaches lib-auth's backend.
func TestBuildProtectedAuthChain_AuthEnabled_IncludesValidateTenantClaims(t *testing.T) {
	t.Parallel()

	extractor, err := NewTenantExtractor(
		true, // authEnabled=true
		true,
		DefaultTenantID,
		DefaultTenantSlug,
		testTokenSecret,
		"development",
	)
	require.NoError(t, err)

	authClient := authMiddleware.NewAuthClient("http://authz.local", true, nil)

	chain, err := BuildProtectedAuthChain(authClient, extractor, "resource", []string{"read", "write"})
	require.NoError(t, err)
	require.NotNil(t, chain)

	// len(actions)=2, +1 validateTenantClaims, +1 ExtractTenant = 4.
	assert.Len(t, chain, 4, "auth enabled: chain must include validateTenantClaims")
}

// TestBuildProtectedAuthChain_AuthEnabled_NilAuthClient_Rejected pins the
// fail-fast guard: when extractor.authEnabled=true, a nil authClient is a
// protected-route misconfiguration and must be rejected at chain-build time.
// Without this guard, Authorize(nil) would install a hard-coded 500 handler
// and every protected request would fail at runtime instead of at startup.
func TestBuildProtectedAuthChain_AuthEnabled_NilAuthClient_Rejected(t *testing.T) {
	t.Parallel()

	extractor, err := NewTenantExtractor(
		true, // authEnabled=true
		true,
		DefaultTenantID,
		DefaultTenantSlug,
		testTokenSecret,
		"development",
	)
	require.NoError(t, err)

	chain, err := BuildProtectedAuthChain(nil, extractor, "resource", []string{"read"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNilAuthClient)
	assert.Nil(t, chain)
}

// TestBuildProtectedAuthChain_Order is the behavioural equivalent of the
// chain-length assertions above: it wires the chain onto a fiber app, sends a
// request, and records the order in which each middleware fires. The test
// passes when the observed sequence matches the contract documented on
// BuildProtectedAuthChain: Authorize(action) handlers run first (one per
// action), then ExtractTenant, then the terminal handler.
func TestBuildProtectedAuthChain_Order(t *testing.T) {
	t.Parallel()

	// Auth disabled on the extractor so we do not need a live authz
	// backend and tenant defaults are populated unconditionally. A non-nil
	// auth-disabled lib-auth client is still required by the fail-fast
	// guard's sibling contract (Authorize(nil) returns a 500 handler that
	// would abort the chain before ExtractTenant runs, making ordering
	// unobservable). With Enabled=false, authClient.Authorize is a pure
	// c.Next() pass-through.
	extractor, err := NewTenantExtractor(
		false, false, DefaultTenantID, DefaultTenantSlug, "", "development",
	)
	require.NoError(t, err)

	authClient := authMiddleware.NewAuthClient("", false, nil)

	chain, err := BuildProtectedAuthChain(authClient, extractor, "resource", []string{"read", "write"})
	require.NoError(t, err)
	require.Len(t, chain, 3, "auth disabled: [Authorize(read), Authorize(write), ExtractTenant]")

	// Wrap each returned handler in a recorder that appends its chain
	// position to observed BEFORE delegating. This proves the fiber
	// runtime invokes them in the order BuildProtectedAuthChain returned
	// them, which is the ordering contract under test.
	var observed []int

	wrapped := make([]fiber.Handler, 0, len(chain)+1)
	for idx, h := range chain {
		pos := idx
		inner := h

		wrapped = append(wrapped, func(c *fiber.Ctx) error {
			observed = append(observed, pos)
			return inner(c)
		})
	}

	// Terminal handler records its position and asserts the observable
	// side effect of ExtractTenant (tenant id populated from defaults).
	terminalPos := len(chain)
	wrapped = append(wrapped, func(c *fiber.Ctx) error {
		observed = append(observed, terminalPos)

		assert.Equal(t, DefaultTenantID, GetTenantID(c.UserContext()),
			"ExtractTenant must run before the terminal handler so tenant id is populated")

		return c.SendStatus(http.StatusOK)
	})

	app := fiber.New()
	app.Get("/t", wrapped...)

	req := httptest.NewRequest(http.MethodGet, "/t", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"chain must reach the terminal handler when auth is disabled end-to-end")

	// Positions 0..1 are Authorize(read), Authorize(write); position 2 is
	// ExtractTenant; position 3 is the terminal handler. Any reordering
	// would change this sequence.
	assert.Equal(t, []int{0, 1, 2, 3}, observed,
		"middleware must fire in the order returned by BuildProtectedAuthChain")
}
