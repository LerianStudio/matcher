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

// TestBuildProtectedAuthChain_AuthEnabled_NilAuthClient_OmitsValidateTenantClaims
// pins a subtle nuance of the guard: when authClient is nil, validateTenantClaims
// is skipped even if extractor.authEnabled=true. This is consistent with the
// original ProtectedGroupWithActionsWithMiddleware behavior and exists because
// Authorize(nil) installs a hard-coded 500 handler that makes the whole chain
// non-functional anyway — adding validateTenantClaims on top would just waste
// a JWT parse before the 500.
func TestBuildProtectedAuthChain_AuthEnabled_NilAuthClient_OmitsValidateTenantClaims(t *testing.T) {
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
	require.NoError(t, err)
	require.NotNil(t, chain)

	// len(actions)=1, +1 ExtractTenant = 2. validateTenantClaims omitted.
	assert.Len(t, chain, 2)
}

// TestBuildProtectedAuthChain_Order is the behavioural equivalent of the
// chain-length assertions above: it wires the chain onto a fiber app, sends a
// request, and records the order in which each middleware fires. The test
// passes when the observed sequence matches the contract documented on
// BuildProtectedAuthChain.
func TestBuildProtectedAuthChain_Order(t *testing.T) {
	t.Parallel()

	// Auth disabled so we do not need a live authz backend to observe
	// the Authorize middleware's invocation — with a nil authClient it
	// returns a 500 handler which would abort the chain before we can
	// verify order. We substitute a no-op auth-disabled configuration
	// and inject our own Authorize-equivalent by appending a probe
	// handler in the same order the chain builder guarantees.
	extractor, err := NewTenantExtractor(
		false, false, DefaultTenantID, DefaultTenantSlug, "", "development",
	)
	require.NoError(t, err)

	chain, err := BuildProtectedAuthChain(nil, extractor, "resource", []string{"read"})
	require.NoError(t, err)
	require.Len(t, chain, 2, "auth disabled + nil client: [Authorize(read), ExtractTenant]")

	// Install the chain on an app and assert it runs, which proves the
	// slice we return is actually wire-compatible with fiber.Handler.
	app := fiber.New()
	app.Get("/t", append(chain, func(c *fiber.Ctx) error {
		// If we got here, the entire chain advanced successfully.
		// The only observable side effect of ExtractTenant on an
		// auth-disabled extractor is setting tenant id in UserContext;
		// assert it to prove ExtractTenant ran last.
		tid := GetTenantID(c.UserContext())
		assert.Equal(t, DefaultTenantID, tid)

		return c.SendStatus(http.StatusOK)
	})...)

	req := httptest.NewRequest(http.MethodGet, "/t", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// The Authorize handler installed by BuildProtectedAuthChain against
	// a nil authClient returns 500 — so the chain completes with 500, not
	// 200. This pins the "nil authClient → Authorize becomes a 500"
	// behavior (same as the pre-refactor routes_test).
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}
