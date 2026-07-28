package hermesacp

import "testing"

// allowRefreshHarvest records one provider as leaving its refresh token valid
// after use for the length of a test, so paths that move material carrying a
// refresh token are reachable while the shipped set is empty.
func allowRefreshHarvest(t *testing.T, providerID string) {
	t.Helper()

	original := authRefreshSurvivesUse
	authRefreshSurvivesUse = map[string]struct{}{providerID: {}}

	t.Cleanup(func() { authRefreshSurvivesUse = original })
}

func TestAuthCacheableGatesOnTheRefreshTokenAndItsProvider(t *testing.T) {
	if len(authRefreshSurvivesUse) != 0 {
		t.Fatalf("authRefreshSurvivesUse = %v, want no provider recorded", authRefreshSurvivesUse)
	}

	if !authCacheable("xai-oauth", "") {
		t.Fatal("material with no refresh token was refused")
	}

	if authCacheable("xai-oauth", "refresh") {
		t.Fatal("a refresh token crossed for a provider that is not recorded")
	}

	allowRefreshHarvest(t, "stable-oauth")

	if !authCacheable("stable-oauth", "refresh") {
		t.Fatal("a refresh token was refused for a provider whose refresh token survives use")
	}

	if authCacheable("xai-oauth", "refresh") {
		t.Fatal("a refresh token crossed for a provider outside the recorded set")
	}
}
