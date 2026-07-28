package hermesacp

// authRefreshSurvivesUse names the providers whose refresh token stays valid
// after it has been presented for a refresh. Copying such a token out of the
// store the harness refreshes in place costs nothing, because using it does not
// replace it.
//
// A provider that replaces its refresh token on refresh and revokes the
// previous one leaves every copy one refresh away from rejection, so it is
// absent here and its material never crosses this adapter's boundary. The set
// is empty: no provider this harness enumerates has been shown to leave its
// refresh token valid after use.
var authRefreshSurvivesUse = map[string]struct{}{}

// authCacheable reports whether material may cross the credential and injection
// legs. Material with no refresh token holds nothing a refresh can invalidate
// and crosses whatever its provider does; material with one crosses only for a
// provider whose refresh token survives its own use.
func authCacheable(providerID string, refreshToken string) bool {
	if refreshToken == "" {
		return true
	}

	_, ok := authRefreshSurvivesUse[providerID]

	return ok
}
