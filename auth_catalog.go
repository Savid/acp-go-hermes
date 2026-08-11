package hermesacp

import (
	"context"
	"encoding/json"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	nativehermes "github.com/savid/acp-go-hermes/internal/hermes"
	"golang.org/x/text/unicode/norm"
)

// Method entry types on the wire.
const (
	authMethodTypeOAuth = "oauth"
)

// Display-field bounds. A value violating its bound is dropped, never
// truncated.
const (
	authMaxURLBytes      = 2048
	authMaxMessageBytes  = 2048
	authMaxUserCodeBytes = 64
	authMaxLabelBytes    = 256
)

const authMaxCallbackBytes = 4096

// authUserCodePattern is anchored: a substring match accepts a code with markup
// wrapped around it.
var authUserCodePattern = regexp.MustCompile(`\A[A-Za-z0-9-]+\z`)

// authCatalogMethod is one entry of the current catalog, paired with the native
// flow the adapter needs to drive it.
type authCatalogMethod struct {
	ID    string
	Type  string
	Label string
	// Flow is the native flow discriminator. The start shapes differ per flow,
	// so it is carried rather than re-derived.
	Flow string
}

type authMethodEntry struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Label string `json:"label"`
}

type authMethodsResult struct {
	Providers  map[string][]authMethodEntry `json:"providers"`
	Generation string                       `json:"generation"`
}

// methods enumerates the catalog and mints the generation that names this exact
// result. The catalog is what the adapter enumerates: hermes publishes no
// parity contract, so no completeness claim crosses and free entry of an
// unlisted provider is never offered.
func (p *providerAuth) methods(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, err
	}

	client := session.authNativeClient()
	if client == nil {
		return nil, authFailed(authCauseTransport, "", "", "")
	}

	if !nativeProviderAuthHomeSupported(client) {
		generation, tokenErr := newAuthToken()
		if tokenErr != nil {
			return nil, authFailed(authCauseProcess, "", "", "")
		}

		p.mu.Lock()
		p.generation = generation
		p.catalog = map[string][]authCatalogMethod{}
		p.mu.Unlock()

		return authMethodsResult{Providers: map[string][]authMethodEntry{}, Generation: generation}, nil
	}

	oauth, err := client.AuthProviders(ctx)
	if err != nil {
		return nil, authFailed(authNativeCause(err), "", "", "")
	}

	methods, entries := buildAuthCatalog(oauth)

	generation, err := newAuthToken()
	if err != nil {
		return nil, authFailed(authCauseProcess, "", "", "")
	}

	p.mu.Lock()
	p.generation = generation
	p.catalog = methods
	p.mu.Unlock()

	return authMethodsResult{Providers: entries, Generation: generation}, nil
}

// buildAuthCatalog publishes only native OAuth methods. External methods read
// another harness's credential and API-key methods persist into the isolated
// session home, so neither is a durable provider-auth method.
func buildAuthCatalog(oauth []nativehermes.AuthProvider) (map[string][]authCatalogMethod, map[string][]authMethodEntry) {
	methods := make(map[string][]authCatalogMethod, len(oauth))
	entries := make(map[string][]authMethodEntry, len(oauth))

	ids := make([]string, 0, len(oauth))
	byID := make(map[string][]authCatalogMethod, len(oauth))

	appendMethod := func(providerID string, method authCatalogMethod) {
		if _, seen := byID[providerID]; !seen {
			ids = append(ids, providerID)
		}

		byID[providerID] = append(byID[providerID], method)
	}

	for _, provider := range oauth {
		if provider.ID == "" {
			continue
		}

		if provider.Flow != nativehermes.AuthFlowDeviceCode && provider.Flow != nativehermes.AuthFlowPKCE {
			continue
		}

		label, ok := authDisplayText(provider.Name, authMaxLabelBytes)
		if !ok {
			continue
		}

		appendMethod(provider.ID, authCatalogMethod{
			ID:    provider.Flow,
			Type:  authMethodTypeOAuth,
			Label: label,
			Flow:  provider.Flow,
		})
	}

	sort.Strings(ids)

	for _, providerID := range ids {
		resolved := byID[providerID]
		published := make([]authMethodEntry, 0, len(resolved))

		for _, method := range resolved {
			published = append(published, authMethodEntry{ID: method.ID, Type: method.Type, Label: method.Label})
		}

		methods[providerID] = resolved
		entries[providerID] = published
	}

	return methods, entries
}

// authDisplayText normalises a native presentation string to NFC and measures
// its bounds and categories on that normalised form, which is also the form the
// adapter relays, persists, and returns. Normalising after measuring bounds a
// string nobody sends.
func authDisplayText(value string, maxBytes int) (string, bool) {
	normalized := norm.NFC.String(value)
	if normalized == "" || len(normalized) > maxBytes || !utf8.ValidString(normalized) {
		return "", false
	}

	for _, r := range normalized {
		if !authDisplayRune(r) {
			return "", false
		}
	}

	return normalized, true
}

// authDisplayRune restricts free text to Unicode categories L, N, P, S, and Zs.
// Every C* category is rejected, which is also what excludes every
// bidirectional override and embedding character: a label is the provider name
// in the one place a human decides which account to bind.
func authDisplayRune(r rune) bool {
	switch {
	case unicode.IsLetter(r), unicode.IsNumber(r), unicode.IsPunct(r), unicode.IsSymbol(r):
		return true
	case unicode.Is(unicode.Zs, r):
		return true
	default:
		return false
	}
}

// authDisplayURL applies the url bound: at most 2048 bytes, scheme exactly
// https, no userinfo, no fragment.
func authDisplayURL(value string) (string, bool) {
	normalized := norm.NFC.String(value)
	if normalized == "" || len(normalized) > authMaxURLBytes {
		return "", false
	}

	parsed, err := url.Parse(normalized)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Fragment != "" || parsed.Host == "" {
		return "", false
	}

	return normalized, true
}

// authDisplayUserCode applies the userCode bound with an anchored pattern.
func authDisplayUserCode(value string) (string, bool) {
	normalized := norm.NFC.String(value)
	if normalized == "" || len(normalized) > authMaxUserCodeBytes || !authUserCodePattern.MatchString(normalized) {
		return "", false
	}

	return normalized, true
}

// authLoopbackHost reports whether a minted authorization URL redirects through
// a loopback listener. Such a method is not brokered: the adapter cannot relay
// a URL whose completion lands on a socket the owner's browser cannot reach.
func authLoopbackHost(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	if isLoopbackHostname(parsed.Hostname()) {
		return true
	}

	redirect := parsed.Query().Get("redirect_uri")
	if redirect == "" {
		return false
	}

	target, err := url.Parse(redirect)
	if err != nil {
		return false
	}

	return isLoopbackHostname(target.Hostname())
}

func isLoopbackHostname(host string) bool {
	switch strings.ToLower(host) {
	case "127.0.0.1", "::1", "localhost":
		return true
	default:
		return strings.HasSuffix(strings.ToLower(host), ".localhost")
	}
}

// validateAuthInputs enforces that an authorize request answers exactly the
// prompts the catalog published. Hermes publishes none — its native start takes
// no operator input on either flow — so any supplied key is a caller addressing
// failure, and no prompt answer ever reaches a native call or a URL.
func validateAuthInputs(inputs map[string]string) error {
	if len(inputs) > 0 {
		return invalidAuthField(authFieldInputs)
	}

	return nil
}
