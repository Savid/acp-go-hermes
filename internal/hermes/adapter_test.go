package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"testing"
)

func TestPermissionRequestRoute(t *testing.T) {
	if got := (PermissionRequest{ReplyRoute: PermissionRouteAPI}).Route(); got != PermissionRouteAPI {
		t.Fatalf("explicit ReplyRoute = %q, want api", got)
	}
	if got := (PermissionRequest{Action: "read"}).Route(); got != PermissionRouteAPI {
		t.Fatalf("action route = %q, want api", got)
	}
	if got := (PermissionRequest{}).Route(); got != PermissionRouteSession {
		t.Fatalf("default route = %q, want session", got)
	}
}

func TestQuestionRequestRoute(t *testing.T) {
	if got := (QuestionRequest{ReplyRoute: QuestionRouteAPI}).Route(); got != QuestionRouteAPI {
		t.Fatalf("explicit ReplyRoute = %q, want api", got)
	}
	if got := (QuestionRequest{}).Route(); got != QuestionRouteSession {
		t.Fatalf("default route = %q, want session", got)
	}
}

func TestMissingLiveSessionMappingErrorMessage(t *testing.T) {
	err := MissingLiveSessionMappingError{StoredSessionID: "stored-1"}
	if err.Error() == "" {
		t.Fatal("expected a non-empty error message")
	}
}

func TestTurnFailureErrorAccessors(t *testing.T) {
	provider := NewProviderTurnFailure("model overloaded", 503, "overloaded")
	if provider.Cause() != CauseProvider || provider.Message() != "model overloaded" {
		t.Fatalf("provider failure = %+v", provider)
	}
	if provider.StatusCode() != 503 || provider.ProviderCode() != "overloaded" {
		t.Fatalf("provider codes = %d/%q", provider.StatusCode(), provider.ProviderCode())
	}
	if provider.Error() != "model overloaded" {
		t.Fatalf("Error with message = %q", provider.Error())
	}

	bare := NewTurnFailure(CauseTimeout, "")
	if bare.Error() != "timeout turn failure" {
		t.Fatalf("Error fallback = %q", bare.Error())
	}
	if bare.StatusCode() != 0 || bare.ProviderCode() != "" {
		t.Fatalf("bare codes = %d/%q", bare.StatusCode(), bare.ProviderCode())
	}
}

func TestStreamErrorHelpers(t *testing.T) {
	se := NewStreamError(9, errors.New("boom"))
	if se.Error() != "boom" || errors.Unwrap(se) == nil {
		t.Fatalf("stream error = %v", se)
	}
	if got := StreamErrorEpoch(se); got != 9 {
		t.Fatalf("epoch = %d, want 9", got)
	}
	if got := StreamErrorEpoch(errors.New("plain")); got != 0 {
		t.Fatalf("non-stream epoch = %d, want 0", got)
	}
}

func TestIntFromNumberBranches(t *testing.T) {
	for _, tc := range []struct {
		value any
		want  int
		ok    bool
	}{
		{float64(12), 12, true},
		{float64(0), 0, false},
		{math.MaxFloat64, 0, false},
		{int(7), 7, true},
		{int(-1), 0, false},
		{json.Number("21"), 21, true},
		{json.Number("nope"), 0, false},
		{json.Number("-3"), 0, false},
		{"string", 0, false},
	} {
		got, ok := IntFromNumber(tc.value)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("IntFromNumber(%#v) = %d,%v want %d,%v", tc.value, got, ok, tc.want, tc.ok)
		}
	}
}

func TestSplitModelValueBranches(t *testing.T) {
	if p, m := splitModelValue("", "fp", "fm"); p != "fp" || m != "fm" {
		t.Fatalf("empty = %q/%q", p, m)
	}
	if p, m := splitModelValue("openai/gpt", "", ""); p != "openai" || m != "gpt" {
		t.Fatalf("qualified = %q/%q", p, m)
	}
	if p, m := splitModelValue("bare", "fp", "fm"); p != "fp" || m != "bare" {
		t.Fatalf("unqualified = %q/%q", p, m)
	}
}

func TestNativeUnmarshalJSONErrorBranches(t *testing.T) {
	var part Part
	if err := part.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("Part.UnmarshalJSON accepted invalid JSON")
	}
	if err := part.UnmarshalJSON([]byte(`{"text":"hi"}`)); err != nil {
		t.Fatalf("Part.UnmarshalJSON valid: %v", err)
	}

	var event TurnEvent
	if err := event.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("TurnEvent.UnmarshalJSON accepted invalid JSON")
	}
	if err := event.UnmarshalJSON([]byte(`{"type":"x"}`)); err != nil {
		t.Fatalf("TurnEvent.UnmarshalJSON valid: %v", err)
	}

	var providers ProvidersResponse
	if err := providers.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatal("ProvidersResponse.UnmarshalJSON accepted invalid JSON")
	}
	if err := providers.UnmarshalJSON([]byte(`{"providers":[]}`)); err != nil {
		t.Fatalf("ProvidersResponse.UnmarshalJSON valid: %v", err)
	}
}

func TestStartServerDefaultsLoggerOnFailure(t *testing.T) {
	ctx := context.Background()
	// No Logger provided: exercises the slog.Default() fallback before the
	// missing executable makes startup fail.
	if _, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{
		Root:           t.TempDir(),
		ExecutablePath: filepath.Join(t.TempDir(), "missing-hermes"),
	})); err == nil {
		t.Fatal("StartServer with missing executable unexpectedly succeeded")
	}

	// An escaping seed-file path makes materializeHermesConfig reject the
	// startup before the process launches.
	if _, err := StartServer(ctx, darwinTestStartOptions(t, StartOptions{
		Root:      t.TempDir(),
		SeedFiles: map[string]string{"../escape": "data"},
	})); err == nil {
		t.Fatal("StartServer with escaping seed file unexpectedly succeeded")
	}
}
