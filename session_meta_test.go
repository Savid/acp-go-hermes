package hermesacp

import (
	"errors"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestLifecycleMetaStrictAllowlist(t *testing.T) {
	tests := []struct {
		name string
		meta map[string]any
		err  bool
	}{
		{name: "foreign ignored", meta: map[string]any{"codex": map[string]any{"deleted": true}}},
		{name: "trace ignored", meta: map[string]any{"traceparent": "00-abc"}},
		{name: "own unknown rejected", meta: map[string]any{hermesMetaKey: map[string]any{"goals": []any{}}}, err: true},
		{name: "own option unknown rejected", meta: map[string]any{hermesMetaKey: map[string]any{"options": map[string]any{"foo": "bar"}}}, err: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sessionMetaFromLifecycle(tt.meta)
			if tt.err && err == nil {
				t.Fatal("expected error")
			}
			if !tt.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestOutputSchemaUnsupported(t *testing.T) {
	_, err := sessionMetaFromLifecycle(HermesOptions{OutputSchema: map[string]any{"type": "object"}}.Meta())
	if err == nil {
		t.Fatal("outputSchema unexpectedly accepted")
	}
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error type = %T", err)
	}
	if reqErr.Data == nil {
		t.Fatalf("missing error data: %#v", reqErr)
	}
}
