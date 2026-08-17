package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	hermesacp "github.com/savid/acp-go-hermes"
)

const testProcessIsolationConfigPath = "/test/process-isolation.json"

func stubProcessIsolationConfig(t *testing.T) {
	t.Helper()

	original := processIsolationConfigLoader
	processIsolationConfigLoader = func(path string) (processIsolationConfig, error) {
		if path != testProcessIsolationConfigPath {
			t.Fatalf("process isolation config path = %q", path)
		}

		return processIsolationConfig{
			UID:                 20001,
			GID:                 20001,
			BaseEnvironment:     map[string]string{"PATH": "/usr/bin", "HOME": "/var/empty/acp", "USER": "acp", "LOGNAME": "acp"},
			StandaloneOwnerID:   "test-owner",
			StandaloneStateRoot: "/var/empty/acp",
		}, nil
	}
	t.Cleanup(func() { processIsolationConfigLoader = original })
}

func isolatedArgs(args ...string) []string {
	return append([]string{"-" + processIsolationConfigFlag, testProcessIsolationConfigPath}, args...)
}

func TestDecodeProcessIsolationConfigStrict(t *testing.T) {
	config, err := decodeProcessIsolationConfig([]byte(`{"uid":20001,"gid":20002,"baseEnvironment":{"PATH":"/usr/bin"},"inheritEnvironment":["AMP_API_KEY"],"standaloneOwnerId":"deployment-a","standaloneStateRoot":"/var/lib/acp"}`))
	if err != nil {
		t.Fatal(err)
	}
	if config.UID != 20001 || config.GID != 20002 || config.BaseEnvironment["PATH"] != "/usr/bin" || len(config.InheritEnvironment) != 1 ||
		config.StandaloneOwnerID != "deployment-a" || config.StandaloneStateRoot != "/var/lib/acp" {
		t.Fatalf("decoded config = %#v", config)
	}

	for _, document := range []string{
		`{"uid":1,"gid":2,"baseEnvironment":{},"unknown":true}`,
		`{"uid":1,"gid":2,"baseEnvironment":{}} {}`,
		`{"uid":1,"uid":2,"gid":2,"baseEnvironment":{}}`,
		`{"uid":1,"gid":2,"baseEnvironment":{},"standaloneOwnerId":"a","standaloneOwnerId":"b"}`,
		`{"uid":1,"gid":2,"baseEnvironment":{},"standaloneStateRoot":"/a","standaloneStateRoot":"/b"}`,
		`{"uid":1,"gid":2,"baseEnvironment":{"PATH":"/bin","PATH":"/usr/bin"}}`,
		`{"uid":1,"gid":2,"baseEnvironment":{}} trailing`,
		`{"uid":1 "gid":2}`,
		`[1,`,
		``,
	} {
		if _, err := decodeProcessIsolationConfig([]byte(document)); err == nil {
			t.Fatalf("decode unexpectedly accepted %q", document)
		}
	}
	if _, err := decodeProcessIsolationConfig([]byte{0xff}); err == nil {
		t.Fatal("invalid UTF-8 was accepted")
	}
	if err := scanJSONValue(json.NewDecoder(bytes.NewBufferString(`]`))); err == nil {
		t.Fatal("unexpected closing delimiter was accepted")
	}
	if err := scanJSONDelimitedValue(json.NewDecoder(bytes.NewBufferString(`null`)), json.Delim(']')); err == nil {
		t.Fatal("unexpected delimiter was accepted")
	}
}

// TestRunWithoutProcessIsolationConfigUsesOrdinaryMode proves an omitted flag
// is ordinary standalone mode rather than a configuration error: the policy
// loader is never called, no WithProcessIsolation option is appended, and the
// command reaches Serve.
func TestRunWithoutProcessIsolationConfigUsesOrdinaryMode(t *testing.T) {
	originalLoader, originalServe := processIsolationConfigLoader, serve
	t.Cleanup(func() { processIsolationConfigLoader, serve = originalLoader, originalServe })

	loaded := 0
	processIsolationConfigLoader = func(string) (processIsolationConfig, error) {
		loaded++

		return processIsolationConfig{}, errors.New("loader must not run for an omitted flag")
	}

	served := 0

	var options []hermesacp.Option

	serve = func(_ context.Context, _ io.Reader, _ io.Writer, opts ...hermesacp.Option) error {
		served++
		options = append(options, opts...)

		return nil
	}

	var stderr strings.Builder
	if code := run(t.Context(), nil, strings.NewReader(""), &strings.Builder{}, &stderr); code != 0 {
		t.Fatalf("run code = %d, stderr = %q", code, stderr.String())
	}

	if loaded != 0 {
		t.Fatalf("policy loader ran %d times for an omitted flag", loaded)
	}

	if served != 1 {
		t.Fatalf("serve calls = %d", served)
	}

	applied := hermesacp.Options{}
	for _, option := range options {
		option(&applied)
	}

	if applied.ProcessIsolation != nil {
		t.Fatalf("omitted flag appended a policy %+v", applied.ProcessIsolation)
	}
}

// TestRunWithExplicitProcessIsolationConfigIsFailClosed proves a supplied path
// calls the platform loader and that a loader failure exits before serving,
// with no retry that drops the option.
func TestRunWithExplicitProcessIsolationConfigIsFailClosed(t *testing.T) {
	originalLoader, originalServe := processIsolationConfigLoader, serve
	t.Cleanup(func() { processIsolationConfigLoader, serve = originalLoader, originalServe })

	served := 0
	serve = func(context.Context, io.Reader, io.Writer, ...hermesacp.Option) error {
		served++

		return nil
	}

	loaded := 0
	processIsolationConfigLoader = func(path string) (processIsolationConfig, error) {
		loaded++

		if path != testProcessIsolationConfigPath {
			t.Fatalf("process isolation config path = %q", path)
		}

		return processIsolationConfig{}, errors.New("policy is unreadable")
	}

	var stderr strings.Builder
	if code := run(t.Context(), isolatedArgs(), strings.NewReader(""), &strings.Builder{}, &stderr); code != 1 {
		t.Fatalf("run code = %d, stderr = %q", code, stderr.String())
	}

	if loaded != 1 || served != 0 {
		t.Fatalf("loader calls = %d, serve calls = %d", loaded, served)
	}

	if !strings.Contains(stderr.String(), "policy is unreadable") {
		t.Fatalf("stderr = %q", stderr.String())
	}

	// A supplied path that loads successfully appends the policy verbatim.
	stubProcessIsolationConfig(t)

	var options []hermesacp.Option

	serve = func(_ context.Context, _ io.Reader, _ io.Writer, opts ...hermesacp.Option) error {
		served++
		options = append(options, opts...)

		return nil
	}

	if code := run(t.Context(), isolatedArgs(), strings.NewReader(""), &strings.Builder{}, &stderr); code != 0 {
		t.Fatalf("run code = %d, stderr = %q", code, stderr.String())
	}

	applied := hermesacp.Options{}
	for _, option := range options {
		option(&applied)
	}

	if applied.ProcessIsolation == nil || applied.ProcessIsolation.UID != 20001 ||
		applied.ProcessIsolation.StandaloneOwnerID != "test-owner" {
		t.Fatalf("supplied policy = %+v", applied.ProcessIsolation)
	}
}
