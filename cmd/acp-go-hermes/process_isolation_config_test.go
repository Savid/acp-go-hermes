package main

import (
	"strings"
	"testing"
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
			UID:             20001,
			GID:             20001,
			BaseEnvironment: map[string]string{"PATH": "/usr/bin", "HOME": "/var/empty/acp", "USER": "acp", "LOGNAME": "acp"},
		}, nil
	}
	t.Cleanup(func() { processIsolationConfigLoader = original })
}

func isolatedArgs(args ...string) []string {
	return append([]string{"-" + processIsolationConfigFlag, testProcessIsolationConfigPath}, args...)
}

func TestDecodeProcessIsolationConfigStrict(t *testing.T) {
	config, err := decodeProcessIsolationConfig([]byte(`{"uid":20001,"gid":20002,"baseEnvironment":{"PATH":"/usr/bin"},"inheritEnvironment":["AMP_API_KEY"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if config.UID != 20001 || config.GID != 20002 || config.BaseEnvironment["PATH"] != "/usr/bin" || len(config.InheritEnvironment) != 1 {
		t.Fatalf("decoded config = %#v", config)
	}

	for _, document := range []string{
		`{"uid":1,"gid":2,"baseEnvironment":{},"unknown":true}`,
		`{"uid":1,"gid":2,"baseEnvironment":{}} {}`,
		`{"uid":1,"uid":2,"gid":2,"baseEnvironment":{}}`,
		`{"uid":1,"gid":2,"baseEnvironment":{"PATH":"/bin","PATH":"/usr/bin"}}`,
		``,
	} {
		if _, err := decodeProcessIsolationConfig([]byte(document)); err == nil {
			t.Fatalf("decode unexpectedly accepted %q", document)
		}
	}
}

func TestRunRequiresProcessIsolationConfig(t *testing.T) {
	var stderr strings.Builder
	if code := run(t.Context(), nil, strings.NewReader(""), &strings.Builder{}, &stderr); code != 2 {
		t.Fatalf("run code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "-"+processIsolationConfigFlag+" is required") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
