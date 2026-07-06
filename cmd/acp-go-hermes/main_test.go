package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	hermesacp "github.com/savid/acp-go-hermes"
)

func TestRunVersionAndFlagError(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()
	agentVersion = func() string { return "v-test" }

	var stdout bytes.Buffer
	if code := run(context.Background(), []string{"-version"}, strings.NewReader(""), &stdout, io.Discard); code != 0 {
		t.Fatalf("run version code = %d", code)
	}
	if strings.TrimSpace(stdout.String()) != "v-test" {
		t.Fatalf("version stdout = %q", stdout.String())
	}
	if code := run(context.Background(), []string{"-unknown"}, strings.NewReader(""), io.Discard, io.Discard); code != 2 {
		t.Fatalf("flag error code = %d, want 2", code)
	}
}

func TestRunServeSuccessAndError(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()
	agentVersion = func() string { return "v-test" }

	var gotOptions []hermesacp.Option
	serve = func(ctx context.Context, input io.Reader, output io.Writer, opts ...hermesacp.Option) error {
		gotOptions = append([]hermesacp.Option(nil), opts...)
		_, _ = io.Copy(io.Discard, input)
		_, _ = output.Write([]byte{})
		if ctx.Err() != nil {
			return ctx.Err()
		}

		return nil
	}
	if code := run(context.Background(), []string{
		"-path", "hermes",
		"-home", "/tmp/home",
		"-model", "openai/gpt-test",
		"-debug",
	}, strings.NewReader(""), io.Discard, io.Discard); code != 0 {
		t.Fatalf("serve success code = %d", code)
	}
	if len(gotOptions) == 0 {
		t.Fatal("serve received no options")
	}

	serve = func(context.Context, io.Reader, io.Writer, ...hermesacp.Option) error {
		return errors.New("boom")
	}
	var stderr bytes.Buffer
	if code := run(context.Background(), nil, strings.NewReader(""), io.Discard, &stderr); code != 1 {
		t.Fatalf("serve error code = %d", code)
	}
	if !strings.Contains(stderr.String(), "boom") {
		t.Fatalf("stderr = %q", stderr.String())
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	serve = func(context.Context, io.Reader, io.Writer, ...hermesacp.Option) error {
		return context.Canceled
	}
	if code := run(cancelled, nil, strings.NewReader(""), io.Discard, io.Discard); code != 0 {
		t.Fatalf("cancelled serve code = %d", code)
	}

	serve = func(ctx context.Context, _ io.Reader, _ io.Writer, _ ...hermesacp.Option) error {
		proc, err := os.FindProcess(os.Getpid())
		if err != nil {
			return err
		}
		if err := proc.Signal(syscall.SIGTERM); err != nil {
			return err
		}
		<-ctx.Done()

		return ctx.Err()
	}
	if code := run(context.Background(), nil, strings.NewReader(""), io.Discard, io.Discard); code != 143 {
		t.Fatalf("signalled serve code = %d", code)
	}
}

func TestSeedFileFlag(t *testing.T) {
	var flag seedFileFlag
	if err := flag.Set("config.yaml=/host/config.yaml"); err != nil {
		t.Fatalf("Set valid: %v", err)
	}
	for _, value := range []string{"", "noequals", "=missing-rel", "missing-host="} {
		if err := (&seedFileFlag{}).Set(value); err == nil {
			t.Fatalf("Set(%q) accepted invalid value", value)
		}
	}

	dir := t.TempDir()
	hostPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(hostPath, []byte("model: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ok := seedFileFlag{pairs: []seedFilePair{{relative: "config.yaml", hostPath: hostPath}}}
	seeded, err := ok.contents()
	if err != nil {
		t.Fatalf("contents: %v", err)
	}
	if seeded["config.yaml"] != "model: {}\n" {
		t.Fatalf("contents = %#v", seeded)
	}
	missing := seedFileFlag{pairs: []seedFilePair{{relative: "config.yaml", hostPath: filepath.Join(dir, "absent")}}}
	if _, err := missing.contents(); err == nil {
		t.Fatal("contents accepted missing host file")
	}
}

func TestRunSeedFileFlag(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()
	agentVersion = func() string { return "v-test" }

	dir := t.TempDir()
	hostPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(hostPath, []byte("model: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var gotOptions []hermesacp.Option
	serve = func(_ context.Context, input io.Reader, _ io.Writer, opts ...hermesacp.Option) error {
		gotOptions = append([]hermesacp.Option(nil), opts...)
		_, _ = io.Copy(io.Discard, input)

		return nil
	}
	if code := run(context.Background(), []string{
		"-seed-file", "config.yaml=" + hostPath,
	}, strings.NewReader(""), io.Discard, io.Discard); code != 0 {
		t.Fatalf("seed-file run code = %d", code)
	}
	if len(gotOptions) == 0 {
		t.Fatal("serve received no options")
	}

	if code := run(context.Background(), []string{
		"-seed-file", "invalid",
	}, strings.NewReader(""), io.Discard, io.Discard); code != 2 {
		t.Fatalf("invalid seed-file code = %d, want 2", code)
	}

	var stderr bytes.Buffer
	if code := run(context.Background(), []string{
		"-seed-file", "config.yaml=" + filepath.Join(dir, "absent"),
	}, strings.NewReader(""), io.Discard, &stderr); code != 2 {
		t.Fatalf("missing host file code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "seed file") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestSignals(t *testing.T) {
	signals := forwardedSignals()
	if len(signals) == 0 {
		t.Fatal("no forwarded signals")
	}
	ch := make(chan os.Signal, 1)
	if pendingSignal(ch) != nil {
		t.Fatal("empty channel returned signal")
	}
	ch <- syscall.SIGTERM
	if got := pendingSignal(ch); got != syscall.SIGTERM {
		t.Fatalf("pendingSignal = %v", got)
	}
	if signalCode(syscall.SIGTERM) != 143 {
		t.Fatalf("SIGTERM code = %d", signalCode(syscall.SIGTERM))
	}
	if signalCode(fakeSignal("fake")) != 1 {
		t.Fatalf("fake signal code = %d", signalCode(fakeSignal("fake")))
	}
}

func TestMainAndVersion(t *testing.T) {
	restore := replaceGlobals(t)
	defer restore()
	oldArgs := os.Args
	oldBuildVersion := buildVersion
	defer func() {
		os.Args = oldArgs
		buildVersion = oldBuildVersion
	}()

	serve = func(context.Context, io.Reader, io.Writer, ...hermesacp.Option) error {
		return errors.New("main failed")
	}
	os.Args = []string{"acp-go-hermes"}
	exitCode := -1
	exit = func(code int) { exitCode = code }
	main()
	if exitCode != 1 {
		t.Fatalf("main exit code = %d", exitCode)
	}
	buildVersion = ""
	if version() != "dev" {
		t.Fatalf("empty build version = %q", version())
	}
	buildVersion = "v-test"
	if version() != "v-test" {
		t.Fatalf("build version = %q", version())
	}
}

func replaceGlobals(t *testing.T) func() {
	t.Helper()
	oldServe := serve
	oldVersion := agentVersion
	oldExit := exit

	return func() {
		serve = oldServe
		agentVersion = oldVersion
		exit = oldExit
	}
}

type fakeSignal string

func (s fakeSignal) String() string {
	return string(s)
}

func (s fakeSignal) Signal() {}
