package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"

	hermesacp "github.com/savid/acp-go-hermes"
)

// seedFileFlag collects repeatable -seed-file <relpath>=<hostpath> values,
// reading each host file's contents into a map keyed by the relative path.
type seedFileFlag struct {
	files map[string]string
}

func (s *seedFileFlag) String() string {
	if s == nil || len(s.files) == 0 {
		return ""
	}

	return strings.Join(slices.Sorted(func(yield func(string) bool) {
		for name := range s.files {
			if !yield(name) {
				return
			}
		}
	}), ",")
}

func (s *seedFileFlag) Set(value string) error {
	relPath, hostPath, ok := strings.Cut(value, "=")

	relPath = strings.TrimSpace(relPath)
	hostPath = strings.TrimSpace(hostPath)

	if !ok || relPath == "" || hostPath == "" {
		return fmt.Errorf("invalid -seed-file %q: expected <relpath>=<hostpath>", value)
	}

	contents, err := os.ReadFile(hostPath)
	if err != nil {
		return fmt.Errorf("read seed file %q: %w", hostPath, err)
	}

	if s.files == nil {
		s.files = make(map[string]string)
	}

	s.files[relPath] = string(contents)

	return nil
}

func main() {
	if code := run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr); code != 0 {
		os.Exit(code)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	flags := flag.NewFlagSet("acp-go-hermes", flag.ContinueOnError)
	flags.SetOutput(stderr)

	nativePath := flags.String("path", "", "hermes executable; a bare name is searched on PATH")
	home := flags.String("home", "", "hermes config root passed as HERMES_HOME; empty inherits hermes's own resolution")
	scratchDir := flags.String("scratch-dir", "", "parent directory for ephemeral adapter state; empty means the system temp directory")
	model := flags.String("model", "", "default model for new sessions as provider/id")
	seedFiles := &seedFileFlag{}
	flags.Var(seedFiles, "seed-file", "file seeded into hermes's config root as <relpath>=<hostpath>; repeatable")
	debug := flags.Bool("debug", false, "write debug logs to stderr")
	printVersion := flags.Bool("version", false, "print adapter version and exit")

	if err := flags.Parse(args); err != nil {
		return 2
	}

	if *printVersion {
		_, _ = fmt.Fprintln(stdout, version())

		return 0
	}

	level := slog.LevelWarn
	if *debug {
		level = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))

	telemetry, err := configureTelemetry(ctx, logger, version())
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-hermes: configure OpenTelemetry: %v\n", err)

		return 1
	}

	logger = telemetry.logger

	ctx, stop := signal.NotifyContext(ctx, forwardedSignals()...)
	defer stop()

	options := []hermesacp.Option{
		hermesacp.WithAgentVersion(version()),
		hermesacp.WithExecutablePath(*nativePath),
		hermesacp.WithHome(*home),
		hermesacp.WithScratchDir(*scratchDir),
		hermesacp.WithDefaultModel(*model),
		hermesacp.WithLogger(logger),
	}
	if len(seedFiles.files) > 0 {
		options = append(options, hermesacp.WithSeedFiles(seedFiles.files))
	}

	options = append(options, telemetry.options...)

	serveErr := hermesacp.Serve(ctx, stdin, stdout, options...)
	shutdownErr := shutdownTelemetry(context.Background(), telemetry.shutdown)

	if serveErr != nil && ctx.Err() == nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-hermes: %v\n", serveErr)

		return 1
	}

	if shutdownErr != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-hermes: shutdown OpenTelemetry: %v\n", shutdownErr)

		return 1
	}

	return 0
}
