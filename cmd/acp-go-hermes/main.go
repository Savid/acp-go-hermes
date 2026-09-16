package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"

	"github.com/savid/acp-go-core/process"
	hermesacp "github.com/savid/acp-go-hermes"
)

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
	scratchDir := flags.String("scratch-dir", "", "accepted and ignored; this adapter allocates no ephemeral state")
	model := flags.String("model", "", "default model for new sessions as provider/id")
	seedFiles := &process.SeedFileFlag{}
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
	if len(seedFiles.Files) > 0 {
		options = append(options, hermesacp.WithSeedFiles(seedFiles.Files))
	}

	options = append(options, telemetry.options...)

	serveErr := hermesacp.Serve(ctx, stdin, stdout, options...)
	shutdownErr := telemetry.shutdown(context.Background())

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
