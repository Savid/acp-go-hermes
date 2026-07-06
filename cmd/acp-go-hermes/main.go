package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"

	hermesacp "github.com/savid/acp-go-hermes"
)

var serve = hermesacp.Serve
var agentVersion = version
var exit = os.Exit
var shutdownOpenTelemetry = shutdownTelemetry

func main() {
	if code := run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr); code != 0 {
		exit(code)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	flags := flag.NewFlagSet("acp-go-hermes", flag.ContinueOnError)
	flags.SetOutput(stderr)

	hermesPath := flags.String("path", "", "path to hermes CLI")
	hermesHome := flags.String("home", "", "parent root for isolated Hermes session state")
	model := flags.String("model", "", "default Hermes model as provider/model")
	debug := flags.Bool("debug", false, "write debug logs to stderr")
	printVersion := flags.Bool("version", false, "print adapter version and exit")
	var seedFiles seedFileFlag
	flags.Var(&seedFiles, "seed-file", "seed a file into the session config root as <relpath>=<hostpath> (repeatable)")

	if err := flags.Parse(args); err != nil {
		return 2
	}

	if *printVersion {
		_, _ = fmt.Fprintln(stdout, agentVersion())

		return 0
	}
	seeded, err := seedFiles.contents()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-hermes: %v\n", err)
		return 2
	}

	logger := slog.New(slog.DiscardHandler)
	if *debug {
		logger = slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	signals := forwardedSignals()
	receivedSignals := make(chan os.Signal, 1)

	signal.Notify(receivedSignals, signals...)
	defer signal.Stop(receivedSignals)

	ctx, stop := signal.NotifyContext(ctx, signals...)
	defer stop()

	version := agentVersion()

	telemetry, err := configureTelemetry(ctx, logger, version)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-hermes: configure OpenTelemetry: %v\n", err)

		return 1
	}

	logger = telemetry.logger

	opts := make([]hermesacp.Option, 0, 5+len(telemetry.options))
	opts = append(opts,
		hermesacp.WithAgentVersion(version),
		hermesacp.WithExecutablePath(*hermesPath),
		hermesacp.WithHome(*hermesHome),
		hermesacp.WithDefaultModel(*model),
		hermesacp.WithLogger(logger),
	)
	if len(seeded) > 0 {
		opts = append(opts, hermesacp.WithSeedFiles(seeded))
	}
	opts = append(opts, telemetry.options...)

	err = serve(ctx, stdin, stdout, opts...)

	shutdownErr := shutdownOpenTelemetry(context.Background(), telemetry.shutdown)
	if shutdownErr != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-hermes: shutdown OpenTelemetry: %v\n", shutdownErr)

		return 1
	}

	if err != nil && ctx.Err() == nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-hermes: %v\n", err)

		return 1
	}

	if sig := pendingSignal(receivedSignals); sig != nil {
		return signalCode(sig)
	}

	return 0
}

// seedFileFlag collects repeatable -seed-file <relpath>=<hostpath> pairs. The
// relative path is confined to the Hermes session config root by the adapter;
// the host path is read into file contents passed to WithSeedFiles.
type seedFileFlag struct {
	pairs []seedFilePair
}

type seedFilePair struct {
	relative string
	hostPath string
}

func (f *seedFileFlag) String() string {
	return ""
}

func (f *seedFileFlag) Set(value string) error {
	relative, hostPath, ok := strings.Cut(value, "=")
	if !ok || relative == "" || hostPath == "" {
		return fmt.Errorf("invalid -seed-file %q, want <relpath>=<hostpath>", value)
	}
	f.pairs = append(f.pairs, seedFilePair{relative: relative, hostPath: hostPath})

	return nil
}

// contents reads each host file into the relative-path keyed map consumed by
// hermesacp.WithSeedFiles.
func (f *seedFileFlag) contents() (map[string]string, error) {
	seeded := make(map[string]string, len(f.pairs))
	for _, pair := range f.pairs {
		data, err := os.ReadFile(pair.hostPath)
		if err != nil {
			return nil, fmt.Errorf("read seed file %q: %w", pair.hostPath, err)
		}
		seeded[pair.relative] = string(data)
	}

	return seeded, nil
}

func pendingSignal(signals <-chan os.Signal) os.Signal {
	select {
	case sig := <-signals:
		return sig
	default:
		return nil
	}
}
