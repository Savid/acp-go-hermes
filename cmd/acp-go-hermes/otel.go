package main

import (
	"context"
	"log/slog"

	"github.com/savid/acp-go-core/observer/exporters"
	hermesacp "github.com/savid/acp-go-hermes"
)

// telemetry is the configured bundle plus the agent options it maps onto.
type telemetry struct {
	logger   *slog.Logger
	options  []hermesacp.Option
	shutdown func(context.Context) error
}

// configureTelemetry reads the standard OTEL_* environment and maps each
// configured provider onto an agent option.
func configureTelemetry(ctx context.Context, baseLogger *slog.Logger, version string) (telemetry, error) {
	bundle, err := exporters.Configure(ctx, exporters.Config{Vendor: "hermes", Version: version, Logger: baseLogger})
	if err != nil {
		return telemetry{}, err
	}

	options := []hermesacp.Option{}
	if bundle.Propagator != nil {
		options = append(options, hermesacp.WithTextMapPropagator(bundle.Propagator))
	}

	if bundle.TracerProvider != nil {
		options = append(options, hermesacp.WithTracerProvider(bundle.TracerProvider))
	}

	if bundle.MeterProvider != nil {
		options = append(options, hermesacp.WithMeterProvider(bundle.MeterProvider))
	}

	return telemetry{logger: bundle.Logger, options: options, shutdown: bundle.Shutdown}, nil
}
