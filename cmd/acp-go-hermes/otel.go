package main

import (
	"context"
	"log/slog"

	"github.com/savid/acp-go-core/observer/exporters"
	hermesacp "github.com/savid/acp-go-hermes"
)

// configureTelemetry builds the exporters the OTEL_* environment enables and
// maps the configured providers onto the agent's options.
func configureTelemetry(ctx context.Context, baseLogger *slog.Logger, version string) (exporters.Bundle, []hermesacp.Option, error) {
	bundle, err := exporters.Configure(ctx, exporters.Config{Vendor: "hermes", Version: version, Logger: baseLogger})
	if err != nil {
		return exporters.Bundle{}, nil, err
	}

	options := []hermesacp.Option{hermesacp.WithTextMapPropagator(bundle.Propagator)}
	if bundle.TracerProvider != nil {
		options = append(options, hermesacp.WithTracerProvider(bundle.TracerProvider))
	}

	if bundle.MeterProvider != nil {
		options = append(options, hermesacp.WithMeterProvider(bundle.MeterProvider))
	}

	return bundle, options, nil
}
