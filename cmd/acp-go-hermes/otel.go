package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/contrib/propagators/autoprop"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	hermesacp "github.com/savid/acp-go-hermes"
	"github.com/savid/acp-go-hermes/internal/observer"
)

const defaultServiceName = "acp-go-hermes"

type telemetryConfig struct {
	logger  *slog.Logger
	options []hermesacp.Option

	shutdown func(context.Context) error
}

func configureTelemetry(ctx context.Context, baseLogger *slog.Logger, version string) (telemetryConfig, error) {
	config := telemetryConfig{
		logger:  baseLogger,
		options: []hermesacp.Option{hermesacp.WithTextMapPropagator(autoprop.NewTextMapPropagator())},
	}

	res, err := telemetryResource(ctx, version)
	if err != nil {
		return telemetryConfig{}, err
	}

	var shutdowns []func(context.Context) error

	if telemetrySignalEnabled("OTEL_TRACES_EXPORTER", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", true) {
		exporter, err := autoexport.NewSpanExporter(ctx)
		if err != nil {
			return telemetryConfig{}, err
		}

		if !autoexport.IsNoneSpanExporter(exporter) {
			provider := sdktrace.NewTracerProvider(
				sdktrace.WithResource(res),
				sdktrace.WithBatcher(exporter),
			)
			config.options = append(config.options, hermesacp.WithTracerProvider(provider))
			shutdowns = append(shutdowns, provider.Shutdown)
		}
	}

	if telemetrySignalEnabled("OTEL_METRICS_EXPORTER", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", true) {
		reader, err := autoexport.NewMetricReader(ctx)
		if err != nil {
			return telemetryConfig{}, err
		}

		if !autoexport.IsNoneMetricReader(reader) {
			provider := sdkmetric.NewMeterProvider(
				sdkmetric.WithResource(res),
				sdkmetric.WithReader(reader),
			)
			config.options = append(config.options, hermesacp.WithMeterProvider(provider))
			shutdowns = append(shutdowns, provider.Shutdown)
		}
	}

	if telemetrySignalEnabled("OTEL_LOGS_EXPORTER", "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", false) {
		exporter, err := autoexport.NewLogExporter(ctx)
		if err != nil {
			return telemetryConfig{}, err
		}

		if !autoexport.IsNoneLogExporter(exporter) {
			provider := sdklog.NewLoggerProvider(
				sdklog.WithResource(res),
				sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
			)
			handler := otelslog.NewHandler(
				observer.InstrumentationName,
				otelslog.WithLoggerProvider(provider),
				otelslog.WithVersion(version),
			)
			config.logger = slog.New(joinSlogHandlers(baseLogger.Handler(), handler))

			shutdowns = append(shutdowns, provider.Shutdown)
		}
	}

	config.shutdown = func(ctx context.Context) error {
		var errs []error
		for i := len(shutdowns) - 1; i >= 0; i-- {
			errs = append(errs, shutdowns[i](ctx))
		}

		return errors.Join(errs...)
	}

	return config, nil
}

func telemetryResource(ctx context.Context, version string) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{semconv.ServiceVersion(version)}
	if os.Getenv("OTEL_SERVICE_NAME") == "" {
		attrs = append(attrs, semconv.ServiceName(defaultServiceName))
	}

	return resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
	)
}

func telemetrySignalEnabled(exporterEnv string, endpointEnv string, genericOTLP bool) bool {
	if value := os.Getenv(exporterEnv); value != "" {
		return value != "none"
	}

	if os.Getenv(endpointEnv) != "" {
		return true
	}

	if !genericOTLP {
		return false
	}

	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_HEADERS") != ""
}

func shutdownTelemetry(ctx context.Context, shutdown func(context.Context) error) error {
	if shutdown == nil {
		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	return shutdown(shutdownCtx)
}

type joinedSlogHandler struct {
	handlers []slog.Handler
}

func joinSlogHandlers(handlers ...slog.Handler) slog.Handler {
	joined := joinedSlogHandler{handlers: make([]slog.Handler, 0, len(handlers))}
	for _, handler := range handlers {
		if handler != nil {
			joined.handlers = append(joined.handlers, handler)
		}
	}

	return joined
}

func (h joinedSlogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}

	return false
}

func (h joinedSlogHandler) Handle(ctx context.Context, record slog.Record) error {
	var errs []error

	for _, handler := range h.handlers {
		if handler.Enabled(ctx, record.Level) {
			errs = append(errs, handler.Handle(ctx, record))
		}
	}

	return errors.Join(errs...)
}

func (h joinedSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, 0, len(h.handlers))
	for _, handler := range h.handlers {
		handlers = append(handlers, handler.WithAttrs(attrs))
	}

	return joinedSlogHandler{handlers: handlers}
}

func (h joinedSlogHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, 0, len(h.handlers))
	for _, handler := range h.handlers {
		handlers = append(handlers, handler.WithGroup(name))
	}

	return joinedSlogHandler{handlers: handlers}
}
