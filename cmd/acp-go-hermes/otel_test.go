package main

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigureTelemetryWithoutExporters(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")

	logger := slog.New(slog.DiscardHandler)
	telemetry, err := configureTelemetry(context.Background(), logger, "test")
	require.NoError(t, err)
	require.Len(t, telemetry.options, 1)
	require.NoError(t, shutdownTelemetry(context.Background(), telemetry.shutdown))
	require.NoError(t, shutdownTelemetry(context.Background(), nil))
}

func TestConfigureTelemetryWithConsoleExporters(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "console")
	t.Setenv("OTEL_METRICS_EXPORTER", "console")
	t.Setenv("OTEL_LOGS_EXPORTER", "console")

	logger := slog.New(slog.DiscardHandler)
	telemetry, err := configureTelemetry(context.Background(), logger, "test")
	require.NoError(t, err)
	require.Len(t, telemetry.options, 3)
	require.NotNil(t, telemetry.logger)
	require.True(t, telemetry.logger.Enabled(context.Background(), slog.LevelInfo))
	require.NoError(t, shutdownTelemetry(context.Background(), telemetry.shutdown))
}

func TestTelemetrySignalEnabled(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "")
	require.False(t, telemetrySignalEnabled("OTEL_TRACES_EXPORTER", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", true))

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
	require.True(t, telemetrySignalEnabled("OTEL_TRACES_EXPORTER", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", true))
	require.False(t, telemetrySignalEnabled("OTEL_LOGS_EXPORTER", "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", false))

	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://localhost:4318")
	require.True(t, telemetrySignalEnabled("OTEL_LOGS_EXPORTER", "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", false))
}
