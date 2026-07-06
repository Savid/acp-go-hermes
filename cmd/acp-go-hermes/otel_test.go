package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	hermesacp "github.com/savid/acp-go-hermes"
)

func TestConfigureTelemetry(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "console")
	t.Setenv("OTEL_METRICS_EXPORTER", "console")
	t.Setenv("OTEL_LOGS_EXPORTER", "console")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=test")

	base := slog.New(slog.DiscardHandler)
	config, err := configureTelemetry(context.Background(), base, "test-version")
	if err != nil {
		t.Fatalf("configureTelemetry returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := shutdownTelemetry(context.Background(), config.shutdown); err != nil {
			t.Fatalf("shutdownTelemetry returned error: %v", err)
		}
	})

	if config.logger == base {
		t.Fatal("logger was not wrapped for OTEL logs")
	}

	var options hermesacp.Options
	for _, opt := range config.options {
		opt(&options)
	}
	if options.TextMapPropagator == nil || options.TracerProvider == nil || options.MeterProvider == nil {
		t.Fatalf("telemetry options = %#v", options)
	}
}

func TestConfigureTelemetryNoExporters(t *testing.T) {
	config, err := configureTelemetry(context.Background(), slog.New(slog.DiscardHandler), "test-version")
	if err != nil {
		t.Fatalf("configureTelemetry returned error: %v", err)
	}
	if err := config.shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}

	var options hermesacp.Options
	for _, opt := range config.options {
		opt(&options)
	}
	if options.TextMapPropagator == nil {
		t.Fatal("text map propagator was not configured")
	}
	if options.TracerProvider != nil || options.MeterProvider != nil {
		t.Fatalf("unexpected telemetry providers = %#v", options)
	}
}

func TestConfigureTelemetryErrors(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "bad")
	if config, err := configureTelemetry(context.Background(), slog.New(slog.DiscardHandler), "test-version"); err == nil || config.logger != nil {
		t.Fatalf("resource error config=%#v err=%v", config, err)
	}

	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "invalid")
	if config, err := configureTelemetry(context.Background(), slog.New(slog.DiscardHandler), "test-version"); err == nil || config.logger != nil {
		t.Fatalf("trace error config=%#v err=%v", config, err)
	}

	t.Setenv("OTEL_TRACES_EXPORTER", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "")
	t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "invalid")
	if config, err := configureTelemetry(context.Background(), slog.New(slog.DiscardHandler), "test-version"); err == nil || config.logger != nil {
		t.Fatalf("metric error config=%#v err=%v", config, err)
	}

	t.Setenv("OTEL_METRICS_EXPORTER", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "")
	t.Setenv("OTEL_LOGS_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "invalid")
	if config, err := configureTelemetry(context.Background(), slog.New(slog.DiscardHandler), "test-version"); err == nil || config.logger != nil {
		t.Fatalf("log error config=%#v err=%v", config, err)
	}
}

func TestTelemetrySignalEnabled(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	if telemetrySignalEnabled("OTEL_TRACES_EXPORTER", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", true) {
		t.Fatal("none trace exporter enabled telemetry")
	}

	t.Setenv("OTEL_TRACES_EXPORTER", "console")
	if !telemetrySignalEnabled("OTEL_TRACES_EXPORTER", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", true) {
		t.Fatal("explicit trace exporter did not enable telemetry")
	}

	t.Setenv("OTEL_TRACES_EXPORTER", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4318/v1/traces")
	if !telemetrySignalEnabled("OTEL_TRACES_EXPORTER", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", true) {
		t.Fatal("trace endpoint did not enable telemetry")
	}

	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
	if !telemetrySignalEnabled("OTEL_TRACES_EXPORTER", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", true) {
		t.Fatal("generic OTLP endpoint did not enable telemetry")
	}
	if telemetrySignalEnabled("OTEL_LOGS_EXPORTER", "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", false) {
		t.Fatal("generic OTLP endpoint enabled logs without generic fallback")
	}

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	if !telemetrySignalEnabled("OTEL_METRICS_EXPORTER", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", true) {
		t.Fatal("generic OTLP protocol did not enable telemetry")
	}

	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "api-key=value")
	if !telemetrySignalEnabled("OTEL_METRICS_EXPORTER", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", true) {
		t.Fatal("generic OTLP headers did not enable telemetry")
	}

	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "")
	if telemetrySignalEnabled("OTEL_METRICS_EXPORTER", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", true) {
		t.Fatal("empty OTEL env enabled telemetry")
	}
}

func TestShutdownTelemetry(t *testing.T) {
	if err := shutdownTelemetry(context.Background(), nil); err != nil {
		t.Fatalf("nil shutdown returned error: %v", err)
	}

	errBoom := errors.New("boom")
	err := shutdownTelemetry(context.Background(), func(context.Context) error { return errBoom })
	if !errors.Is(err, errBoom) {
		t.Fatalf("shutdown error = %v", err)
	}
}

func TestJoinedSlogHandler(t *testing.T) {
	var left bytes.Buffer
	var right bytes.Buffer
	leftHandler := slog.NewTextHandler(&left, &slog.HandlerOptions{Level: slog.LevelInfo})
	rightHandler := slog.NewTextHandler(&right, &slog.HandlerOptions{Level: slog.LevelWarn})
	handler := joinSlogHandlers(nil, leftHandler, rightHandler)

	if !handler.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("joined handler did not enable info")
	}
	if err := handler.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "hello", 0)); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if !strings.Contains(left.String(), "hello") || strings.Contains(right.String(), "hello") {
		t.Fatalf("left=%q right=%q", left.String(), right.String())
	}

	grouped := handler.WithAttrs([]slog.Attr{slog.String("component", "test")}).WithGroup("group")
	if err := grouped.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelWarn, "warn", 0)); err != nil {
		t.Fatalf("grouped Handle returned error: %v", err)
	}
	if !strings.Contains(left.String(), "component=test") || !strings.Contains(right.String(), "component=test") {
		t.Fatalf("left=%q right=%q", left.String(), right.String())
	}

	empty := joinSlogHandlers(nil)
	if empty.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("empty handler enabled logs")
	}
	if err := empty.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelError, "drop", 0)); err != nil {
		t.Fatalf("empty Handle returned error: %v", err)
	}
}

func TestRunHandlesTelemetryConfigError(t *testing.T) {
	originalServe := serve
	t.Cleanup(func() { serve = originalServe })

	serve = func(context.Context, io.Reader, io.Writer, ...hermesacp.Option) error {
		t.Fatal("serve should not be called")

		return nil
	}

	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "invalid")

	var stderr bytes.Buffer
	code := run(context.Background(), nil, bytes.NewBuffer(nil), bytes.NewBuffer(nil), &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "configure OpenTelemetry") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestRunHandlesTelemetryShutdownError(t *testing.T) {
	originalServe := serve
	originalShutdown := shutdownOpenTelemetry
	t.Cleanup(func() {
		serve = originalServe
		shutdownOpenTelemetry = originalShutdown
	})

	serve = func(context.Context, io.Reader, io.Writer, ...hermesacp.Option) error {
		return nil
	}
	shutdownOpenTelemetry = func(context.Context, func(context.Context) error) error {
		return errors.New("flush failed")
	}

	var stderr bytes.Buffer
	code := run(context.Background(), nil, bytes.NewBuffer(nil), bytes.NewBuffer(nil), &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "shutdown OpenTelemetry") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}
