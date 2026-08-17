package observer

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestObserverExtractAndInjectTraceEnv(t *testing.T) {
	observer := New(Config{Version: "test-version"})
	ctx := observer.Extract(context.Background(), map[string]any{
		metaTraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		metaTraceState:  "vendor=value",
		metaBaggage:     "user=alice",
		"ignored":       "value",
	})

	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		t.Fatal("trace context was not extracted")
	}

	env := map[string]string{"EXISTING": "1"}
	injected := observer.InjectTraceEnv(ctx, env)
	if injected["TRACEPARENT"] == "" || injected["TRACESTATE"] != "vendor=value" || injected["BAGGAGE"] != "user=alice" {
		t.Fatalf("injected env = %#v", injected)
	}
	if injected["EXISTING"] != "1" {
		t.Fatalf("existing env missing: %#v", injected)
	}
	injected["EXISTING"] = "changed"
	if env["EXISTING"] != "1" {
		t.Fatalf("InjectTraceEnv did not clone env: %#v", env)
	}
	fresh := observer.InjectTraceEnv(ctx, nil)
	if fresh["TRACEPARENT"] == "" {
		t.Fatalf("fresh injected env = %#v", fresh)
	}
}

func TestObserverNilAndEmptyBranches(t *testing.T) {
	var observer *Observer
	ctx := context.Background()
	if got := observer.Extract(ctx, map[string]any{metaTraceParent: "bad"}); got != ctx {
		t.Fatal("nil Extract changed context")
	}

	env := map[string]string{"A": "B"}
	cloned := observer.InjectTraceEnv(ctx, env)
	if cloned["A"] != "B" {
		t.Fatalf("nil InjectTraceEnv = %#v", cloned)
	}
	cloned["A"] = "changed"
	if env["A"] != "B" {
		t.Fatalf("nil InjectTraceEnv did not clone env: %#v", env)
	}

	observer = New(Config{})
	if got := observer.Extract(ctx, nil); got != ctx {
		t.Fatal("empty Extract changed context")
	}
	if got := observer.Extract(ctx, map[string]any{metaTraceParent: 7}); got != ctx {
		t.Fatal("non-string Extract changed context")
	}
	untracedEnv := map[string]string{"A": "B"}
	got := observer.InjectTraceEnv(ctx, untracedEnv)
	if got["A"] != "B" || got["TRACEPARENT"] != "" {
		t.Fatalf("untraced InjectTraceEnv = %#v", got)
	}
	got["A"] = "changed"
	if untracedEnv["A"] != "B" {
		t.Fatalf("untraced InjectTraceEnv did not clone env: %#v", untracedEnv)
	}
}

func TestObserverRecordsACPRequestsAndProcessStarts(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	observer := New(Config{
		MeterProvider:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
		Propagator:     propagation.TraceContext{},
		TracerProvider: tracerProvider,
		Version:        "test-version",
	})

	ctx, finish := observer.StartACPRequest(context.Background(), "session/new")
	finish(nil)

	_, finish = observer.StartACPRequest(ctx, "session/prompt")
	finish(errors.New("boom"))

	observer.RecordHermesProcessStart(ctx)
	ctx, finishPrompt := observer.StartPrompt(ctx, map[string]any{}, "openai/gpt-test")
	observer.ObserveFirstPromptUpdate(ctx)
	observer.ObserveFirstPromptUpdate(ctx)
	observer.ObserveFirstPromptUpdate(context.Background())
	finishPrompt(PromptResult{
		CachedReadTokens:  4,
		CachedWriteTokens: 5,
		InputTokens:       1,
		OutputTokens:      2,
		ThoughtTokens:     6,
		TotalTokens:       3,
		Model:             "openai/gpt-test",
		StopReason:        "end_turn",
	})
	_, finishPrompt = observer.StartPrompt(ctx, nil, "")
	finishPrompt(PromptResult{Err: context.Canceled, StopReason: "cancelled"})
	observer.AddActiveSession(ctx, 1)
	observer.AddActiveSession(ctx, -1)
	observer.RecordRawEventEmitFailure(ctx)

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("Collect returned error: %v", err)
	}
	if got := int64MetricSum(metrics, "acp_go_hermes.acp.request.count"); got != 2 {
		t.Fatalf("request count = %d, want 2", got)
	}
	if got := int64MetricSum(metrics, "acp_go_hermes.hermes.process.start.count"); got != 1 {
		t.Fatalf("process start count = %d, want 1", got)
	}
	if got := histogramMetricCount(metrics, "acp_go_hermes.acp.request.duration"); got != 2 {
		t.Fatalf("request duration count = %d, want 2", got)
	}
	if got := int64MetricSum(metrics, "acp_go_hermes.session.prompt.count"); got != 2 {
		t.Fatalf("prompt count = %d, want 2", got)
	}
	if got := int64MetricSum(metrics, "acp_go_hermes.session.cancel.count"); got != 1 {
		t.Fatalf("prompt cancel count = %d, want 1", got)
	}
	if got := int64MetricSum(metrics, "acp_go_hermes.session.active"); got != 0 {
		t.Fatalf("active session sum = %d, want 0", got)
	}
	if got := histogramMetricCount(metrics, "acp_go_hermes.session.prompt.duration"); got != 2 {
		t.Fatalf("prompt duration count = %d, want 2", got)
	}
	if got := histogramMetricCount(metrics, "gen_ai.client.operation.duration"); got != 2 {
		t.Fatalf("gen ai duration count = %d, want 2", got)
	}
	if got := histogramMetricCount(metrics, "gen_ai.client.operation.time_to_first_chunk"); got != 1 {
		t.Fatalf("first chunk count = %d, want 1", got)
	}
	if got := int64HistogramMetricCount(metrics, "gen_ai.client.token.usage"); got != 3 {
		t.Fatalf("token usage count = %d, want 3", got)
	}
	ended := spanRecorder.Ended()
	if len(ended) != 4 {
		t.Fatalf("ended spans = %d, want 4", len(ended))
	}
	if ended[0].Status().Code != codes.Ok || ended[1].Status().Code != codes.Error {
		t.Fatalf("span statuses = %#v %#v", ended[0].Status(), ended[1].Status())
	}

	var nilObserver *Observer
	ctx, finish = nilObserver.StartACPRequest(context.Background(), "noop")
	finish(errors.New("ignored"))
	nilObserver.RecordHermesProcessStart(ctx)
	ctx, finishPrompt = nilObserver.StartPrompt(ctx, nil, "")
	finishPrompt(PromptResult{Err: errors.New("ignored")})
	nilObserver.ObserveFirstPromptUpdate(ctx)
	nilObserver.AddActiveSession(ctx, 1)
	nilObserver.RecordRawEventEmitFailure(ctx)
}

func TestObserverHelpers(t *testing.T) {
	now := monotonicNow()
	if got := monotonicSince(now.Add(-time.Second)); got <= 0 {
		t.Fatalf("monotonicSince = %f", got)
	}
	if cloneEnv(nil) != nil {
		t.Fatal("cloneEnv nil returned non-nil")
	}
	env := map[string]string{}
	setUpper(env, "A", "")
	if _, ok := env["A"]; ok {
		t.Fatalf("setUpper wrote empty value: %#v", env)
	}
	setUpper(env, "A", "B")
	if env["A"] != "B" {
		t.Fatalf("setUpper did not write value: %#v", env)
	}
	if got := outcomeFromPrompt(PromptResult{}); got != outcomeOK {
		t.Fatalf("outcomeFromPrompt ok = %q", got)
	}
	if got := outcomeFromPrompt(PromptResult{Err: errors.New("x")}); got != outcomeError {
		t.Fatalf("outcomeFromPrompt err = %q", got)
	}
	if got := outcomeFromPrompt(PromptResult{StopReason: "canceled"}); got != outcomeCanceled {
		t.Fatalf("outcomeFromPrompt canceled = %q", got)
	}
	if firstNonEmpty("", " value ") != " value " || firstNonEmpty("", " ") != "" {
		t.Fatal("firstNonEmpty failed")
	}
	if len(cloneAttrs(nil)) != 0 {
		t.Fatal("cloneAttrs nil returned values")
	}
}

func int64MetricSum(metrics metricdata.ResourceMetrics, name string) int64 {
	var sum int64
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			data, ok := metric.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, point := range data.DataPoints {
				sum += point.Value
			}
		}
	}

	return sum
}

func histogramMetricCount(metrics metricdata.ResourceMetrics, name string) uint64 {
	var count uint64
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			data, ok := metric.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, point := range data.DataPoints {
				count += point.Count
			}
		}
	}

	return count
}

func int64HistogramMetricCount(metrics metricdata.ResourceMetrics, name string) uint64 {
	var count uint64
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			data, ok := metric.Data.(metricdata.Histogram[int64])
			if !ok {
				continue
			}
			for _, point := range data.DataPoints {
				count += point.Count
			}
		}
	}

	return count
}
