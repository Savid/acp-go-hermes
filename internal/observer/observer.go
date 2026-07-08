package observer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const (
	InstrumentationName = "github.com/savid/acp-go-hermes"

	attrACPMethod                       = "acp.method"
	attrHermesClient                    = "hermes.client"
	attrHermesProcessKind               = "hermes.process.kind"
	attrGenAIOperation                  = "gen_ai.operation.name"
	attrGenAIProvider                   = "gen_ai.provider.name"
	attrGenAIRequestModel               = "gen_ai.request.model"
	attrGenAIResponseModel              = "gen_ai.response.model"
	attrGenAIStopReason                 = "gen_ai.response.finish_reasons"
	attrGenAITokenType                  = "gen_ai.token.type"                        // #nosec G101 -- OTel semantic-convention attribute, not a secret.
	attrGenAIUsageCacheCreationTokens   = "gen_ai.usage.cache_creation.input_tokens" // #nosec G101 -- OTel semantic-convention attribute, not a secret.
	attrGenAIUsageCacheReadTokens       = "gen_ai.usage.cache_read.input_tokens"     // #nosec G101 -- OTel semantic-convention attribute, not a secret.
	attrGenAIUsageInputTokens           = "gen_ai.usage.input_tokens"                // #nosec G101 -- OTel semantic-convention attribute, not a secret.
	attrGenAIUsageOutputTokens          = "gen_ai.usage.output_tokens"               // #nosec G101 -- OTel semantic-convention attribute, not a secret.
	attrGenAIUsageReasoningOutputTokens = "gen_ai.usage.reasoning.output_tokens"     // #nosec G101 -- OTel semantic-convention attribute, not a secret.
	attrGenAIUsageTotalTokens           = "gen_ai.usage.total_tokens"                // #nosec G101 -- OTel semantic-convention attribute, not a secret.
	attrOperation                       = "operation"
	attrOutcome                         = "outcome"
	attrStopReason                      = "stop_reason"

	hermesClientValue  = "hermes-serve"
	genAIOperationChat = "chat"
	genAIProviderValue = "hermes"

	envBaggage     = "BAGGAGE"
	envTraceParent = "TRACEPARENT"
	envTraceState  = "TRACESTATE"

	metaBaggage     = "baggage"
	metaTraceParent = "traceparent"
	metaTraceState  = "tracestate"

	outcomeCanceled = "canceled"
	outcomeError    = "error"
	outcomeOK       = "ok"
)

type Config struct {
	MeterProvider  metric.MeterProvider
	Propagator     propagation.TextMapPropagator
	TracerProvider trace.TracerProvider
	Version        string
}

type Observer struct {
	propagator propagation.TextMapPropagator
	tracer     trace.Tracer

	acpRequestCount     metric.Int64Counter
	acpRequestDuration  metric.Float64Histogram
	genAIDuration       metric.Float64Histogram
	genAITokenUsage     metric.Int64Histogram
	firstPromptChunk    metric.Float64Histogram
	promptCancelCount   metric.Int64Counter
	promptCount         metric.Int64Counter
	promptDuration      metric.Float64Histogram
	processStartCount   metric.Int64Counter
	sessionActive       metric.Int64UpDownCounter
	rawEventEmitFailure metric.Int64Counter
}

type PromptResult struct {
	CachedReadTokens  int
	CachedWriteTokens int
	Err               error
	InputTokens       int
	Model             string
	OutputTokens      int
	StopReason        string
	ThoughtTokens     int
	TotalTokens       int
}

type promptStateKey struct{}

type promptState struct {
	start time.Time
	model string

	mu       sync.Mutex
	observed bool
}

func New(config Config) *Observer {
	tracerProvider := config.TracerProvider
	if tracerProvider == nil {
		tracerProvider = tracenoop.NewTracerProvider()
	}

	meterProvider := config.MeterProvider
	if meterProvider == nil {
		meterProvider = metricnoop.NewMeterProvider()
	}

	propagator := config.Propagator
	if propagator == nil {
		propagator = propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		)
	}

	var (
		tracerOptions []trace.TracerOption
		meterOptions  []metric.MeterOption
	)

	if config.Version != "" {
		tracerOptions = append(tracerOptions, trace.WithInstrumentationVersion(config.Version))
		meterOptions = append(meterOptions, metric.WithInstrumentationVersion(config.Version))
	}

	meter := meterProvider.Meter(InstrumentationName, meterOptions...)
	observer := &Observer{
		propagator: propagator,
		tracer:     tracerProvider.Tracer(InstrumentationName, tracerOptions...),
	}
	observer.acpRequestCount = mustInt64Counter(meter, "acp_go_hermes.acp.request.count", "ACP requests.")
	observer.acpRequestDuration = mustFloat64Histogram(meter, "acp_go_hermes.acp.request.duration", "ACP request duration.")
	observer.genAIDuration = mustFloat64Histogram(meter, "gen_ai.client.operation.duration", "Hermes prompt operation duration.")
	observer.genAITokenUsage = mustInt64Histogram(meter, "gen_ai.client.token.usage", "{token}", "Hermes token usage.")
	observer.firstPromptChunk = mustFloat64Histogram(meter, "gen_ai.client.operation.time_to_first_chunk", "Time to first ACP prompt update.")
	observer.promptCancelCount = mustInt64Counter(meter, "acp_go_hermes.session.cancel.count", "Cancelled prompt turns.")
	observer.promptCount = mustInt64Counter(meter, "acp_go_hermes.session.prompt.count", "Prompt turns.")
	observer.promptDuration = mustFloat64Histogram(meter, "acp_go_hermes.session.prompt.duration", "Prompt turn duration.")
	observer.processStartCount = mustInt64Counter(meter, "acp_go_hermes.hermes.process.start.count", "Hermes serve process starts.")
	observer.sessionActive = mustInt64UpDownCounter(meter, "acp_go_hermes.session.active", "Active Hermes sessions.")
	observer.rawEventEmitFailure = mustInt64Counter(meter, "acp_go_hermes.session.raw_event.emit_failure.count", "Raw event notification emit failures.")

	return observer
}

func mustInt64Counter(meter metric.Meter, name string, description string) metric.Int64Counter {
	instrument, _ := meter.Int64Counter(name, metric.WithDescription(description))

	return instrument
}

func mustInt64Histogram(meter metric.Meter, name string, unit string, description string) metric.Int64Histogram {
	instrument, _ := meter.Int64Histogram(name, metric.WithUnit(unit), metric.WithDescription(description))

	return instrument
}

func mustFloat64Histogram(meter metric.Meter, name string, description string) metric.Float64Histogram {
	instrument, _ := meter.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(description))

	return instrument
}

func mustInt64UpDownCounter(meter metric.Meter, name string, description string) metric.Int64UpDownCounter {
	instrument, _ := meter.Int64UpDownCounter(name, metric.WithDescription(description))

	return instrument
}

func (o *Observer) Extract(ctx context.Context, meta map[string]any) context.Context {
	if o == nil || len(meta) == 0 {
		return ctx
	}

	carrier := propagation.MapCarrier{}

	for _, key := range []string{metaTraceParent, metaTraceState, metaBaggage} {
		value, _ := meta[key].(string)
		if value != "" {
			carrier[key] = value
		}
	}

	if len(carrier) == 0 {
		return ctx
	}

	return o.propagator.Extract(ctx, carrier)
}

func (o *Observer) InjectTraceEnv(ctx context.Context, env map[string]string) map[string]string {
	if o == nil {
		return cloneEnv(env)
	}

	carrier := propagation.MapCarrier{}
	o.propagator.Inject(ctx, carrier)

	if len(carrier) == 0 {
		return cloneEnv(env)
	}

	out := cloneEnv(env)
	if out == nil {
		out = map[string]string{}
	}

	setUpper(out, envTraceParent, carrier.Get(metaTraceParent))
	setUpper(out, envTraceState, carrier.Get(metaTraceState))
	setUpper(out, envBaggage, carrier.Get(metaBaggage))

	return out
}

func (o *Observer) StartACPRequest(ctx context.Context, method string) (context.Context, func(error)) {
	if o == nil {
		return ctx, func(error) {}
	}

	ctx, span := o.tracer.Start(ctx, "acp.request",
		trace.WithAttributes(
			attribute.String(attrOperation, "acp.request"),
			attribute.String(attrACPMethod, method),
		),
	)
	o.acpRequestCount.Add(ctx, 1, metric.WithAttributes(attribute.String(attrACPMethod, method)))

	start := monotonicNow()

	return ctx, func(err error) {
		elapsed := monotonicSince(start)

		attrs := []attribute.KeyValue{attribute.String(attrACPMethod, method)}
		if err != nil {
			attrs = append(attrs, attribute.String(attrOutcome, outcomeError))

			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			attrs = append(attrs, attribute.String(attrOutcome, outcomeOK))

			span.SetStatus(codes.Ok, "")
		}

		o.acpRequestDuration.Record(ctx, elapsed, metric.WithAttributes(attrs...))
		span.End()
	}
}

func (o *Observer) StartPrompt(ctx context.Context, meta map[string]any, model string) (context.Context, func(PromptResult)) {
	if o == nil {
		return ctx, func(PromptResult) {}
	}

	ctx = o.Extract(ctx, meta)
	attrs := promptAttrs(model)
	ctx, span := o.tracer.Start(ctx, "session.prompt", trace.WithAttributes(attrs...))
	state := &promptState{start: monotonicNow(), model: model}
	ctx = context.WithValue(ctx, promptStateKey{}, state)

	return ctx, func(result PromptResult) {
		finalAttrs := promptAttrs(firstNonEmpty(result.Model, model))

		finalAttrs = append(finalAttrs, promptUsageAttrs(result)...)
		if result.StopReason != "" {
			finalAttrs = append(finalAttrs,
				attribute.String(attrStopReason, result.StopReason),
				attribute.StringSlice(attrGenAIStopReason, []string{result.StopReason}),
			)
		}

		outcome := outcomeFromPrompt(result)
		metricAttrs := append(cloneAttrs(finalAttrs), attribute.String(attrOutcome, outcome))

		if result.Err != nil {
			span.RecordError(result.Err)
			span.SetStatus(codes.Error, result.Err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}

		span.SetAttributes(metricAttrs...)
		span.End()

		elapsed := monotonicSince(state.start)

		o.promptCount.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		o.promptDuration.Record(ctx, elapsed, metric.WithAttributes(metricAttrs...))
		o.genAIDuration.Record(ctx, elapsed, metric.WithAttributes(metricAttrs...))

		if outcome == outcomeCanceled {
			o.promptCancelCount.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		}

		o.recordTokenUsage(ctx, result, finalAttrs)
	}
}

func (o *Observer) ObserveFirstPromptUpdate(ctx context.Context) {
	if o == nil {
		return
	}

	state, _ := ctx.Value(promptStateKey{}).(*promptState)
	if state == nil {
		return
	}

	state.mu.Lock()
	if state.observed {
		state.mu.Unlock()

		return
	}

	state.observed = true
	start := state.start
	model := state.model
	state.mu.Unlock()

	o.firstPromptChunk.Record(ctx, monotonicSince(start), metric.WithAttributes(promptAttrs(model)...))
}

func (o *Observer) RecordHermesProcessStart(ctx context.Context) {
	if o == nil {
		return
	}

	o.processStartCount.Add(ctx, 1, metric.WithAttributes(attribute.String(attrHermesProcessKind, "serve")))
}

func (o *Observer) AddActiveSession(ctx context.Context, delta int64) {
	if o == nil || delta == 0 {
		return
	}

	o.sessionActive.Add(ctx, delta)
}

// RecordRawEventEmitFailure counts a failed raw-event notification emit. Raw
// events are non-authoritative, so the failure is recorded here rather than
// aborting the prompt turn.
func (o *Observer) RecordRawEventEmitFailure(ctx context.Context) {
	if o == nil {
		return
	}

	o.rawEventEmitFailure.Add(ctx, 1)
}

func (o *Observer) recordTokenUsage(ctx context.Context, result PromptResult, attrs []attribute.KeyValue) {
	for _, item := range []struct {
		name  string
		value int
	}{
		{name: "input", value: result.InputTokens},
		{name: "output", value: result.OutputTokens},
		{name: "total", value: result.TotalTokens},
	} {
		if item.value <= 0 {
			continue
		}

		tokenAttrs := append(cloneAttrs(attrs), attribute.String(attrGenAITokenType, item.name))
		o.genAITokenUsage.Record(ctx, int64(item.value), metric.WithAttributes(tokenAttrs...))
	}
}

func promptAttrs(model string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(attrHermesClient, hermesClientValue),
		attribute.String(attrGenAIProvider, genAIProviderValue),
		attribute.String(attrGenAIOperation, genAIOperationChat),
	}
	if strings.TrimSpace(model) != "" {
		attrs = append(attrs,
			attribute.String(attrGenAIRequestModel, model),
			attribute.String(attrGenAIResponseModel, model),
		)
	}

	return attrs
}

func promptUsageAttrs(result PromptResult) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 3)
	if result.InputTokens > 0 {
		attrs = append(attrs, attribute.Int(attrGenAIUsageInputTokens, result.InputTokens))
	}

	if result.OutputTokens > 0 {
		attrs = append(attrs, attribute.Int(attrGenAIUsageOutputTokens, result.OutputTokens))
	}

	if result.CachedReadTokens > 0 {
		attrs = append(attrs, attribute.Int(attrGenAIUsageCacheReadTokens, result.CachedReadTokens))
	}

	if result.CachedWriteTokens > 0 {
		attrs = append(attrs, attribute.Int(attrGenAIUsageCacheCreationTokens, result.CachedWriteTokens))
	}

	if result.ThoughtTokens > 0 {
		attrs = append(attrs, attribute.Int(attrGenAIUsageReasoningOutputTokens, result.ThoughtTokens))
	}

	if result.TotalTokens > 0 {
		attrs = append(attrs, attribute.Int(attrGenAIUsageTotalTokens, result.TotalTokens))
	}

	return attrs
}

func outcomeFromPrompt(result PromptResult) string {
	if result.Err != nil {
		if errors.Is(result.Err, context.Canceled) {
			return outcomeCanceled
		}

		return outcomeError
	}

	if strings.EqualFold(result.StopReason, "cancelled") || strings.EqualFold(result.StopReason, "canceled") {
		return outcomeCanceled
	}

	return outcomeOK
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}

	return ""
}

func cloneAttrs(attrs []attribute.KeyValue) []attribute.KeyValue {
	return append([]attribute.KeyValue(nil), attrs...)
}

func cloneEnv(env map[string]string) map[string]string {
	if len(env) == 0 {
		return nil
	}

	out := make(map[string]string, len(env))
	for key, value := range env {
		out[key] = value
	}

	return out
}

func setUpper(env map[string]string, key string, value string) {
	if value != "" {
		env[key] = value
	}
}
