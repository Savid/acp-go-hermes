package observer

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type runtimeObserver struct {
	stages metric.Float64Histogram
}

func newRuntimeObserver(meter metric.Meter, prefix string) *runtimeObserver {
	return &runtimeObserver{
		stages: mustFloat64Histogram(meter, prefix+".runtime.startup.stage.duration", "Native startup stage duration."),
	}
}

func (o *Observer) ObserveNativeStartup(ctx context.Context, lifecycle, stage string, elapsed time.Duration, err error) {
	if o == nil || o.runtime == nil {
		return
	}

	status := "ok"
	if err != nil {
		status = "error"
	}

	o.runtime.stages.Record(ctx, elapsed.Seconds(), metric.WithAttributes(
		attribute.String("runtime.lifecycle", lifecycle),
		attribute.String("runtime.stage", stage),
		attribute.String("runtime.status", status),
	))
}
