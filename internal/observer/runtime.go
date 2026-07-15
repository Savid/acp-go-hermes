package observer

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type runtimeObserver struct {
	admissions metric.Int64Counter
	resources  metric.Int64UpDownCounter
	processes  metric.Int64UpDownCounter
	stages     metric.Float64Histogram

	mu        sync.Mutex
	snapshots map[string]int
}

func newRuntimeObserver(meter metric.Meter, prefix string) *runtimeObserver {
	return &runtimeObserver{
		admissions: mustInt64Counter(meter, prefix+".runtime.resource.admission.count", "Runtime resource admission decisions."),
		resources:  mustInt64UpDownCounter(meter, prefix+".runtime.resource.active", "Live native-root permits and adapter scratch-root reservations."),
		processes:  mustInt64UpDownCounter(meter, prefix+".runtime.process.active", "Live home-lock supervisors and proven provider descendants."),
		stages:     mustFloat64Histogram(meter, prefix+".runtime.startup.stage.duration", "Native startup stage duration."),
		snapshots:  make(map[string]int),
	}
}

func (o *Observer) RecordRuntimeResourceAdmission(ctx context.Context, resource, lifecycle, outcome string) {
	if o == nil || o.runtime == nil {
		return
	}

	o.runtime.admissions.Add(ctx, 1, metric.WithAttributes(
		attribute.String("runtime.resource", resource),
		attribute.String("runtime.lifecycle", lifecycle),
		attribute.String("runtime.outcome", outcome),
	))
}

func (o *Observer) AddRuntimeResource(ctx context.Context, resource string, delta int64) {
	if o == nil || o.runtime == nil || delta == 0 {
		return
	}

	o.runtime.resources.Add(ctx, delta, metric.WithAttributes(attribute.String("runtime.resource", resource)))
}

func (o *Observer) AddRuntimeProcess(ctx context.Context, process string, delta int64) {
	if o == nil || o.runtime == nil || delta == 0 {
		return
	}

	o.runtime.processes.Add(ctx, delta, metric.WithAttributes(attribute.String("runtime.process", process)))
}

func (o *Observer) SetRuntimeProcess(ctx context.Context, process string, count int) {
	if o == nil || o.runtime == nil || count < 0 {
		return
	}

	o.runtime.mu.Lock()
	previous := o.runtime.snapshots[process]
	o.runtime.snapshots[process] = count
	o.runtime.mu.Unlock()

	o.AddRuntimeProcess(ctx, process, int64(count-previous))
}

func (o *Observer) ObserveRuntimeStartupStage(ctx context.Context, lifecycle, stage string, elapsed time.Duration, err error) {
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
