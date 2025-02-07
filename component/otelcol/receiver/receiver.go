// Package receiver utilities to create a Flow component from OpenTelemetry
// Collector receivers.
package receiver

import (
	"context"
	"errors"
	"os"

	"github.com/grafana/agent/component"
	"github.com/grafana/agent/component/otelcol"
	"github.com/grafana/agent/component/otelcol/internal/fanoutconsumer"
	"github.com/grafana/agent/component/otelcol/internal/scheduler"
	"github.com/grafana/agent/pkg/build"
	"github.com/grafana/agent/pkg/util/zapadapter"
	otelcomponent "go.opentelemetry.io/collector/component"
	otelconfig "go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/otel/metric"
)

// Arguments is an extension of component.Arguments which contains necessary
// settings for OpenTelemetry Collector receivers.
type Arguments interface {
	component.Arguments

	// Convert converts the Arguments into an OpenTelemetry Collector receiver
	// configuration.
	Convert() otelconfig.Receiver

	// Extensions returns the set of extensions that the configured component is
	// allowed to use.
	Extensions() map[otelconfig.ComponentID]otelcomponent.Extension

	// Exporters returns the set of exporters that are exposed to the configured
	// component.
	Exporters() map[otelconfig.DataType]map[otelconfig.ComponentID]otelcomponent.Exporter

	// NextConsumers returns the set of consumers to send data to.
	NextConsumers() *otelcol.ConsumerArguments
}

// Receiver is a Flow component shim which manages an OpenTelemetry Collector
// receiver component.
type Receiver struct {
	ctx    context.Context
	cancel context.CancelFunc

	opts    component.Options
	factory otelcomponent.ReceiverFactory

	sched *scheduler.Scheduler
}

var (
	_ component.Component       = (*Receiver)(nil)
	_ component.HealthComponent = (*Receiver)(nil)
)

// New creates a new Flow component which encapsulates an OpenTelemetry
// Collector receiver. args must hold a value of the argument type registered
// with the Flow component.
//
// If the registered Flow component registers exported fields, it is the
// responsibility of the caller to export values when needed; the Receiver
// component never exports any values.
func New(opts component.Options, f otelcomponent.ReceiverFactory, args Arguments) (*Receiver, error) {
	ctx, cancel := context.WithCancel(context.Background())

	r := &Receiver{
		ctx:    ctx,
		cancel: cancel,

		opts:    opts,
		factory: f,

		sched: scheduler.New(opts.Logger),
	}
	if err := r.Update(args); err != nil {
		return nil, err
	}
	return r, nil
}

// Run starts the Receiver component.
func (r *Receiver) Run(ctx context.Context) error {
	defer r.cancel()
	return r.sched.Run(ctx)
}

// Update implements component.Component. It will convert the Arguments into
// configuration for OpenTelemetry Collector receiver configuration and manage
// the underlying OpenTelemetry Collector receiver.
func (r *Receiver) Update(args component.Arguments) error {
	rargs := args.(Arguments)

	host := scheduler.NewHost(
		r.opts.Logger,
		scheduler.WithHostExtensions(rargs.Extensions()),
		scheduler.WithHostExporters(rargs.Exporters()),
	)

	settings := otelcomponent.ReceiverCreateSettings{
		TelemetrySettings: otelcomponent.TelemetrySettings{
			Logger: zapadapter.New(r.opts.Logger),

			TracerProvider: r.opts.Tracer,

			// TODO(rfratto): expose metrics statistics.
			MeterProvider: metric.NewNoopMeterProvider(),
		},

		BuildInfo: otelcomponent.BuildInfo{
			Command:     os.Args[0],
			Description: "Grafana Agent",
			Version:     build.Version,
		},
	}

	receiverConfig := rargs.Convert()

	var (
		next        = rargs.NextConsumers()
		nextTraces  = fanoutconsumer.Traces(next.Traces)
		nextMetrics = fanoutconsumer.Metrics(next.Metrics)
		nextLogs    = fanoutconsumer.Logs(next.Logs)
	)

	// Create instances of the receiver from our factory for each of our
	// supported telemetry signals.
	var components []otelcomponent.Component

	tracesReceiver, err := r.factory.CreateTracesReceiver(r.ctx, settings, receiverConfig, nextTraces)
	if err != nil && !errors.Is(err, otelcomponent.ErrDataTypeIsNotSupported) {
		return err
	} else if tracesReceiver != nil {
		components = append(components, tracesReceiver)
	}

	metricsReceiver, err := r.factory.CreateMetricsReceiver(r.ctx, settings, receiverConfig, nextMetrics)
	if err != nil && !errors.Is(err, otelcomponent.ErrDataTypeIsNotSupported) {
		return err
	} else if metricsReceiver != nil {
		components = append(components, metricsReceiver)
	}

	logsReceiver, err := r.factory.CreateLogsReceiver(r.ctx, settings, receiverConfig, nextLogs)
	if err != nil && !errors.Is(err, otelcomponent.ErrDataTypeIsNotSupported) {
		return err
	} else if logsReceiver != nil {
		components = append(components, logsReceiver)
	}

	// Schedule the components to run once our component is running.
	r.sched.Schedule(host, components...)
	return nil
}

// CurrentHealth implements component.HealthComponent.
func (r *Receiver) CurrentHealth() component.Health {
	return r.sched.CurrentHealth()
}
