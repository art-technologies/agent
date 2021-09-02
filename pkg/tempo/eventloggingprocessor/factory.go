package eventloggingprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

// TypeStr is the unique identifier for the Event Logging processor.
const TypeStr = "event_logging"

// Config holds the configuration for the Event Logging processor.
type Config struct {
	configmodels.ProcessorSettings `mapstructure:",squash"`

	LoggingConfig *EventLoggingConfig `mapstructure:"event_logging"`
}

// EventLoggingConfig holds config information for event logging
type EventLoggingConfig struct {
	Backend           string         `mapstructure:"backend" yaml:"backend"`
	LokiName          string         `mapstructure:"loki_name" yaml:"loki_name"`
	SpanAttributes    []string       `mapstructure:"span_attributes" yaml:"span_attributes"`
	ProcessAttributes []string       `mapstructure:"process_attributes" yaml:"process_attributes"`
	Overrides         OverrideConfig `mapstructure:"overrides" yaml:"overrides"`
	Timeout           time.Duration  `mapstructure:"timeout" yaml:"timeout"`
	SvcNames          []string       `mapstructure:"svc_names" yaml:"svc_names"`
}

// OverrideConfig contains overrides for various strings
type OverrideConfig struct {
	LokiTag      string `mapstructure:"loki_tag" yaml:"loki_tag"`
	ServiceKey   string `mapstructure:"service_key" yaml:"service_key"`
	SpanNameKey  string `mapstructure:"span_name_key" yaml:"span_name_key"`
	TraceIDKey   string `mapstructure:"trace_id_key" yaml:"trace_id_key"`
	SpanIDKey    string `mapstructure:"span_id_key" yaml:"span_id_key"`
	EventNameKey string `mapstructure:"event_name_key" yaml:"event_name_key"`
}

const (
	// BackendLoki is the backend config value for sending logs to a Loki pipeline
	BackendLoki = "loki"
	// BackendStdout is the backend config value for sending logs to stdout
	BackendStdout = "stdout"
)

// NewFactory returns a new factory for the Attributes processor.
func NewFactory() component.ProcessorFactory {
	return processorhelper.NewFactory(
		TypeStr,
		createDefaultConfig,
		processorhelper.WithTraces(createTraceProcessor),
	)
}

func createDefaultConfig() configmodels.Processor {
	return &Config{
		ProcessorSettings: configmodels.ProcessorSettings{
			TypeVal: TypeStr,
			NameVal: TypeStr,
		},
	}
}

func createTraceProcessor(
	_ context.Context,
	_ component.ProcessorCreateParams,
	cfg configmodels.Processor,
	nextConsumer consumer.TracesConsumer,
) (component.TracesProcessor, error) {
	oCfg := cfg.(*Config)

	return newTraceProcessor(nextConsumer, oCfg.LoggingConfig)
}
