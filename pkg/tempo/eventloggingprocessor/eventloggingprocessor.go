package eventloggingprocessor

import (
	"context"
	"errors"
	"fmt"
	"time"

	util "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-logfmt/logfmt"
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/translator/conventions"
	"go.uber.org/atomic"

	"github.com/grafana/agent/pkg/loki"
	"github.com/grafana/agent/pkg/tempo/contextkeys"
)

const (
	defaultLokiTag     = "tempo_event"
	defaultServiceKey  = "svc"
	defaultSpanNameKey = "span"
	defaultTraceIDKey  = "tid"
	defaultSpanIDKey = "sid"
	defaultEventNameKey = "msg"

	defaultTimeout = time.Millisecond

	typeEvent model.LabelValue = "event"
)

type eventLoggingProcessor struct {
	nextConsumer consumer.TracesConsumer

	cfg          *EventLoggingConfig
	logToStdout  bool
	lokiInstance *loki.Instance
	svcNames     map[string]bool
	done         atomic.Bool

	logger log.Logger
}

func newTraceProcessor(nextConsumer consumer.TracesConsumer, cfg *EventLoggingConfig) (component.TracesProcessor, error) {
	logger := log.With(util.Logger, "component", "tempo event logging")

	if nextConsumer == nil {
		return nil, componenterror.ErrNilNextConsumer
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}

	if cfg.Backend == "" {
		cfg.Backend = BackendStdout
	}

	if cfg.Backend != BackendLoki && cfg.Backend != BackendStdout {
		return nil, errors.New("eventLoggingProcessor requires a backend of type 'loki' or 'stdout'")
	}

	logToStdout := false
	if cfg.Backend == BackendStdout {
		logToStdout = true
	}

	var svcNames map[string]bool
	if len(cfg.SvcNames) > 0 {
		svcNames = make(map[string]bool, len(cfg.SvcNames))
		for _, v := range cfg.SvcNames {
			svcNames[v] = true
		}
	}

	cfg.Overrides.LokiTag = override(cfg.Overrides.LokiTag, defaultLokiTag)
	cfg.Overrides.ServiceKey = override(cfg.Overrides.ServiceKey, defaultServiceKey)
	cfg.Overrides.SpanNameKey = override(cfg.Overrides.SpanNameKey, defaultSpanNameKey)
	cfg.Overrides.TraceIDKey = override(cfg.Overrides.TraceIDKey, defaultTraceIDKey)
	cfg.Overrides.SpanIDKey = override(cfg.Overrides.SpanIDKey, defaultSpanIDKey)
	cfg.Overrides.EventNameKey = override(cfg.Overrides.EventNameKey, defaultEventNameKey)

	return &eventLoggingProcessor{
		nextConsumer: nextConsumer,
		cfg:          cfg,
		logToStdout:  logToStdout,
		logger:       logger,
		done:         atomic.Bool{},
		svcNames:     svcNames,
	}, nil
}

func (p *eventLoggingProcessor) ConsumeTraces(ctx context.Context, td pdata.Traces) error {
	rsLen := td.ResourceSpans().Len()

	for i := 0; i < rsLen; i++ {
		rs := td.ResourceSpans().At(i)
		ilsLen := rs.InstrumentationLibrarySpans().Len()

		for j := 0; j < ilsLen; j++ {
			ils := rs.InstrumentationLibrarySpans().At(j)
			spanLen := ils.Spans().Len()

			for k := 0; k < spanLen; k++ {
				span := ils.Spans().At(k)

				if span.Events().Len() == 0 {
					continue
				}

				var svc string
				svcAtt, ok := rs.Resource().Attributes().Get(conventions.AttributeServiceName)
				if ok {
					svc = svcAtt.StringVal()
				}

				if p.svcNames != nil && !p.svcNames[svc] {
					continue
				}


				eventLen := span.Events().Len()
				for l := 0; l < eventLen; l++ {
					event := span.Events().At(l)
					kvs := p.spanKeyVals(span)
					kvs = append(kvs, p.processKeyVals(rs.Resource(), svc)...)
					kvs = append(kvs, p.eventKeyVals(event)...)
					if err := p.exportToLoki(kvs...); err != nil {
						level.Error(p.logger).Log("msg", "failed to export event", "err", err)
					}
				}

				span.Events().Resize(0)
				span.SetDroppedEventsCount(uint32(span.Events().Len()))
			}
		}
	}

	return p.nextConsumer.ConsumeTraces(ctx, td)
}

func (p *eventLoggingProcessor) GetCapabilities() component.ProcessorCapabilities {
	return component.ProcessorCapabilities{MutatesConsumedData: true}
}

// Start is invoked during service startup.
func (p *eventLoggingProcessor) Start(ctx context.Context, h component.Host) error {
	loki := ctx.Value(contextkeys.Loki).(*loki.Loki)

	if loki == nil {
		return fmt.Errorf("key does not contain a Loki instance")
	}

	if !p.logToStdout {
		p.lokiInstance = loki.Instance(p.cfg.LokiName)
		if p.lokiInstance == nil {
			return fmt.Errorf("loki instance %s not found", p.cfg.LokiName)
		}
	}
	return nil
}

// Shutdown is invoked during service shutdown.
func (p *eventLoggingProcessor) Shutdown(context.Context) error {
	p.done.Store(true)

	return nil
}

func (p *eventLoggingProcessor) processKeyVals(resource pdata.Resource, svc string) []interface{} {
	atts := make([]interface{}, 0, 2) // 2 for service name
	atts = append(atts, p.cfg.Overrides.ServiceKey, svc)

	rsAtts := resource.Attributes()
	for _, name := range p.cfg.ProcessAttributes {
		att, ok := rsAtts.Get(name)
		if ok {
			// name/key val pairs
			atts = append(atts, name)
			atts = append(atts, attributeValue(att))
		}
	}

	return atts
}

func (p *eventLoggingProcessor) spanKeyVals(span pdata.Span) []interface{} {
	atts := make([]interface{}, 0, 6) // 6 for trace id, span name and span id

	atts = append(atts,
		p.cfg.Overrides.TraceIDKey, span.TraceID().HexString(),
		p.cfg.Overrides.SpanIDKey, span.SpanID().HexString(),
		p.cfg.Overrides.SpanNameKey, span.Name(),
	)

	for _, name := range p.cfg.SpanAttributes {
		if att, ok := span.Attributes().Get(name); ok {
			atts = append(atts, name,  attributeValue(att))
		}
	}

	return atts
}

func (p *eventLoggingProcessor) eventKeyVals(event pdata.SpanEvent) []interface{} {
	atts := make([]interface{}, 0, 2+event.Attributes().Len()*2)

	atts = append(atts,
		p.cfg.Overrides.EventNameKey, event.Name(),
	)
	event.Attributes().ForEach(func(name string, value pdata.AttributeValue) {
			atts = append(atts, name,  attributeValue(value))
	})

	return atts
}

func (p *eventLoggingProcessor) exportToLoki(keyvals ...interface{}) error {
	if p.done.Load() {
		return nil
	}

	line, err := logfmt.MarshalKeyvals(keyvals...)
	if err != nil {
		return err
	}

	// if we're logging to stdout, log and bail
	if p.logToStdout {
		level.Info(p.logger).Log(keyvals...)
		return nil
	}

	sent := p.lokiInstance.SendEntry(api.Entry{
		Labels: model.LabelSet{
			model.LabelName(p.cfg.Overrides.LokiTag): typeEvent,
		},
		Entry: logproto.Entry{
			Timestamp: time.Now(),
			Line:      string(line),
		},
	}, p.cfg.Timeout)

	if !sent {
		return fmt.Errorf("loki entry not sent")
	}

	return nil
}

func attributeValue(att pdata.AttributeValue) interface{} {
	switch att.Type() {
	case pdata.AttributeValueSTRING:
		return att.StringVal()
	case pdata.AttributeValueINT:
		return att.IntVal()
	case pdata.AttributeValueDOUBLE:
		return att.DoubleVal()
	case pdata.AttributeValueBOOL:
		return att.BoolVal()
	case pdata.AttributeValueMAP:
		return att.MapVal()
	case pdata.AttributeValueARRAY:
		return att.ArrayVal()
	}
	return nil
}

func override(cfgValue string, defaultValue string) string {
	if cfgValue == "" {
		return defaultValue
	}
	return cfgValue
}
