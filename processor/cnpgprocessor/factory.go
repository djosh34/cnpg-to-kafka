package cnpgprocessor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"

	"github.com/djosh34/cnpg-to-kafka/internal/cnpg"
	"github.com/djosh34/cnpg-to-kafka/internal/event"
)

// NewFactory returns the factory of the cnpg processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(component.MustNewType("cnpg"), func() component.Config {
		return &Config{}
	}, processor.WithLogs(createLogs, component.StabilityLevelBeta))
}

type cnpgProcessor struct {
	cfg    *Config
	logger *zap.Logger
	// mu makes the processor handle one batch at a time, because the session
	// table has no lock.
	mu       sync.Mutex
	rules    event.Rules
	sessions *event.Sessions
}

func createLogs(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Logs) (processor.Logs, error) {
	p := &cnpgProcessor{cfg: cfg.(*Config), logger: set.Logger, sessions: event.NewSessions()}
	return processorhelper.NewLogs(ctx, set, cfg, next, p.processLogs,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(p.start),
	)
}

// start looks up the source IP address and sets the rules.
func (p *cnpgProcessor) start(ctx context.Context, _ component.Host) error {
	addresses, err := net.DefaultResolver.LookupHost(ctx, p.cfg.SourceHostname)
	if err == nil && len(addresses) == 0 {
		err = errors.New("no address")
	}
	if err != nil {
		return fmt.Errorf("look up source_hostname %q: %w", p.cfg.SourceHostname, err)
	}
	p.rules = event.Rules{
		ApplicationName:    p.cfg.AdditionalFields.ApplicationName,
		Host:               event.HostData{SourceHostname: p.cfg.SourceHostname, SourceIP: addresses[0]},
		HighPrivilegeRoles: p.cfg.HighPrivilegeRoles,
		TrustedConnections: p.cfg.TrustedConnections,
	}
	return nil
}

// processLogs keeps the records that make an event and sets the event in
// their attributes. It drops every other record, and a batch without events.
func (p *cnpgProcessor) processLogs(_ context.Context, logs plog.Logs) (plog.Logs, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, resource := range logs.ResourceLogs().All() {
		pod := podKey(resource.Resource().Attributes())
		for _, scope := range resource.ScopeLogs().All() {
			scope.LogRecords().RemoveIf(func(record plog.LogRecord) bool {
				return !p.process(pod, record)
			})
		}
	}
	if logs.LogRecordCount() == 0 {
		return logs, processorhelper.ErrSkipProcessingData
	}
	return logs, nil
}

// process reports whether a record carries an event after it.
func (p *cnpgProcessor) process(pod string, record plog.LogRecord) bool {
	// After a failed export the receiver sends the same batch again, with the
	// events that this processor already set.
	if event.HasAttributes(record.Attributes()) {
		return true
	}
	line, ok := cnpg.Parse([]byte(record.Body().AsString()))
	if !ok {
		return false
	}
	e, ok, err := p.rules.Make(p.sessions, pod, line.Record)
	if err != nil {
		p.logger.Warn("drop connection event with a log_time that does not parse", zap.Error(err))
		return false
	}
	if ok {
		e.SetAttributes(record.Attributes())
	}
	return ok
}

// podKey returns namespace/pod from the resource attributes that the
// receiver's container operator sets.
func podKey(resource pcommon.Map) string {
	get := func(key string) string {
		if value, ok := resource.Get(key); ok {
			return value.AsString()
		}
		return ""
	}
	return get("k8s.namespace.name") + "/" + get("k8s.pod.name")
}
