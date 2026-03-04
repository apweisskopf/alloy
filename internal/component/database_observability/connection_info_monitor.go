package database_observability

import (
	"context"
	"database/sql"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ConnectionCheckInterval is how often the connection_info collector pings the DB to verify connectivity.
const ConnectionCheckInterval = 60 * time.Second

// ConnectionChecksThreshold is the number of consecutive failed pings before unregistering the metric,
// and the number of consecutive successful pings before re-registering it after a disconnect.
const ConnectionChecksThreshold = 3

// ConnectionInfoMonitorConfig optionally overrides the default check interval and threshold.
// Used by tests to run the monitor with shorter intervals. If nil, defaults are used.
type ConnectionInfoMonitorConfig struct {
	CheckInterval   time.Duration
	ChecksThreshold int
}

// ConnectionInfoMonitorState holds state for the connection_info ping loop.
// Callers should initialize MetricRegistered to true when the metric is first set.
// If Threshold is 0, ConnectionChecksThreshold is used.
type ConnectionInfoMonitorState struct {
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	MetricRegistered     bool
	Threshold            int // 0 means use ConnectionChecksThreshold
}

// ConnectionInfoMonitorTick runs one connectivity check: pings db, then updates state and registry
// (unregisters after threshold consecutive failures, re-registers after threshold consecutive
// successes). Call this from an existing tick loop (e.g. the component's Run goroutine) to avoid
// creating a separate goroutine.
// labelValues must contain exactly 6 values: provider_name, provider_region, provider_account,
// db_instance_identifier, engine, engine_version.
func ConnectionInfoMonitorTick(ctx context.Context, db *sql.DB, registry *prometheus.Registry, infoMetric *prometheus.GaugeVec, labelValues []string, state *ConnectionInfoMonitorState) {
	if state == nil {
		return
	}
	threshold := state.Threshold
	if threshold <= 0 {
		threshold = ConnectionChecksThreshold
	}
	if err := db.PingContext(ctx); err != nil {
		state.ConsecutiveFailures++
		state.ConsecutiveSuccesses = 0
		if state.MetricRegistered && state.ConsecutiveFailures >= threshold {
			registry.Unregister(infoMetric)
			state.MetricRegistered = false
			state.ConsecutiveFailures = 0
		}
	} else {
		state.ConsecutiveFailures = 0
		if state.MetricRegistered {
			state.ConsecutiveSuccesses = 0
		} else {
			state.ConsecutiveSuccesses++
			if state.ConsecutiveSuccesses >= threshold {
				registry.MustRegister(infoMetric)
				infoMetric.WithLabelValues(labelValues[0], labelValues[1], labelValues[2], labelValues[3], labelValues[4], labelValues[5]).Set(1)
				state.MetricRegistered = true
				state.ConsecutiveSuccesses = 0
			}
		}
	}
}

// RunConnectionInfoMonitor runs the connection check loop in a goroutine. Use this only when you
// cannot integrate ConnectionInfoMonitorTick into an existing tick loop (e.g. in tests). Production
// MySQL and Postgres components call ConnectionInfoMonitorTick from their existing Run goroutine.
func RunConnectionInfoMonitor(ctx context.Context, db *sql.DB, registry *prometheus.Registry, infoMetric *prometheus.GaugeVec, labelValues []string, onStopped func(), config *ConnectionInfoMonitorConfig) (cancel context.CancelFunc) {
	interval := ConnectionCheckInterval
	threshold := ConnectionChecksThreshold
	if config != nil {
		if config.CheckInterval > 0 {
			interval = config.CheckInterval
		}
		if config.ChecksThreshold > 0 {
			threshold = config.ChecksThreshold
		}
	}
	ctx, cancel = context.WithCancel(ctx)
	state := &ConnectionInfoMonitorState{MetricRegistered: true, Threshold: threshold}
	go func() {
		defer onStopped()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			ConnectionInfoMonitorTick(ctx, db, registry, infoMetric, labelValues, state)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return cancel
}
