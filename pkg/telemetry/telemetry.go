package telemetry

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	reportsMetric = "reports_total"
)

var (
	requestLabels = []string{"source_deployment", "target_deployment"}
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "requests_total",
			Help: "Total number of requests",
		},
		requestLabels,
	)

	responseLabels = append(requestLabels, []string{"http_status_code", "classification"}...)
	responsesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "responses_total",
			Help: "Total number of responses",
		},
		responseLabels,
	)

	responseLatencyBuckets = append(append(append(append(append(
		prometheus.LinearBuckets(1, 1, 5),
		prometheus.LinearBuckets(10, 10, 5)...),
		prometheus.LinearBuckets(100, 100, 5)...),
		prometheus.LinearBuckets(1000, 1000, 5)...),
		prometheus.LinearBuckets(10000, 10000, 5)...),
	)

	responseLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "response_latency_ms",
			Help:    "Response latency in milliseconds",
			Buckets: responseLatencyBuckets,
		},
		requestLabels,
	)

	reportsLabels = []string{"pod"}
	reportsTotal  = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: reportsMetric,
			Help: "Total number of telemetry reports received",
		},
		reportsLabels,
	)
)

func GeneratePromLabels() []string {
	kubeResourceTypes := []string{
		"job",
		"replica_set",
		"deployment",
		"daemon_set",
		"replication_controller",
		"namespace",
	}
	constantLabels := []string{
		"direction",
		"authority",
		"status_code",
		"grpc_status_code",
	}

	destinationLabels := make([]string, len(kubeResourceTypes))

	for i, label := range kubeResourceTypes {
		destinationLabels[i] = fmt.Sprintf("dst_%s", label)
	}
	return append(append(constantLabels, kubeResourceTypes...), destinationLabels...)
}
