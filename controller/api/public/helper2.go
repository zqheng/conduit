package public

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

const (
	reqTotalQuery = "sum(irate(request_total{namepace=\"%s\"}[10m])) by (%s)"
)

type promResult struct {
	res []*labelledSample
	err error
}
type sampleVal struct {
	Value       float64 `json:"value"`
	TimestampMs int64   `json:"timestamp_ms"`
}
type labelledSample struct {
	Labels map[string]string `json:"labels"`
	Value  *sampleVal        `json:"values"`
}

func (h *handler) podRequestRate(ctx context.Context, namespace string) promResult {
	query := fmt.Sprintf(reqTotalQuery, namespace, "k8s_pod_template_hash") // k8s_pod_template_hash is grouped pods; use pod name when available
	return h.queryProm(ctx, query)
}

func (h *handler) deployRequestRate(ctx context.Context, namespace string) promResult {
	query := fmt.Sprintf(reqTotalQuery, namespace, "k8s_deployment")
	return h.queryProm(ctx, query)
}

func (h *handler) queryProm(ctx context.Context, query string) promResult {
	result := promResult{}
	queryRsp, err := h.QueryProm(ctx, query)
	if err != nil {
		result.err = err
		return result
	}
	result.res = queryRsp
	return result
}

func (h *handler) QueryProm(ctx context.Context, query string) ([]*labelledSample, error) {
	log.Debugf("Query request: %+v", query)
	end := time.Now()
	samples := make([]*labelledSample, 0)

	// single data point (aka summary) query
	res, err := h.prometheusAPI.Query(ctx, query, end)
	if err != nil {
		log.Errorf("Query(%+v, %+v) failed with: %+v", query, end, err)
		return nil, err
	}
	log.Debugf("Query response: %+v", res)

	if res.Type() != model.ValVector {
		err = fmt.Errorf("Unexpected query result type (expected Vector): %s", res.Type())
		log.Error(err)
		return nil, err
	}

	for _, s := range res.(model.Vector) {
		samples = append(samples, convertSample(s))
	}

	return samples, nil
}

func convertSample(sample *model.Sample) *labelledSample {
	value := sampleVal{
		Value:       float64(sample.Value),
		TimestampMs: int64(sample.Timestamp),
	}

	return &labelledSample{Value: &value, Labels: metricToMap(sample.Metric)}
}

func metricToMap(metric model.Metric) map[string]string {
	labels := make(map[string]string)
	for k, v := range metric {
		labels[string(k)] = string(v)
	}
	return labels
}
