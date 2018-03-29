package public

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
	pb "github.com/runconduit/conduit/controller/gen/public"
	log "github.com/sirupsen/logrus"
)

func (s *grpcServer) StatV2(ctx context.Context, req *pb.MetricRequestV2) (*pb.MetricResponseV2, error) {
	var err error
	resultsCh := make(chan metricResultV2)
	metrics := make([]*pb.MetricSeriesV2, 0)

	// kick off requests
	for _, metric := range req.Metrics {
		go func(metric pb.MetricName) { resultsCh <- s.queryMetric2(ctx, req, metric) }(metric)
	}

	// process results
	for _ = range req.Metrics {
		result := <-resultsCh
		if result.err != nil {
			log.Errorf("Stat -> queryMetric2 failed with: %s", result.err)
			err = result.err
		} else {
			for i := range result.series {
				metrics = append(metrics, &result.series[i])
			}
		}
	}

	// if an error occurred, return the error, along with partial results
	return &pb.MetricResponseV2{Metrics: metrics}, err
}

func (s *grpcServer) queryMetric2(ctx context.Context, req *pb.MetricRequestV2, metric pb.MetricName) metricResultV2 {
	result := metricResultV2{}

	switch metric {
	case pb.MetricName_REQUEST_RATE:
		result.series, result.err = s.requestRate2(ctx, req)
	default:
		result.series = nil
		result.err = fmt.Errorf("unsupported metric: %s", metric)
	}

	log.Info(result.series)
	return result
}

type metricFn func(context.Context, string) promResult

func (s *grpcServer) allResourcesRequestRate(ctx context.Context, namespace string) promResult {
	var err error
	resources := []metricFn{s.deployRequestRate, s.podRequestRate} // todo add all resources
	resultsCh := make(chan promResult)
	metrics := make([]*labelledSample, 0)

	// kick off requests
	for _, getFn := range resources {
		go func() { resultsCh <- getFn(ctx, namespace) }()
	}
	// process results
	for _ = range resources {
		metricsResult := <-resultsCh
		if metricsResult.err != nil {
			log.Errorf("Stat -> queryMetric2 -> allResourcesRequestRate failed with: %s", metricsResult.err)
			err = metricsResult.err
		} else {
			metrics = append(metrics, metricsResult.res...)
		}
	}

	return promResult{res: metrics, err: err}
}

func (s *grpcServer) requestRate2(ctx context.Context, req *pb.MetricRequestV2) ([]pb.MetricSeriesV2, error) {
	var result promResult

	switch req.Resource {
	case "all":
		result = s.allResourcesRequestRate(ctx, req.Namespace)
	case "deployments":
		result = s.deployRequestRate(ctx, req.Namespace)
	case "pods":
		result = s.podRequestRate(ctx, req.Namespace)
	default:
		result.err = errors.New("Invalid resource specified")
	}

	if result.err != nil {
		return nil, result.err
	}

	return processRequestRateV2(result.res), nil
}

func (s *grpcServer) podRequestRate(ctx context.Context, namespace string) promResult {
	query := fmt.Sprintf(reqTotalQuery, namespace, "k8s_pod_template_hash") // k8s_pod_template_hash is grouped pods; use pod name when available
	return s.queryProm(ctx, query)
}

func (s *grpcServer) deployRequestRate(ctx context.Context, namespace string) promResult {
	query := fmt.Sprintf(reqTotalQuery, namespace, "k8s_deployment")
	return s.queryProm(ctx, query)
}

func (s *grpcServer) queryProm(ctx context.Context, query string) promResult {
	result := promResult{}
	queryRsp, err := s.QueryProm(ctx, query)
	if err != nil {
		result.err = err
		return result
	}
	result.res = queryRsp
	return result
}

func (s *grpcServer) QueryProm(ctx context.Context, query string) ([]*labelledSample, error) {
	log.Debugf("Query request: %+v", query)
	end := time.Now()
	samples := make([]*labelledSample, 0)

	// single data point (aka summary) query
	res, err := s.prometheusAPI.Query(ctx, query, end)
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

func processRequestRateV2(samples []*labelledSample) []pb.MetricSeriesV2 {
	result := make([]pb.MetricSeriesV2, 0)

	for _, s := range samples {
		datapoint := pb.MetricDatapoint{
			Value:       &pb.MetricValue{Value: &pb.MetricValue_Gauge{Gauge: s.Value.Value}},
			TimestampMs: s.Value.TimestampMs,
		}
		series := pb.MetricSeriesV2{
			Name:       pb.MetricName_REQUEST_RATE,
			Datapoints: []*pb.MetricDatapoint{&datapoint},
			Metadata:   s.Labels,
		}
		result = append(result, series)
	}

	return result
}
