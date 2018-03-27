package public

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	promApi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/golang/protobuf/jsonpb"
	common "github.com/runconduit/conduit/controller/gen/common"
	healthcheckPb "github.com/runconduit/conduit/controller/gen/common/healthcheck"
	tapPb "github.com/runconduit/conduit/controller/gen/controller/tap"
	telemPb "github.com/runconduit/conduit/controller/gen/controller/telemetry"
	pb "github.com/runconduit/conduit/controller/gen/public"
	"github.com/runconduit/conduit/controller/util"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
)

var (
	jsonMarshaler   = jsonpb.Marshaler{EmitDefaults: true}
	jsonUnmarshaler = jsonpb.Unmarshaler{}
	statPath        = fullUrlPathFor("Stat")
	statV2Path      = fullUrlPathFor("StatV2")
	versionPath     = fullUrlPathFor("Version")
	listPodsPath    = fullUrlPathFor("ListPods")
	tapPath         = fullUrlPathFor("Tap")
	selfCheckPath   = fullUrlPathFor("SelfCheck")
	newStatPath     = fullUrlPathFor("New")
)

type handler struct {
	grpcServer          pb.ApiServer
	k8sClient           *kubernetes.Clientset
	prometheusAPI       promv1.API
	controllerNamespace string
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.WithFields(log.Fields{
		"req.Method": req.Method, "req.URL": req.URL, "req.Form": req.Form,
	}).Debugf("Serving %s %s", req.Method, req.URL.Path)
	// Validate request method
	// if req.Method != http.MethodPost {
	// 	writeErrorToHttpResponse(w, fmt.Errorf("POST required"))
	// 	return
	// }

	// Serve request
	switch req.URL.Path {
	case statPath:
		h.handleStat(w, req)
	case statV2Path:
		h.handleStatV2(w, req)
	case newStatPath:
		h.handleNewStat(w, req)
	case versionPath:
		h.handleVersion(w, req)
	case listPodsPath:
		h.handleListPods(w, req)
	case tapPath:
		h.handleTap(w, req)
	case selfCheckPath:
		h.handleSelfCheck(w, req)
	default:
		http.NotFound(w, req)
	}

}

func (h *handler) handleStat(w http.ResponseWriter, req *http.Request) {
	var protoRequest pb.MetricRequest
	err := httpRequestToProto(req, &protoRequest)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}

	rsp, err := h.grpcServer.Stat(req.Context(), &protoRequest)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}

	err = writeProtoToHttpResponse(w, rsp)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}
}

func (h *handler) handleStatV2(w http.ResponseWriter, req *http.Request) {
	var protoRequest pb.MetricRequestV2

	ns := req.URL.Query().Get("namespace") // remove for actual
	protoRequest = pb.MetricRequestV2{
		Metrics:   []pb.MetricName{pb.MetricName_REQUEST_RATE},
		Namespace: ns,
	}
	// err := httpRequestToProto(req, &protoRequest)
	// if err != nil {
	// 	writeErrorToHttpResponse(w, err)
	// 	return
	// }

	rsp, err := h.grpcServer.StatV2(req.Context(), &protoRequest)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}

	err = writeProtoToHttpResponse(w, rsp)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}
}

func (h *handler) handleNewStat(w http.ResponseWriter, req *http.Request) {
	ns := req.URL.Query().Get("namespace")

	var err error
	var result []byte

	switch resource := req.URL.Query().Get("resource"); resource {
	case "all":
		rsp, err := h.getAllResourceMetrics(req.Context(), ns)
		if err != nil {
			writeErrorToHttpResponse(w, err)
			return
		}
		result, err = json.Marshal(rsp)
	case "deployments":
		rsp, err := h.getDeploymentMetrics(req.Context(), ns)
		if err != nil {
			writeErrorToHttpResponse(w, err)
			return
		}
		result, err = json.Marshal(rsp)
	case "pods":
		rsp, err := h.getPodMetrics(req.Context(), ns)
		if err != nil {
			writeErrorToHttpResponse(w, err)
			return
		}
		result, err = json.Marshal(rsp)
	default:
		writeErrorToHttpResponse(w, errors.New("specify a resource type"))
		return
	}

	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

func (h *handler) handleVersion(w http.ResponseWriter, req *http.Request) {
	var protoRequest pb.Empty
	err := httpRequestToProto(req, &protoRequest)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}

	rsp, err := h.grpcServer.Version(req.Context(), &protoRequest)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}

	err = writeProtoToHttpResponse(w, rsp)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}
}

func (h *handler) handleSelfCheck(w http.ResponseWriter, req *http.Request) {
	var protoRequest healthcheckPb.SelfCheckRequest
	err := httpRequestToProto(req, &protoRequest)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}

	rsp, err := h.grpcServer.SelfCheck(req.Context(), &protoRequest)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}

	err = writeProtoToHttpResponse(w, rsp)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}
}

func (h *handler) handleListPods(w http.ResponseWriter, req *http.Request) {
	var protoRequest pb.Empty
	err := httpRequestToProto(req, &protoRequest)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}

	rsp, err := h.grpcServer.ListPods(req.Context(), &protoRequest)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}

	err = writeProtoToHttpResponse(w, rsp)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}
}

func (h *handler) handleTap(w http.ResponseWriter, req *http.Request) {
	flushableWriter, err := newStreamingWriter(w)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}

	var protoRequest pb.TapRequest
	err = httpRequestToProto(req, &protoRequest)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}

	server := tapServer{w: flushableWriter, req: req}
	err = h.grpcServer.Tap(&protoRequest, server)
	if err != nil {
		writeErrorToHttpResponse(w, err)
		return
	}
}

type tapServer struct {
	w   flushableResponseWriter
	req *http.Request
}

func (s tapServer) Send(msg *common.TapEvent) error {
	err := writeProtoToHttpResponse(s.w, msg)
	if err != nil {
		writeErrorToHttpResponse(s.w, err)
		return err
	}

	s.w.Flush()
	return nil
}

// satisfy the pb.Api_TapServer interface
func (s tapServer) SetHeader(metadata.MD) error  { return nil }
func (s tapServer) SendHeader(metadata.MD) error { return nil }
func (s tapServer) SetTrailer(metadata.MD)       { return }
func (s tapServer) Context() context.Context     { return s.req.Context() }
func (s tapServer) SendMsg(interface{}) error    { return nil }
func (s tapServer) RecvMsg(interface{}) error    { return nil }

func fullUrlPathFor(method string) string {
	return ApiRoot + ApiPrefix + method
}

func NewServer(addr string, k8sClient *kubernetes.Clientset, prometheusClient promApi.Client, telemetryClient telemPb.TelemetryClient, tapClient tapPb.TapClient, controllerNamespace string) *http.Server {
	baseHandler := &handler{
		grpcServer:          newGrpcServer(k8sClient, prometheusClient, telemetryClient, tapClient, controllerNamespace),
		k8sClient:           k8sClient,
		prometheusAPI:       promv1.NewAPI(prometheusClient),
		controllerNamespace: controllerNamespace,
	}

	instrumentedHandler := util.WithTelemetry(baseHandler)

	return &http.Server{
		Addr:    addr,
		Handler: instrumentedHandler,
	}
}
