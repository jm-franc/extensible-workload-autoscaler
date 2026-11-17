package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type fakeProviderLister struct {
	classes map[string]*xasv1.MetricProviderClass
}

func (f *fakeProviderLister) List(selector labels.Selector) ([]*xasv1.MetricProviderClass, error) {
	var out []*xasv1.MetricProviderClass
	for _, c := range f.classes {
		out = append(out, c)
	}
	return out, nil
}

func (f *fakeProviderLister) Get(name string) (*xasv1.MetricProviderClass, error) {
	c, ok := f.classes[name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "xas.io", Resource: "metricproviderclasses"}, name)
	}
	return c, nil
}

type fakeXASServerClient struct {
	pb.XASServerClient
	policies []*pb.Policy
	ingested []*pb.IngestMetricsRequest
}

func (f *fakeXASServerClient) ListPolicies(ctx context.Context, in *pb.ListPoliciesRequest, opts ...grpc.CallOption) (*pb.ListPoliciesResponse, error) {
	return pb.ListPoliciesResponse_builder{
		Policies: f.policies,
	}.Build(), nil
}

func (f *fakeXASServerClient) IngestMetrics(ctx context.Context, in *pb.IngestMetricsRequest, opts ...grpc.CallOption) (*pb.IngestMetricsResponse, error) {
	f.ingested = append(f.ingested, in)
	return pb.IngestMetricsResponse_builder{Success: new(true)}.Build(), nil
}

func TestScrapeAndSend(t *testing.T) {
	ingestNow := time.Unix(1720000100, 0)
	sampleTS := time.Unix(1720000060, 0)

	dtSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		val := 320.0
		_ = json.NewEncoder(w).Encode(metricDataResponse{
			Result: []metricSeriesCollection{
				{
					MetricID: r.URL.Query().Get("metricSelector"),
					Data: []metricSeries{
						{
							DimensionMap: map[string]string{"service": "checkout"},
							Timestamps:   []int64{sampleTS.UnixMilli()},
							Values:       []*float64{&val},
						},
					},
				},
			},
		})
	}))
	defer dtSrv.Close()

	lister := &fakeProviderLister{
		classes: map[string]*xasv1.MetricProviderClass{
			"dynatrace": {
				ObjectMeta: metav1.ObjectMeta{Name: "dynatrace"},
				Spec: xasv1.MetricProviderClassSpec{
					Type: ProviderType,
					Config: map[string]string{
						"apiUrl": dtSrv.URL,
					},
				},
			},
			"kubelet": {
				ObjectMeta: metav1.ObjectMeta{Name: "kubelet"},
				Spec: xasv1.MetricProviderClassSpec{
					Type: "Kubelet",
				},
			},
		},
	}

	grpcFake := &fakeXASServerClient{
		policies: []*pb.Policy{
			pb.Policy_builder{
				Id: pb.PolicyId_builder{
					ClusterName: new("default"),
					Namespace:   new("shop"),
					Name:        new("checkout-scaler"),
				}.Build(),
				Metrics: []*pb.MetricDefinition{
					pb.MetricDefinition_builder{
						Name:     new("cpu"),
						Provider: new("kubelet"),
					}.Build(),
					pb.MetricDefinition_builder{
						Name:     new("checkout_rps"),
						Provider: new("dynatrace"),
						Params: map[string]string{
							"metricSelector": "builtin:service.requestCount.total:splitBy()",
						},
					}.Build(),
				},
				RecommenderMetrics: map[string]*pb.MetricDefinitionList{
					"rec-1": pb.MetricDefinitionList_builder{
						Definitions: []*pb.MetricDefinition{
							pb.MetricDefinition_builder{
								Name:            new("owned_latency"),
								RecommenderName: new("rec-1"),
								Provider:        new("dynatrace"),
								Params: map[string]string{
									"metricSelector": "builtin:service.response.time",
								},
							}.Build(),
						},
					}.Build(),
				},
			}.Build(),
		},
	}

	p := &DynatraceMetricsProvider{
		providerLister:  lister,
		grpcClient:      grpcFake,
		httpClient:      dtSrv.Client(),
		clusterName:     "default",
		defaultAPIToken: "test-token",
		now:             func() time.Time { return ingestNow },
	}

	p.scrapeAndSend(context.Background())

	if len(grpcFake.ingested) != 1 {
		t.Fatalf("IngestMetrics calls = %d, want 1", len(grpcFake.ingested))
	}

	want := pb.IngestMetricsRequest_builder{
		ClusterName: new("default"),
		Timestamp:   timestamppb.New(ingestNow),
		PolicyMetrics: []*pb.PolicyMetricBatch{
			pb.PolicyMetricBatch_builder{
				Namespace: new("shop"),
				Name:      new("checkout-scaler"),
				Batches: []*pb.MetricBatch{
					pb.MetricBatch_builder{
						PodName: new(""),
						Samples: []*pb.MetricSample{
							pb.MetricSample_builder{
								Name:      new("checkout_rps"),
								Labels:    map[string]string{"service": "checkout"},
								Gauge:     new(320.0),
								Timestamp: timestamppb.New(sampleTS),
							}.Build(),
						},
					}.Build(),
					pb.MetricBatch_builder{
						PodName: new(""),
						Samples: []*pb.MetricSample{
							pb.MetricSample_builder{
								Name:            new("owned_latency"),
								RecommenderName: new("rec-1"),
								Labels:          map[string]string{"service": "checkout"},
								Gauge:           new(320.0),
								Timestamp:       timestamppb.New(sampleTS),
							}.Build(),
						},
					}.Build(),
				},
			}.Build(),
		},
	}.Build()

	if diff := cmp.Diff(want, grpcFake.ingested[0], protocmp.Transform()); diff != "" {
		t.Errorf("IngestMetricsRequest mismatch (-want +got):\n%s", diff)
	}
}

func TestRunLoopAndErrorPaths(t *testing.T) {
	badJSONSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("metricSelector") {
		case "bad_json":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{not-json"))
		case "err_500":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("internal server error"))
		default:
			v := 10.0
			_ = json.NewEncoder(w).Encode(metricDataResponse{
				Warnings: []string{"top-level warning"},
				Result: []metricSeriesCollection{
					{
						MetricID: "pod_metric",
						Warnings: []string{"series warning"},
						Data: []metricSeries{
							{
								DimensionMap: map[string]string{"k8s.pod.name": "pod-a"},
								Timestamps:   []int64{time.Unix(1720000000, 0).UnixMilli()},
								Values:       []*float64{&v},
							},
						},
					},
				},
			})
		}
	}))
	defer badJSONSrv.Close()

	lister := &fakeProviderLister{
		classes: map[string]*xasv1.MetricProviderClass{
			"dynatrace": {
				ObjectMeta: metav1.ObjectMeta{Name: "dynatrace"},
				Spec: xasv1.MetricProviderClassSpec{
					Type: ProviderType,
					Config: map[string]string{
						"apiUrl": badJSONSrv.URL,
					},
				},
			},
			"no-url": {
				ObjectMeta: metav1.ObjectMeta{Name: "no-url"},
				Spec: xasv1.MetricProviderClassSpec{
					Type: ProviderType,
				},
			},
		},
	}

	grpcFake := &fakeXASServerClient{
		policies: []*pb.Policy{
			pb.Policy_builder{
				Id: pb.PolicyId_builder{
					ClusterName: new("default"),
					Namespace:   new("ns"),
					Name:        new("pol"),
				}.Build(),
				Metrics: []*pb.MetricDefinition{
					// Missing class
					pb.MetricDefinition_builder{Name: new("m0"), Provider: new("missing-class")}.Build(),
					// Missing metricSelector
					pb.MetricDefinition_builder{Name: new("m1"), Provider: new("dynatrace")}.Build(),
					// Missing apiUrl
					pb.MetricDefinition_builder{Name: new("m2"), Provider: new("no-url"), Params: map[string]string{"metricSelector": "x"}}.Build(),
					// Invalid metricType
					pb.MetricDefinition_builder{Name: new("m3"), Provider: new("dynatrace"), Params: map[string]string{"metricSelector": "x", "metricType": "histogram"}}.Build(),
					// Malformed JSON
					pb.MetricDefinition_builder{Name: new("m4"), Provider: new("dynatrace"), Params: map[string]string{"metricSelector": "bad_json"}}.Build(),
					// HTTP 500 plain text
					pb.MetricDefinition_builder{Name: new("m5"), Provider: new("dynatrace"), Params: map[string]string{"metricSelector": "err_500"}}.Build(),
					// Container scope missing container dimension -> skipped
					pb.MetricDefinition_builder{Name: new("m6"), Provider: new("dynatrace"), Scope: new("Container"), Params: map[string]string{"metricSelector": "ok"}}.Build(),
					// Pod scope succeeds
					pb.MetricDefinition_builder{Name: new("m7"), Provider: new("dynatrace"), Scope: new("Pod"), Params: map[string]string{"metricSelector": "ok"}}.Build(),
				},
			}.Build(),
		},
	}

	p := &DynatraceMetricsProvider{
		providerLister:  lister,
		grpcClient:      grpcFake,
		httpClient:      badJSONSrv.Client(),
		clusterName:     "default",
		scrapeInterval:  10 * time.Millisecond,
		defaultAPIToken: "tok",
		now:             func() time.Time { return time.Unix(1720000100, 0) },
	}

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	p.Run(ctx)

	if len(grpcFake.ingested) == 0 {
		t.Fatal("expected at least one IngestMetrics call from Run loop")
	}
	batches := grpcFake.ingested[0].GetPolicyMetrics()[0].GetBatches()
	if len(batches) != 1 || batches[0].GetPodName() != "pod-a" || batches[0].GetSamples()[0].GetName() != "m7" {
		t.Errorf("unexpected ingested batches: %v", batches)
	}
}
