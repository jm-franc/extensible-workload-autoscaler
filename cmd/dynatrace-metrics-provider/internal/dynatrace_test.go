package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
)

type fakeKubeClient struct {
	kubernetes.Interface
	secrets []*corev1.Secret
	gets    atomic.Int32
}

type fakeCoreV1 struct {
	corev1client.CoreV1Interface
	parent *fakeKubeClient
}

type fakeSecrets struct {
	corev1client.SecretInterface
	parent    *fakeKubeClient
	namespace string
}

func newFakeKubeClient(secrets ...*corev1.Secret) *fakeKubeClient {
	return &fakeKubeClient{secrets: secrets}
}

func (f *fakeKubeClient) CoreV1() corev1client.CoreV1Interface {
	return &fakeCoreV1{parent: f}
}

func (c *fakeCoreV1) Secrets(namespace string) corev1client.SecretInterface {
	return &fakeSecrets{parent: c.parent, namespace: namespace}
}

func (s *fakeSecrets) Get(_ context.Context, name string, _ metav1.GetOptions) (*corev1.Secret, error) {
	s.parent.gets.Add(1)
	for _, sec := range s.parent.secrets {
		if sec.Namespace == s.namespace && sec.Name == name {
			return sec, nil
		}
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
}

func TestBuildQueryURL(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		params  queryParams
		want    string
		wantErr bool
	}{
		{
			name:    "SaaS base URL",
			baseURL: "https://mySampleEnv.live.dynatrace.com",
			params: queryParams{
				metricSelector: "builtin:host.cpu.usage",
				from:           "now-5m",
			},
			want: "https://mySampleEnv.live.dynatrace.com/api/v2/metrics/query?from=now-5m&metricSelector=builtin%3Ahost.cpu.usage",
		},
		{
			name:    "SaaS base URL with trailing slash",
			baseURL: "https://mySampleEnv.live.dynatrace.com/",
			params: queryParams{
				metricSelector: "builtin:host.cpu.usage",
				from:           "now-5m",
			},
			want: "https://mySampleEnv.live.dynatrace.com/api/v2/metrics/query?from=now-5m&metricSelector=builtin%3Ahost.cpu.usage",
		},
		{
			name:    "ActiveGate environment URL",
			baseURL: "https://myActiveGate.local:9999/e/env-123/",
			params: queryParams{
				metricSelector: "builtin:containers.cpu.usageMilliCores",
				entitySelector: `type("CLOUD_APPLICATION_INSTANCE")`,
				mzSelector:     `mzName("prod")`,
				resolution:     "1m",
				from:           "now-10m",
				to:             "now",
			},
			want: "https://myActiveGate.local:9999/e/env-123/api/v2/metrics/query?entitySelector=type%28%22CLOUD_APPLICATION_INSTANCE%22%29&from=now-10m&metricSelector=builtin%3Acontainers.cpu.usageMilliCores&mzSelector=mzName%28%22prod%22%29&resolution=1m&to=now",
		},
		{
			name:    "URL already ending in /api/v2",
			baseURL: "https://mySampleEnv.live.dynatrace.com/api/v2",
			params: queryParams{
				metricSelector: "ext:queue.depth",
			},
			want: "https://mySampleEnv.live.dynatrace.com/api/v2/metrics/query?metricSelector=ext%3Aqueue.depth",
		},
		{
			name:    "URL already ending in /api/v2/metrics/query",
			baseURL: "https://mySampleEnv.live.dynatrace.com/api/v2/metrics/query",
			params: queryParams{
				metricSelector: "ext:queue.depth",
			},
			want: "https://mySampleEnv.live.dynatrace.com/api/v2/metrics/query?metricSelector=ext%3Aqueue.depth",
		},
		{
			name:    "Missing scheme",
			baseURL: "mySampleEnv.live.dynatrace.com",
			params:  queryParams{metricSelector: "m"},
			wantErr: true,
		},
		{
			name:    "Unsupported scheme",
			baseURL: "ftp://mySampleEnv.live.dynatrace.com",
			params:  queryParams{metricSelector: "m"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildQueryURL(tc.baseURL, tc.params)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got URL %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("buildQueryURL() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestProcessDynatraceMetric_GlobalAndFiltering(t *testing.T) {
	fixedNow := time.Unix(1720000120, 0) // t = 120s
	t1 := time.Unix(1720000000, 0)       // t = 0s
	t2 := time.Unix(1720000060, 0)       // t = 60s
	tFuture := time.Unix(1720000180, 0)  // t = 180s (incomplete future Dynatrace slot)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Api-Token dt0c01.secret-token" {
			t.Errorf("Authorization header = %q, want %q", got, "Api-Token dt0c01.secret-token")
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("Accept header = %q, want %q", got, "application/json")
		}
		if r.URL.Path != "/api/v2/metrics/query" {
			t.Errorf("request path = %q, want /api/v2/metrics/query", r.URL.Path)
		}
		q := r.URL.Query()
		if got := q.Get("metricSelector"); got != "ext:sqs.messages.visible" {
			t.Errorf("metricSelector = %q, want ext:sqs.messages.visible", got)
		}
		if got := q.Get("entitySelector"); got != `type("CUSTOM_DEVICE")` {
			t.Errorf("entitySelector = %q, want type(\"CUSTOM_DEVICE\")", got)
		}
		if got := q.Get("from"); got != "now-5m" {
			t.Errorf("from = %q, want now-5m", got)
		}

		v1 := 120.5
		v2 := 180.0
		vFuture := 12.0
		resp := metricDataResponse{
			TotalCount: 1,
			Resolution: "1m",
			Result: []metricSeriesCollection{
				{
					MetricID: "ext:sqs.messages.visible",
					Data: []metricSeries{
						{
							DimensionMap: map[string]string{
								"queue": "orders",
							},
							// Include out-of-order points, a nil slot, and an incomplete future slot.
							Timestamps: []int64{
								t2.UnixMilli(),
								t1.UnixMilli(),
								t1.Add(30 * time.Second).UnixMilli(),
								tFuture.UnixMilli(),
							},
							Values: []*float64{
								&v2,
								&v1,
								nil,
								&vFuture,
							},
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	kubeClient := newFakeKubeClient(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dynatrace-creds",
			Namespace: "xas-system",
		},
		Data: map[string][]byte{
			// Trailing newline is common in Secrets created from files.
			"apiToken": []byte("dt0c01.secret-token\n"),
		},
	})

	class := &xasv1.MetricProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: "dynatrace"},
		Spec: xasv1.MetricProviderClassSpec{
			Type: ProviderType,
			Config: map[string]string{
				"apiUrl":             srv.URL,
				"apiTokenSecretName": "dynatrace-creds",
			},
		},
	}

	def := pb.MetricDefinition_builder{
		Name:     new("queue_depth"),
		Provider: new("dynatrace"),
		Params: map[string]string{
			"metricSelector": "ext:sqs.messages.visible",
			"entitySelector": `type("CUSTOM_DEVICE")`,
		},
	}.Build()

	p := &DynatraceMetricsProvider{
		kubeClient:             kubeClient,
		httpClient:             srv.Client(),
		defaultSecretNamespace: "xas-system",
		now:                    func() time.Time { return fixedNow },
	}

	got := p.processDynatraceMetric(context.Background(), "app-ns", def, class, make(map[secretCacheKey]secretCacheEntry), make(map[queryCacheKey]queryCacheEntry))

	want := []*pb.MetricBatch{
		pb.MetricBatch_builder{
			PodName: new(""),
			Samples: []*pb.MetricSample{
				pb.MetricSample_builder{
					Name:      new("queue_depth"),
					Labels:    map[string]string{"queue": "orders"},
					Gauge:     new(120.5),
					Timestamp: timestamppb.New(t1),
				}.Build(),
				pb.MetricSample_builder{
					Name:      new("queue_depth"),
					Labels:    map[string]string{"queue": "orders"},
					Gauge:     new(180.0),
					Timestamp: timestamppb.New(t2),
				}.Build(),
			},
		}.Build(),
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("processDynatraceMetric mismatch (-want +got):\n%s", diff)
	}
}

func TestProcessDynatraceMetric_PodAndContainerScopeAndCounter(t *testing.T) {
	fixedNow := time.Unix(1720000120, 0)
	t1 := time.Unix(1720000060, 0)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vPod2 := 42.0
		vPod1 := 21.0
		vMissingPod := 99.0
		resp := metricDataResponse{
			Result: []metricSeriesCollection{
				{
					MetricID: "builtin:containers.cpu.usageMilliCores",
					Data: []metricSeries{
						{
							DimensionMap: map[string]string{
								"k8s.pod.name":       "app-pod-2",
								"k8s.container.name": "worker",
							},
							Timestamps: []int64{t1.UnixMilli()},
							Values:     []*float64{&vPod2},
						},
						{
							DimensionMap: map[string]string{
								"k8s.pod.name":       "app-pod-1",
								"k8s.container.name": "worker",
							},
							Timestamps: []int64{t1.UnixMilli()},
							Values:     []*float64{&vPod1},
						},
						{
							// Missing k8s.pod.name: must be skipped when pod-scoped.
							DimensionMap: map[string]string{
								"k8s.container.name": "worker",
							},
							Timestamps: []int64{t1.UnixMilli()},
							Values:     []*float64{&vMissingPod},
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	class := &xasv1.MetricProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: "dynatrace"},
		Spec: xasv1.MetricProviderClassSpec{
			Type: ProviderType,
			Config: map[string]string{
				"apiUrl": srv.URL,
			},
		},
	}

	def := pb.MetricDefinition_builder{
		Name:            new("cpu-target"),
		RecommenderName: new("vpa-sizing"),
		Provider:        new("dynatrace"),
		Scope:           new("PodContainer"),
		Params: map[string]string{
			"metric":     "builtin:containers.cpu.usageMilliCores",
			"metricType": "counter",
			"resource":   "cpu",
		},
	}.Build()

	p := &DynatraceMetricsProvider{
		httpClient:      srv.Client(),
		defaultAPIToken: "env-token",
		now:             func() time.Time { return fixedNow },
	}

	got := p.processDynatraceMetric(context.Background(), "app-ns", def, class, nil, nil)

	want := []*pb.MetricBatch{
		pb.MetricBatch_builder{
			PodName:       new("app-pod-1"),
			ContainerName: new("worker"),
			Samples: []*pb.MetricSample{
				pb.MetricSample_builder{
					Name:            new("cpu-target"),
					RecommenderName: new("vpa-sizing"),
					ResourceName:    new("cpu"),
					Labels: map[string]string{
						"k8s.pod.name":       "app-pod-1",
						"k8s.container.name": "worker",
					},
					Counter:   new(21.0),
					Timestamp: timestamppb.New(t1),
				}.Build(),
			},
		}.Build(),
		pb.MetricBatch_builder{
			PodName:       new("app-pod-2"),
			ContainerName: new("worker"),
			Samples: []*pb.MetricSample{
				pb.MetricSample_builder{
					Name:            new("cpu-target"),
					RecommenderName: new("vpa-sizing"),
					ResourceName:    new("cpu"),
					Labels: map[string]string{
						"k8s.pod.name":       "app-pod-2",
						"k8s.container.name": "worker",
					},
					Counter:   new(42.0),
					Timestamp: timestamppb.New(t1),
				}.Build(),
			},
		}.Build(),
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("processDynatraceMetric mismatch (-want +got):\n%s", diff)
	}
}

func TestSecretNamespaceIsolation(t *testing.T) {
	kubeClient := newFakeKubeClient(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "admin-secret",
				Namespace: "xas-system",
			},
			Data: map[string][]byte{"apiToken": []byte("admin-token")},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "tenant-secret",
				Namespace: "tenant-a",
			},
			Data: map[string][]byte{"customKey": []byte("tenant-token")},
		},
	)

	p := &DynatraceMetricsProvider{
		kubeClient:             kubeClient,
		defaultSecretNamespace: "xas-system",
	}

	class := &xasv1.MetricProviderClass{
		Spec: xasv1.MetricProviderClassSpec{Type: ProviderType},
	}

	t.Run("ScalingPolicy params cannot read Secret from another namespace", func(t *testing.T) {
		def := pb.MetricDefinition_builder{
			Name: new("m"),
			Params: map[string]string{
				"apiTokenSecretName":      "admin-secret",
				"apiTokenSecretNamespace": "xas-system", // Must be ignored in policy params.
			},
		}.Build()

		_, err := p.resolveAPIToken(context.Background(), "tenant-a", def, class, make(map[secretCacheKey]secretCacheEntry))
		if err == nil {
			t.Fatal("expected error when ScalingPolicy references Secret in another namespace, got nil")
		}
	})

	t.Run("ScalingPolicy params reads Secret from own namespace", func(t *testing.T) {
		def := pb.MetricDefinition_builder{
			Name: new("m"),
			Params: map[string]string{
				"apiTokenSecretName": "tenant-secret",
				"apiTokenSecretKey":  "customKey",
			},
		}.Build()

		got, err := p.resolveAPIToken(context.Background(), "tenant-a", def, class, make(map[secretCacheKey]secretCacheEntry))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "tenant-token" {
			t.Errorf("token = %q, want tenant-token", got)
		}
	})
}

func TestQueryAndSecretCachingAndTLS(t *testing.T) {
	fixedNow := time.Unix(1720000120, 0)
	t1 := time.Unix(1720000060, 0)

	var httpCalls atomic.Int32
	tlsSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpCalls.Add(1)
		v := 55.0
		_ = json.NewEncoder(w).Encode(metricDataResponse{
			Result: []metricSeriesCollection{
				{
					MetricID: "m1",
					Data: []metricSeries{
						{
							Timestamps: []int64{t1.UnixMilli()},
							Values:     []*float64{&v},
						},
					},
				},
			},
		})
	}))
	defer tlsSrv.Close()

	kubeClient := newFakeKubeClient(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dt-secret",
			Namespace: "custom-ns",
		},
		Data: map[string][]byte{"apiToken": []byte("tls-token")},
	})

	p, err := NewDynatraceMetricsProvider(Options{
		KubeClient:    kubeClient,
		ServerAddress: "localhost:8080",
		ClusterName:   "default",
	})
	if err != nil {
		t.Fatalf("NewDynatraceMetricsProvider: %v", err)
	}
	defer p.grpcConn.Close()
	p.now = func() time.Time { return fixedNow }

	classVerify := &xasv1.MetricProviderClass{
		Spec: xasv1.MetricProviderClassSpec{
			Type: ProviderType,
			Config: map[string]string{
				"apiUrl":                  tlsSrv.URL,
				"apiTokenSecretName":      "dt-secret",
				"apiTokenSecretNamespace": "custom-ns",
				"insecureSkipVerify":      "false",
			},
		},
	}

	def1 := pb.MetricDefinition_builder{
		Name:   new("m_p50"),
		Params: map[string]string{"metricSelector": "m1"},
	}.Build()
	def2 := pb.MetricDefinition_builder{
		Name:   new("m_p99"),
		Params: map[string]string{"metricSelector": "m1"},
	}.Build()

	// Self-signed TLS should fail when insecureSkipVerify is false.
	if got := p.processDynatraceMetric(context.Background(), "default", def1, classVerify, make(map[secretCacheKey]secretCacheEntry), make(map[queryCacheKey]queryCacheEntry)); got != nil {
		t.Fatalf("expected nil batches for untrusted TLS cert, got %v", got)
	}

	// With insecureSkipVerify=true, two metrics sharing the same query should hit Dynatrace only once.
	classInsecure := classVerify.DeepCopy()
	classInsecure.Spec.Config["insecureSkipVerify"] = "true"

	secrets := make(map[secretCacheKey]secretCacheEntry)
	queries := make(map[queryCacheKey]queryCacheEntry)
	b1 := p.processDynatraceMetric(context.Background(), "default", def1, classInsecure, secrets, queries)
	b2 := p.processDynatraceMetric(context.Background(), "default", def2, classInsecure, secrets, queries)
	if len(b1) != 1 || len(b2) != 1 {
		t.Fatalf("expected 1 batch for each metric, got %d and %d", len(b1), len(b2))
	}
	if b1[0].GetSamples()[0].GetName() != "m_p50" || b2[0].GetSamples()[0].GetName() != "m_p99" {
		t.Errorf("unexpected sample names: %q, %q", b1[0].GetSamples()[0].GetName(), b2[0].GetSamples()[0].GetName())
	}
	if gotCalls := httpCalls.Load(); gotCalls != 1 {
		t.Errorf("HTTP server calls = %d, want 1 (cached across identical queries)", gotCalls)
	}
	// 1 Get during classVerify + 1 Get cached across both defs in classInsecure = 2 total.
	if gotGets := kubeClient.gets.Load(); gotGets != 2 {
		t.Errorf("Secret Get calls = %d, want 2 (1 in first cycle, 1 cached in second cycle)", gotGets)
	}
}

func TestFormatAPIErrorAndMultiMetricLabels(t *testing.T) {
	errBody := []byte(`{
		"error": {
			"code": 400,
			"message": "Constraints violated.",
			"constraintViolations": [
				{"path": "metricSelector", "message": "Unknown metric key"}
			]
		}
	}`)
	err := formatAPIError(400, errBody)
	wantErr := "Dynatrace API returned HTTP 400: Constraints violated. (metricSelector: Unknown metric key)"
	if err.Error() != wantErr {
		t.Errorf("formatAPIError() = %q, want %q", err.Error(), wantErr)
	}

	t1 := time.Unix(1720000060, 0)
	vA, vB := 10.0, 20.0
	resp := &metricDataResponse{
		Result: []metricSeriesCollection{
			{
				MetricID: "builtin:host.cpu.user",
				Data: []metricSeries{
					{
						DimensionMap: map[string]string{"dt.entity.host": "HOST-1"},
						Timestamps:   []int64{t1.UnixMilli()},
						Values:       []*float64{&vA},
					},
				},
			},
			{
				MetricID: "builtin:host.cpu.system",
				Data: []metricSeries{
					{
						DimensionMap: map[string]string{"dt.entity.host": "HOST-1"},
						Timestamps:   []int64{t1.UnixMilli()},
						Values:       []*float64{&vB},
					},
				},
			},
		},
	}
	def := pb.MetricDefinition_builder{Name: new("host_cpu")}.Build()
	batches := buildMetricBatches(def, resp, sampleOptions{})
	if len(batches) != 1 || len(batches[0].GetSamples()) != 2 {
		t.Fatalf("expected 1 batch with 2 samples, got %v", batches)
	}
	if batches[0].GetSamples()[0].GetLabels()["metricId"] != "builtin:host.cpu.user" ||
		batches[0].GetSamples()[1].GetLabels()["metricId"] != "builtin:host.cpu.system" {
		t.Errorf("expected metricId label to disambiguate multi-metric series, got %v", batches[0].GetSamples())
	}
}
