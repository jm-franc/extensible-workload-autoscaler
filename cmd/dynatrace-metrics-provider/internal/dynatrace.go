package internal

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
)

const (
	defaultFromTimeframe    = "now-5m"
	defaultAPITokenKey      = "apiToken"
	defaultPodDimension     = "k8s.pod.name"
	defaultContainerDim     = "k8s.container.name"
	metricsQueryEndpoint    = "/api/v2/metrics/query"
	metricsAPIV2Prefix      = "/api/v2"
	maxSuccessResponseBytes = 16 << 20 // 16 MiB
	maxErrorResponseBytes   = 64 << 10 // 64 KiB
)

// metricDataResponse models the JSON payload returned by Dynatrace
// GET /api/v2/metrics/query.
type metricDataResponse struct {
	TotalCount int                      `json:"totalCount"`
	Resolution string                   `json:"resolution"`
	Warnings   []string                 `json:"warnings"`
	Result     []metricSeriesCollection `json:"result"`
}

type metricSeriesCollection struct {
	MetricID string         `json:"metricId"`
	Warnings []string       `json:"warnings"`
	Data     []metricSeries `json:"data"`
}

type metricSeries struct {
	DimensionMap map[string]string `json:"dimensionMap"`
	Dimensions   []*string         `json:"dimensions"`
	Timestamps   []int64           `json:"timestamps"`
	Values       []*float64        `json:"values"`
}

type errorEnvelope struct {
	Error *apiError `json:"error"`
}

type apiError struct {
	Code                 int                   `json:"code"`
	Message              string                `json:"message"`
	ConstraintViolations []constraintViolation `json:"constraintViolations"`
}

type constraintViolation struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

type secretCacheKey struct {
	namespace string
	name      string
}

type secretCacheEntry struct {
	data map[string][]byte
	err  error
}

type queryCacheKey struct {
	url      string
	token    string
	insecure bool
}

type queryCacheEntry struct {
	resp      *metricDataResponse
	fetchedAt time.Time
	err       error
}

type batchKey struct {
	podName       string
	containerName string
}

type dataPoint struct {
	ts  time.Time
	val float64
}

func (p *DynatraceMetricsProvider) processDynatraceMetric(
	ctx context.Context,
	policyNamespace string,
	m *pb.MetricDefinition,
	class *xasv1.MetricProviderClass,
	secrets map[secretCacheKey]secretCacheEntry,
	queries map[queryCacheKey]queryCacheEntry,
) []*pb.MetricBatch {
	getParam := func(key string) string {
		if val, ok := m.GetParams()[key]; ok {
			return val
		}
		if class != nil {
			if val, ok := class.Spec.Config[key]; ok {
				return val
			}
		}
		return ""
	}

	metricSelector := getParam("metricSelector")
	if metricSelector == "" {
		metricSelector = getParam("metric")
	}
	if metricSelector == "" {
		slog.Warn("Missing 'metricSelector' parameter for Dynatrace metric", "policyMetric", m.GetName())
		return nil
	}

	apiURL := getParam("apiUrl")
	if apiURL == "" {
		apiURL = p.defaultAPIURL
	}
	if apiURL == "" {
		slog.Error("Missing Dynatrace 'apiUrl' configuration", "policyMetric", m.GetName())
		return nil
	}

	isCounter, ok := parseMetricType(getParam("metricType"))
	if !ok {
		slog.Error("Unsupported 'metricType' for Dynatrace metric (expected 'gauge' or 'counter')", "policyMetric", m.GetName(), "metricType", getParam("metricType"))
		return nil
	}

	from := getParam("from")
	if from == "" {
		from = defaultFromTimeframe
	}

	queryURL, err := buildQueryURL(apiURL, queryParams{
		metricSelector: metricSelector,
		entitySelector: getParam("entitySelector"),
		mzSelector:     getParam("mzSelector"),
		resolution:     getParam("resolution"),
		from:           from,
		to:             getParam("to"),
	})
	if err != nil {
		slog.Error("Invalid Dynatrace query URL", "policyMetric", m.GetName(), "apiUrl", apiURL, "error", err)
		return nil
	}

	apiToken, err := p.resolveAPIToken(ctx, policyNamespace, m, class, secrets)
	if err != nil {
		slog.Error("Failed to resolve Dynatrace API token", "policyMetric", m.GetName(), "error", err)
		return nil
	}

	insecureTLS := strings.EqualFold(getParam("insecureSkipVerify"), "true")
	data, fetchedAt, err := p.queryDynatrace(ctx, queryURL, apiToken, insecureTLS, queries)
	if err != nil {
		slog.Error("Failed to query Dynatrace Metrics API", "policyMetric", m.GetName(), "metricSelector", metricSelector, "error", err)
		return nil
	}

	for _, w := range data.Warnings {
		slog.Warn("Dynatrace query warning", "policyMetric", m.GetName(), "warning", w)
	}
	for _, col := range data.Result {
		for _, w := range col.Warnings {
			slog.Warn("Dynatrace metric series warning", "policyMetric", m.GetName(), "metricId", col.MetricID, "warning", w)
		}
	}

	return buildMetricBatches(m, data, sampleOptions{
		isCounter:          isCounter,
		podDimension:       getParam("podDimension"),
		containerDimension: getParam("containerDimension"),
		resourceName:       getParam("resource"),
		maxTimestamp:       fetchedAt,
	})
}

func parseMetricType(raw string) (isCounter bool, ok bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "gauge":
		return false, true
	case "counter":
		return true, true
	default:
		return false, false
	}
}

type queryParams struct {
	metricSelector string
	entitySelector string
	mzSelector     string
	resolution     string
	from           string
	to             string
}

// buildQueryURL constructs the full Dynatrace GET /api/v2/metrics/query URL
// for both SaaS (https://<env>.live.dynatrace.com) and ActiveGate / Managed
// (https://<host>:9999/e/<env>) base URLs.
func buildQueryURL(rawBaseURL string, params queryParams) (string, error) {
	u, err := url.Parse(strings.TrimSpace(rawBaseURL))
	if err != nil {
		return "", err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("URL must have http or https scheme and non-empty host: %q", rawBaseURL)
	}

	path := strings.TrimRight(u.Path, "/")
	switch {
	case strings.HasSuffix(path, metricsQueryEndpoint):
		u.Path = path
	case strings.HasSuffix(path, metricsAPIV2Prefix):
		u.Path = path + "/metrics/query"
	default:
		u.Path = path + metricsQueryEndpoint
	}

	q := u.Query()
	q.Set("metricSelector", params.metricSelector)
	if params.entitySelector != "" {
		q.Set("entitySelector", params.entitySelector)
	}
	if params.mzSelector != "" {
		q.Set("mzSelector", params.mzSelector)
	}
	if params.resolution != "" {
		q.Set("resolution", params.resolution)
	}
	if params.from != "" {
		q.Set("from", params.from)
	}
	if params.to != "" {
		q.Set("to", params.to)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// resolveAPIToken resolves the Dynatrace API token for a metric query.
//
// Precedence:
//  1. apiTokenSecretName in ScalingPolicy metric params (always looked up in
//     the ScalingPolicy's own namespace to prevent cross-namespace Secret reads).
//  2. apiTokenSecretName in MetricProviderClass config (looked up in
//     apiTokenSecretNamespace if set on the class, otherwise the provider's
//     default secret namespace).
//  3. Provider-level default API token (--api-token / DYNATRACE_API_TOKEN).
func (p *DynatraceMetricsProvider) resolveAPIToken(
	ctx context.Context,
	policyNamespace string,
	m *pb.MetricDefinition,
	class *xasv1.MetricProviderClass,
	secrets map[secretCacheKey]secretCacheEntry,
) (string, error) {
	if secretName := strings.TrimSpace(m.GetParams()["apiTokenSecretName"]); secretName != "" {
		key := strings.TrimSpace(m.GetParams()["apiTokenSecretKey"])
		if key == "" && class != nil {
			key = strings.TrimSpace(class.Spec.Config["apiTokenSecretKey"])
		}
		if key == "" {
			key = defaultAPITokenKey
		}
		return p.readSecretToken(ctx, policyNamespace, secretName, key, secrets)
	}

	if class != nil {
		if secretName := strings.TrimSpace(class.Spec.Config["apiTokenSecretName"]); secretName != "" {
			secretNS := strings.TrimSpace(class.Spec.Config["apiTokenSecretNamespace"])
			if secretNS == "" {
				secretNS = p.defaultSecretNamespace
			}
			if secretNS == "" {
				secretNS = defaultSecretNamespace
			}
			key := strings.TrimSpace(m.GetParams()["apiTokenSecretKey"])
			if key == "" {
				key = strings.TrimSpace(class.Spec.Config["apiTokenSecretKey"])
			}
			if key == "" {
				key = defaultAPITokenKey
			}
			return p.readSecretToken(ctx, secretNS, secretName, key, secrets)
		}
	}

	token := strings.TrimSpace(p.defaultAPIToken)
	if token == "" {
		return "", fmt.Errorf("no API token configured: set apiTokenSecretName or DYNATRACE_API_TOKEN")
	}
	return token, nil
}

func (p *DynatraceMetricsProvider) readSecretToken(
	ctx context.Context,
	namespace, name, key string,
	secrets map[secretCacheKey]secretCacheEntry,
) (string, error) {
	if p.kubeClient == nil {
		return "", fmt.Errorf("kubernetes client not initialized to read Secret %s/%s", namespace, name)
	}

	ck := secretCacheKey{namespace: namespace, name: name}
	entry, cached := secrets[ck]
	if !cached {
		sec, err := p.kubeClient.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			entry = secretCacheEntry{err: err}
		} else {
			entry = secretCacheEntry{data: sec.Data}
		}
		if secrets != nil {
			secrets[ck] = entry
		}
	}
	if entry.err != nil {
		return "", fmt.Errorf("reading Secret %s/%s: %w", namespace, name, entry.err)
	}

	raw, ok := entry.data[key]
	if !ok {
		return "", fmt.Errorf("Secret %s/%s does not contain key %q", namespace, name, key)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", fmt.Errorf("Secret %s/%s has empty value for key %q", namespace, name, key)
	}
	return token, nil
}

func (p *DynatraceMetricsProvider) queryDynatrace(
	ctx context.Context,
	queryURL, apiToken string,
	insecureTLS bool,
	queries map[queryCacheKey]queryCacheEntry,
) (*metricDataResponse, time.Time, error) {
	ck := queryCacheKey{url: queryURL, token: apiToken, insecure: insecureTLS}
	if queries != nil {
		if entry, ok := queries[ck]; ok {
			return entry.resp, entry.fetchedAt, entry.err
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)
	if err != nil {
		return nil, time.Time{}, err
	}
	req.Header.Set("Authorization", "Api-Token "+apiToken)
	req.Header.Set("Accept", "application/json")

	client := p.httpClient
	if insecureTLS && p.insecureHTTPClient != nil {
		client = p.insecureHTTPClient
	}
	if client == nil {
		client = http.DefaultClient
	}

	httpResp, err := client.Do(req)
	if err != nil {
		if queries != nil {
			queries[ck] = queryCacheEntry{err: err}
		}
		return nil, time.Time{}, err
	}
	defer httpResp.Body.Close()

	fetchedAt := p.clockNow()

	if httpResp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(httpResp.Body, maxErrorResponseBytes))
		queryErr := formatAPIError(httpResp.StatusCode, errBody)
		if queries != nil {
			queries[ck] = queryCacheEntry{err: queryErr}
		}
		return nil, time.Time{}, queryErr
	}

	var parsed metricDataResponse
	dec := json.NewDecoder(io.LimitReader(httpResp.Body, maxSuccessResponseBytes))
	if err := dec.Decode(&parsed); err != nil {
		parseErr := fmt.Errorf("decoding Dynatrace response: %w", err)
		if queries != nil {
			queries[ck] = queryCacheEntry{err: parseErr}
		}
		return nil, time.Time{}, parseErr
	}

	if queries != nil {
		queries[ck] = queryCacheEntry{resp: &parsed, fetchedAt: fetchedAt}
	}
	return &parsed, fetchedAt, nil
}

func formatAPIError(statusCode int, body []byte) error {
	var env errorEnvelope
	if err := json.Unmarshal(body, &env); err == nil && env.Error != nil && env.Error.Message != "" {
		if len(env.Error.ConstraintViolations) > 0 {
			details := make([]string, 0, len(env.Error.ConstraintViolations))
			for _, cv := range env.Error.ConstraintViolations {
				if cv.Path != "" {
					details = append(details, fmt.Sprintf("%s: %s", cv.Path, cv.Message))
				} else {
					details = append(details, cv.Message)
				}
			}
			return fmt.Errorf("Dynatrace API returned HTTP %d: %s (%s)", statusCode, env.Error.Message, strings.Join(details, "; "))
		}
		return fmt.Errorf("Dynatrace API returned HTTP %d: %s", statusCode, env.Error.Message)
	}
	msg := strings.TrimSpace(string(body))
	if msg != "" {
		return fmt.Errorf("Dynatrace API returned HTTP %d: %s", statusCode, msg)
	}
	return fmt.Errorf("Dynatrace API returned HTTP %d", statusCode)
}

type sampleOptions struct {
	isCounter          bool
	podDimension       string
	containerDimension string
	resourceName       string
	maxTimestamp       time.Time
}

func buildMetricBatches(m *pb.MetricDefinition, resp *metricDataResponse, opts sampleOptions) []*pb.MetricBatch {
	if resp == nil || len(resp.Result) == 0 {
		return nil
	}

	podDim := opts.podDimension
	containerDim := opts.containerDimension
	scope := m.GetScope()
	switch scope {
	case "Pod":
		if podDim == "" {
			podDim = defaultPodDimension
		}
	case "PodContainer", "Container":
		if podDim == "" {
			podDim = defaultPodDimension
		}
		if containerDim == "" {
			containerDim = defaultContainerDim
		}
	}

	grouped := make(map[batchKey][]*pb.MetricSample)
	multiMetric := len(resp.Result) > 1

	for _, col := range resp.Result {
		for _, ser := range col.Data {
			var podName, containerName string
			if podDim != "" {
				podName = ser.DimensionMap[podDim]
				if podName == "" {
					slog.Debug("Skipping Dynatrace series missing pod dimension", "policyMetric", m.GetName(), "podDimension", podDim)
					continue
				}
			}
			if containerDim != "" {
				containerName = ser.DimensionMap[containerDim]
				if containerName == "" && (scope == "PodContainer" || scope == "Container") {
					slog.Debug("Skipping Dynatrace series missing container dimension", "policyMetric", m.GetName(), "containerDimension", containerDim)
					continue
				}
			}

			var labels map[string]string
			if len(ser.DimensionMap) > 0 {
				labels = maps.Clone(ser.DimensionMap)
			}
			if multiMetric && col.MetricID != "" {
				if labels == nil {
					labels = make(map[string]string, 1)
				}
				if _, exists := labels["metricId"]; !exists {
					labels["metricId"] = col.MetricID
				}
			}

			n := min(len(ser.Timestamps), len(ser.Values))
			if n == 0 {
				continue
			}

			points := make([]dataPoint, 0, n)
			for i := range n {
				if ser.Values[i] == nil {
					continue
				}
				val := *ser.Values[i]
				if math.IsNaN(val) || math.IsInf(val, 0) {
					continue
				}
				ts := time.UnixMilli(ser.Timestamps[i])
				// Dynatrace time slots are stamped with their end time; a slot
				// whose end time lies in the future is still open and holds
				// incomplete data.
				if !opts.maxTimestamp.IsZero() && ts.After(opts.maxTimestamp) {
					continue
				}
				points = append(points, dataPoint{ts: ts, val: val})
			}

			if len(points) == 0 {
				continue
			}

			slices.SortFunc(points, func(a, b dataPoint) int {
				return a.ts.Compare(b.ts)
			})

			bk := batchKey{podName: podName, containerName: containerName}
			for _, pt := range points {
				sb := pb.MetricSample_builder{
					Name:      new(m.GetName()),
					Labels:    labels,
					Timestamp: timestamppb.New(pt.ts),
				}
				if m.GetRecommenderName() != "" {
					sb.RecommenderName = new(m.GetRecommenderName())
				}
				if opts.resourceName != "" {
					sb.ResourceName = new(opts.resourceName)
				}
				if opts.isCounter {
					sb.Counter = new(pt.val)
				} else {
					sb.Gauge = new(pt.val)
				}
				grouped[bk] = append(grouped[bk], sb.Build())
			}
		}
	}

	if len(grouped) == 0 {
		return nil
	}

	keys := make([]batchKey, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b batchKey) int {
		if c := cmp.Compare(a.podName, b.podName); c != 0 {
			return c
		}
		return cmp.Compare(a.containerName, b.containerName)
	})

	batches := make([]*pb.MetricBatch, 0, len(keys))
	for _, k := range keys {
		bb := pb.MetricBatch_builder{
			PodName: new(k.podName),
			Samples: grouped[k],
		}
		if k.containerName != "" {
			bb.ContainerName = new(k.containerName)
		}
		batches = append(batches, bb.Build())
	}
	return batches
}
