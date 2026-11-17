// Package internal implements the Dynatrace metric provider for xAS.
package internal

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/client-go/kubernetes"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

// ProviderType is the MetricProviderClass spec.type handled by this provider.
const ProviderType = "Dynatrace"

const (
	defaultScrapeInterval  = 5 * time.Second
	defaultSecretNamespace = "xas-system"
	httpTimeout            = 10 * time.Second
)

// Options configures a DynatraceMetricsProvider.
type Options struct {
	KubeClient             kubernetes.Interface
	ProviderLister         listers.MetricProviderClassLister
	ServerAddress          string
	ClusterName            string
	ScrapeInterval         time.Duration
	DefaultAPIURL          string
	DefaultAPIToken        string
	DefaultSecretNamespace string
}

// DynatraceMetricsProvider queries the Dynatrace Metrics API v2 for policies
// referencing a Dynatrace MetricProviderClass and pushes samples to the xAS
// Server.
type DynatraceMetricsProvider struct {
	kubeClient     kubernetes.Interface
	providerLister listers.MetricProviderClassLister

	grpcConn   *grpc.ClientConn
	grpcClient pb.XASServerClient

	httpClient         *http.Client
	insecureHTTPClient *http.Client

	clusterName            string
	scrapeInterval         time.Duration
	defaultAPIURL          string
	defaultAPIToken        string
	defaultSecretNamespace string

	now func() time.Time
}

// NewDynatraceMetricsProvider creates a DynatraceMetricsProvider connected to
// the xAS Server at opts.ServerAddress.
func NewDynatraceMetricsProvider(opts Options) (*DynatraceMetricsProvider, error) {
	conn, err := grpc.NewClient(opts.ServerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("cannot connect to xAS Server: %w", err)
	}

	interval := opts.ScrapeInterval
	if interval <= 0 {
		interval = defaultScrapeInterval
	}
	secretNS := opts.DefaultSecretNamespace
	if secretNS == "" {
		secretNS = defaultSecretNamespace
	}

	return &DynatraceMetricsProvider{
		kubeClient:             opts.KubeClient,
		providerLister:         opts.ProviderLister,
		grpcConn:               conn,
		grpcClient:             pb.NewXASServerClient(conn),
		httpClient:             &http.Client{Timeout: httpTimeout},
		insecureHTTPClient:     &http.Client{Timeout: httpTimeout, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}},
		clusterName:            opts.ClusterName,
		scrapeInterval:         interval,
		defaultAPIURL:          opts.DefaultAPIURL,
		defaultAPIToken:        opts.DefaultAPIToken,
		defaultSecretNamespace: secretNS,
		now:                    time.Now,
	}, nil
}

// Run periodically scrapes Dynatrace metrics and sends them to the xAS Server
// until ctx is cancelled.
func (p *DynatraceMetricsProvider) Run(ctx context.Context) {
	if p.grpcConn != nil {
		defer p.grpcConn.Close()
	}

	interval := p.scrapeInterval
	if interval <= 0 {
		interval = defaultScrapeInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.scrapeAndSend(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (p *DynatraceMetricsProvider) clockNow() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

func (p *DynatraceMetricsProvider) scrapeAndSend(ctx context.Context) {
	slog.Debug("Dynatrace scrape cycle starting...")

	cycleCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	resp, err := p.grpcClient.ListPolicies(cycleCtx, pb.ListPoliciesRequest_builder{
		ClusterName: new(p.clusterName),
	}.Build())
	if err != nil {
		slog.Error("Error listing policies", "error", err)
		return
	}

	secrets := make(map[secretCacheKey]secretCacheEntry)
	queries := make(map[queryCacheKey]queryCacheEntry)

	var policyBatches []*pb.PolicyMetricBatch

	for _, pol := range resp.GetPolicies() {
		var policyMetrics []*pb.MetricBatch
		ns := pol.GetId().GetNamespace()

		for _, m := range policy.MetricDefinitions(pol) {
			class, err := p.providerLister.Get(m.GetProvider())
			if err != nil {
				slog.Warn("MetricProviderClass not found for metric", "class", m.GetProvider(), "metric", m.GetName())
				continue
			}
			if class.Spec.Type != ProviderType {
				continue
			}

			batches := p.processDynatraceMetric(cycleCtx, ns, m, class, secrets, queries)
			if len(batches) > 0 {
				policyMetrics = append(policyMetrics, batches...)
			}
		}

		if len(policyMetrics) > 0 {
			policyBatches = append(policyBatches, pb.PolicyMetricBatch_builder{
				Namespace: new(ns),
				Name:      new(pol.GetId().GetName()),
				Batches:   policyMetrics,
			}.Build())
		}
	}

	if len(policyBatches) > 0 {
		req := pb.IngestMetricsRequest_builder{
			ClusterName:   new(p.clusterName),
			Timestamp:     timestamppb.New(p.clockNow()),
			PolicyMetrics: policyBatches,
		}.Build()
		p.sendBatch(ctx, req)
	}
}

func (p *DynatraceMetricsProvider) sendBatch(ctx context.Context, req *pb.IngestMetricsRequest) {
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	slog.Debug("Sending Dynatrace metrics batch", "policies", len(req.GetPolicyMetrics()))
	resp, err := p.grpcClient.IngestMetrics(sendCtx, req)
	if err != nil {
		slog.Error("Failed to send Dynatrace metrics batch", "error", err)
		return
	}
	slog.Info("Dynatrace metrics batch sent", "success", resp.GetSuccess())
}
