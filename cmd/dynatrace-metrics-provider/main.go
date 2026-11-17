package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	provider "github.com/gke-labs/extensible-workload-autoscaler/cmd/dynatrace-metrics-provider/internal"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/logging"
	clientset "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/clientset/versioned"
	informers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/informers/externalversions"
)

func main() {
	var serverAddress string
	var clusterName string
	var kubeconfig string
	var scrapeInterval time.Duration
	var defaultAPIURL string
	var defaultAPIToken string
	var secretNamespace string
	var debug bool

	defaultNS := os.Getenv("POD_NAMESPACE")
	if defaultNS == "" {
		defaultNS = "xas-system"
	}

	flag.StringVar(&serverAddress, "server-address", "xas-server:8080", "Address of the xAS Server (host:port)")
	flag.StringVar(&clusterName, "cluster-name", "default", "Name of the cluster this provider is running in")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.DurationVar(&scrapeInterval, "scrape-interval", 5*time.Second, "Interval between Dynatrace metric queries")
	flag.StringVar(&defaultAPIURL, "api-url", os.Getenv("DYNATRACE_API_URL"), "Default Dynatrace environment URL (or set DYNATRACE_API_URL)")
	flag.StringVar(&defaultAPIToken, "api-token", os.Getenv("DYNATRACE_API_TOKEN"), "Default Dynatrace API token (or set DYNATRACE_API_TOKEN)")
	flag.StringVar(&secretNamespace, "secret-namespace", defaultNS, "Default namespace for Secret references in MetricProviderClass")
	flag.BoolVar(&debug, "debug", false, "Enable debug logging")
	flag.Parse()

	logging.Setup(debug)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var cfg *rest.Config
	var err error
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		slog.Error("Error building kubeconfig", "error", err)
		os.Exit(1)
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Error("Error building kubernetes clientset", "error", err)
		os.Exit(1)
	}

	xasClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		slog.Error("Error building xAS clientset", "error", err)
		os.Exit(1)
	}

	factory := informers.NewSharedInformerFactory(xasClient, 30*time.Second)
	providerLister := factory.XAS().V1().MetricProviderClasses().Lister()

	p, err := provider.NewDynatraceMetricsProvider(provider.Options{
		KubeClient:             kubeClient,
		ProviderLister:         providerLister,
		ServerAddress:          serverAddress,
		ClusterName:            clusterName,
		ScrapeInterval:         scrapeInterval,
		DefaultAPIURL:          defaultAPIURL,
		DefaultAPIToken:        defaultAPIToken,
		DefaultSecretNamespace: secretNamespace,
	})
	if err != nil {
		slog.Error("Error initializing Dynatrace metrics provider", "error", err)
		os.Exit(1)
	}

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	slog.Info("xAS Dynatrace Metrics Provider starting...")
	p.Run(ctx)
}
