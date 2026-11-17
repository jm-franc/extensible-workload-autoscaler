# xAS User Guide

## Description of the `ScalingPolicy` Resource

With xAS, autoscaling is configured using `ScalingPolicy` objects.

They define what to scale (e.g., a Deployment), the metrics on which the
autoscaling decision is based, and how to turn metrics into scaling
recommendations.

### Defining the Autoscaling Target

Similarly to
[`HorizontalPodAutoscaler`](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/)
objects, one starts by defining the autoscaling *target*, i.e. an object with a
[`scale` subresource](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#scale-subresource)
whose `replicas` field will be updated by the autoscaler.

The policy can also specify the target's minimum and maximum number of replicas.

### Defining Autoscaling Metrics

Metrics are defined in the `spec.metrics` section of the `ScalingPolicy`.

Metrics come from various, pluggable *metric providers*. Each must have a name
that is unique within the `ScalingPolicy` object. The metric provider is given
configuration parameters via a string-to-string map; it is the responsibility of
the provider to define which parameters are expected. xAS pre-defines a handful
of core metric providers, such as the *kubelet* provider for CPU and memory
metrics.

Metric providers emit metrics of a given *type* (i.e. *gauge*, *counter* or
*histogram*), and can optionally hold *labels* (i.e. a set of key/value pairs).
Since ultimately autoscaling decisions are based on a single number for each
metric, xAS includes a metrics processing pipeline that aggregates raw data into
a single number, known as a *control metric*.

In practice, this aggregation step typically takes the form of an `aggregation`
field with values such as "Avg" to compute the average across all the pods.

### Defining Recommenders

Recommenders read metrics and provide scaling recommendations. For example, they
can read pods CPU usage, compare it to a target value, and propose a new replica
count. They are defined via the `scaling` and `activation` fields of the
`ScalingPolicy`, each containing configuration parameters via a string-to-string
map. Recommenders can run in `DryRun` mode, in which case their recommendations
are computed and displayed, but otherwise ignored.

Similarly to metric providers, new recommenders can be added to xAS as pluggable
components. Out of the box, xAS provides two recommenders:

-   *Linear*: Implements a logic identical to standard
    `HorizontalPodAutoscaler`, i.e. adjusts the number of replicas linearly
    based on the ratio between the measured vs target metric value.
-   *Cron*: Proposes a fixed replica count based on cron-like expressions.

Recommenders come in two flavors: *scaling* and *activation*:

-   *Scaling*: This is the most common one, providing a recommended number of
    replicas. If multiple scaling recommenders are defined, their
    recommendations are aggregated using the *maximum* of all recommendations.
-   *Activation*: The second one provides recommendations on whether the number
    of replicas should be set to 0, i.e. whether the application can be shut
    down.

## Monitoring Metric Providers and Recommenders

The `kubectl describe scalingpolicy` command allows you to inspect metric
providers and recommenders.

The `status` field displays, for each recommender, the last recommended value,
as well as the final number of replicas that will be set to the autoscaling
target.

## Example Usage

### Scale Linearly on CPU Utilization

Below is a sample autoscaling policy that scales the Deployment with name
`my-app`. This Deployment must be located in the same namespace as the
`ScalingPolicy` object, i.e. `my-app-ns`.

It is configured to compute the average CPU utilization of the pods associated
with the `my-app` Deployment. A linear scaling policy computes a number of
replicas such that this utilization stays at a 50% target value.

```yaml
apiVersion: xas.io/v1
kind: ScalingPolicy
metadata:
  name: my-app-scaler
  namespace: my-app-ns
spec:
  scaleTargetRef:
    kind: Deployment
    name: my-app
  minReplicas: 1
  maxReplicas: 10

  metrics:
    - name: "cpu-utilization"
      provider: "kubelet"
      params:
        type: "cpu"
        mode: "utilization"
      gauge:
        aggregation: "Avg"

  scaling:
    - name: "cpu-scale"
      type: "Linear"
      params: { metric: "cpu-utilization", target: "0.5" }
```

### Scale Linearly on Throughput

This example scales based on Prometheus application metrics. The target
`prometheus-rate` Deployment is expected to be composed of pods that expose a
`http_requests_total` metric on the `8080` port of the default `/metrics` HTTP
endpoint.

The `http_requests_total` metric is transformed into a query-per-second (QPS)
metric thanks to a 'rate' transformation over a 1 minute window. The QPS values
from all pods are then summed to obtain the total QPS of the Deployment.

A linear scaling is computed to keep this QPS at a target value of 10 QPS per
pod.

```yaml
apiVersion: xas.io/v1
kind: ScalingPolicy
metadata:
  name: prometheus-rate
  labels:
    sample: prometheus-rate
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: prometheus-rate
  minReplicas: 1
  maxReplicas: 10

  metrics:
    - name: "qps"
      provider: "prometheus"
      params:
        metric: "http_requests_total"
        port: "8080"
      rate:
        window: "1m"
        aggregation: "Sum" # Total QPS across all pods

  scaling:
    - recommender: "linear"
      name: "qps_target"
      params:
        metric: "qps"
        target: "10.0" # Scale to maintain 10 QPS per pod
```

### Scale on Dynatrace Metrics

The `Dynatrace` metric provider queries the
[Dynatrace Metrics API v2](https://docs.dynatrace.com/docs/dynatrace-api/environment-api/metric-v2/get-data-points)
(`GET /api/v2/metrics/query`) using an access token with the `metrics.read`
scope.

Configure the Dynatrace environment URL and API token `Secret` once on a
`MetricProviderClass` (or override them per metric in `ScalingPolicy` `params`,
where `apiTokenSecretName` is always read from the `ScalingPolicy`'s own
namespace):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: dynatrace-token
  namespace: xas-system
type: Opaque
stringData:
  apiToken: "dt0c01.<your-token>"
---
apiVersion: xas.io/v1
kind: MetricProviderClass
metadata:
  name: dynatrace
spec:
  type: Dynatrace
  config:
    apiUrl: "https://<your-environment-id>.live.dynatrace.com"
    apiTokenSecretName: "dynatrace-token"
    apiTokenSecretNamespace: "xas-system"
```

Supported configuration keys and metric `params`:

-   `apiUrl`: Dynatrace SaaS (`https://<env>.live.dynatrace.com`) or ActiveGate
    (`https://<activegate>:9999/e/<env>`) base URL.
-   `apiTokenSecretName`, `apiTokenSecretKey` (default `apiToken`), and (on
    `MetricProviderClass` only) `apiTokenSecretNamespace` (default `xas-system`).
    Alternatively, set `DYNATRACE_API_TOKEN` on the provider Deployment.
-   `insecureSkipVerify`: Set to `"true"` to skip TLS verification when
    querying an in-cluster ActiveGate with a self-signed certificate.
-   `metricSelector` (or `metric`): Required Dynatrace metric selector.
-   `entitySelector`, `mzSelector`, `resolution`, `from` (default `now-5m`),
    `to`: Optional Dynatrace query parameters.
-   `metricType`: `"gauge"` (default) or `"counter"`.
-   `podDimension` / `containerDimension`: Optional `dimensionMap` keys used to
    attribute series to individual pods or containers (automatically defaulting
    to `k8s.pod.name` and `k8s.container.name` when `scope` is `Pod`,
    `PodContainer`, or `Container`).
-   `resource`: Optional Kubernetes resource name (e.g. `cpu` or `memory`).

```yaml
apiVersion: xas.io/v1
kind: ScalingPolicy
metadata:
  name: checkout-autoscaler
  namespace: shop
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: checkout
  minReplicas: 1
  maxReplicas: 20

  metrics:
    - name: "service_rps"
      provider: "dynatrace"
      params:
        metricSelector: "builtin:service.requestCount.total:splitBy():rate(1s)"
        entitySelector: 'type("SERVICE"),tag("app:checkout")'
      gauge:
        aggregation: "Avg"

  scaling:
    - recommender: "linear"
      name: "rps_target"
      params:
        metric: "service_rps"
        target: "100.0" # Target 100 RPS per replica
```
