# k8s-rightsize

Right-size Kubernetes CPU and memory requests, limits and namespace quotas from two weeks of container metrics,
**with a confidence score that says when the data can't be trusted.**

Most right-sizing tools take a percentile of observed usage and add headroom. That goes wrong exactly where it
matters: a container throttled at its CPU limit, or OOMKilled at its memory limit, *never shows its real demand*, so
sizing it from what was observed locks in the shortage. This tool detects those cases (and leaks, crashes, HPAs and
batch jobs), sizes each one differently, and tells you how far to trust every number.

- **Pure Python standard library.** No dependencies, no cluster access: it reads a CSV.
- **Per-container recommendations** under a conservative and a balanced profile, with staged step-downs for large cuts.
- **Separate CPU and memory confidence scores (1–10)**, each with the reasons behind the deductions.
- **Ready-to-review manifests:** strategic-merge patches per workload, plus a `ResourceQuota` and `LimitRange` per namespace.
- **A self-contained HTML report** with an action list, per-workload sheets, charts, rollout plan and trade-offs.

## Quick start

```sh
python analyze.py examples/demo-metrics-14d.csv.gz --config examples/demo-config.json
open out/report.html            # or start out/report.html on Windows
```

The demo is a 14-day sample export of 10 containers. It covers every class the engine knows: a CPU-throttled
service, an OOMKilled store, a leaking cache, a crash-looping worker, a nightly batch job, an autoscaled router and more.

To run it on your own cluster:

```sh
kubectl -n monitoring port-forward svc/prometheus-operated 9090 &      # or however you reach Prometheus
python export_prometheus.py --url http://localhost:9090 --days 14 --out metrics.csv.gz
python analyze.py metrics.csv.gz
```

Requires Python 3.9 or newer.

## What you get

```
out/
  report.html              the report: summary, action list, workload sheets, quotas, rollout plan, method
  recommendations.csv      one row per workload/container: current vs recommended, confidence, reasons, trade-offs
  recommendations.json     everything the report renders, for your own tooling
  manifests/<ns>.yaml      patches + ResourceQuota + LimitRange (manifests/<cluster>/<ns>.yaml for multi-cluster exports)
```

A manifest excerpt from the demo:

```yaml
---
# confidence  app: CPU 4/10, memory 9/10, istio-proxy: CPU 8/10, memory 9/10
# kubectl -n commerce patch deployment checkout-service --patch-file <this document>
spec:
  template:
    metadata:
      annotations:
        # istio-proxy is sized through these annotations
        sidecar.istio.io/proxyCPU: "200m"
        sidecar.istio.io/proxyMemory: "128Mi"
        sidecar.istio.io/proxyMemoryLimit: "192Mi"
    spec:
      containers:
      - name: app
        resources:
          requests:
            cpu: "850m"
            memory: "832Mi"
          limits:
            memory: "1Gi"
            cpu: null   # removes today's 500m limit; use "1700m" instead if your policy requires CPU limits
```

The manifests are a starting point for review, not something to `kubectl apply` blindly. Apply the P1 changes first,
follow the step plans for big cuts, and use the *transition* quota until the last step is done (the report explains why).

## Getting the data

### What the engine needs

For every container, a sample every few minutes (5 minutes is ideal, anything from 15 s to 15 min works) over
**at least 14 days** of:

| Column | Required | Meaning | Prometheus source |
|---|---|---|---|
| `timestamp` | yes | sample time | |
| `namespace`, `pod`, `container` | yes | identity | labels |
| `workload` | yes | owning Deployment/StatefulSet/DaemonSet/CronJob name | `kube_pod_owner` + `kube_replicaset_owner` / `kube_job_owner` |
| `cpu_usage_seconds_total` | one of these two | cumulative CPU seconds (a counter) | `container_cpu_usage_seconds_total` |
| `cpu_usage_cores` | | CPU rate in cores, if your source gives rates instead | e.g. Datadog `kubernetes.cpu.usage.total` ÷ 1e9 |
| `memory_working_set` | yes | working-set memory | `container_memory_working_set_bytes` |
| `cluster` | no (default `default`) | cluster name; keeps same-named workloads in different clusters apart | external label |
| `workload_kind` | no (default `Unknown`) | `Deployment`, `StatefulSet`, `DaemonSet`, `CronJob`, `Job`, `ReplicaSet`, `Pod` | owner kind |
| `cpu_throttled_seconds_total` / `cpu_throttled_cores` | no (default 0) | CFS throttled time, counter or rate to match the CPU column | `container_cpu_cfs_throttled_seconds_total` |
| `cpu_request`, `cpu_limit` | no | current settings: `500m`, `0.5`, `2` | `kube_pod_container_resource_requests` / `_limits{resource="cpu"}` |
| `memory_request`, `memory_limit` | no | current settings: bytes or `512Mi`, `1Gi`, `500M` | same, `resource="memory"` |
| `restart_count` | no | cumulative restarts | `kube_pod_container_status_restarts_total` |
| `last_termination_reason` | no | e.g. `OOMKilled`, `Error` | `kube_pod_container_status_last_terminated_reason` |

The format is **long**: one row per container per timestamp. Extra columns are ignored. Leave a cell empty when a
value is missing; the engine never treats missing as zero.

What it accepts and cleans for you:

- **Timestamps:** ISO 8601 with `Z`, an offset (`+02:00`) or none (taken as UTC), or Unix epoch seconds/milliseconds.
- **Quantities:** CPU in cores or millicores; memory in bytes, scientific notation, binary (`Ki`, `Mi`, `Gi`…) or decimal (`k`, `M`, `G`…) units.
- **Files:** `.csv` or `.csv.gz`, UTF-8 with or without a BOM (Excel exports are fine).
- **Defects:** duplicate rows, `NaN`/`<nil>` values, counter resets after restarts, collection gaps and jittered scrape
  times. Each one is counted in the report's data-quality table. Containers with too little data are listed as skipped, not guessed.

The sample interval is detected from the data (`--interval` overrides it).

### From Prometheus: `export_prometheus.py`

Works with any Prometheus-compatible API (Prometheus, Thanos, Mimir, VictoriaMetrics) that has:

- **cAdvisor metrics**, scraped from the kubelet (standard in kube-prometheus-stack and most managed offerings), and
- **kube-state-metrics v2 or newer** (for requests, limits, restarts and owners).

```sh
python export_prometheus.py --url http://localhost:9090 --out metrics.csv.gz \
    [--days 14] [--step 5m] [--namespace 'shop|payments'] [--exclude-namespace 'kube-.*'] \
    [--cluster-label cluster | --cluster prod-eu] \
    [--header 'X-Scope-OrgID: tenant-1'] [--bearer-token-file token.txt] [--end 2026-07-01T00:00:00Z]
```

It queries one day at a time and streams rows to the file, so memory stays bounded on large clusters. For Mimir,
include the API prefix in the URL (`http://mimir/prometheus`). Use `--cluster-label` when one Prometheus stores
several clusters.

**Retention:** kube-prometheus-stack keeps **10 days** by default (`prometheus.prometheusSpec.retention`). Raise it to
15d+, or export from long-term storage (Thanos, Mimir). With less history the tool still runs; confidence scores drop
and the report says why.

### From somewhere else

Anything that can produce the columns above works. Write the CSV yourself, or open an issue/PR with an exporter.

- **Datadog, CloudWatch Container Insights, Azure Monitor** report CPU as a rate. Use the `cpu_usage_cores`
  (and `cpu_throttled_cores`) columns instead of the counters.
- **`kubectl top` / metrics-server** keep no history, so they can't be used.

## Configuration

Facts about your environment go in a JSON file passed with `--config`. Every key is optional; see
[`examples/demo-config.json`](examples/demo-config.json).

```json
{
  "workload_tiers": { "checkout": "latency", "payments/ledger-worker": "background", "prod/ml/embedder": "batch" },
  "hpa": { "shop/web": { "target_cpu_utilization": 0.6 }, "shop/admin": false },
  "hpa_target_utilization": 0.70,
  "injected_sidecars": ["vault-agent"],
  "limitrange": { "default_request_cpu": "100m", "default_request_memory": "128Mi", "default_memory_limit": "512Mi" },
  "stale_after_days": 30
}
```

| Key | Effect |
|---|---|
| `workload_tiers` | `latency` → CPU request from p99, `background` → p95, `batch` → p95 of the busy window. Keys are `workload`, `namespace/workload` or `cluster/namespace/workload` (most specific wins). Unlisted: CronJob/Job → batch, everything else → latency (the cautious choice). |
| `hpa` | Known HPAs and their CPU target. The CPU request of an HPA-scaled workload is never lowered on its own, because that changes scaling. `false` means "no HPA" and turns off inference. HPAs not listed are inferred from pod churn and cost 1 confidence point. |
| `hpa_target_utilization` | Target assumed for inferred HPAs. |
| `injected_sidecars` | Containers added by a mutating webhook. No spec patch is written for them, because the webhook would overwrite it. `istio-proxy` and `linkerd-proxy` are built in and sized through their pod annotations. |
| `limitrange` | Defaults written into each namespace's `LimitRange`. |
| `stale_after_days` | Data older than this loses a confidence point and triggers a warning banner. |

The sizing method itself (percentiles, headroom, thresholds) is a set of named constants at the top of `analyze.py`.

## How it works

1. **Profile** each container across all its pods: CPU rate percentiles from the counter deltas, memory percentiles,
   a daily-max memory trend line, throttled share, restarts by reason, replicas over time, and gaps.
2. **Classify** it. The first match wins:

   | Class | Signal | What the recommendation does |
   |---|---|---|
   | Memory-capped (OOMKilled) | OOMKills, or working set ≥ 97% of the limit | raises the limit 50% to expose real demand; re-measure |
   | CPU-starved by limit | > 5% of CPU time throttled, or p95 at the limit | estimates demand from the throttled share; drops the CPU limit |
   | Memory growing | linear growth > 0.5%/day, R² ≥ 0.8 | never shrinks the request; limit covers 30 more days; says "fix the leak" |
   | Crashing | non-OOM restarts | sizes from stable periods; holds cuts when confidence ≤ 5 |
   | Bursty batch | `batch` tier | reserves for the busy window; offers the cheaper burst option |
   | Autoscaled (HPA) | configured, or many more distinct pods than peak replicas | holds the CPU request |
   | Short history | < 13 days | normal sizing, lower confidence |
   | Clean, steady | none of the above | percentile × headroom |

3. **Size** requests and limits under both profiles. Memory limit = max × 1.5 (conservative). No CPU limit by default,
   with the value to use if your policy requires one. Cuts deeper than 50% are split into steps.
4. **Score confidence** separately for CPU and memory, starting at 10 and deducting for capped usage, growth, short
   history, crashes, burstiness, an unknown HPA, gaps, a single replica or stale data.
5. **Prioritise:** P1 = failing or about to fail, P2 = needs a decision or a fix first, P3 = safe reclaim.
6. **Quotas:** recommended requests × (peak replicas + 25% rolling-update surge for Deployments) × 1.3 buffer, plus a
   *transition* quota sized to the larger of current and recommended values, so the rollout isn't blocked mid-way.

The report's *Method* section covers the full rubric.

## Limitations

- **5-minute averages hide sub-minute spikes.** That's fine for requests, but if you keep CPU limits, validate them with finer data.
- **No cost model.** Deltas are in cores and GiB. Multiply by your own rates.
- **No node packing.** The tool doesn't know your node sizes, so check that large new requests still fit.
- **Patches cover built-in kinds only.** Jobs (immutable templates), bare Pods and custom kinds such as Argo Rollouts get the values as comments instead of a patch.
- **Memory use grows with the export.** Expect about 1 GB of RAM per million rows (roughly 250 containers × 14 days at 5 minutes). Use `--namespace` on the exporter to split very large clusters.

## Development

```sh
python -m unittest discover -s tests      # tests/test_report.py also needs Node.js; it is skipped without it
```

The tests build synthetic exports for every class and input defect (`tests/helpers.py`), run the exporter against a
fake Prometheus, check the generated YAML, render the report's JavaScript under Node, and pin the demo's results.
A change that moves the demo numbers in `tests/test_demo.py` needs a reason in the PR.

The report is `report_template.html`: one file, with the data embedded at build time and no external scripts.

## License

[MIT](LICENSE)
