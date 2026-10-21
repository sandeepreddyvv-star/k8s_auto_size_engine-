"""Right-size Kubernetes CPU/memory requests, limits and namespace quotas from container metrics.

Usage:  python analyze.py metrics.csv[.gz] [--out out] [--asof YYYY-MM-DD] [--config rightsize.json] [--interval SECONDS]

Reads a CSV of container samples (CPU usage as a cumulative counter or a rate, memory working set,
current requests/limits, restart counts; README.md has the schema), and writes:
  out/recommendations.csv     one row per workload/container
  out/recommendations.json    everything the report renders
  out/manifests/<ns>.yaml     resources patches, ResourceQuota and LimitRange per namespace
                              (out/manifests/<cluster>/<ns>.yaml when the export spans several clusters)
  out/report.html             rendered from report_template.html

Standard library only. Every threshold below is a named constant so the method is auditable;
facts about your environment (workload tiers, HPAs, sidecars, LimitRange defaults) go in the config file.
"""
import argparse, csv, gzip, json, math, re, sys
from collections import defaultdict, Counter
from datetime import datetime, date, timezone
from pathlib import Path

if sys.version_info < (3, 9):
    sys.exit('analyze.py needs Python 3.9 or newer')

# ---------------------------------------------------------------------------
# Assumptions you may want to edit
# ---------------------------------------------------------------------------
# Defaults for the config file (--config). See README.md and examples/demo-config.json.
DEFAULT_CONFIG = dict(
    # tier drives the percentile used for CPU requests: latency | background | batch. Keys are
    # "workload", "namespace/workload" or "cluster/namespace/workload". Unlisted workloads default to
    # "batch" (CronJob/Job) or "latency" (everything else): the cautious choice.
    workload_tiers={},
    # Known HPAs, same keys: {"target_cpu_utilization": 0.7}, or false for "no HPA" (stops inference).
    hpa={},
    hpa_target_utilization=0.70,    # assumed for HPAs inferred from pod churn
    # Containers injected by a webhook (other than Istio/Linkerd, which are built in): no spec patch is written.
    injected_sidecars=[],
    limitrange=dict(default_request_cpu='100m', default_request_memory='128Mi', default_memory_limit='512Mi'),
    stale_after_days=30,
)
CFG = DEFAULT_CONFIG
TIERS = ('latency', 'background', 'batch')
DEPLOYMENT_MAX_SURGE = 0.25     # Kubernetes default rolling-update surge

PROFILES = {
    # conservative is the primary recommendation; balanced is shown for comparison
    'conservative': dict(cpu_lat_pct=0.99, cpu_bg_pct=0.95, cpu_headroom=1.25,
                         mem_req_mult=1.15, mem_lim_mult=1.5, quota_buffer=1.30, step_cap=0.5),
    'balanced':     dict(cpu_lat_pct=0.95, cpu_bg_pct=0.90, cpu_headroom=1.15,
                         mem_req_mult=1.05, mem_lim_mult=1.25, quota_buffer=1.15, step_cap=0.5),
}
CPU_LIMIT_MULT = 2.0            # alternative policy: CPU limit >= 2x request ...
CPU_LIMIT_PEAK_MULT = 1.5       # ... and >= 1.5x the highest observed rate
THROTTLE_CENSORED = 0.05        # >5% of CPU time throttled: real demand is hidden
MEM_AT_LIMIT = 0.97             # working set >= 97% of limit: real demand is hidden
GROWTH_PCT_PER_DAY = 0.005      # memory trend counts as growth above 0.5%/day ...
GROWTH_R2 = 0.80                # ... with a near-linear fit
LEAK_RUNWAY_DAYS = 30           # limit for a growing workload covers this many more days
GAP_FACTOR = 1.5                # an interval longer than 1.5x the sample interval is a collection gap

# Mesh sidecars are sized through pod annotations, not the container spec.
MESH_SIDECARS = {
    'istio-proxy': dict(cpu='sidecar.istio.io/proxyCPU', cpu_limit='sidecar.istio.io/proxyCPULimit',
                        memory='sidecar.istio.io/proxyMemory', memory_limit='sidecar.istio.io/proxyMemoryLimit'),
    'linkerd-proxy': dict(cpu='config.linkerd.io/proxy-cpu-request', cpu_limit='config.linkerd.io/proxy-cpu-limit',
                          memory='config.linkerd.io/proxy-memory-request', memory_limit='config.linkerd.io/proxy-memory-limit'),
}
KINDS = {k.lower(): k for k in ('Deployment', 'StatefulSet', 'DaemonSet', 'ReplicaSet', 'Job', 'CronJob', 'Pod')}
TEMPLATE_KINDS = ('Deployment', 'StatefulSet', 'DaemonSet', 'ReplicaSet')   # pod template at spec.template, patchable

# ---------------------------------------------------------------------------
# Input schema
# ---------------------------------------------------------------------------
REQUIRED_COLUMNS = ('timestamp', 'namespace', 'workload', 'pod', 'container', 'memory_working_set')
CPU_COLUMNS = ('cpu_usage_seconds_total', 'cpu_usage_cores')     # one of these is required
OPTIONAL_COLUMNS = ('cluster', 'workload_kind', 'cpu_throttled_seconds_total', 'cpu_throttled_cores',
                    'cpu_request', 'cpu_limit', 'memory_request', 'memory_limit', 'restart_count', 'last_termination_reason')


class InputError(Exception):
    pass


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
MEM_UNITS = {'': 1, 'Ki': 2**10, 'Mi': 2**20, 'Gi': 2**30, 'Ti': 2**40, 'Pi': 2**50, 'Ei': 2**60,
             'k': 1e3, 'K': 1e3, 'M': 1e6, 'G': 1e9, 'T': 1e12, 'P': 1e15, 'E': 1e18}
MiB, GiB = 2**20, 2**30
EPOCH = datetime(1970, 1, 1)


def parse_float(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def parse_cpu(v):
    v = (v or '').strip()
    if v.endswith('m'):
        x = parse_float(v[:-1])
        return None if x is None else x / 1000
    return parse_float(v) if v else None


def parse_mem_unit(v):
    """Kubernetes quantity or plain bytes -> (bytes, unit), or (None, None) if unparseable."""
    m = re.fullmatch(r'([\d.]+(?:[eE][+-]?\d+)?)([A-Za-z]*)', (v or '').strip())
    if not m or m.group(2) not in MEM_UNITS or parse_float(m.group(1)) is None:
        return None, None
    return float(m.group(1)) * MEM_UNITS[m.group(2)], m.group(2)


def parse_mem(v):
    return parse_mem_unit(v)[0]


def parse_ts(s):
    """ISO 8601 (Z, offset or naive = UTC) or Unix epoch seconds/milliseconds -> naive UTC datetime."""
    s = (s or '').strip()
    n = parse_float(s)
    if n is not None:
        return datetime.fromtimestamp(n / 1000 if n > 1e11 else n, timezone.utc).replace(tzinfo=None)
    if s.endswith(('Z', 'z')):
        s = s[:-1] + '+00:00'           # fromisoformat accepts "Z" only from Python 3.11
    t = datetime.fromisoformat(s)
    return t.astimezone(timezone.utc).replace(tzinfo=None) if t.tzinfo else t


def iso(t):
    return t.isoformat() + 'Z'


def pct(sorted_vals, p):
    if not sorted_vals:
        return None
    return sorted_vals[min(len(sorted_vals) - 1, max(0, math.ceil(p * len(sorted_vals)) - 1))]


def linfit(ys):
    n = len(ys)
    if n < 3:
        return 0.0, 0.0
    xs = range(n)
    mx, my = (n - 1) / 2, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    slope = sxy / sxx
    ss_tot = sum((y - my) ** 2 for y in ys)
    ss_res = sum((y - (my + slope * (x - mx))) ** 2 for x, y in zip(xs, ys))
    return slope, (1 - ss_res / ss_tot) if ss_tot else 0.0


def interval_label(sec):
    if sec % 60:
        return f'{sec:g}-second'
    return f'{sec // 60:g}-minute' if sec % 3600 else f'{sec // 3600:g}-hour'


# ---------------------------------------------------------------------------
# Rounding and formatting to Kubernetes quantities
# ---------------------------------------------------------------------------
def round_cpu(c):
    c = max(c, 0.05)
    return math.ceil(c * 20) / 20 if c < 1 else math.ceil(c * 10) / 10


def round_mem(b):
    step = 64 * MiB if b < GiB else 128 * MiB if b < 4 * GiB else 256 * MiB
    return math.ceil(max(b, 64 * MiB) / step) * step


def fmt_cpu(c):
    if c is None:
        return ''
    return str(int(round(c))) if abs(c - round(c)) < 1e-9 else f'{int(round(c * 1000))}m'


def fmt_mem(b):
    if b is None:
        return ''
    if b % GiB == 0:
        return f'{int(b // GiB)}Gi'
    if b % MiB == 0:
        return f'{int(b // MiB)}Mi'
    return f'{int(b // 1e6)}M'


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def load_config(path):
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    if not path:
        return cfg
    try:
        user = json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, ValueError) as e:
        raise InputError(f'cannot read config {path}: {e}')
    unknown = sorted(set(user) - set(cfg) - {'$comment'})
    if unknown:
        raise InputError(f'unknown config key(s) in {path}: {", ".join(unknown)}. Known: {", ".join(sorted(cfg))}')
    for k, v in user.items():
        if k == 'limitrange':
            cfg[k].update(v)
        elif k != '$comment':
            cfg[k] = v
    bad = {k: v for k, v in cfg['workload_tiers'].items() if v not in TIERS}
    if bad:
        raise InputError(f'workload_tiers values must be one of {", ".join(TIERS)}; got {bad}')
    for k in ('default_request_cpu',):
        if parse_cpu(cfg['limitrange'][k]) is None:
            raise InputError(f'limitrange.{k} is not a CPU quantity: {cfg["limitrange"][k]!r}')
    for k in ('default_request_memory', 'default_memory_limit'):
        if parse_mem(cfg['limitrange'][k]) is None:
            raise InputError(f'limitrange.{k} is not a memory quantity: {cfg["limitrange"][k]!r}')
    return cfg


def lookup(table, cluster, ns, wl):
    for k in (f'{cluster}/{ns}/{wl}', f'{ns}/{wl}', wl):
        if k in table:
            return True, table[k]
    return False, None


# ---------------------------------------------------------------------------
# Load and clean
# ---------------------------------------------------------------------------
def open_text(path):
    if str(path).endswith('.gz'):
        return gzip.open(path, 'rt', newline='', encoding='utf-8-sig')
    return open(path, newline='', encoding='utf-8-sig')


def load(path):
    dq = Counter()
    seen = set()
    series = defaultdict(list)          # (cluster, ns, workload, container, pod) -> rows
    meta = {}
    with open_text(path) as f:
        rd = csv.DictReader(f)
        cols = {c.strip() for c in rd.fieldnames or []}
        missing = [c for c in REQUIRED_COLUMNS if c not in cols]
        if not cols & set(CPU_COLUMNS):
            missing.append(' or '.join(CPU_COLUMNS))
        if missing:
            raise InputError(f'{path} is missing required column(s): {", ".join(missing)}.\n'
                             f'  required: {", ".join(REQUIRED_COLUMNS)}, and {" or ".join(CPU_COLUMNS)}\n'
                             f'  optional: {", ".join(OPTIONAL_COLUMNS)}\n'
                             f'  found:    {", ".join(sorted(cols)) or "(no header)"}\n'
                             '  See "Input format" in README.md.')
        rate_mode = 'cpu_usage_seconds_total' not in cols
        cpu_col = 'cpu_usage_cores' if rate_mode else 'cpu_usage_seconds_total'
        thr_col = 'cpu_throttled_cores' if rate_mode else 'cpu_throttled_seconds_total'
        for r in rd:
            r = {(k or '').strip(): (v or '').strip() for k, v in r.items() if k}
            try:
                t = parse_ts(r['timestamp'])
            except (ValueError, OverflowError, OSError):
                dq['rows with unparseable timestamp dropped'] += 1
                continue
            cluster = r.get('cluster') or 'default'
            key = (cluster, r['namespace'], r['workload'], r['container'], r['pod'])
            if (key, t) in seen:
                dq['duplicate rows removed'] += 1
                continue
            seen.add((key, t))
            cpu = parse_float(r[cpu_col])
            if cpu is None:
                dq['rows with missing or unparseable CPU dropped'] += 1
                continue
            mem, unit = parse_mem_unit(r['memory_working_set'])
            if mem is None:
                dq['memory values missing or unparseable, dropped'] += 1
            else:
                dq[f'memory values in unit "{unit or "bytes"}"'] += 1
            rc = parse_float(r.get('restart_count'))
            series[key].append(dict(
                t=t, cpu=cpu, thr=parse_float(r.get(thr_col)) or 0.0, mem=mem,
                spec=(r.get('cpu_request', ''), r.get('cpu_limit', ''), r.get('memory_request', ''), r.get('memory_limit', '')),
                restarts=None if rc is None else int(rc), reason=r.get('last_termination_reason', '')))
            kind = r.get('workload_kind', '')
            meta[key[:3]] = KINDS.get(kind.lower(), kind) or 'Unknown'
    for rs in series.values():
        rs.sort(key=lambda x: x['t'])
        if rate_mode:
            # turn rates into the cumulative counters the rest of the pipeline expects
            cpu = thr = 0.0
            prev = None
            for r in rs:
                if prev is not None:
                    dt = (r['t'] - prev).total_seconds()
                    cpu += max(r['cpu'], 0) * dt
                    thr += max(r['thr'], 0) * dt
                prev = r['t']
                r['cpu'], r['thr'] = cpu, thr
    if rate_mode:
        dq['CPU given as a rate (cpu_usage_cores)'] += 1
    return series, meta, dq


def detect_interval(series):
    """Median spacing between consecutive samples, in seconds."""
    diffs = Counter()
    for rs in series.values():
        for a, b in zip(rs, rs[1:]):
            d = round((b['t'] - a['t']).total_seconds())
            if d > 0:
                diffs[d] += 1
    if not diffs:
        return 300
    half, run = sum(diffs.values()) / 2, 0
    for d in sorted(diffs):
        run += diffs[d]
        if run >= half:
            return d


# ---------------------------------------------------------------------------
# Profile one container (all its pods)
# ---------------------------------------------------------------------------
def profile(pods, dq, interval):
    rates, mems, by_hour, daily_mem = [], [], defaultdict(list), defaultdict(float)
    usage_total = thr_total = 0.0
    restarts, reasons = 0, Counter()
    resets = gaps = gap_minutes = 0
    specs = Counter()
    first = min(rs[0]['t'] for rs in pods.values())
    last = max(rs[-1]['t'] for rs in pods.values())
    for rs in pods.values():
        for r in rs:
            specs[r['spec']] += 1
            if r['mem'] is not None:
                mems.append(r['mem'])
                d = r['t'].date().isoformat()
                daily_mem[d] = max(daily_mem[d], r['mem'])
        for a, b in zip(rs, rs[1:]):
            dt = (b['t'] - a['t']).total_seconds()
            if dt > GAP_FACTOR * interval:
                gaps += 1
                gap_minutes += (dt - interval) / 60
            if a['restarts'] is not None and b['restarts'] is not None and b['restarts'] > a['restarts']:
                restarts += b['restarts'] - a['restarts']
                if b['reason']:
                    reasons[b['reason']] += b['restarts'] - a['restarts']
            du, dth = b['cpu'] - a['cpu'], b['thr'] - a['thr']
            if du < 0 or dth < 0:          # counter reset after a container restart
                resets += 1
                continue
            rate = du / dt
            rates.append(rate)
            by_hour[b['t'].hour].append(rate)
            usage_total += du
            thr_total += dth
    rates.sort(); mems.sort()
    cpu_req, cpu_lim, mem_req, mem_lim = specs.most_common(1)[0][0]
    days = sorted(daily_mem)
    mem_daily = [daily_mem[d] for d in days]
    # drop partial first/last day from the trend fit when the window is long enough
    fit = mem_daily[1:-1] if len(mem_daily) > 5 else mem_daily
    slope, r2 = linfit(fit)
    if resets:
        dq['CPU counter resets skipped (restarts)'] += resets
    return dict(
        spec=dict(cpu_request=parse_cpu(cpu_req), cpu_limit=parse_cpu(cpu_lim),
                  mem_request=parse_mem(mem_req), mem_limit=parse_mem(mem_lim),
                  raw=dict(cpu_request=cpu_req, cpu_limit=cpu_lim, mem_request=mem_req, mem_limit=mem_lim)),
        spec_changes=len(specs) - 1,
        history_days=round((last - first).total_seconds() / 86400 + interval / 86400, 1),
        first=iso(first), last=iso(last),
        cpu={k: pct(rates, p) for k, p in [('p50', .5), ('p90', .9), ('p95', .95), ('p99', .99)]} | {'max': rates[-1] if rates else None},
        cpu_samples=len(rates),
        throttle_ratio=thr_total / usage_total if usage_total else 0.0,
        mem={k: pct(mems, p) for k, p in [('p50', .5), ('p95', .95), ('p99', .99)]} | {'max': mems[-1] if mems else None},
        mem_daily_max_mib=[round(m / MiB) for m in mem_daily],
        mem_slope_mib_day=round(slope / MiB, 1), mem_trend_r2=round(r2, 3),
        cpu_hourly_p95=[round(pct(sorted(by_hour[h]), .95) or 0, 3) for h in range(24)],
        restarts=restarts, termination_reasons=dict(reasons),
        gaps=gaps, gap_hours=round(gap_minutes / 60, 1), counter_resets=resets,
        _rates=rates,
    )


def replica_stats(pods, interval):
    """pods: (container, pod) -> rows for one workload. Samples are bucketed by interval so jittered scrapes line up."""
    per_bucket = defaultdict(set)
    names = set()
    for (c, p), rs in pods.items():
        names.add(p)
        for r in rs:
            per_bucket[round((r['t'] - EPOCH).total_seconds() / interval)].add(p)
    counts = [len(v) for v in per_bucket.values()]
    return dict(avg=round(sum(counts) / len(counts), 2), peak=max(counts), distinct_pods=len(names),
                max_pod_lifetime_days=round(max((rs[-1]['t'] - rs[0]['t']).total_seconds() / 86400 for rs in pods.values()), 1))


# ---------------------------------------------------------------------------
# Classification, sizing, confidence
# ---------------------------------------------------------------------------
def classify(p, kind, reps, tier, hpa):
    s = p['spec']
    oom = p['termination_reasons'].get('OOMKilled', 0)
    hpa_inferred = hpa is None and kind == 'Deployment' and reps['distinct_pods'] > 3 * reps['peak']
    flags = dict(
        tier=tier,
        cpu_censored=p['throttle_ratio'] > THROTTLE_CENSORED or bool(s['cpu_limit'] and p['cpu']['p95'] >= 0.95 * s['cpu_limit']),
        mem_censored=oom > 0 or bool(s['mem_limit'] and p['mem']['max'] >= MEM_AT_LIMIT * s['mem_limit']),
        mem_growing=(p['mem_slope_mib_day'] * MiB > GROWTH_PCT_PER_DAY * p['mem']['p50'] and p['mem_trend_r2'] >= GROWTH_R2),
        crashing=sum(v for k, v in p['termination_reasons'].items() if k != 'OOMKilled') > 0,
        short_history=p['history_days'] < 13,
        hpa=bool(hpa) or hpa_inferred,
        hpa_inferred=hpa_inferred,
        single_replica=reps['peak'] == 1,
        no_mem_limit=s['mem_limit'] is None,
        no_cpu_limit=s['cpu_limit'] is None,
        bursty=p['cpu']['p50'] < 0.2 * p['cpu']['p95'] or (p['cpu']['max'] > 2.5 * p['cpu']['p95']),
        kind_mismatch=kind in ('CronJob', 'Job') and reps['max_pod_lifetime_days'] > 1,
    )
    if flags['mem_censored']:
        cls = 'Memory-capped (OOMKilled)'
    elif flags['cpu_censored']:
        cls = 'CPU-starved by limit'
    elif flags['mem_growing']:
        cls = 'Memory growing'
    elif flags['crashing']:
        cls = 'Crashing'
    elif tier == 'batch':
        cls = 'Bursty batch'
    elif flags['hpa']:
        cls = 'Autoscaled (HPA)'
    elif flags['short_history']:
        cls = 'Short history'
    else:
        cls = 'Clean, steady'
    return cls, flags


def staged(current, target, cap, rnd):
    """Steps from current to target, never cutting more than (1-cap) of the current value per step."""
    if current is None or target >= current * cap:
        return [target]
    steps, v = [], current
    while target < v * cap:
        v = rnd(v * cap)
        steps.append(v)
    if steps[-1] <= target * 1.25:      # a last step this close to the target adds nothing
        steps.pop()
    return steps + [target]


def size(p, flags, prof, reps):
    s, c, m = p['spec'], p['cpu'], p['mem']
    notes = []
    tier = flags['tier']

    # --- CPU request ---
    if tier == 'batch':
        thr = 0.25 * c['p99']
        busy = sorted(r for r in p['_rates'] if r >= thr)
        busy_p95 = pct(busy, .95)
        cpu_req = busy_p95 * prof['cpu_headroom']
        hours = [h for h, v in enumerate(p['cpu_hourly_p95']) if v >= thr]
        p['batch_busy'] = dict(share=round(len(busy) / len(p['_rates']), 3), p95=busy_p95,
                               window=f'{min(hours):02d}:00–{max(hours) + 1:02d}:00 UTC' if hours else 'irregular')
    else:
        q = prof['cpu_lat_pct'] if tier == 'latency' else prof['cpu_bg_pct']
        cpu_req = pct(p['_rates'], q) * prof['cpu_headroom']
    if flags['cpu_censored']:
        # usage is pinned at the limit; scale by the throttled share as a floor for real demand
        cpu_req = max(cpu_req, c['p99'] * (1 + p['throttle_ratio']) * prof['cpu_headroom'])
    cpu_req = round_cpu(cpu_req)
    if flags['hpa'] and s['cpu_request'] and cpu_req < s['cpu_request']:
        notes.append('hpa-hold')
        cpu_req = s['cpu_request']
    cpu_lim_alt = round_cpu(max(CPU_LIMIT_MULT * cpu_req, CPU_LIMIT_PEAK_MULT * c['max']))

    # --- memory ---
    if flags['mem_censored'] and s['mem_limit']:
        mem_lim = round_mem(s['mem_limit'] * 1.5)
        mem_req = round_mem(s['mem_limit'] * 1.25)
    elif flags['mem_growing']:
        # never shrink the request of a container that is still growing
        mem_req = round_mem(max(m['max'] * prof['mem_req_mult'], s['mem_request'] or 0))
        projected = m['max'] + p['mem_slope_mib_day'] * MiB * LEAK_RUNWAY_DAYS
        mem_lim = round_mem(max(projected * 1.25, mem_req, s['mem_limit'] or 0))
        p['mem_projection'] = dict(projected_30d_mib=round(projected / MiB))
    else:
        mem_req = round_mem(m['max'] * prof['mem_req_mult'])
        mem_lim = round_mem(m['max'] * prof['mem_lim_mult'])
    mem_lim = max(mem_lim, mem_req)

    return dict(
        cpu_request=cpu_req, cpu_limit_primary=None, cpu_limit_alt=cpu_lim_alt,
        mem_request=mem_req, mem_limit=mem_lim, mem_guaranteed=mem_lim,
        cpu_steps=staged(s['cpu_request'], cpu_req, prof['step_cap'], round_cpu),
        mem_steps=staged(s['mem_request'], mem_req, prof['step_cap'], round_mem),
        notes=notes,
    )


def runway_days(p, limit_bytes):
    slope = p['mem_slope_mib_day'] * MiB
    if not limit_bytes or slope <= 0:
        return None
    return round((limit_bytes - p['mem']['max']) / slope)


def confidence(p, flags, reps, stale_days, interval):
    cpu, mem = [], []
    both = []
    if p['history_days'] < 7:
        both.append((-3, f'only {p["history_days"]:g} days of history'))
    elif p['history_days'] < 13:
        both.append((-2, f'only {p["history_days"]:g} days of history (no second weekly cycle)'))
    if stale_days > CFG['stale_after_days']:
        both.append((-1, f'data ends {stale_days} days ago'))
    if p['gaps']:
        both.append((-1, f'{p["gaps"]} collection gap(s), {p["gap_hours"]} h missing'))
    if flags['crashing']:
        n = sum(v for k, v in p['termination_reasons'].items() if k != 'OOMKilled')
        both.append((-2 if n > 10 else -1, f'{n} non-OOM restarts: behaviour is unstable'))
    if flags['single_replica']:
        both.append((-1, 'single replica: one sample of behaviour'))
    if flags['kind_mismatch']:
        both.append((-1, f'labelled {p["kind"]} but pods live the whole window: lifecycle unclear'))

    if flags['cpu_censored']:
        pts = -5 if p['throttle_ratio'] > 0.20 else -3
        cpu.append((pts, f'{p["throttle_ratio"]:.0%} of CPU time throttled: true demand not observed'))
    if flags['bursty']:
        cpu.append((-2, f'bursty: {interval_label(interval)} averages hide short spikes'))
    elif p['cpu']['p99'] > 1.5 * p['cpu']['p50']:
        cpu.append((-1, 'moderate variance between median and p99'))
    if flags['hpa_inferred']:
        cpu.append((-1, 'HPA config not in export: request also sets scaling behaviour'))
    if p['counter_resets']:
        cpu.append((-1, f'{p["counter_resets"]} CPU counter resets around restarts'))

    if flags['mem_censored']:
        oom = p['termination_reasons'].get('OOMKilled', 0)
        mem.append((-5 if oom else -3, f'working set pinned at limit{f", {oom} OOMKills" if oom else ""}: true demand not observed'))
    if flags['mem_growing']:
        mem.append((-4, f'memory grows {p["mem_slope_mib_day"]} MiB/day with no plateau'))
    if p['mem']['max'] > 1.5 * p['mem']['p95']:
        mem.append((-1, 'rare memory peaks well above p95'))

    def score(items):
        return max(1, min(10, 10 + sum(x for x, _ in items)))
    return (dict(score=score(both + cpu), reasons=[r for _, r in both + cpu], points=[x for x, _ in both + cpu]),
            dict(score=score(both + mem), reasons=[r for _, r in both + mem], points=[x for x, _ in both + mem]))


def qos(containers, use_rec, cpu_limits):
    guaranteed = True
    for c in containers:
        if use_rec:
            r = c['rec']
            cr, cl = r['cpu_request'], (r['cpu_limit_alt'] if cpu_limits else None)
            mr, ml = r['mem_request'], r['mem_limit']
        else:
            s = c['spec']
            cr, cl, mr, ml = s['cpu_request'], s['cpu_limit'], s['mem_request'], s['mem_limit']
        if not (cr and cl and mr and ml and abs(cr - cl) < 1e-9 and mr == ml):
            guaranteed = False
    return 'Guaranteed' if guaranteed else 'Burstable'


# ---------------------------------------------------------------------------
# Narrative per class (trade-offs the reader needs to make the call)
# ---------------------------------------------------------------------------
def tradeoffs(c):
    f, s, r, p = c['flags'], c['spec'], c['rec'], c
    out = []
    if f['cpu_censored']:
        out.append(f'CPU is pinned at the {fmt_cpu(s["cpu_limit"])} limit with {p["throttle_ratio"]:.0%} of CPU time throttled, '
                   f'so observed usage is a floor, not the real demand. The {fmt_cpu(r["cpu_request"])} request is an estimate. '
                   'Remove the limit (or raise it to the alternative value), run a week, then re-size from the new data.')
    if f['mem_censored']:
        oom = p['termination_reasons'].get('OOMKilled', 0)
        if s['mem_limit']:
            out.append(f'Memory sits at the {fmt_mem(s["mem_limit"])} limit' + (f' and the container was OOMKilled {oom} times' if oom else '') +
                       f'. The real peak is unknown. The recommendation raises the limit by 50% to {fmt_mem(r["mem_limit"])} to stop the kills '
                       'and expose true usage; expect to revise it after a week. '
                       'Check for a leak or an unbounded in-process cache too: if usage climbs to the new limit, more memory is not the fix.')
        else:
            out.append(f'OOMKilled {oom} times with no memory limit set: the node ran out of memory, not the container. The observed peak '
                       'is cut short by the kills, so treat these values as a floor and check node memory pressure.')
    if f['mem_growing']:
        cur_rw = runway_days(p, s['mem_limit'])
        new_rw = runway_days(p, r['mem_limit'])
        out.append(f'Memory climbs about {p["mem_slope_mib_day"]} MiB/day in a straight line (R² {p["mem_trend_r2"]}) and never drops. ' +
                   (f'At the current {fmt_mem(s["mem_limit"])} limit it runs out roughly {cur_rw} days after the data ends; the '
                    if cur_rw is not None else 'There is no limit today; the ') +
                   f'{fmt_mem(r["mem_limit"])} limit buys about {new_rw} days. A bigger limit only postpones the OOMKill: '
                   'fix the leak or cap the cache (e.g. maxmemory with an eviction policy) and re-size after it plateaus.')
    if f['crashing']:
        rs = {k: v for k, v in p['termination_reasons'].items() if k != 'OOMKilled'}
        n = sum(rs.values())
        why = ', '.join(f'"{k}"' if len(rs) == 1 else f'{v} "{k}"' for k, v in sorted(rs.items(), key=lambda kv: -kv[1]))
        out.append(f'{n} restarts (reason {why}), not OOMKilled: this points to an application fault, not a resource shortage. '
                   'Sizing is based on stable periods. Fix the crash before trusting these numbers.')
    if f['hpa']:
        held = 'hpa-hold' in r['notes']
        target = c['hpa_target']
        seen = (f'{p["replicas"]["distinct_pods"]} pods came and went (peak {p["replicas"]["peak"]} at once), which indicates an HPA. '
                if f['hpa_inferred'] else 'This workload has an HPA. ')
        out.append(seen + 'The HPA scales on usage as a percentage of the CPU request, so lowering the request makes it scale out sooner '
                   'and run more pods. ' + ('The CPU request is held at its current value for that reason; lower it only together '
                   f'with the HPA target ({"assumed " if f["hpa_inferred"] else ""}{target:.0%}).' if held else ''))
    if f['tier'] == 'batch':
        b = p.get('batch_busy', {})
        share = b.get('share', 0)
        when = f'{b["window"]} daily' if b.get('window', 'irregular') != 'irregular' else 'at irregular times'
        small = f'({fmt_cpu(s["cpu_request"])} CPU) ' if s['cpu_request'] else ''
        out.append(f'Busy {share:.0%} of the time ({when}), near idle otherwise. The recommendation reserves '
                   f'enough for the run ({fmt_cpu(r["cpu_request"])} CPU, {fmt_mem(r["mem_request"])} memory) so the job is not starved or '
                   f'evicted mid-run. That reservation is idle ~{round(24 * (1 - share))} h/day if the pods really run continuously. '
                   f'Cheaper option: keep a small request {small}and burst to a {fmt_cpu(r["cpu_limit_alt"])} limit, accepting slower runs '
                   'when the node is busy and a higher eviction risk under memory pressure.')
    if f['bursty'] and f['tier'] != 'batch':
        out.append(f'Usage is peaky: median {fmt_cpu(round_cpu(p["cpu"]["p50"]))} vs p95 {fmt_cpu(round_cpu(p["cpu"]["p95"]))} CPU. '
                   f'The {fmt_cpu(r["cpu_request"])} request reserves the peak around the clock. Cheaper option: request near p90 '
                   'and let it burst with no CPU limit. That works while the node has spare CPU; under contention the container '
                   'only gets its request share and latency rises at peak.')
    if c['qos']['current'] == 'Guaranteed' and c['qos']['primary'] != 'Guaranteed':
        out.append('Currently Guaranteed QoS (requests equal limits). With no CPU limit it becomes Burstable and is evicted '
                   'before Guaranteed pods under node memory pressure. To keep Guaranteed, set CPU limit = CPU request and '
                   'memory limit = memory request.')
    if c.get('blocked'):
        out.append('Do not reduce yet: confidence is too low to cut safely. Fix the crash, collect a clean week, then apply the step plan.')
    if f['kind_mismatch']:
        out.append(f'Labelled {c["kind"]}, yet the same pods exist for the whole {p["history_days"]:g} days. Real {c["kind"]} pods exist only '
                   'while a run is in progress, so a run-sized reservation costs little. Confirm how this workload is deployed.')
    if f['short_history']:
        out.append(f'Only {p["history_days"]:g} days of data, so a monthly peak or a second weekly cycle may be missing. '
                   'Recheck once 14+ days exist.')
    if f['no_mem_limit']:
        out.append('No memory limit today: one runaway pod can push the node into memory pressure and get its neighbours '
                   f'evicted. Add the {fmt_mem(r["mem_limit"])} limit.')
    if c['container'] in MESH_SIDECARS:
        a = MESH_SIDECARS[c['container']]
        out.append(f'Sidecar: set these through the pod annotations {a["cpu"]}, {a["cpu_limit"]}, {a["memory"]} and '
                   f'{a["memory_limit"]}, or the mesh-wide defaults, rather than the container spec.')
    elif c['container'] in CFG['injected_sidecars']:
        out.append('Injected by a webhook: set these through the injector\'s own configuration. A patch to the pod spec is '
                   'overwritten when the pod is created.')
    if s['mem_limit'] and p['mem']['max'] > 0.8 * s['mem_limit'] and not f['mem_censored']:
        out.append(f'Peak memory is {p["mem"]["max"] / s["mem_limit"]:.0%} of the current limit, with little room for a traffic spike.')
    if len(r['cpu_steps']) > 1 or len(r['mem_steps']) > 1:
        out.append('Large cut: apply it in steps (see the step plan) and hold each step for at least a few days.')
    if not out:
        out.append(f'Stable, unconstrained usage over {p["history_days"]:g} days. Sized directly from the observed percentiles plus headroom.')
    return out


def action(cur, new):
    if cur is None:
        return 'set'
    if new > cur * 1.05:
        return 'increase'
    if new < cur * 0.95:
        return 'decrease'
    return 'hold'


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv=None):
    global CFG
    ap = argparse.ArgumentParser(description='Right-size Kubernetes requests, limits and quotas from a metrics CSV.')
    ap.add_argument('csv', help='metrics export (.csv or .csv.gz)')
    ap.add_argument('--out', default='out', help='output directory (default: out)')
    ap.add_argument('--asof', default=date.today().isoformat(), help='report date, YYYY-MM-DD (default: today)')
    ap.add_argument('--config', help='JSON config: workload tiers, HPAs, sidecars, LimitRange defaults')
    ap.add_argument('--interval', type=float, help='sample interval in seconds (default: detected from the data)')
    a = ap.parse_args(argv)
    try:
        CFG = load_config(a.config)
        asof = datetime.fromisoformat(a.asof)
        series, meta, dq = load(a.csv)
    except InputError as e:
        sys.exit(f'error: {e}')
    except ValueError as e:
        sys.exit(f'error: --asof must be YYYY-MM-DD ({e})')
    except OSError as e:
        sys.exit(f'error: cannot read {a.csv}: {e}')
    if not series:
        sys.exit(f'error: no usable rows in {a.csv}')
    interval = a.interval or detect_interval(series)
    out = Path(a.out); (out / 'manifests').mkdir(parents=True, exist_ok=True)

    groups = defaultdict(dict)          # (cluster, ns, workload, container) -> pod -> rows
    by_workload = defaultdict(dict)     # (cluster, ns, workload) -> (container, pod) -> rows
    for (cl, ns, wl, ctr, pod), rs in series.items():
        groups[(cl, ns, wl, ctr)][pod] = rs
        by_workload[(cl, ns, wl)][(ctr, pod)] = rs
    clusters = sorted({k[0] for k in series})
    multi = len(clusters) > 1

    window_end = max(rs[-1]['t'] for rs in series.values())
    window_start = min(rs[0]['t'] for rs in series.values())
    stale_days = (asof - window_end).days

    containers, skipped = [], []
    reps_cache = {}
    for (cl, ns, wl, ctr), pods in sorted(groups.items()):
        kind = meta[(cl, ns, wl)]
        p = profile(pods, dq, interval)
        if not p['cpu_samples'] or p['mem']['max'] is None:
            why = 'fewer than two CPU samples in every pod' if not p['cpu_samples'] else 'no parseable memory values'
            skipped.append(dict(cluster=cl, namespace=ns, workload=wl, container=ctr, reason=why))
            dq['containers skipped (too little data)'] += 1
            continue
        if (cl, ns, wl) not in reps_cache:
            reps_cache[(cl, ns, wl)] = replica_stats(by_workload[(cl, ns, wl)], interval)
        reps = reps_cache[(cl, ns, wl)]
        _, tier = lookup(CFG['workload_tiers'], cl, ns, wl)
        tier = tier or ('batch' if kind in ('CronJob', 'Job') else 'latency')
        known, hpa = lookup(CFG['hpa'], cl, ns, wl)
        hpa = (hpa or False) if known else None          # None = unknown, False = known absent, dict = known HPA
        cls, flags = classify(p, kind, reps, tier, hpa)
        p.update(cluster=cl, namespace=ns, workload=wl, container=ctr, kind=kind, replicas=reps, flags=flags,
                 ns_label=f'{cl}/{ns}' if multi else ns, **{'class': cls})
        p['hpa_target'] = (hpa.get('target_cpu_utilization') if isinstance(hpa, dict) else None) or CFG['hpa_target_utilization']
        p['rec'] = size(p, flags, PROFILES['conservative'], reps)
        p['rec_balanced'] = size(dict(p), flags, PROFILES['balanced'], reps)
        p['confidence_cpu'], p['confidence_mem'] = confidence(p, flags, reps, stale_days, interval)
        s, r = p['spec'], p['rec']
        p['action_cpu'] = action(s['cpu_request'], r['cpu_request'])
        p['action_mem'] = action(s['mem_request'], r['mem_request'])
        avg = reps['avg']
        p['delta'] = dict(
            cpu_cores=round((r['cpu_request'] - (s['cpu_request'] or 0)) * avg, 3),
            mem_gib=round((r['mem_request'] - (s['mem_request'] or 0)) * avg / GiB, 3),
            cpu_cores_balanced=round((p['rec_balanced']['cpu_request'] - (s['cpu_request'] or 0)) * avg, 3),
            mem_gib_balanced=round((p['rec_balanced']['mem_request'] - (s['mem_request'] or 0)) * avg / GiB, 3))
        if flags['mem_growing']:
            p['runway'] = dict(current_limit_days=runway_days(p, s['mem_limit']), new_limit_days=runway_days(p, r['mem_limit']))
        containers.append(p)
    if not containers:
        sys.exit(f'error: no container in {a.csv} has enough data to analyse ({len(skipped)} skipped)')

    # workload-level QoS
    by_wl = defaultdict(list)
    for c in containers:
        by_wl[(c['cluster'], c['namespace'], c['workload'])].append(c)
    for cs in by_wl.values():
        q = dict(current=qos(cs, False, False), primary=qos(cs, True, False), cpu_limit_policy=qos(cs, True, True))
        for c in cs:
            c['qos'] = q

    for c in containers:
        c['blocked'] = c['flags']['crashing'] and min(c['confidence_cpu']['score'], c['confidence_mem']['score']) <= 5 \
            and 'decrease' in (c['action_cpu'], c['action_mem'])
        if c['blocked']:
            c['action_cpu'] = 'hold (fix first)' if c['action_cpu'] == 'decrease' else c['action_cpu']
            c['action_mem'] = 'hold (fix first)' if c['action_mem'] == 'decrease' else c['action_mem']
        c['tradeoffs'] = tradeoffs(c)

    # priority: fix what is breaking first, then capture savings
    for c in containers:
        f = c['flags']
        if f['mem_censored'] or f['cpu_censored'] or f['mem_growing']:
            c['priority'] = 'P1'
        elif f['crashing'] or f['no_mem_limit'] or f['kind_mismatch']:
            c['priority'] = 'P2'
        else:
            c['priority'] = 'P3'

    # namespace quotas
    namespaces = []
    lr = CFG['limitrange']
    for cl, ns in sorted({(c['cluster'], c['namespace']) for c in containers}):
        cs = [c for c in containers if c['cluster'] == cl and c['namespace'] == ns]
        tot = Counter()
        for c in cs:
            reps = c['replicas']
            surge = math.ceil(reps['peak'] * DEPLOYMENT_MAX_SURGE) if c['kind'] == 'Deployment' else 0
            n = reps['peak'] + surge
            s, r = c['spec'], c['rec']
            tot['cur_cpu'] += (s['cpu_request'] or 0) * n
            tot['cur_mem'] += (s['mem_request'] or 0) * n
            tot['cpu'] += r['cpu_request'] * n
            tot['mem'] += r['mem_request'] * n
            tot['mem_lim'] += r['mem_limit'] * n
            tot['cpu_lim'] += r['cpu_limit_alt'] * n
            tot['tr_cpu'] += max(s['cpu_request'] or 0, r['cpu_request']) * n
            tot['tr_mem'] += max(s['mem_request'] or 0, r['mem_request']) * n
            tot['tr_mem_lim'] += max(s['mem_limit'] or 0, r['mem_limit']) * n
            tot['avg_cpu'] += r['cpu_request'] * reps['avg']
            tot['cur_avg_cpu'] += (s['cpu_request'] or 0) * reps['avg']
            tot['avg_mem'] += r['mem_request'] * reps['avg']
            tot['cur_avg_mem'] += (s['mem_request'] or 0) * reps['avg']
        pods = sum(c['replicas']['peak'] + (math.ceil(c['replicas']['peak'] * DEPLOYMENT_MAX_SURGE) if c['kind'] == 'Deployment' else 0)
                   for c in {(c['workload']): c for c in cs}.values())
        buf = PROFILES['conservative']['quota_buffer']
        quota = {
            'requests.cpu': fmt_cpu(round_cpu(tot['cpu'] * buf)),
            'requests.memory': fmt_mem(round_mem(tot['mem'] * buf)),
            'limits.memory': fmt_mem(round_mem(tot['mem_lim'] * buf)),
            'pods': str(math.ceil(pods * buf)),
        }
        max_lim = max(c['rec']['mem_limit'] for c in cs)
        max_cpu_lim = max(c['rec']['cpu_limit_alt'] for c in cs)
        namespaces.append(dict(
            cluster=cl, namespace=ns, label=f'{cl}/{ns}' if multi else ns, workloads=sorted({c['workload'] for c in cs}),
            quota=quota,
            quota_transition={'requests.cpu': fmt_cpu(round_cpu(tot['tr_cpu'] * buf)),
                              'requests.memory': fmt_mem(round_mem(tot['tr_mem'] * buf)),
                              'limits.memory': fmt_mem(round_mem(tot['tr_mem_lim'] * buf)), 'pods': quota['pods']},
            quota_cpu_limit_alt=fmt_cpu(round_cpu(tot['cpu_lim'] * buf)),
            sized_for=dict(cpu=round(tot['cpu'], 2), mem_gib=round(tot['mem'] / GiB, 2), pods=pods),
            current_equivalent=dict(cpu=round(tot['cur_cpu'], 2), mem_gib=round(tot['cur_mem'] / GiB, 2)),
            reserved_now=dict(cpu=round(tot['cur_avg_cpu'], 2), mem_gib=round(tot['cur_avg_mem'] / GiB, 2)),
            reserved_rec=dict(cpu=round(tot['avg_cpu'], 2), mem_gib=round(tot['avg_mem'] / GiB, 2)),
            limitrange=dict(defaultRequest=dict(cpu=lr['default_request_cpu'], memory=lr['default_request_memory']),
                            default=dict(memory=lr['default_memory_limit']),
                            max=dict(memory=fmt_mem(round_mem(max(max_lim * 1.25, parse_mem(lr['default_memory_limit'])))),
                                     cpu=fmt_cpu(round_cpu(max_cpu_lim * 1.25)))),
            confidence_cpu=min(c['confidence_cpu']['score'] for c in cs),
            confidence_mem=min(c['confidence_mem']['score'] for c in cs),
        ))

    totals = dict(
        cpu_now=round(sum(n['reserved_now']['cpu'] for n in namespaces), 2),
        cpu_rec=round(sum(n['reserved_rec']['cpu'] for n in namespaces), 2),
        mem_now=round(sum(n['reserved_now']['mem_gib'] for n in namespaces), 2),
        mem_rec=round(sum(n['reserved_rec']['mem_gib'] for n in namespaces), 2),
        cpu_balanced_delta=round(sum(c['delta']['cpu_cores_balanced'] for c in containers), 2),
        mem_balanced_delta=round(sum(c['delta']['mem_gib_balanced'] for c in containers), 2),
    )

    report = dict(
        generated=a.asof, source=Path(a.csv).name, cluster=clusters, multi_cluster=multi,
        window=dict(start=iso(window_start), end=iso(window_end), stale_days=stale_days,
                    days=round((window_end - window_start).total_seconds() / 86400 + interval / 86400, 1)),
        interval_seconds=interval, interval_label=interval_label(interval),
        samples=sum(len(rs) for rs in series.values()), data_quality=dict(dq), skipped=skipped,
        assumptions=dict(profiles=PROFILES, hpa_target=CFG['hpa_target_utilization'], max_surge=DEPLOYMENT_MAX_SURGE,
                         cpu_limit_mult=CPU_LIMIT_MULT, cpu_limit_peak_mult=CPU_LIMIT_PEAK_MULT,
                         throttle_censored=THROTTLE_CENSORED, mem_at_limit=MEM_AT_LIMIT, leak_runway_days=LEAK_RUNWAY_DAYS,
                         stale_after_days=CFG['stale_after_days'], tiers=CFG['workload_tiers'], hpa=CFG['hpa'],
                         injected_sidecars=CFG['injected_sidecars'], limitrange=lr),
        totals=totals, namespaces=namespaces,
        containers=[{k: v for k, v in c.items() if not k.startswith('_')} for c in containers],
    )
    for c in report['containers']:
        for k in ('rec', 'rec_balanced'):
            r = c[k]
            r['fmt'] = dict(cpu_request=fmt_cpu(r['cpu_request']), cpu_limit_alt=fmt_cpu(r['cpu_limit_alt']),
                            mem_request=fmt_mem(r['mem_request']), mem_limit=fmt_mem(r['mem_limit']),
                            cpu_steps=[fmt_cpu(x) for x in r['cpu_steps']], mem_steps=[fmt_mem(x) for x in r['mem_steps']])

    (out / 'recommendations.json').write_text(json.dumps(report, indent=1, default=str), encoding='utf-8')
    write_csv(out / 'recommendations.csv', report['containers'])
    write_manifests(out / 'manifests', report)
    tpl = Path(__file__).with_name('report_template.html')
    if tpl.exists():
        data = json.dumps(report, default=str).replace('</', '<\\/')     # keep "</script>" in a value from closing the tag
        (out / 'report.html').write_text(tpl.read_text(encoding='utf-8').replace('/*__DATA__*/null', data), encoding='utf-8')
    note = f', {len(skipped)} skipped' if skipped else ''
    print(f'{len(containers)} containers, {len(namespaces)} namespaces{note}, {interval_label(interval)} samples -> {out}/')


def write_csv(path, containers):
    cols = ['priority', 'cluster', 'namespace', 'workload', 'container', 'kind', 'class',
            'cur_cpu_request', 'cur_cpu_limit', 'cur_mem_request', 'cur_mem_limit',
            'rec_cpu_request', 'rec_cpu_limit_primary', 'rec_cpu_limit_if_required', 'rec_mem_request', 'rec_mem_limit',
            'cpu_steps', 'mem_steps', 'action_cpu', 'action_mem', 'confidence_cpu', 'confidence_mem',
            'cpu_p50', 'cpu_p95', 'cpu_p99', 'cpu_max', 'throttle_pct', 'mem_p50_mib', 'mem_max_mib',
            'mem_slope_mib_day', 'avg_replicas', 'peak_replicas', 'restarts', 'history_days',
            'qos_current', 'qos_recommended', 'confidence_cpu_reasons', 'confidence_mem_reasons', 'tradeoffs']
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(cols)
        for c in containers:
            s, r, rf = c['spec']['raw'], c['rec'], c['rec']['fmt']
            w.writerow([c['priority'], c['cluster'], c['namespace'], c['workload'], c['container'], c['kind'], c['class'],
                        s['cpu_request'], s['cpu_limit'], s['mem_request'], s['mem_limit'],
                        rf['cpu_request'], 'none', rf['cpu_limit_alt'], rf['mem_request'], rf['mem_limit'],
                        ' > '.join(rf['cpu_steps']), ' > '.join(rf['mem_steps']), c['action_cpu'], c['action_mem'],
                        c['confidence_cpu']['score'], c['confidence_mem']['score'],
                        *(round(c['cpu'][k], 3) for k in ('p50', 'p95', 'p99', 'max')), round(c['throttle_ratio'] * 100, 1),
                        round(c['mem']['p50'] / MiB), round(c['mem']['max'] / MiB), c['mem_slope_mib_day'],
                        c['replicas']['avg'], c['replicas']['peak'], c['restarts'], c['history_days'],
                        c['qos']['current'], c['qos']['primary'],
                        '; '.join(c['confidence_cpu']['reasons']), '; '.join(c['confidence_mem']['reasons']),
                        ' | '.join(c['tradeoffs'])])


def no_patch_reason(kind):
    if kind == 'Job':
        return 'Job pod templates are immutable; set these values wherever the Job is created.'
    if kind == 'Pod':
        return 'bare Pod with no controller; set these values wherever the Pod is defined.'
    if kind == 'Unknown':
        return 'workload kind not in the export; set these values in the workload spec.'
    return (f'{kind} is not a built-in workload kind, and a merge patch would replace its whole container list; '
            'set these values in its source.')


def write_manifests(d, report):
    by_ns = defaultdict(list)
    for c in report['containers']:
        by_ns[(c['cluster'], c['namespace'])].append(c)
    for n in report['namespaces']:
        cl, ns = n['cluster'], n['namespace']
        L = [f'# {n["label"]}: generated by analyze.py from {report["source"]} '
             f'({report["window"]["start"][:10]} to {report["window"]["end"][:10]}). Review before applying.',
             '# Primary policy: no CPU limits. Each container lists the CPU limit to use if your policy requires one.', '']
        wl_map = defaultdict(list)
        for c in by_ns[(cl, ns)]:
            wl_map[(c['workload'], c['kind'])].append(c)
        for (wl, kind), cs in sorted(wl_map.items()):
            L += ['---', f'# confidence  ' + ', '.join(f'{c["container"]}: CPU {c["confidence_cpu"]["score"]}/10, memory {c["confidence_mem"]["score"]}/10' for c in cs)]
            if kind not in TEMPLATE_KINDS and kind != 'CronJob':
                L += [f'# {kind} {wl}: no patch generated: {no_patch_reason(kind)}']
                for c in cs:
                    rf = c['rec']['fmt']
                    L += [f'#   {c["container"]}: requests cpu "{rf["cpu_request"]}", memory "{rf["mem_request"]}"; '
                          f'limits memory "{rf["mem_limit"]}" (cpu "{rf["cpu_limit_alt"]}" only if your policy requires CPU limits)']
                L.append('')
                continue
            L += [f'# kubectl -n {ns} patch {kind.lower()} {wl} --patch-file <this document>']
            if kind == 'CronJob':
                L += ['spec:', '  jobTemplate:', '    spec:', '      template:']
                ind = '        '
            else:
                L += ['spec:', '  template:']
                ind = '    '
            mesh = [c for c in cs if c['container'] in MESH_SIDECARS]
            injected = [c for c in cs if c['container'] in report['assumptions']['injected_sidecars'] and c not in mesh]
            app = [c for c in cs if c not in mesh and c not in injected]
            if mesh:
                L += [f'{ind}metadata:', f'{ind}  annotations:']
                for c in mesh:
                    a, rf = MESH_SIDECARS[c['container']], c['rec']['fmt']
                    L += [f'{ind}    # {c["container"]} is sized through these annotations',
                          f'{ind}    {a["cpu"]}: "{rf["cpu_request"]}"',
                          f'{ind}    {a["memory"]}: "{rf["mem_request"]}"',
                          f'{ind}    {a["memory_limit"]}: "{rf["mem_limit"]}"',
                          f'{ind}    # {a["cpu_limit"]}: "{rf["cpu_limit_alt"]}"   # only if your policy requires CPU limits']
            if app or injected:
                L += [f'{ind}spec:', f'{ind}  containers:']
            for c in injected:
                rf = c['rec']['fmt']
                L += [f'{ind}  # {c["container"]} is injected by a webhook; set it in the injector\'s config: '
                      f'cpu "{rf["cpu_request"]}", memory "{rf["mem_request"]}", memory limit "{rf["mem_limit"]}"']
            for c in app:
                rf = c['rec']['fmt']
                steps = ''
                if len(rf['cpu_steps']) > 1 or len(rf['mem_steps']) > 1:
                    steps = f'  # staged: cpu {" > ".join(rf["cpu_steps"])}; memory {" > ".join(rf["mem_steps"])}'
                # a strategic-merge patch keeps fields it does not mention, so an existing CPU limit must be removed explicitly
                cur_lim = c['spec']['raw']['cpu_limit']
                cpu_lim = (f'{ind}        cpu: null   # removes today\'s {cur_lim} limit; use "{rf["cpu_limit_alt"]}" instead if your policy requires CPU limits'
                           if cur_lim else f'{ind}        # cpu: "{rf["cpu_limit_alt"]}"   # only if your policy requires CPU limits')
                L += [f'{ind}  - name: {c["container"]}{steps}',
                      f'{ind}    resources:',
                      f'{ind}      requests:',
                      f'{ind}        cpu: "{rf["cpu_request"]}"',
                      f'{ind}        memory: "{rf["mem_request"]}"',
                      f'{ind}      limits:',
                      f'{ind}        memory: "{rf["mem_limit"]}"',
                      cpu_lim]
            L.append('')
        q = n['quota']
        L += ['---', 'apiVersion: v1', 'kind: ResourceQuota', 'metadata:', f'  name: {ns}-compute', f'  namespace: {ns}',
              f'  annotations:', f'    sizing: "peak replicas + rolling-update surge, x{PROFILES["conservative"]["quota_buffer"]} buffer"',
              'spec:', '  hard:'] + [f'    {k}: "{v}"' for k, v in q.items()] + [
              f'    # limits.cpu: "{n["quota_cpu_limit_alt"]}"   # only with a CPU-limit policy; it forces every pod to declare a CPU limit',
              '    # Apply this final quota after the last resize step. Until then use the transition values:',
              '    #   ' + ', '.join(f'{k}: "{v}"' for k, v in n['quota_transition'].items()), '',
              '---', 'apiVersion: v1', 'kind: LimitRange', 'metadata:', f'  name: {ns}-defaults', f'  namespace: {ns}',
              'spec:', '  limits:', '    - type: Container',
              '      defaultRequest:', f'        cpu: "{n["limitrange"]["defaultRequest"]["cpu"]}"', f'        memory: "{n["limitrange"]["defaultRequest"]["memory"]}"',
              '      default:', f'        memory: "{n["limitrange"]["default"]["memory"]}"',
              '      max:', f'        memory: "{n["limitrange"]["max"]["memory"]}"', '']
        target = d / re.sub(r'[^\w.-]', '_', cl) if report['multi_cluster'] else d
        target.mkdir(parents=True, exist_ok=True)
        (target / f'{ns}.yaml').write_text('\n'.join(L), encoding='utf-8')


if __name__ == '__main__':
    main()
