"""Export the metrics analyze.py needs from Prometheus (or any Prometheus-compatible API) to CSV.

Usage:  python export_prometheus.py --url http://localhost:9090 [--days 14] [--step 5m] [--out metrics.csv]
                                    [--namespace REGEX] [--cluster-label cluster | --cluster NAME]
                                    [--header 'X-Scope-OrgID: tenant'] [--bearer-token-file PATH] [--end ISO]

Needs cAdvisor metrics (kubelet) and kube-state-metrics v2+. Queries one day at a time and writes rows as it
goes, so memory use stays bounded. Workload names come from kube_pod_owner, kube_replicaset_owner and
kube_job_owner: Deployment pods resolve through their ReplicaSet and CronJob pods through their Job.

Standard library only.
"""
import argparse, csv, gzip, json, re, sys, time, urllib.error, urllib.parse, urllib.request
from collections import defaultdict
from datetime import datetime, timezone

COLUMNS = ['timestamp', 'cluster', 'namespace', 'workload', 'workload_kind', 'pod', 'container',
           'cpu_usage_seconds_total', 'cpu_throttled_seconds_total', 'memory_working_set',
           'cpu_request', 'cpu_limit', 'memory_request', 'memory_limit', 'restart_count', 'last_termination_reason']


def parse_duration(s):
    m = re.fullmatch(r'(\d+(?:\.\d+)?)([smhd])', s)
    if not m:
        raise argparse.ArgumentTypeError(f'not a duration like 30s, 5m, 1h: {s!r}')
    sec = float(m.group(1)) * {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}[m.group(2)]
    if sec < 1 or not sec.is_integer():
        raise argparse.ArgumentTypeError(f'step must be a whole number of seconds: {s!r}')
    return int(sec)


def parse_end(s):
    if s == 'now':
        return time.time()
    if s.endswith(('Z', 'z')):
        s = s[:-1] + '+00:00'
    t = datetime.fromisoformat(s)
    return (t if t.tzinfo else t.replace(tzinfo=timezone.utc)).timestamp()


class Prometheus:
    def __init__(self, url, headers, timeout=300):
        self.url, self.headers, self.timeout = url.rstrip('/'), headers, timeout

    def get(self, path, params):
        body = urllib.parse.urlencode(params, doseq=True).encode()
        req = urllib.request.Request(f'{self.url}{path}', data=body, method='POST',
                                     headers={'Content-Type': 'application/x-www-form-urlencoded', **self.headers})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                doc = json.load(r)
        except urllib.error.HTTPError as e:
            msg = e.read().decode(errors='replace')
            try:
                msg = json.loads(msg).get('error', msg)
            except ValueError:
                pass
            raise SystemExit(f'error: {path} returned HTTP {e.code}: {msg[:500]}')
        except urllib.error.URLError as e:
            raise SystemExit(f'error: cannot reach {self.url}: {e.reason}')
        if doc.get('status') != 'success':
            raise SystemExit(f'error: {path}: {doc.get("error", doc)}')
        if doc.get('warnings'):
            print(f'warning: {path}: {"; ".join(doc["warnings"])}', file=sys.stderr)
        return doc['data']

    def range(self, query, start, end, step):
        return self.get('/api/v1/query_range', dict(query=query, start=start, end=end, step=step))['result']

    def series(self, match, start, end):
        return self.get('/api/v1/series', {'match[]': match, 'start': start, 'end': end})


def owners(prom, ns_sel, cl, start, end):
    """(cluster, namespace, pod) -> (workload, kind), resolving ReplicaSet -> Deployment and Job -> CronJob."""
    def ck(lbl):
        return lbl.get(cl, '') if cl else ''
    rs_owner, job_owner = {}, {}
    for s in prom.series(f'kube_replicaset_owner{{{ns_sel}}}', start, end):
        if s.get('owner_kind') not in (None, '', '<none>'):
            rs_owner[(ck(s), s['namespace'], s['replicaset'])] = (s['owner_name'], s['owner_kind'])
    for s in prom.series(f'kube_job_owner{{{ns_sel}}}', start, end):
        if s.get('owner_kind') not in (None, '', '<none>'):
            job_owner[(ck(s), s['namespace'], s['job_name'])] = (s['owner_name'], s['owner_kind'])
    out = {}
    for s in prom.series(f'kube_pod_owner{{{ns_sel}}}', start, end):
        key = (ck(s), s['namespace'], s['pod'])
        kind, name = s.get('owner_kind', ''), s.get('owner_name', '')
        if s.get('owner_is_controller') == 'false' and key in out:
            continue
        if kind in ('', '<none>'):
            out[key] = (s['pod'], 'Pod')
        elif kind == 'ReplicaSet':
            out[key] = rs_owner.get((key[0], key[1], name), (name, 'ReplicaSet'))
        elif kind == 'Job':
            out[key] = job_owner.get((key[0], key[1], name), (name, 'Job'))
        else:
            out[key] = (name, kind)
    return out


def fmt_num(v):
    x = float(v)
    return str(int(x)) if x.is_integer() else f'{x:.6g}'


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def main(argv=None):
    ap = argparse.ArgumentParser(description='Export container metrics from Prometheus for analyze.py.')
    ap.add_argument('--url', required=True, help='Prometheus base URL, e.g. http://localhost:9090 (Mimir: include /prometheus)')
    ap.add_argument('--days', type=float, default=14, help='days of history (default 14)')
    ap.add_argument('--step', type=parse_duration, default=300, help='sample interval (default 5m)')
    ap.add_argument('--end', type=parse_end, default='now', help='end of the window, ISO 8601 or "now" (default)')
    ap.add_argument('--namespace', default='.+', help='namespace regex (default: all)')
    ap.add_argument('--exclude-namespace', default='kube-system', help='namespace regex to skip (default: kube-system; "" for none)')
    ap.add_argument('--cluster-label', help='label that names the cluster, when one Prometheus holds several')
    ap.add_argument('--cluster', default='default', help='cluster name to write when --cluster-label is not used')
    ap.add_argument('--header', action='append', default=[], help='extra HTTP header "Name: value" (repeatable)')
    ap.add_argument('--bearer-token-file', help='file holding a bearer token')
    ap.add_argument('--out', default='container_metrics.csv', help='output path; .gz compresses (default container_metrics.csv)')
    a = ap.parse_args(argv)

    headers = {}
    for h in a.header:
        k, sep, v = h.partition(':')
        if not sep:
            ap.error(f'--header must look like "Name: value": {h!r}')
        headers[k.strip()] = v.strip()
    if a.bearer_token_file:
        with open(a.bearer_token_file, encoding='utf-8') as f:
            headers['Authorization'] = f'Bearer {f.read().strip()}'
    prom = Prometheus(a.url, headers)

    step = a.step
    end = int(a.end // step * step)
    start = int(end - a.days * 86400 + step)
    cl = a.cluster_label
    ns_sel = f'namespace=~"{a.namespace}"' + (f',namespace!~"{a.exclude_namespace}"' if a.exclude_namespace else '')
    c_sel = f'{ns_sel},container!="",container!="POD"'
    by = 'namespace,pod,container' + (f',{cl}' if cl else '')
    queries = dict(
        cpu=f'max by ({by}) (container_cpu_usage_seconds_total{{{c_sel}}})',
        thr=f'max by ({by}) (container_cpu_cfs_throttled_seconds_total{{{c_sel}}})',
        mem=f'max by ({by}) (container_memory_working_set_bytes{{{c_sel}}})',
        req=f'max by ({by},resource) (kube_pod_container_resource_requests{{{ns_sel},resource=~"cpu|memory"}})',
        lim=f'max by ({by},resource) (kube_pod_container_resource_limits{{{ns_sel},resource=~"cpu|memory"}})',
        restarts=f'max by ({by}) (kube_pod_container_status_restarts_total{{{ns_sel}}})',
        reason=f'max by ({by},reason) (kube_pod_container_status_last_terminated_reason{{{ns_sel}}}) == 1',
    )

    print(f'resolving workload owners {iso(start)} .. {iso(end)}', file=sys.stderr)
    own = owners(prom, ns_sel, cl, start, end)
    unowned = set()
    chunk = max(step, (86400 // step) * step)
    rows = 0
    opener = gzip.open if a.out.endswith('.gz') else open
    with opener(a.out, 'wt', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        t0 = start
        while t0 <= end:
            t1 = min(t0 + chunk - step, end)
            got = {k: prom.range(q, t0, t1, step) for k, q in queries.items()}
            vals = defaultdict(lambda: defaultdict(dict))       # metric -> key -> ts -> value
            for k in ('cpu', 'thr', 'mem', 'restarts'):
                for s in got[k]:
                    m = s['metric']
                    key = (m.get(cl, a.cluster) if cl else a.cluster, m['namespace'], m['pod'], m['container'])
                    vals[k][key] = {int(float(ts)): v for ts, v in s['values']}
            for k in ('req', 'lim'):
                for s in got[k]:
                    m = s['metric']
                    key = (m.get(cl, a.cluster) if cl else a.cluster, m['namespace'], m['pod'], m['container'])
                    vals[f'{k}_{m["resource"]}'][key] = {int(float(ts)): v for ts, v in s['values']}
            for s in got['reason']:
                m = s['metric']
                key = (m.get(cl, a.cluster) if cl else a.cluster, m['namespace'], m['pod'], m['container'])
                for ts, _ in s['values']:
                    vals['reason'][key][int(float(ts))] = m.get('reason', '')
            for key in sorted(vals['cpu']):
                cluster, ns, pod, ctr = key
                wl, kind = own.get((cluster if cl else '', ns, pod), (None, None))
                if wl is None:
                    unowned.add((ns, pod))
                    wl, kind = pod, ''
                for ts, cpu in sorted(vals['cpu'][key].items()):
                    g = lambda k, fmt=fmt_num: fmt(vals[k][key][ts]) if ts in vals[k].get(key, {}) else ''
                    w.writerow([iso(ts), cluster, ns, wl, kind, pod, ctr, cpu, g('thr', str), g('mem'),
                                g('req_cpu'), g('lim_cpu'), g('req_memory'), g('lim_memory'), g('restarts'),
                                vals['reason'].get(key, {}).get(ts, '')])
                    rows += 1
            print(f'  {iso(t0)[:10]}: {len(vals["cpu"])} containers, {rows} rows so far', file=sys.stderr)
            t0 = t1 + step
    if not rows:
        print('warning: no rows written. Check --url, --namespace, and that cAdvisor metrics are scraped.', file=sys.stderr)
    if unowned:
        print(f'warning: {len(unowned)} pod(s) had no kube_pod_owner series; their pod name is used as the workload '
              f'(e.g. {", ".join("/".join(p) for p in sorted(unowned)[:3])})', file=sys.stderr)
    print(f'{rows} rows -> {a.out}', file=sys.stderr)


if __name__ == '__main__':
    main()
