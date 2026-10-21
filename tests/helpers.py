"""Synthetic exports for the tests: each builder returns rows shaped like a real export."""
import csv, gzip, io, json, math, os, sys, tempfile
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import analyze  # noqa: E402

MiB, GiB = 2**20, 2**30
START = datetime(2026, 7, 1)
COLUMNS = ['timestamp', 'cluster', 'namespace', 'workload', 'workload_kind', 'pod', 'container',
           'cpu_usage_seconds_total', 'cpu_throttled_seconds_total', 'memory_working_set',
           'cpu_request', 'cpu_limit', 'memory_request', 'memory_limit', 'restart_count', 'last_termination_reason']


def iso_z(t):
    return t.strftime('%Y-%m-%dT%H:%M:%SZ')


def series(workload='web', container='app', namespace='shop', cluster='prod', kind='Deployment',
           pods=None, days=14, interval=300, cpu=lambda i, t: 0.2, mem=lambda i, t: 200 * MiB,
           thr=lambda i, t: 0.0, restarts=lambda i, t: 0, reason=lambda i, t: '',
           spec=('500m', '', '512Mi', '1Gi'), ts=iso_z, start=START):
    """Rows for one container. pods: list of names (alive the whole window) or (name, first_day, last_day)."""
    n = int(days * 86400 / interval)
    pods = pods or [f'{workload}-1', f'{workload}-2']
    rows = []
    for k, pod in enumerate(pods):
        name, lo, hi = (pod, 0, days) if isinstance(pod, str) else pod
        used = thr_tot = 0.0
        for i in range(int(lo * 86400 / interval), min(n, int(hi * 86400 / interval))):
            t = start + timedelta(seconds=i * interval)
            used += cpu(i, t) * interval
            thr_tot += thr(i, t) * interval
            rows.append(dict(timestamp=ts(t), cluster=cluster, namespace=namespace, workload=workload, workload_kind=kind,
                             pod=name, container=container, cpu_usage_seconds_total=f'{used:.3f}',
                             cpu_throttled_seconds_total=f'{thr_tot:.3f}', memory_working_set=str(int(mem(i, t))),
                             cpu_request=spec[0], cpu_limit=spec[1], memory_request=spec[2], memory_limit=spec[3],
                             restart_count=str(restarts(i, t)), last_termination_reason=reason(i, t)))
    return rows


def noise(i, scale=0.05):
    """Deterministic jitter in [-scale, scale]."""
    return scale * math.sin(i * 12.9898) * math.cos(i * 4.1414)


def write(path, rows, columns=COLUMNS, encoding='utf-8'):
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'wt', newline='', encoding=encoding) as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)
    return path


def run(rows=None, csv_path=None, config=None, args=(), columns=COLUMNS, tmp=None):
    """Write rows, run analyze.main, return (report dict, out dir)."""
    tmp = Path(tmp or tempfile.mkdtemp(prefix='rightsize-test-'))
    if csv_path is None:
        csv_path = write(tmp / 'in.csv', rows, columns)
    argv = [str(csv_path), '--out', str(tmp / 'out'), '--asof', '2026-07-16']
    if config is not None:
        (tmp / 'config.json').write_text(json.dumps(config), encoding='utf-8')
        argv += ['--config', str(tmp / 'config.json')]
    with redirect_stdout(io.StringIO()):
        analyze.main(argv + list(args))
    return json.loads((tmp / 'out' / 'recommendations.json').read_text(encoding='utf-8')), tmp / 'out'


def by_name(report):
    return {(c['workload'], c['container']): c for c in report['containers']}
