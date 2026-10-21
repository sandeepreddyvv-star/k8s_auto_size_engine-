"""export_prometheus.py against a fake Prometheus API: owner resolution, joins, and a CSV analyze.py accepts."""
import csv, io, json, sys, tempfile, threading, unittest, urllib.parse
from contextlib import redirect_stderr
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from helpers import ROOT, run

sys.path.insert(0, str(ROOT))
import export_prometheus  # noqa: E402

END = 1782950400          # 2026-07-02T00:00:00Z
PODS = [('shop', 'web-7d9f8c6b5-abcde', 'app'), ('shop', 'web-7d9f8c6b5-fghij', 'app'),
        ('shop', 'nightly-29012345-xyz12', 'job'), ('shop', 'loner', 'main')]
OWNERS = [
    dict(namespace='shop', pod='web-7d9f8c6b5-abcde', owner_kind='ReplicaSet', owner_name='web-7d9f8c6b5', owner_is_controller='true'),
    dict(namespace='shop', pod='web-7d9f8c6b5-fghij', owner_kind='ReplicaSet', owner_name='web-7d9f8c6b5', owner_is_controller='true'),
    dict(namespace='shop', pod='nightly-29012345-xyz12', owner_kind='Job', owner_name='nightly-29012345', owner_is_controller='true'),
]
RS = [dict(namespace='shop', replicaset='web-7d9f8c6b5', owner_kind='Deployment', owner_name='web')]
JOBS = [dict(namespace='shop', job_name='nightly-29012345', owner_kind='CronJob', owner_name='nightly')]


class FakePrometheus(BaseHTTPRequestHandler):
    queries = []

    def log_message(self, *a):
        pass

    def do_POST(self):
        form = urllib.parse.parse_qs(self.rfile.read(int(self.headers['Content-Length'])).decode())
        if self.path == '/api/v1/series':
            m = form['match[]'][0]
            data = OWNERS if m.startswith('kube_pod_owner') else RS if m.startswith('kube_replicaset_owner') else JOBS
        else:
            q, t0, t1, step = form['query'][0], int(float(form['start'][0])), int(float(form['end'][0])), int(float(form['step'][0]))
            FakePrometheus.queries.append(q)
            data = dict(resultType='matrix', result=self.matrix(q, t0, t1, step))
        body = json.dumps(dict(status='success', data=data)).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(body)

    @staticmethod
    def matrix(q, t0, t1, step):
        out = []
        for ns, pod, ctr in PODS:
            m = dict(namespace=ns, pod=pod, container=ctr)
            ts = range(t0, t1 + 1, step)
            if 'cpu_usage_seconds_total' in q:
                vals = [(t, str(0.25 * (t - END + 86400))) for t in ts]
            elif 'throttled' in q:
                vals = [(t, '0') for t in ts]
            elif 'working_set' in q:
                vals = [(t, str(300 * 2**20)) for t in ts]
            elif 'resource_requests' in q:
                out.append(dict(metric=dict(m, resource='cpu'), values=[(t, '0.5') for t in ts]))
                m, vals = dict(m, resource='memory'), [(t, '5.36870912e+08') for t in ts]
            elif 'resource_limits' in q:
                m, vals = dict(m, resource='memory'), [(t, '1073741824') for t in ts]
            elif 'restarts' in q:
                vals = [(t, '1' if t > END - 3600 and pod == 'loner' else '0') for t in ts]
            elif 'last_terminated_reason' in q:
                if pod != 'loner':
                    continue
                m, vals = dict(m, reason='OOMKilled'), [(t, '1') for t in ts if t > END - 3600]
            out.append(dict(metric=m, values=vals))
        return out


class Exporter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), FakePrometheus)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.url = f'http://127.0.0.1:{cls.server.server_port}'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def export(self, *args):
        out = Path(tempfile.mkdtemp()) / 'metrics.csv'
        with redirect_stderr(io.StringIO()):
            export_prometheus.main(['--url', self.url, '--days', '1', '--step', '5m', '--end', '2026-07-02T00:00:00Z',
                                    '--cluster', 'test', '--out', str(out), *args])
        return out, list(csv.DictReader(out.open(encoding='utf-8')))

    def test_rows_and_owners(self):
        path, rows = self.export()
        self.assertEqual(len(rows), 4 * 288)
        self.assertEqual(list(rows[0].keys()), export_prometheus.COLUMNS)
        wl = {r['pod']: (r['workload'], r['workload_kind']) for r in rows}
        self.assertEqual(wl['web-7d9f8c6b5-abcde'], ('web', 'Deployment'))
        self.assertEqual(wl['nightly-29012345-xyz12'], ('nightly', 'CronJob'))
        self.assertEqual(wl['loner'], ('loner', ''))
        r = rows[0]
        self.assertEqual((r['cpu_request'], r['memory_request'], r['memory_limit'], r['cpu_limit']), ('0.5', '536870912', '1073741824', ''))
        self.assertEqual(r['timestamp'], '2026-07-01T00:05:00Z')
        self.assertEqual(rows[-1]['timestamp'], '2026-07-02T00:00:00Z')
        self.assertTrue(any(r['last_termination_reason'] == 'OOMKilled' for r in rows))

    def test_namespace_filters_reach_every_query(self):
        FakePrometheus.queries = []
        self.export('--namespace', 'shop|cart')
        self.assertTrue(FakePrometheus.queries)
        for q in FakePrometheus.queries:
            self.assertIn('namespace=~"shop|cart"', q)
            self.assertIn('namespace!~"kube-system"', q)

    def test_output_feeds_analyze(self):
        path, _ = self.export()
        report, _ = run(csv_path=path)
        got = {(c['workload'], c['kind']) for c in report['containers']}
        self.assertEqual(got, {('web', 'Deployment'), ('nightly', 'CronJob'), ('loner', 'Unknown')})
        self.assertEqual(report['interval_seconds'], 300)


if __name__ == '__main__':
    unittest.main()
