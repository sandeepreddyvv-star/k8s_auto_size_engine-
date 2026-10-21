"""The HTML report renders for any dataset, and says nothing about workloads that are not in it."""
import json, shutil, subprocess, unittest
from pathlib import Path

from helpers import GiB, MiB, noise, run, series

NODE = shutil.which('node')
RENDER = Path(__file__).with_name('render_report.js')
DEMO_NAMES = ('checkout-service', 'feature-store', 'session-cache', 'batch-embedder', 'api-gateway',
              'inference-router', 'metrics-aggregator', 'recommendation-api', 'prod-us-central1')


def render(rows, **kw):
    _, out = run(rows, **kw)
    res = subprocess.run([NODE, str(RENDER), str(out / 'report.html')], capture_output=True, text=True, encoding='utf-8')
    if res.returncode:
        raise AssertionError(f'report script failed:\n{res.stderr}')
    return json.loads(res.stdout)


@unittest.skipUnless(NODE, 'node is not installed')
class Report(unittest.TestCase):
    def test_single_clean_workload(self):
        r = render(series(cpu=lambda i, t: 0.2 + noise(i, 0.02), spec=('1', '', '1Gi', '2Gi')))
        self.assertEqual(r['title'], 'prod Right-Sizing')
        self.assertIn('No workload is short of resources today', r['html'])
        self.assertIn('Nothing in this group.', r['html'])
        for name in DEMO_NAMES:
            self.assertNotIn(name, r['html'])

    def test_every_class_together(self):
        rows = (series(workload='starved', cpu=lambda i, t: 0.5, thr=lambda i, t: 0.15, spec=('500m', '500m', '512Mi', '1Gi'))
                + series(workload='oom', mem=lambda i, t: 0.99 * GiB, spec=('500m', '', '1Gi', '1Gi'),
                         restarts=lambda i, t: i // 288, reason=lambda i, t: 'OOMKilled' if i >= 288 else '')
                + series(workload='leaky', mem=lambda i, t: 500 * MiB + i * 300 / 86400 * 40 * MiB, spec=('500m', '', '1Gi', '2Gi'))
                + series(workload='crashy', restarts=lambda i, t: i // 72, reason=lambda i, t: 'Error' if i >= 72 else '')
                + series(workload='nightly', kind='CronJob', cpu=lambda i, t: 2.0 if 1 <= t.hour < 3 else 0.01)
                + series(workload='scaled', pods=[(f's-{d}-{k}', d, d + 1) for d in range(14) for k in range(2)])
                + series(workload='young', days=5, spec=('', '', '', ''))
                + series(workload='mesh', container='linkerd-proxy'))
        r = render(rows, args=['--asof', '2026-09-30'])
        h = r['html']
        for text in ('starved</b> loses', 'oom</b> was OOMKilled', 'leaky</b> memory grows', 'nightly</b> run',
                     "Fix crashy's crash", 'Confirm how nightly is deployed', "Set young's memory limit now",
                     'scaled desired replicas', 'data ends', 'More history</b> for <b>young'):
            self.assertIn(text, h)

    def test_multi_cluster(self):
        r = render(series(cluster='us') + series(cluster='eu'))
        self.assertEqual(r['title'], 'eu and us Right-Sizing')
        self.assertIn('data-ns="eu/shop"', r['html'])
        self.assertIn('data-ns="us/shop"', r['html'])


if __name__ == '__main__':
    unittest.main()
