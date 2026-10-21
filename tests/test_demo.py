"""Regression check on the bundled demo export: these values were reviewed by hand, so changes need a reason."""
import json
import unittest

from helpers import ROOT, by_name, run

EXPECTED = {
    # (workload, container): class, priority, cpu request, memory request, memory limit, confidence cpu, memory
    ('checkout-service', 'app'): ('CPU-starved by limit', 'P1', '850m', '832Mi', '1Gi', 4, 9),
    ('checkout-service', 'istio-proxy'): ('Clean, steady', 'P3', '200m', '128Mi', '192Mi', 8, 9),
    ('api-gateway', 'gateway'): ('Clean, steady', 'P3', '450m', '1152Mi', '1536Mi', 8, 9),
    ('batch-embedder', 'embedder'): ('Bursty batch', 'P2', '4900m', '7936Mi', '10Gi', 6, 8),
    ('feature-store', 'store'): ('Memory-capped (OOMKilled)', 'P1', '1400m', '2560Mi', '3Gi', 7, 4),
    ('inference-router', 'router'): ('Autoscaled (HPA)', 'P3', '500m', '640Mi', '832Mi', 8, 9),
    ('recommendation-api', 'recsvc'): ('Short history', 'P2', '2800m', '1664Mi', '2176Mi', 6, 7),
    ('metrics-aggregator', 'aggregator'): ('Crashing', 'P2', '100m', '384Mi', '512Mi', 2, 5),
    ('notification-worker', 'worker'): ('Clean, steady', 'P3', '400m', '576Mi', '704Mi', 7, 8),
    ('session-cache', 'cache'): ('Memory growing', 'P1', '400m', '6Gi', '8960Mi', 8, 5),
}


class Demo(unittest.TestCase):
    def test_demo_export(self):
        report, out = run(csv_path=ROOT / 'examples' / 'demo-metrics-14d.csv.gz',
                          config=json.loads((ROOT / 'examples' / 'demo-config.json').read_text()),
                          args=['--asof', '2026-10-19'])
        got = {k: (c['class'], c['priority'], c['rec']['fmt']['cpu_request'], c['rec']['fmt']['mem_request'],
                   c['rec']['fmt']['mem_limit'], c['confidence_cpu']['score'], c['confidence_mem']['score'])
               for k, c in by_name(report).items()}
        self.assertEqual(got, EXPECTED)
        self.assertEqual(report['totals'], {'cpu_now': 19.7, 'cpu_rec': 26.25, 'mem_now': 48.57, 'mem_rec': 51.64,
                                            'cpu_balanced_delta': 3.65, 'mem_balanced_delta': 0.73})
        self.assertEqual(sorted(p.name for p in (out / 'manifests').iterdir()),
                         ['commerce.yaml', 'edge.yaml', 'ml.yaml', 'observability.yaml', 'platform.yaml'])


if __name__ == '__main__':
    unittest.main()
