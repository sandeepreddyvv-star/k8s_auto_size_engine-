"""One synthetic workload per class: the engine must recognise each pattern and size it the documented way."""
import unittest

from helpers import GiB, MiB, by_name, noise, run, series


def one(rows, config=None):
    report, _ = run(rows, config=config)
    return report['containers'][0]


class Classes(unittest.TestCase):
    def test_clean_steady(self):
        c = one(series(cpu=lambda i, t: 0.2 + noise(i, 0.02), mem=lambda i, t: 200 * MiB + noise(i, 5 * MiB),
                       spec=('1', '', '1Gi', '2Gi')))
        self.assertEqual(c['class'], 'Clean, steady')
        self.assertEqual(c['action_cpu'], 'decrease')
        self.assertEqual(c['rec']['fmt']['cpu_request'], '300m')        # p99 0.22 x 1.25, rounded up to 50m
        self.assertEqual(c['rec']['fmt']['mem_request'], '256Mi')       # max 205Mi x 1.15, rounded up to 64Mi
        self.assertGreater(len(c['rec']['cpu_steps']), 1, 'a cut from 1 core to 300m should be staged')
        self.assertGreaterEqual(c['confidence_cpu']['score'], 8)

    def test_cpu_starved(self):
        c = one(series(cpu=lambda i, t: 0.5, thr=lambda i, t: 0.15, spec=('500m', '500m', '512Mi', '1Gi')))
        self.assertEqual(c['class'], 'CPU-starved by limit')
        self.assertEqual(c['priority'], 'P1')
        self.assertGreater(c['rec']['cpu_request'], 0.5)
        self.assertLessEqual(c['confidence_cpu']['score'], 5)

    def test_oom_killed(self):
        c = one(series(mem=lambda i, t: 0.99 * GiB, spec=('500m', '', '1Gi', '1Gi'),
                       restarts=lambda i, t: i // 288, reason=lambda i, t: 'OOMKilled' if i >= 288 else ''))
        self.assertEqual(c['class'], 'Memory-capped (OOMKilled)')
        self.assertEqual(c['rec']['fmt']['mem_limit'], '1536Mi')        # current limit x 1.5
        self.assertIn('OOMKilled', c['tradeoffs'][0])

    def test_memory_growing(self):
        c = one(series(mem=lambda i, t: 500 * MiB + i * 300 / 86400 * 40 * MiB, spec=('500m', '', '1Gi', '2Gi')))
        self.assertEqual(c['class'], 'Memory growing')
        self.assertAlmostEqual(c['mem_slope_mib_day'], 40, delta=1)
        self.assertGreaterEqual(c['rec']['mem_request'], GiB, 'request of a growing container never shrinks')
        self.assertIn('runway', c)

    def test_crashing(self):
        c = one(series(restarts=lambda i, t: i // 72, reason=lambda i, t: 'Error' if i >= 72 else '',
                       spec=('1', '', '1Gi', '2Gi')))
        self.assertEqual(c['class'], 'Crashing')
        self.assertEqual(c['priority'], 'P2')
        self.assertTrue(any('non-OOM restarts' in r for r in c['confidence_cpu']['reasons']))
        self.assertIn('"Error"', ' '.join(c['tradeoffs']))

    def test_crashing_and_unsure_blocks_cuts(self):
        # crashes plus a single replica and bursty CPU push confidence to 5 or below: cuts are held
        c = one(series(pods=['web-1'], cpu=lambda i, t: 0.05 if i % 10 else 1.5, restarts=lambda i, t: i // 72,
                       reason=lambda i, t: 'Error' if i >= 72 else '', spec=('4', '', '1Gi', '2Gi')))
        self.assertTrue(c['blocked'])
        self.assertEqual(c['action_cpu'], 'hold (fix first)')

    def test_batch(self):
        c = one(series(kind='CronJob', cpu=lambda i, t: 2.0 if 1 <= t.hour < 3 else 0.01, spec=('100m', '', '512Mi', '1Gi')))
        self.assertEqual(c['class'], 'Bursty batch')
        self.assertEqual(c['batch_busy']['window'], '01:00–03:00 UTC')
        self.assertEqual(c['rec']['fmt']['cpu_request'], '2500m')       # busy p95 2.0 x 1.25
        self.assertTrue(c['flags']['kind_mismatch'], 'CronJob pods alive all window')

    def hpa_rows(self):
        # two pods at a time, replaced every day: 28 distinct pods, peak 2
        pods = [(f'web-{d}-{k}', d, d + 1) for d in range(14) for k in range(2)]
        return series(pods=pods, cpu=lambda i, t: 0.3 + noise(i, 0.02), spec=('1', '', '512Mi', '1Gi'))

    def test_hpa_inferred(self):
        c = one(self.hpa_rows())
        self.assertEqual(c['class'], 'Autoscaled (HPA)')
        self.assertTrue(c['flags']['hpa_inferred'])
        self.assertEqual(c['rec']['cpu_request'], 1.0, 'request held so HPA behaviour does not change')
        self.assertIn('HPA config not in export: request also sets scaling behaviour', c['confidence_cpu']['reasons'])

    def test_hpa_from_config(self):
        c = one(self.hpa_rows(), config={'hpa': {'shop/web': {'target_cpu_utilization': 0.6}}})
        self.assertEqual(c['class'], 'Autoscaled (HPA)')
        self.assertFalse(c['flags']['hpa_inferred'])
        self.assertEqual(c['hpa_target'], 0.6)
        self.assertNotIn('HPA config not in export: request also sets scaling behaviour', c['confidence_cpu']['reasons'])

    def test_hpa_ruled_out_by_config(self):
        c = one(self.hpa_rows(), config={'hpa': {'web': False}})
        self.assertFalse(c['flags']['hpa'])
        self.assertNotEqual(c['class'], 'Autoscaled (HPA)')

    def test_short_history(self):
        c = one(series(days=5))
        self.assertEqual(c['class'], 'Short history')
        self.assertTrue(any('5 days of history' in r for r in c['confidence_cpu']['reasons']))

    def test_tier_from_config(self):
        rows = series(cpu=lambda i, t: 0.2 + noise(i, 0.1))
        lat = one(rows)
        bg = one(rows, config={'workload_tiers': {'prod/shop/web': 'background'}})
        self.assertEqual(lat['flags']['tier'], 'latency')
        self.assertEqual(bg['flags']['tier'], 'background')
        self.assertLessEqual(bg['rec']['cpu_request'], lat['rec']['cpu_request'])

    def test_no_requests_set(self):
        c = one(series(spec=('', '', '', '')))
        self.assertEqual(c['action_cpu'], 'set')
        self.assertTrue(c['flags']['no_mem_limit'])
        self.assertEqual(c['priority'], 'P2')


if __name__ == '__main__':
    unittest.main()
