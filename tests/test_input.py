"""Input handling: the export formats, cadences and defects real exports have."""
import random, unittest
from datetime import timedelta, timezone

from helpers import analyze  # noqa: E402  (helpers puts the repo root on sys.path)
from helpers import COLUMNS, MiB, by_name, noise, run, series, write

STEADY = dict(cpu=lambda i, t: 0.2 + noise(i, 0.05), mem=lambda i, t: 200 * MiB + noise(i, 20 * MiB))


def recs(report):
    return {k: (c['rec'], c['cpu'], c['mem'], c['class']) for k, c in by_name(report).items()}


class Parsing(unittest.TestCase):
    def test_cpu(self):
        self.assertEqual(analyze.parse_cpu('500m'), 0.5)
        self.assertEqual(analyze.parse_cpu('1.5'), 1.5)
        self.assertEqual(analyze.parse_cpu('2'), 2.0)
        self.assertIsNone(analyze.parse_cpu(''))
        self.assertIsNone(analyze.parse_cpu('abc'))
        self.assertIsNone(analyze.parse_cpu('NaN'))

    def test_memory(self):
        self.assertEqual(analyze.parse_mem('1Gi'), 2**30)
        self.assertEqual(analyze.parse_mem('512Mi'), 512 * 2**20)
        self.assertEqual(analyze.parse_mem('500M'), 500e6)
        self.assertEqual(analyze.parse_mem('128k'), 128e3)
        self.assertEqual(analyze.parse_mem('1073741824'), 2**30)
        self.assertEqual(analyze.parse_mem('1.073741824e+09'), 2**30)
        for bad in ('', 'NaN', '<nil>', '12Zi'):
            self.assertIsNone(analyze.parse_mem(bad), bad)

    def test_timestamps_all_mean_the_same_instant(self):
        want = analyze.parse_ts('2026-07-01T12:00:00Z')
        for s in ('2026-07-01T12:00:00', '2026-07-01 12:00:00+00:00', '2026-07-01T14:00:00+02:00',
                  '2026-07-01T07:00:00-05:00', '1782907200', '1782907200000', '1782907200.0'):
            self.assertEqual(analyze.parse_ts(s), want, s)


class Formats(unittest.TestCase):
    def setUp(self):
        self.rows = series(**STEADY)
        self.base = recs(run(self.rows)[0])

    def test_offset_and_epoch_timestamps_match_utc(self):
        for fmt in (lambda t: (t.replace(tzinfo=timezone.utc) + timedelta(hours=2)).isoformat().replace('+00:00', '+02:00'),
                    lambda t: str(int(t.replace(tzinfo=timezone.utc).timestamp())),
                    lambda t: str(int(t.replace(tzinfo=timezone.utc).timestamp() * 1000))):
            self.assertEqual(recs(run(series(ts=fmt, **STEADY))[0]), self.base)

    def test_gzip_and_bom(self):
        import tempfile, pathlib
        d = pathlib.Path(tempfile.mkdtemp())
        self.assertEqual(recs(run(csv_path=write(d / 'in.csv.gz', self.rows))[0]), self.base)
        self.assertEqual(recs(run(csv_path=write(d / 'bom.csv', self.rows, encoding='utf-8-sig'))[0]), self.base)

    def test_cpu_as_rate(self):
        rows = series(**STEADY)
        rate_rows = []
        for i, r in enumerate(rows):
            r = dict(r)
            r['cpu_usage_cores'] = str(STEADY['cpu'](i % 4032, None))
            r['cpu_throttled_cores'] = '0'
            rate_rows.append(r)
        cols = [c for c in COLUMNS if c not in ('cpu_usage_seconds_total', 'cpu_throttled_seconds_total')] + \
               ['cpu_usage_cores', 'cpu_throttled_cores']
        report, _ = run(rate_rows, columns=cols)
        got = recs(report)
        for k in self.base:
            self.assertEqual(got[k][0]['fmt'], self.base[k][0]['fmt'])
            self.assertAlmostEqual(got[k][1]['p95'], self.base[k][1]['p95'], places=3)

    def test_only_required_columns(self):
        cols = ['timestamp', 'namespace', 'workload', 'pod', 'container', 'cpu_usage_seconds_total', 'memory_working_set']
        report, out = run(self.rows, columns=cols)
        c = report['containers'][0]
        self.assertEqual(c['kind'], 'Unknown')
        self.assertEqual(c['cluster'], 'default')
        self.assertIn('no patch generated', (out / 'manifests' / 'shop.yaml').read_text())

    def test_missing_column_is_a_clear_error(self):
        cols = ['timestamp', 'namespace', 'workload', 'pod', 'container', 'memory_working_set']
        with self.assertRaises(SystemExit) as e:
            run(self.rows, columns=cols)
        self.assertIn('missing required column', str(e.exception.code))
        self.assertIn('cpu_usage_seconds_total or cpu_usage_cores', str(e.exception.code))


class Cadence(unittest.TestCase):
    def test_detected_interval_and_no_false_gaps(self):
        for interval, days in ((60, 3), (300, 14), (900, 14)):
            report, _ = run(series(interval=interval, days=days, **STEADY))
            self.assertEqual(report['interval_seconds'], interval)
            self.assertEqual(report['containers'][0]['gaps'], 0, interval)
            self.assertEqual(report['containers'][0]['history_days'], days)

    def test_real_gap_is_found(self):
        rows = [r for r in series(**STEADY) if not ('2026-07-05T10:00' <= r['timestamp'] < '2026-07-05T12:00')]
        c = run(rows)[0]['containers'][0]
        self.assertEqual(c['gaps'], 2)                                  # one per pod
        self.assertAlmostEqual(c['gap_hours'], 4.0, delta=0.1)

    def test_jittered_scrapes(self):
        rnd = random.Random(1)
        rows = series(ts=lambda t: (t + timedelta(seconds=rnd.randint(-4, 4))).strftime('%Y-%m-%dT%H:%M:%SZ'), **STEADY)
        c = run(rows)[0]['containers'][0]
        self.assertEqual(c['gaps'], 0)
        self.assertEqual(c['replicas']['peak'], 2)


class Defects(unittest.TestCase):
    def test_clusters_are_kept_apart(self):
        a = series(cluster='us', cpu=lambda i, t: 0.2)
        b = series(cluster='eu', cpu=lambda i, t: 1.2, spec=('2', '', '512Mi', '1Gi'))
        report, out = run(a + b)
        self.assertEqual(len(report['containers']), 2)
        self.assertEqual({c['cluster']: c['rec']['fmt']['cpu_request'] for c in report['containers']}, {'us': '250m', 'eu': '1500m'})
        self.assertTrue((out / 'manifests' / 'us' / 'shop.yaml').exists())
        self.assertTrue((out / 'manifests' / 'eu' / 'shop.yaml').exists())
        self.assertEqual({n['label'] for n in report['namespaces']}, {'us/shop', 'eu/shop'})

    def test_tiny_containers_are_skipped_not_fatal(self):
        rows = series(**STEADY)
        rows += series(workload='blip', pods=['blip-1'], days=300 / 86400)          # a single sample
        rows += [dict(r, memory_working_set='NaN') for r in series(workload='nomem', days=1)]
        report, _ = run(rows)
        self.assertEqual([c['workload'] for c in report['containers']], ['web'])
        self.assertEqual({s['workload'] for s in report['skipped']}, {'blip', 'nomem'})

    def test_nothing_usable_exits(self):
        with self.assertRaises(SystemExit) as e:
            run(series(workload='blip', pods=['blip-1'], days=300 / 86400))
        self.assertIn('enough data', str(e.exception.code))

    def test_bad_values_are_dropped_and_counted(self):
        rows = series(**STEADY)
        rows[10] = dict(rows[10], cpu_usage_seconds_total='NaN')
        rows[11] = dict(rows[11], timestamp='yesterday')
        rows[12] = dict(rows[12], memory_working_set='<nil>')
        dq = run(rows)[0]['data_quality']
        self.assertEqual(dq['rows with missing or unparseable CPU dropped'], 1)
        self.assertEqual(dq['rows with unparseable timestamp dropped'], 1)
        self.assertEqual(dq['memory values missing or unparseable, dropped'], 1)

    def test_blank_restart_count_is_not_a_restart(self):
        rows = series(restarts=lambda i, t: 3)
        for r in rows[::7]:
            r['restart_count'] = ''
        self.assertEqual(run(rows)[0]['containers'][0]['restarts'], 0)

    def test_html_in_values_cannot_break_the_report(self):
        rows = series(cluster='</script><b>x', **STEADY)
        _, out = run(rows)
        self.assertNotIn('</script><b>x', (out / 'report.html').read_text(encoding='utf-8'))


class Config(unittest.TestCase):
    def test_unknown_key(self):
        with self.assertRaises(SystemExit) as e:
            run(series(), config={'workload_hints': {}})
        self.assertIn('unknown config key', str(e.exception.code))

    def test_bad_tier(self):
        with self.assertRaises(SystemExit) as e:
            run(series(), config={'workload_tiers': {'web': 'fast'}})
        self.assertIn('latency, background, batch', str(e.exception.code))

    def test_limitrange_defaults(self):
        report, out = run(series(), config={'limitrange': {'default_request_cpu': '50m'}})
        lr = report['namespaces'][0]['limitrange']
        self.assertEqual(lr['defaultRequest'], {'cpu': '50m', 'memory': '128Mi'})
        self.assertIn('cpu: "50m"', (out / 'manifests' / 'shop.yaml').read_text())


if __name__ == '__main__':
    unittest.main()
