"""Generated manifests: the right patch shape per workload kind, sidecars through annotations, valid YAML."""
import unittest

from helpers import run, series

try:
    import yaml
except ImportError:          # PyYAML is optional; structure checks still run without it
    yaml = None


def manifest(rows, config=None):
    _, out = run(rows, config=config)
    text = (out / 'manifests' / 'shop.yaml').read_text(encoding='utf-8')
    docs = [d for d in yaml.safe_load_all(text) if d] if yaml else None
    return text, docs


class Manifests(unittest.TestCase):
    def test_deployment_with_istio(self):
        text, docs = manifest(series() + series(container='istio-proxy', spec=('100m', '200m', '64Mi', '128Mi')))
        self.assertIn('kubectl -n shop patch deployment web', text)
        self.assertNotIn('- name: istio-proxy', text)
        if docs:
            patch = docs[0]['spec']['template']
            self.assertIn('sidecar.istio.io/proxyCPU', patch['metadata']['annotations'])
            self.assertEqual([c['name'] for c in patch['spec']['containers']], ['app'])
            self.assertEqual({d.get('kind') for d in docs[1:]}, {'ResourceQuota', 'LimitRange'})

    def test_existing_cpu_limit_is_removed(self):
        # without an explicit null the old 500m limit survives the patch and a larger request is rejected
        text, docs = manifest(series(cpu=lambda i, t: 0.5, thr=lambda i, t: 0.15, spec=('500m', '500m', '512Mi', '1Gi')))
        self.assertIn("cpu: null   # removes today's 500m limit", text)
        if docs:
            self.assertIsNone(docs[0]['spec']['template']['spec']['containers'][0]['resources']['limits']['cpu'])
        text, docs = manifest(series(spec=('500m', '', '512Mi', '1Gi')))
        self.assertNotIn('cpu: null', text)

    def test_linkerd_only_sidecar_changes(self):
        text, docs = manifest(series(container='linkerd-proxy'))
        self.assertIn('config.linkerd.io/proxy-memory-limit', text)
        self.assertNotIn('containers:', text.split('kind: ResourceQuota')[0], 'no empty container list')
        if docs:
            self.assertIn('config.linkerd.io/proxy-cpu-request', docs[0]['spec']['template']['metadata']['annotations'])

    def test_injected_sidecar_from_config(self):
        text, _ = manifest(series() + series(container='vault-agent'), config={'injected_sidecars': ['vault-agent']})
        self.assertNotIn('- name: vault-agent', text)
        self.assertIn('vault-agent is injected by a webhook', text)

    def test_cronjob_uses_job_template(self):
        text, docs = manifest(series(kind='CronJob'))
        self.assertIn('patch cronjob web', text)
        if docs:
            self.assertIn('containers', docs[0]['spec']['jobTemplate']['spec']['template']['spec'])

    def test_daemonset_and_statefulset(self):
        for kind in ('DaemonSet', 'StatefulSet', 'statefulset'):
            text, _ = manifest(series(kind=kind))
            self.assertIn(f'patch {kind.lower()} web', text)

    def test_no_patch_for_job_pod_and_custom_kinds(self):
        for kind, why in (('Job', 'immutable'), ('Pod', 'bare Pod'), ('Rollout', 'not a built-in workload kind')):
            text, docs = manifest(series(kind=kind))
            self.assertNotIn('kubectl -n', text, kind)
            self.assertIn(why, text, kind)
            if docs:
                self.assertEqual({d.get('kind') for d in docs}, {'ResourceQuota', 'LimitRange'})


if __name__ == '__main__':
    unittest.main()
