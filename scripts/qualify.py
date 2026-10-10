#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Plan or explicitly run four-rank TensorFold/NCCL collective qualification.

This creates only owned, uniquely labelled test containers and task artifact
folders. No images/packages are installed and no host networking is configured.
All sixteen function counters, changing-data CUDA graphs, actual data values,
selected library/image/transport identity and unchanged host configuration must
pass before the canonical serving receipt becomes qualified.
"""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import datetime
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import sys
import threading
import uuid

BASE_COMMIT = '73cf112295c33aee2b895f329f592f2a9b4b0f97'
PATCH_SHA256 = '8e2b8715d62d2b07a74caca3778da0eff2e7b54caf8a184bb728f179a5d1eba4'
PATCHED_TREE = '01e0e8a3a821ebc92a4d1333981d18ed4eeb96e4'
CONTAINER_LIBRARY = '/opt/spark-ring/lib/libnccl.so.2'
CASES = {(dtype, size, inplace) for dtype in ('torch.float32', 'torch.bfloat16', 'torch.int32', 'torch.int64')
         for size in (16384, 65536, 1048576, 16777216, 67108864) for inplace in (False, True)}
ERROR_FIELDS = {'out_of_buffer', 'req_cqe_error', 'resp_cqe_error', 'req_transport_retries_exceeded',
                'req_rnr_retries_exceeded', 'packet_seq_err', 'local_ack_timeout_err'}


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1048576), b''):
            h.update(chunk)
    return h.hexdigest()


def worker(payload, action):
    """Root worker delivered over a literal sudo -s heredoc; no remote install."""
    if os.geteuid() != 0:
        raise RuntimeError('Qualification host actions require a sudo -n -s shell')
    node, rank = payload['node'], payload['rank']
    recipe, run_id = payload['recipe_id'], payload['run_id']
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]*', recipe) or not re.fullmatch(r'[A-Za-z0-9_-]+', run_id):
        raise RuntimeError('Unsafe qualification identity')
    case = Path('/var/tmp/spark-ring-qualification') / recipe / run_id
    name = payload['container_name']
    labels = {'org.spark-ring.recipe': recipe, 'org.spark-ring.qualification': run_id}

    def command(args, check=True, timeout=120):
        p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        if check and p.returncode:
            raise RuntimeError(f'{shlex.join(args)} failed: {p.stderr.strip()}')
        return p

    def event(action_name, **extra):
        if case.is_dir():
            with (case / 'changes.jsonl').open('a') as f:
                f.write(json.dumps({'utc': utc(), 'action': action_name, **extra}) + '\n')
            os.chmod(case / 'changes.jsonl', 0o600)

    def mutate(args, timeout=120):
        event('command-start', command=shlex.join(args))
        p = command(args, check=False, timeout=timeout)
        event('command-finish', command=shlex.join(args), returncode=p.returncode, stdout=p.stdout, stderr=p.stderr)
        if p.returncode:
            raise RuntimeError(f'{shlex.join(args)} failed: {p.stderr.strip()}')
        return p

    def normalized_routes(family):
        rows = json.loads(command(['ip', '-j', family, 'route', 'show', 'table', 'all']).stdout)
        return sorted(json.dumps({k: v for k, v in row.items() if k not in ('expires', 'lastuse', 'used', 'cache')}, sort_keys=True)
                      for row in rows)

    def snapshot():
        files = {}
        for base in ('/etc/netplan', '/etc/NetworkManager/system-connections', '/run/NetworkManager/system-connections'):
            for p in Path(base).rglob('*'):
                if p.is_file() and not p.is_symlink():
                    files[str(p)] = {'sha256': sha(p), 'mode': p.stat().st_mode & 0o777}
        addresses = json.loads(command(['ip', '-j', 'address', 'show']).stdout)
        addresses = [{'interface': p['ifname'], 'mac': p.get('address'), 'mtu': p.get('mtu'),
                      'up': 'UP' in p.get('flags', []),
                      'addresses': sorted(json.dumps({k: v for k, v in a.items() if k not in ('valid_life_time', 'preferred_life_time')}, sort_keys=True)
                                          for a in p.get('addr_info', []))} for p in addresses]
        controls = [Path('/proc/sys/net/ipv4/ip_forward')]
        controls += list(Path('/proc/sys/net/ipv4/conf').glob('*/rp_filter'))
        controls += list(Path('/proc/sys/net/ipv6/conf').glob('*/forwarding'))
        firewall = {}
        for tool in ('iptables-save', 'ip6tables-save'):
            text = '\n'.join(re.sub(r'\[\d+:\d+\]', '[COUNTERS]', line)
                             for line in command([tool]).stdout.splitlines() if not line.startswith('#'))
            # Record only a comparison hash; do not publish private firewall policy.
            firewall[tool + '_sha256'] = hashlib.sha256(text.encode()).hexdigest()
        return {'files': files, 'addresses': sorted(addresses, key=lambda p: p['interface']),
                'routes4': normalized_routes('-4'), 'routes6': normalized_routes('-6'),
                'firewall': firewall, 'sysctls': {str(p): p.read_text().strip() for p in controls}}

    def existing_container():
        p = command(['docker', 'container', 'inspect', name], check=False)
        if p.returncode:
            # A daemon/connectivity failure is not evidence that a container is absent.
            if not command(['docker', 'info', '--format', '{{.ServerVersion}}'], check=False).returncode:
                return None
            raise RuntimeError('Cannot inspect Docker daemon')
        row = json.loads(p.stdout)[0]
        if any(row['Config'].get('Labels', {}).get(k) != v for k, v in labels.items()):
            raise RuntimeError('Container name belongs to another owner; refusing to act')
        return row

    def own_case():
        if case.is_symlink() or not case.is_dir():
            raise RuntimeError('Qualification task directory is absent or a symlink')
        owner = json.loads((case / 'owner.json').read_text())
        if owner != {'recipe_id': recipe, 'run_id': run_id, 'rank': rank}:
            raise RuntimeError('Qualification directory belongs to another invocation')

    if action == 'preflight':
        if case.exists() or case.is_symlink():
            raise RuntimeError('Qualification task directory already exists')
        if existing_container() is not None:
            raise RuntimeError('Qualification container already exists')
        if command(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader,nounits']).stdout.strip():
            raise RuntimeError('Active GPU compute processes; stop owned serving workloads before qualification')
        image = json.loads(command(['docker', 'image', 'inspect', payload['image']]).stdout)[0]
        if image.get('Architecture') != 'arm64':
            raise RuntimeError('Pinned image must already be available for ARM64; this script never pulls')
        if payload['image'] not in image.get('RepoDigests', []):
            raise RuntimeError('Image digest does not match the pinned image')
        if sha(payload['library']) != payload['library_sha256']:
            raise RuntimeError('Shared library differs from operator SHA256')
        for filename, digest in payload['source_sha256'].items():
            if sha(Path(payload['script_directory']) / filename) != digest:
                raise RuntimeError('Shared qualification source differs from operator source')
        if not Path('/usr/sbin/ethtool').is_file() or not Path('/lib/aarch64-linux-gnu/libmnl.so.0').is_file():
            raise RuntimeError('Host ethtool/libmnl bind dependencies are missing; no packages are installed here')
        interfaces = {p['ifname']: p for p in json.loads(command(['ip', '-j', 'address', 'show']).stdout)}
        management = interfaces.get(node['management_interface'], {})
        if node['management_address'] not in [a['local'] for a in management.get('addr_info', []) if a['family'] == 'inet']:
            raise RuntimeError('Management address/interface is absent')
        roots, gids, ports = {}, {}, []
        address_range = ipaddress.IPv4Network(payload['fabric_address_range'])
        for f in node['fabric']:
            d = Path('/sys/class/infiniband') / f['hca']
            associated = sorted(p.name for p in (d / 'device/net').iterdir())
            if associated != [f['interface']]:
                raise RuntimeError('HCA/netdev association differs: ' + f['hca'])
            p = interfaces.get(f['interface'], {})
            assigned = [a['local'] for a in p.get('addr_info', []) if a['family'] == 'inet']
            if assigned != [f['address']] or p.get('mtu') != payload['mtu'] or 'UP' not in p.get('flags', []):
                raise RuntimeError('Fabric address/MTU/UP mismatch: ' + f['interface'])
            state = (d / 'ports/1/state').read_text().strip()
            if 'ACTIVE' not in state:
                raise RuntimeError('RDMA port not ACTIVE: ' + f['hca'])
            pci = (d / 'device').resolve().name
            domain = pci.split(':')[0]
            roots.setdefault(f['rail'], set()).add(domain)
            selected = []
            for path in sorted((d / 'ports/1/gids').iterdir(), key=lambda p: int(p.name)):
                index, value = path.name, ipaddress.IPv6Address(path.read_text().strip())
                typ = (d / 'ports/1/gid_attrs/types' / index).read_text().strip()
                dev = (d / 'ports/1/gid_attrs/ndevs' / index).read_text().strip()
                if typ == 'RoCE v2' and value.ipv4_mapped and value.ipv4_mapped in address_range:
                    if dev != f['interface'] or str(value.ipv4_mapped) != f['address']:
                        raise RuntimeError('Unexpected IPv4 candidate in fabric range: ' + f['hca'])
                    selected.append(int(index))
            if len(selected) != 1:
                raise RuntimeError('Exactly one IPv4 RoCE-v2 GID per singleton HCA required: ' + f['hca'])
            gids[f['hca']] = selected[0]
            ports.append({'hca': f['hca'], 'interface': f['interface'], 'pci_address': pci, 'gid_index': selected[0]})
        if set(roots) != {0, 1} or any(len(domains) != 1 for domains in roots.values()) or roots[0] == roots[1]:
            raise RuntimeError('Configured rails must identify two separate PCI domains')
        qp = json.loads(command(['rdma', '-j', 'resource', 'show', 'qp']).stdout)
        selected_hcas = set(gids)
        if any(q.get('dev') in selected_hcas and q.get('pid') for q in qp):
            raise RuntimeError('Another userspace QP is active on the selected fabric functions')
        with socket.socket() as test:
            test.bind((node['management_address'], payload['master_port']))
        env = dict(os.environ, SPARK_RING_REQUIRED_HCAS=json.dumps(list(gids)))
        counts = subprocess.run(['python3', str(Path(payload['script_directory']) / 'capture-function-counters.py')],
                                env=env, text=True, capture_output=True, check=True, timeout=60)
        counter = json.loads(counts.stdout)
        return {'utc': utc(), 'rank': rank, 'ports': ports, 'image_id': image['Id'],
                'library_sha256': payload['library_sha256'], 'counter_devices': sorted(counter['devices']), 'snapshot': snapshot()}
    if action == 'stage':
        for parent in (case.parent.parent, case.parent):
            if parent.is_symlink():
                raise RuntimeError('Refusing symlink qualification parent')
            parent.mkdir(mode=0o755, exist_ok=True)
        if case.exists() or case.is_symlink():
            raise RuntimeError('Refusing existing qualification run directory')
        case.mkdir(mode=0o755)
        (case / 'owner.json').write_text(json.dumps({'recipe_id': recipe, 'run_id': run_id, 'rank': rank}) + '\n')
        os.chmod(case / 'owner.json', 0o600)
        event('created-task-directory', path=str(case), rollback='Remove only this invocation directory after preserving evidence')
        for filename, digest in payload['source_sha256'].items():
            source = Path(payload['script_directory']) / filename
            if sha(source) != digest:
                raise RuntimeError('Shared qualification source changed after preflight')
            data = source.read_bytes()
            (case / filename).write_bytes(data)
            os.chmod(case / filename, 0o644)
            event('copied-source', path=str(case / filename), sha256=digest)
        return {'utc': utc(), 'rank': rank, 'directory': str(case)}
    own_case()
    if action == 'create':
        if sha(payload['library']) != payload['library_sha256']:
            raise RuntimeError('Library changed after preflight')
        if existing_container() is not None:
            raise RuntimeError('Owned qualification container already exists')
        args = ['docker', 'create', '--name', name, '--gpus', 'all', '--ipc=host', '--network', 'host',
                '--device', '/dev/infiniband', '--cap-add', 'IPC_LOCK', '--ulimit', 'memlock=-1', '--ulimit', 'stack=67108864',
                '--mount', 'type=bind,src=/usr/sbin/ethtool,dst=/usr/sbin/ethtool,readonly',
                '--mount', 'type=bind,src=/lib/aarch64-linux-gnu/libmnl.so.0,dst=/lib/aarch64-linux-gnu/libmnl.so.0,readonly',
                '--mount', f'type=bind,src={case},dst=/validation',
                '--mount', f"type=bind,src={payload['library']},dst={CONTAINER_LIBRARY},readonly"]
        for key, value in labels.items():
            args += ['--label', key + '=' + value]
        for key, value in payload['environment'].items():
            args += ['--env', key + '=' + value]
        args += ['--entrypoint', 'python3', payload['image'], '/validation/collective-probe.py']
        p = mutate(args)
        return {'utc': utc(), 'rank': rank, 'container_id': p.stdout.strip(), 'container_name': name}
    if action == 'start':
        if existing_container() is None:
            raise RuntimeError('Owned qualification container is absent')
        p = mutate(['docker', 'start', name])
        return {'utc': utc(), 'rank': rank, 'started': p.stdout.strip()}
    if action == 'finish':
        if existing_container() is None:
            raise RuntimeError('Owned qualification container is absent')
        exit_code = command(['docker', 'wait', name], timeout=payload['timeout']).stdout.strip()
        log = command(['docker', 'logs', name], check=False, timeout=120)
        log_text = log.stdout + log.stderr
        (case / 'container.log').write_text(log_text)
        event('collected-container-output', exit_code=exit_code, log_path=str(case / 'container.log'))
        report_path = case / f'rank-{rank}.json'
        report = json.loads(report_path.read_text()) if report_path.is_file() else None
        after = snapshot()
        (case / 'os-after.json').write_text(json.dumps(after, indent=2) + '\n')
        event('saved-read-only-preservation-snapshot', path=str(case / 'os-after.json'))
        return {'utc': utc(), 'rank': rank, 'exit_code': exit_code, 'report': report,
                'log': log_text, 'snapshot': after, 'local_directory': str(case)}
    if action == 'cleanup':
        container = existing_container()
        if container is not None:
            mutate(['docker', 'rm', '-f', name])
        return {'utc': utc(), 'rank': rank, 'owned_container_removed': True, 'audit_directory_retained': str(case)}
    raise RuntimeError('Unknown qualification worker action')


def validate_report(report, rank, hcas, digest):
    if not isinstance(report, dict) or report.get('rank') != rank or report.get('world') != 4:
        return False
    if report.get('library_sha256') != digest or set(report.get('required_hcas', [])) != set(hcas):
        return False
    if report.get('library') != CONTAINER_LIBRARY or report.get('resolved_nccl') != CONTAINER_LIBRARY:
        return False
    if set(report.get('mapped_nccl', [])) != {CONTAINER_LIBRARY}:
        return False
    if not all(report.get(k) is True for k in ('correctness_passed', 'cuda_graph_passed', 'payload_all_hcas_passed')):
        return False
    rows = report.get('results', [])
    if len(rows) != 40 or {(r.get('dtype'), r.get('send_bytes'), r.get('inplace')) for r in rows} != CASES:
        return False
    for row in rows:
        if not all(row.get(k) is True for k in ('correct', 'cuda_graph_correct', 'payload_all_hcas_passed')):
            return False
        traffic = row.get('function_traffic', {})
        if set(traffic) != set(hcas):
            return False
        repeats = 32 if row['send_bytes'] >= 1048576 else 100
        gate = max(65536, row['send_bytes'] * repeats // 4)
        if row.get('repeats') != repeats:
            return False
        for data in traffic.values():
            delta = data.get('deltas', {})
            errors = data.get('errors', {})
            if data.get('minimum_payload_bytes') != gate or data.get('payload_passed') is not True or data.get('errors_passed') is not True:
                return False
            if max(delta.get('rx_vport_rdma_unicast_bytes', -1), delta.get('tx_vport_rdma_unicast_bytes', -1)) < gate:
                return False
            if not ERROR_FIELDS.issubset(errors) or any(v != 0 for v in errors.values()) or any(v < 0 for v in delta.values()):
                return False
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', nargs='?', choices=('plan', 'run'))
    parser.add_argument('--config', help='explicit local configuration JSON')
    parser.add_argument('--plan', action='store_true', help='offline plan alias')
    parser.add_argument('--master-port', type=int, help='qualification TCP port (default bootstrap_port+20)')
    parser.add_argument('--timeout', type=int, default=1800, help='maximum seconds per rank; default 1800')
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--payload', help=argparse.SUPPRESS)
    parser.add_argument('--host-action', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(worker(json.loads(base64.b64decode(args.payload, validate=True)), args.host_action))); return
    if args.plan:
        if args.action not in (None, 'plan'):
            parser.error('--plan cannot be combined with run')
        args.action = 'plan'
    if not args.action or not args.config:
        parser.error('explicit plan/run action and --config are required')
    sys.dont_write_bytecode = True
    from common import IMAGE, environment, library_path, load_config, ssh_args, state_dir, topology_sha256, transport_environment
    cfg = load_config(args.config)
    port = args.master_port if args.master_port is not None else cfg['bootstrap_port'] + 20
    if not 1 <= port <= 65535 or port in (cfg['bootstrap_port'], cfg['api_port']) or not 120 <= args.timeout <= 7200:
        parser.error('Use a distinct valid qualification port and timeout 120..7200 seconds')
    lib = library_path(cfg)
    if args.action == 'plan':
        print(json.dumps({'action': 'plan', 'image': IMAGE, 'library': str(lib),
                          'hosts': [n['name'] for n in cfg['nodes']], 'topology_sha256': topology_sha256(cfg),
                          'master_port': port, 'tests_per_rank': len(CASES),
                          'required_hcas_by_host': {n['name']: [f['hca'] for f in n['fabric']] for n in cfg['nodes']},
                          'guards': ['all-host idle/preflight before staging or GPU start', '40 value-checked gathers per rank',
                                     'changing-data CUDA graph replay', 'all16 function-specific payload/error counters',
                                     'all4 RouteFinal HCAs per rank', 'host routes/files/firewall/forwarding unchanged',
                                     'fresh library/image/topology/transport receipt before serving'],
                          'remote_artifacts': '/var/tmp/spark-ring-qualification/<recipe_id>/<run_id>',
                          'receipt': str(state_dir(cfg) / 'collective-qualification.json')}, indent=2)); return
    examples = [ipaddress.IPv4Network(s) for s in ('192.0.2.0/24', '198.51.100.0/24', '203.0.113.0/24')]
    if any(any(ipaddress.IPv4Address(n['management_address']) in subnet for subnet in examples) for n in cfg['nodes']):
        parser.error('Replace documentation-only management addresses before any host action')
    digest = sha(lib)
    build = json.loads((lib.parent / 'build-receipt.json').read_text())
    expected_build = {'base_commit': BASE_COMMIT, 'patch_sha256': PATCH_SHA256,
                      'patched_tree': PATCHED_TREE, 'library_sha256': digest, 'nccl_get_version': 23007,
                      'native_cpu_test_passed': True, 'native_cpu_library_load_passed': True}
    if any(build.get(k) != v for k, v in expected_build.items()):
        raise RuntimeError('Pinned native build receipt does not match this library; rebuild before qualification')
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]
    state_dir(cfg).mkdir(parents=True, exist_ok=True)
    qualification_lock = (state_dir(cfg) / '.qualification.lock').open('a')
    try:
        fcntl.flock(qualification_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError('Another qualification invocation holds this state directory lock')
    case = state_dir(cfg) / 'qualification-runs' / run_id
    case.mkdir(parents=True, exist_ok=False)
    os.chmod(case, 0o700)
    canonical = state_dir(cfg) / 'collective-qualification.json'
    name = 'spark-ring-qualify-' + run_id.lower()
    source = Path(__file__).read_text()
    script_directory = Path(cfg['shared_root']) / 'scripts'
    source_hashes = {filename: sha(script_directory / filename) for filename in ('collective-probe.py', 'capture-function-counters.py')}
    environments, hcas = {}, {}
    payloads = []
    for rank, node in enumerate(cfg['nodes']):
        host_hcas = [f['hca'] for f in node['fabric']]
        env = environment(cfg, rank, digest)
        environments[node['name']] = env
        hcas[node['name']] = host_hcas
        test_env = {**env, 'RANK': str(rank), 'MASTER_PORT': str(port), 'EXPECTED_NCCL_SHA256': digest,
                    'SPARK_RING_REQUIRED_HCAS': json.dumps(host_hcas), 'PYTHONUNBUFFERED': '1'}
        payloads.append({'node': node, 'rank': rank, 'recipe_id': cfg['recipe_id'], 'run_id': run_id,
                         'container_name': name, 'image': IMAGE, 'library': str(lib), 'library_sha256': digest,
                         'source_sha256': source_hashes, 'script_directory': str(script_directory),
                         'fabric_address_range': cfg['fabric_address_range'], 'mtu': cfg['mtu'],
                         'master_port': port, 'timeout': args.timeout, 'environment': test_env})
    lock = threading.Lock()
    def event(action, **extra):
        with lock:
            with (case / 'events.jsonl').open('a') as f:
                f.write(json.dumps({'utc': utc(), 'action': action, **extra}) + '\n')
    def atomic(path, value):
        temp = path.with_name(path.name + '.' + run_id + '.tmp')
        temp.write_text(json.dumps(value, indent=2) + '\n')
        os.chmod(temp, 0o600)
        os.replace(temp, path)
        event('wrote-receipt', path=str(path), qualified=value.get('qualified'))
    summary = {'schema': 'spark-ring-tensorfold-qualification/v2', 'generated_utc': utc(), 'run_id': run_id,
               'qualified': False, 'correctness_passed': False, 'payload_all_hcas_passed': False, 'cuda_graph_passed': False,
               'host_configuration_unchanged': False, 'qp_diagnostics_all_hcas_passed': False,
               'hosts': [n['name'] for n in cfg['nodes']], 'required_hcas_by_host': hcas,
               'transport_env_by_host': {name: transport_environment(env) for name, env in environments.items()},
               'topology_sha256': topology_sha256(cfg), 'library_sha256': digest, 'image_digest': IMAGE,
               'master_port': port, 'source_sha256': source_hashes, 'run_directory': str(case)}
    event('created-operator-run-directory', path=str(case), network_configuration_mutations=False)
    event('held-exclusive-qualification-lock', path=str(state_dir(cfg) / '.qualification.lock'))
    (case / 'selected-build-receipt.json').write_text(json.dumps(build, indent=2) + '\n')
    event('archived-selected-build-receipt', path=str(case / 'selected-build-receipt.json'))
    if canonical.exists():
        (case / 'previous-canonical.json').write_bytes(canonical.read_bytes())
        event('archived-previous-qualification', path=str(case / 'previous-canonical.json'))
    atomic(canonical, summary)  # A failed attempt must never leave a previous successful receipt current.
    event('recorded-selected-environment', environments=environments)
    def remote(payload, action):
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        prefix = 'import sys\nsys.argv=' + repr(['qualify.py', '--worker', '--payload', encoded, '--host-action', action]) + '\n'
        script = "sudo -n -s <<'SPARK_RING_QUAL_ROOT'\nset -eu\npython3 - <<'SPARK_RING_QUAL_PY'\n" + prefix + source + "\nSPARK_RING_QUAL_PY\nSPARK_RING_QUAL_ROOT"
        node = payload['node']
        event('ssh-start', node=node['name'], host_action=action, exact_script=script)
        try:
            p = subprocess.run(ssh_args(node, cfg) + [script], text=True, capture_output=True,
                               timeout=args.timeout + 150 if action == 'finish' else 180)
            event('ssh-finish', node=node['name'], host_action=action, returncode=p.returncode,
                  stdout=p.stdout, stderr=p.stderr)
        except Exception as exc:
            event('ssh-error', node=node['name'], host_action=action, error=str(exc)); raise
        if p.returncode:
            raise RuntimeError(f"{node['name']} {action}: {p.stderr.strip()}")
        return json.loads(p.stdout)
    def all_hosts(action):
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(remote, p, action) for p in payloads]
            results, errors = [], []
            for rank, future in enumerate(futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    errors.append(f"rank{rank}: {exc}")
            if errors:
                raise RuntimeError('; '.join(errors))
        return results
    error = None
    staged = False
    try:
        before = all_hosts('preflight')
        (case / 'preflight.json').write_text(json.dumps(before, indent=2) + '\n')
        event('all-host-preflight-passed', path=str(case / 'preflight.json'))
        staged = True  # Include cleanup after partial staging/creation failures.
        all_hosts('stage')
        all_hosts('create')
        all_hosts('start')
        finished = all_hosts('finish')
        correctness = True
        preserve = True
        diag_ok = True
        final_qp_hcas = {}
        for rank, result in enumerate(finished):
            node = cfg['nodes'][rank]
            report = result['report']
            (case / f'rank-{rank}.json').write_text(json.dumps(report, indent=2) + '\n')
            (case / f'rank-{rank}.log').write_text(result['log'])
            (case / f'os-after-rank-{rank}.json').write_text(json.dumps(result['snapshot'], indent=2) + '\n')
            correctness &= result['exit_code'] == '0' and validate_report(report, rank, hcas[node['name']], digest)
            preserve &= result['snapshot'] == before[rank]['snapshot']
            diag = sorted(set(re.findall(r'RouteFinal[^\n]* hca=([^\s]+)', result['log'])))
            final_qp_hcas[node['name']] = diag
            diag_ok &= set(diag) == set(hcas[node['name']])
        summary.update(correctness_passed=correctness, cuda_graph_passed=correctness,
                       payload_all_hcas_passed=correctness, host_configuration_unchanged=preserve,
                       qp_diagnostics_all_hcas_passed=diag_ok,
                       qualified=bool(correctness and preserve and diag_ok), final_qp_hcas=final_qp_hcas,
                       container_exit_codes={cfg['nodes'][r]['name']: x['exit_code'] for r, x in enumerate(finished)})
    except Exception as exc:
        error = str(exc)
        summary['failure'] = error
        event('qualification-failed', error=error)
    finally:
        if staged:
            # Each exact name is removed only after matching both ownership labels.
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [pool.submit(remote, payload, 'cleanup') for payload in payloads]
                cleanup_errors = []
                for rank, future in enumerate(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        cleanup_errors.append(f"rank{rank}: {exc}")
                if cleanup_errors:
                    summary['cleanup_errors'] = cleanup_errors
                    summary['qualified'] = False
                    event('cleanup-failed', errors=cleanup_errors)
        summary['finished_utc'] = utc()
        atomic(case / 'qualification.json', summary)
        atomic(canonical, summary)
    print(json.dumps(summary, indent=2))
    if not summary['qualified']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
