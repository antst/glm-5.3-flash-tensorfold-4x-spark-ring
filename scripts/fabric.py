#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Plan, check, apply, verify or roll back dedicated IPv4/MTU fabric profiles.

Only `apply` and `rollback` configure host networking. No manual routes,
forwarding, NAT, firewall, driver/hairpin or global library changes are made.
Private configuration backups remain root-only on each host. Operational
receipts/logs belong in the ignored state directory, never in the public repo.
"""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import socket
import subprocess
import sys
import threading
import uuid


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def worker(payload, action):
    """Runs inside the explicitly requested sudo -s shell on one host."""
    if os.geteuid() != 0:
        raise RuntimeError('Fabric host actions require a root shell (sudo -n -s)')
    node, cfg = payload['node'], payload['config']
    ports = node['fabric']
    tag = payload['topology_sha256']
    if not re.fullmatch(r'[0-9a-f]{64}', tag) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', node['name']):
        raise RuntimeError('Unsafe backup identity')
    backup = Path('/var/backups/spark-ring-public') / tag / node['name']
    state_file = backup / 'state.json'
    events = []

    def command(args, mutation=False):
        if mutation:
            events.append({'utc': utc(), 'command': shlex.join(args), 'stage': 'start'})
            save_events()
        p = subprocess.run(args, text=True, capture_output=True)
        if mutation:
            events.append({'utc': utc(), 'command': shlex.join(args), 'stage': 'finish',
                           'returncode': p.returncode, 'stdout': p.stdout, 'stderr': p.stderr})
            save_events()
        if p.returncode:
            raise RuntimeError(f'{shlex.join(args)} failed: {p.stderr.strip()}')
        return p.stdout

    def save_events():
        if backup.is_dir():
            with (backup / 'changes.jsonl').open('a') as f:
                for event in events:
                    f.write(json.dumps(event) + '\n')
            events.clear()
            os.chmod(backup / 'changes.jsonl', 0o600)

    def normalize_firewall(text):
        return '\n'.join(re.sub(r'\[\d+:\d+\]', '[COUNTERS]', line)
                         for line in text.splitlines() if not line.startswith('#'))

    def routes(family):
        rows = json.loads(command(['ip', '-j', family, 'route', 'show', 'table', 'all']))
        return sorted((json.dumps({k: v for k, v in r.items() if k not in ('expires', 'lastuse', 'used', 'cache')}, sort_keys=True)
                       for r in rows))

    def policy():
        controls = [Path('/proc/sys/net/ipv4/ip_forward'), Path('/proc/sys/net/ipv6/conf/all/forwarding')]
        controls += list(Path('/proc/sys/net/ipv4/conf').glob('*/rp_filter'))
        controls += list(Path('/proc/sys/net/ipv6/conf').glob('*/forwarding'))
        return {'defaults4': json.loads(command(['ip', '-j', '-4', 'route', 'show', 'default'])),
                'defaults6': json.loads(command(['ip', '-j', '-6', 'route', 'show', 'default'])),
                'firewall4': normalize_firewall(command(['iptables-save'])),
                'firewall6': normalize_firewall(command(['ip6tables-save'])),
                'sysctls': {str(p): p.read_text().strip() for p in controls}}

    def files():
        result = {}
        for base in ('/etc/netplan', '/etc/NetworkManager/system-connections', '/run/NetworkManager/system-connections'):
            for p in Path(base).rglob('*'):
                if p.is_file() and not p.is_symlink():
                    result[str(p)] = {'sha256': hashlib.sha256(p.read_bytes()).hexdigest(), 'mode': p.stat().st_mode & 0o777}
        return result

    def inspect():
        links = {x['ifname']: x for x in json.loads(command(['ip', '-j', '-d', 'link', 'show']))}
        addresses = {x['ifname']: x for x in json.loads(command(['ip', '-j', 'address', 'show']))}
        management = addresses.get(node['management_interface'], {})
        if node['management_address'] not in [a['local'] for a in management.get('addr_info', []) if a['family'] == 'inet']:
            raise RuntimeError('Configured management IPv4 address/interface is absent')
        defaults = {r.get('dev') for family in ('-4', '-6')
                    for r in json.loads(command(['ip', '-j', family, 'route', 'show', 'default']))}
        actual = []
        for p in ports:
            dev, hca = p['interface'], p['hca']
            if dev == node['management_interface'] or dev in defaults:
                raise RuntimeError(f'Refusing management/default-route interface {dev}')
            d = Path('/sys/class/infiniband') / hca
            associated = sorted(x.name for x in (d / 'device/net').iterdir())
            if associated != [dev]:
                raise RuntimeError(f'{hca} does not uniquely belong to {dev}')
            link = links[dev]
            values = []
            for f in sorted((d / 'ports/1/gids').glob('*'), key=lambda x: int(x.name)):
                if int(f.read_text().strip().replace(':', ''), 16):
                    values.append({'index': int(f.name), 'gid': f.read_text().strip(),
                                   'type': (d / 'ports/1/gid_attrs/types' / f.name).read_text().strip(),
                                   'netdev': (d / 'ports/1/gid_attrs/ndevs' / f.name).read_text().strip()})
            actual.append({**p, 'mac': link['address'], 'mtu': link['mtu'], 'up': 'UP' in link['flags'],
                           'addrgenmode': link.get('inet6_addr_gen_mode', 'eui64'),
                           'addresses': [{k: v for k, v in a.items() if k not in ('valid_life_time', 'preferred_life_time')}
                                         for a in addresses[dev].get('addr_info', [])],
                           'active_uuid': command(['nmcli', '-g', 'GENERAL.CON-UUID', 'device', 'show', dev]).strip(),
                           'gids': values, 'rdma_state': (d / 'ports/1/state').read_text().strip()})
        return {'host': socket.gethostname(), 'ports': actual, 'policy': policy(), 'files': files(),
                'routes4': routes('-4'), 'routes6': routes('-6')}

    def owned_profiles(state):
        existing = set(command(['nmcli', '-t', '-f', 'UUID', 'connection', 'show']).splitlines())
        for p in state['profiles']:
            if p['uuid'] not in existing:
                continue
            fields = ['connection.id', 'connection.interface-name', 'ipv4.method', 'ipv4.addresses',
                      'ipv4.gateway', 'ipv4.routes', 'ipv4.never-default', 'ipv6.method', '802-3-ethernet.mtu']
            expected = [p['profile'], p['interface'], 'manual', f"{p['address']}/{p['prefixlen']}", '', '', 'yes', 'ignore', str(cfg['mtu'])]
            got = [command(['nmcli', '-g', f, 'connection', 'show', 'uuid', p['uuid']]).strip() for f in fields]
            if got != expected:
                raise RuntimeError(f"Owned profile {p['uuid']} was changed; refusing to overwrite it")
        return existing

    def verify(state):
        after = inspect()
        if after['policy'] != state['before']['policy']:
            raise RuntimeError('Management defaults, firewall, forwarding or reverse-path filters changed')
        if after['routes6'] != state['before']['routes6']:
            raise RuntimeError('Unexpected IPv6 routing change')
        for name, value in state['before']['files'].items():
            if after['files'].get(name) != value:
                raise RuntimeError('An original network configuration file changed: ' + name)
        if not set(state['before']['routes4']).issubset(set(after['routes4'])):
            raise RuntimeError('An original IPv4 route changed/disappeared')
        allowed = {p['interface']: p for p in ports}
        for text in set(after['routes4']) - set(state['before']['routes4']):
            r = json.loads(text)
            if r.get('dev') not in allowed or 'gateway' in r or r.get('protocol') != 'kernel':
                raise RuntimeError('Unexpected route beyond automatic fabric/local routes')
            p = allowed[r['dev']]
            net = ipaddress.IPv4Interface(f"{p['address']}/{p['prefixlen']}").network
            dest = ipaddress.IPv4Network(r['dst'], strict=False)
            allowed_dest = {net, ipaddress.IPv4Network(p['address'] + '/32'),
                            ipaddress.IPv4Network(str(net.network_address) + '/32'),
                            ipaddress.IPv4Network(str(net.broadcast_address) + '/32')}
            if dest not in allowed_dest or r.get('scope') not in ('link', 'host'):
                raise RuntimeError('Unexpected route destination/scope')
        for before, p in zip(state['before']['ports'], after['ports']):
            if [a for a in before['addresses'] if a['family'] == 'inet6'] != [a for a in p['addresses'] if a['family'] == 'inet6']:
                raise RuntimeError('Unexpected fabric IPv6 address change')
            if p['mtu'] != cfg['mtu'] or not p['up'] or 'ACTIVE' not in p['rdma_state']:
                raise RuntimeError('Fabric interface is not UP/ACTIVE at the configured MTU')
            if [a['local'] for a in p['addresses'] if a['family'] == 'inet'] != [p['address']]:
                raise RuntimeError('Fabric IPv4 assignment differs')
            want = ipaddress.IPv6Address('::ffff:' + p['address'])
            matches = [g for g in p['gids'] if g['type'] == 'RoCE v2' and g['netdev'] == p['interface']
                       and ipaddress.IPv6Address(g['gid']) == want]
            if len(matches) != 1:
                raise RuntimeError('Exactly one matching IPv4 RoCE-v2 GID required for ' + p['hca'])
            p['gid_index'] = matches[0]['index']
        existing = owned_profiles(state)
        if any(p['uuid'] not in existing for p in state['profiles']):
            raise RuntimeError('An owned fabric profile is absent')
        return after

    def public(actual, **extra):
        return {'utc': utc(), 'node': node['name'], 'action': action, 'topology_sha256': tag,
                'ports': [{k: p[k] for k in ('interface', 'hca', 'address', 'prefixlen', 'mtu', 'rdma_state')}
                          | ({'gid_index': p['gid_index']} if 'gid_index' in p else {}) for p in actual['ports']], **extra}

    state = json.loads(state_file.read_text()) if state_file.exists() else None
    if state and (state['topology_sha256'] != tag or state['node'] != node['name']):
        raise RuntimeError('Private backup belongs to another topology/node')
    before = inspect()
    if action == 'check':
        if state and state.get('applied'):
            return public(verify(state), ready=True, configured=True, backup=str(backup))
        if command(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader,nounits']).strip():
            raise RuntimeError('Active GPU compute processes; stop owned workloads before fresh fabric setup')
        if state:
            existing = owned_profiles(state)
            if before['policy'] != state['before']['policy']:
                raise RuntimeError('Policy changed since the partial application baseline')
            for name, value in state['before']['files'].items():
                if before['files'].get(name) != value:
                    raise RuntimeError('Original configuration changed since partial application')
            for current, planned in zip(before['ports'], state['profiles']):
                assigned = [a['local'] for a in current['addresses'] if a['family'] == 'inet']
                if assigned and (assigned != [planned['address']] or current['active_uuid'] != planned['uuid']):
                    raise RuntimeError('Another IPv4 configuration appeared during partial application')
            return public(before, ready=True, configured=False, partial_application=True, backup=str(backup))
        prefix = ipaddress.IPv4Network(cfg['fabric_address_range'])
        for r in map(json.loads, before['routes4']):
            if r.get('dst', 'default') != 'default' and ipaddress.IPv4Network(r['dst'], strict=False).overlaps(prefix):
                raise RuntimeError('An existing IPv4 route overlaps fabric_address_range')
        if any(a['family'] == 'inet' for p in before['ports'] for a in p['addresses']):
            raise RuntimeError('Dedicated fabric interfaces already have IPv4; adopt/review them explicitly instead')
        return public(before, ready=True, configured=False)
    if action == 'verify':
        if not state:
            raise RuntimeError('No private baseline exists for this topology; run check/apply first')
        return public(verify(state), passed=True, backup=str(backup))
    if action == 'apply':
        if state and state.get('applied'):
            return public(verify(state), passed=True, unchanged=True, backup=str(backup))
        if command(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader,nounits']).strip():
            raise RuntimeError('Active GPU compute processes; stop owned workloads before fabric changes')
        if state is None:
            if any(a['family'] == 'inet' for p in before['ports'] for a in p['addresses']):
                raise RuntimeError('Refusing addressed fabric interfaces without a task baseline')
            # Initial preconditions are also run on every node by the controller.
            for r in map(json.loads, before['routes4']):
                if r.get('dst', 'default') != 'default' and ipaddress.IPv4Network(r['dst'], strict=False).overlaps(ipaddress.IPv4Network(cfg['fabric_address_range'])):
                    raise RuntimeError('Fabric range overlaps an existing route')
            for parent in (backup.parent.parent, backup.parent, backup):
                if parent.is_symlink():
                    raise RuntimeError('Refusing symlink private backup directory')
                parent.mkdir(mode=0o700, exist_ok=True)
                os.chmod(parent, 0o700)
            for base in ('/etc/netplan', '/etc/NetworkManager/system-connections', '/run/NetworkManager/system-connections'):
                if Path(base).exists():
                    shutil.copytree(base, backup / base.lstrip('/'), symlinks=True)
            state = {'topology_sha256': tag, 'node': node['name'], 'created_utc': utc(), 'before': before,
                     'profiles': [{**p, 'uuid': str(uuid.uuid4()),
                                   'profile': f"spark-ring-{tag[:8]}-r{payload['rank']}-p{p['physical_port']}-rail{p['rail']}"}
                                  for p in before['ports']], 'applied': False}
            state_file.write_text(json.dumps(state, indent=2) + '\n'); os.chmod(state_file, 0o600)
            events.append({'utc': utc(), 'action': 'root-only original configuration backup', 'path': str(backup)})
            save_events()
        existing = owned_profiles(state)
        for p in state['profiles']:
            if p['uuid'] not in existing:
                command(['nmcli', 'connection', 'add', 'type', 'ethernet', 'ifname', p['interface'],
                         'con-name', p['profile'], 'connection.uuid', p['uuid'],
                         'connection.autoconnect', 'yes', 'connection.autoconnect-priority', '100',
                         '802-3-ethernet.mac-address', p['mac'], '802-3-ethernet.mtu', str(cfg['mtu']),
                         'ipv4.method', 'manual', 'ipv4.addresses', f"{p['address']}/{p['prefixlen']}",
                         'ipv4.gateway', '', 'ipv4.routes', '', 'ipv4.dns', '',
                         'ipv4.never-default', 'yes', 'ipv4.ignore-auto-dns', 'yes',
                         'ipv4.may-fail', 'no', 'ipv6.method', 'ignore', 'ipv6.never-default', 'yes'], mutation=True)
            current = command(['nmcli', '-g', 'GENERAL.CON-UUID', 'device', 'show', p['interface']]).strip()
            if current != p['uuid']:
                command(['nmcli', '--wait', '20', 'connection', 'up', 'uuid', p['uuid'], 'ifname', p['interface']], mutation=True)
        after = verify(state)
        state['applied'] = True; state['verified_utc'] = utc()
        state_file.write_text(json.dumps(state, indent=2) + '\n'); os.chmod(state_file, 0o600)
        return public(after, passed=True, backup=str(backup), created_profiles=[p['uuid'] for p in state['profiles']])
    if action == 'rollback':
        if not state:
            raise RuntimeError('No private baseline to roll back')
        existing = owned_profiles(state)
        # Refuse to displace someone else's subsequent configuration.
        for original, current, p in zip(state['before']['ports'], before['ports'], state['profiles']):
            allowed = {p['uuid'], original['active_uuid'], '', '--'}
            if current['active_uuid'] not in allowed:
                raise RuntimeError('Another active fabric profile appeared; refusing rollback')
        for original, p in zip(state['before']['ports'], state['profiles']):
            active = command(['nmcli', '-g', 'GENERAL.CON-UUID', 'device', 'show', p['interface']]).strip()
            if active == p['uuid']:
                command(['nmcli', 'connection', 'down', 'uuid', p['uuid']], mutation=True)
            if p['uuid'] in existing:
                command(['nmcli', 'connection', 'delete', 'uuid', p['uuid']], mutation=True)
            old = original['active_uuid']
            if old and old != '--':
                if old not in existing:
                    raise RuntimeError('Original active profile disappeared; backup retained for manual review')
                command(['nmcli', '--wait', '20', 'connection', 'up', 'uuid', old, 'ifname', p['interface']], mutation=True)
            mode = original['addrgenmode']
            if mode not in ('none', 'eui64', 'stable_secret', 'random'):
                raise RuntimeError('Unsupported original IPv6 address generation mode')
            command(['ip', 'link', 'set', 'dev', p['interface'], 'mtu', str(original['mtu']),
                     'addrgenmode', mode, 'up' if original['up'] else 'down'], mutation=True)
        restored = inspect()
        if restored['policy'] != state['before']['policy'] or restored['routes4'] != state['before']['routes4'] or restored['routes6'] != state['before']['routes6']:
            raise RuntimeError('Rollback policy/routes differ; retained backup requires review')
        for name, value in state['before']['files'].items():
            if restored['files'].get(name) != value:
                raise RuntimeError('An original configuration file differs after rollback: ' + name)
        for old, new in zip(state['before']['ports'], restored['ports']):
            if any(old[key] != new[key] for key in ('mtu', 'up', 'addrgenmode', 'addresses')):
                raise RuntimeError('Rollback interface state differs; retained backup requires review')
        state['applied'] = False; state['rolled_back_utc'] = utc()
        state_file.write_text(json.dumps(state, indent=2) + '\n'); os.chmod(state_file, 0o600)
        return public(restored, rolled_back=True, backup_retained=str(backup))
    raise RuntimeError('Unknown host action')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', nargs='?', choices=('plan', 'check', 'apply', 'verify', 'rollback'))
    parser.add_argument('--config', help='explicit local cluster configuration JSON')
    parser.add_argument('--plan', action='store_true', help='alias for the offline plan action')
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--payload', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.plan:
        if args.action not in (None, 'plan'):
            parser.error('--plan cannot be combined with a host action')
        args.action = 'plan'
    if not args.action:
        parser.error('an explicit action or --plan is required')
    if args.worker:
        payload = json.loads(base64.b64decode(args.payload, validate=True))
        print(json.dumps(worker(payload, args.action), indent=2)); return
    if not args.config:
        parser.error('--config is required for every operator action')
    sys.dont_write_bytecode = True
    from common import load_config, ssh_args, state_dir, topology_sha256
    cfg = load_config(args.config)
    identity = topology_sha256(cfg)
    if args.action == 'plan':
        print(json.dumps({'action': 'plan', 'topology_sha256': identity, 'nodes': cfg['nodes'],
                          'configure': 'Only dedicated fabric IPv4 and MTU via new MAC-bound NM profiles; IPv6 ignored',
                          'preserve': ['management/default routes', 'existing files/profiles', 'firewall/NAT', 'forwarding/rp_filter'],
                          'private_backup': '/var/backups/spark-ring-public/<topology-sha256>/<node>',
                          'audit_output': str(state_dir(cfg) / 'fabric')}, indent=2)); return
    examples = [ipaddress.IPv4Network(s) for s in ('192.0.2.0/24', '198.51.100.0/24', '203.0.113.0/24')]
    if any(any(ipaddress.IPv4Address(n['management_address']) in subnet for subnet in examples) for n in cfg['nodes']):
        parser.error('Replace documentation-only management addresses before any host action')
    audit = state_dir(cfg) / 'fabric'
    audit.mkdir(parents=True, exist_ok=True)
    lock = threading.Lock()
    source = Path(__file__).read_text()
    def run(pair, action):
        rank, node = pair
        payload = {'rank': rank, 'node': node, 'config': {k: cfg[k] for k in ('mtu', 'fabric_address_range')},
                   'topology_sha256': identity}
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        prefix = 'import sys\nsys.argv=' + repr(['fabric.py', action, '--worker', '--payload', encoded]) + '\n'
        script = "sudo -n -s <<'SPARK_RING_PUBLIC_ROOT'\nset -eu\npython3 - <<'SPARK_RING_PUBLIC_PY'\n" + prefix + source + "\nSPARK_RING_PUBLIC_PY\nSPARK_RING_PUBLIC_ROOT"
        start = {'utc': utc(), 'node': node['name'], 'action': action, 'stage': 'start', 'exact_script': script}
        with lock:
            with (audit / 'events.jsonl').open('a') as f:f.write(json.dumps(start) + '\n')
        p = subprocess.run(ssh_args(node, cfg) + [script], text=True, capture_output=True, timeout=300)
        finish = {'utc': utc(), 'node': node['name'], 'action': action, 'stage': 'finish', 'returncode': p.returncode,
                  'stdout': p.stdout, 'stderr': p.stderr}
        with lock:
            with (audit / 'events.jsonl').open('a') as f:f.write(json.dumps(finish) + '\n')
        if p.returncode:raise RuntimeError(f"{node['name']}: fabric {action} failed: {p.stderr.strip()}")
        record = json.loads(p.stdout)
        (audit / f"{utc().replace(':','')}-{node['name']}-{action}.json").write_text(json.dumps(record, indent=2) + '\n')
        return record
    pairs = list(enumerate(cfg['nodes']))
    # All initial read-only preconditions must pass before any host changes.
    if args.action == 'apply':
        with ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(lambda x: run(x, 'check'), pairs))
    with ThreadPoolExecutor(max_workers=4) as pool:
        records = list(pool.map(lambda x: run(x, args.action), pairs))
    print(json.dumps(records, indent=2))


if __name__ == '__main__':
    main()
