#!/usr/bin/env python3
"""Check release sources without SSH, GPUs, downloads or system changes."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parent.parent
EXCLUDED = {'.git', '__pycache__', 'local', 'state', 'logs', 'lib', 'models', 'cache', 'build', 'rigmark-source'}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--write-manifest', action='store_true')
    args = p.parse_args()
    files = sorted(path for path in ROOT.rglob('*') if path.is_file()
                   and not any(part in EXCLUDED for part in path.relative_to(ROOT).parts)
                   and path.name != 'release-manifest.json'
                   and not path.name.startswith('config.local')
                   and path.name not in {'config.json', 'metadata.local.json'})
    patterns = [r'192\.168\.[0-9]+\.[0-9]+', r'/home/[A-Za-z0-9_.-]+',
                r'/shared/' + r'spark-ring/recipe', r'-----BEGIN [A-Z ]*PRIVATE KEY-----',
                r'\bssh-(?:ed25519|rsa)\s+[A-Za-z0-9+/]{50,}={0,3}',
                r'\b(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}\b']
    manifest = {}
    for path in files:
        data = path.read_bytes()
        text = data.decode('utf-8')
        relative = str(path.relative_to(ROOT))
        for pattern in patterns:
            if re.search(pattern, text):
                raise RuntimeError('Private/site-specific content found in ' + relative)
        if path.suffix == '.py':
            ast.parse(text, filename=relative)
        elif path.suffix == '.sh':
            subprocess.run(['bash', '-n', str(path)], check=True)
        elif path.suffix == '.json':
            json.loads(text)
        if path.is_symlink():
            raise RuntimeError('Release sources must not contain symlinks: ' + relative)
        manifest[relative] = {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}
    required = ['README.md', 'LICENSE', 'CREDITS.md', 'config.example.json', 'provenance.json',
                'scripts/common.py', 'scripts/cluster.py', 'scripts/runtime.py', 'scripts/fabric.py',
                'scripts/qualify.py', 'scripts/build-nccl.sh', 'scripts/download-models.py',
                'scripts/chat.py', 'scripts/run-rigmark.py', 'scripts/run-sparkdash.py',
                'docs/benchmark-method.md', 'docs/benchmark-comparison.md', 'docs/phase-one.md',
                'results/mia-comparison.json', 'third_party/sparkdash/LICENSE',
                'third_party/sparkdash/LICENSE-MIT', 'third_party/sparkdash/NOTICE',
                'results/rigmark-result.json', 'results/alex-rigmark-result.json']
    for relative in required:
        if relative not in manifest:
            raise RuntimeError('Missing public recipe file: ' + relative)
    patch = ROOT / 'third_party/sparkring/nccl-2.30.7-dual-pci-domain.patch'
    if hashlib.sha256(patch.read_bytes()).hexdigest() != json.loads((ROOT / 'provenance.json').read_text())['nccl_patch_sha256']:
        raise RuntimeError('Pinned NCCL patch differs')
    receipt = {'schema': 'public-recipe-files/v1', 'files': manifest}
    manifest_path = ROOT / 'release-manifest.json'
    if args.write_manifest:
        manifest_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
    elif json.loads(manifest_path.read_text()) != receipt:
        raise RuntimeError('Release file manifest differs; review changes before regenerating it')
    print(json.dumps({'passed': True, 'files': len(manifest),
                      'checks': ['Python AST', 'Bash syntax', 'JSON parse', 'private-data scan', 'required files', 'patch hash',
                                 'manifest written' if args.write_manifest else 'manifest hashes']}))


if __name__ == '__main__':
    main()
