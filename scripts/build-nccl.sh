#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Run on an ARM64 Spark. Build only in a new task directory; install no packages.
set -euo pipefail
NCCL_SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
NCCL_RECIPE_DIR=$(dirname "$NCCL_SCRIPT_DIR")
NCCL_PATCH="$NCCL_RECIPE_DIR/third_party/sparkring/nccl-2.30.7-dual-pci-domain.patch"
NCCL_TASK_DIR=""
NCCL_PUBLISH_DIR=""
NCCL_PLAN=0
while (( $# )); do
  case "$1" in
    --plan) NCCL_PLAN=1; shift ;;
    --build-dir) NCCL_TASK_DIR=${2:?--build-dir needs a path}; shift 2 ;;
    --publish) NCCL_PUBLISH_DIR=${2:?--publish needs a path}; shift 2 ;;
    --help|-h) printf '%s\n' 'Usage: build-nccl.sh [--plan] [--build-dir /var/tmp/spark-ring-nccl-rebuild-UNIQUE] [--publish NEW_LIB_DIR]'; exit 0 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done
NCCL_TASK_DIR=${NCCL_TASK_DIR:-/var/tmp/spark-ring-nccl-rebuild-$(date -u +%Y%m%dT%H%M%S%NZ)}
if (( NCCL_PLAN )); then
  printf 'Fetch NVIDIA NCCL commit 73cf112295c33aee2b895f329f592f2a9b4b0f97 and verify base tree; apply ONLY cumulative patch SHA256 8e2b8715d62d2b07a74caca3778da0eff2e7b54caf8a184bb728f179a5d1eba4.\n'
  printf 'Build ARM64/SM121 in %s with CUDA toolkit %s; run CPU ABI/GID/root checks; create local library, notices, hashes and change ledger.\n' "$NCCL_TASK_DIR" "${NCCL_TOOLKIT:-/usr/local/cuda}"
  if [[ -n "$NCCL_PUBLISH_DIR" ]]; then printf 'Publish to NEW directory %s; never overwrite an existing artifact directory. Fresh target-image and four-rank qualification required.\n' "$NCCL_PUBLISH_DIR"; fi
  exit 0
fi
case "$(uname -m)" in aarch64|arm64) ;; *) echo 'This builder requires an ARM64 Spark host.' >&2; exit 2;; esac
NCCL_TOOLKIT=${NCCL_TOOLKIT:-/usr/local/cuda}
NCCL_BUILD_JOBS=${NCCL_BUILD_JOBS:-8}
[[ "$NCCL_TASK_DIR" =~ ^/var/tmp/spark-ring-nccl-rebuild-[A-Za-z0-9_-]+$ ]] || { echo 'Use a single /var/tmp/spark-ring-nccl-rebuild-UNIQUE task directory without path traversal.' >&2; exit 2; }
[[ "$NCCL_BUILD_JOBS" =~ ^[1-9][0-9]*$ ]] || { echo 'NCCL_BUILD_JOBS must be a positive integer.' >&2; exit 2; }
test ! -e "$NCCL_TASK_DIR"
if [[ -n "$NCCL_PUBLISH_DIR" ]]; then [[ "$NCCL_PUBLISH_DIR" = /* ]]; test ! -e "$NCCL_PUBLISH_DIR"; test ! -L "$NCCL_PUBLISH_DIR"; fi
printf '%s  %s\n' '8e2b8715d62d2b07a74caca3778da0eff2e7b54caf8a184bb728f179a5d1eba4' "$NCCL_PATCH" | sha256sum --check --status -
for NCCL_TOOL in git gcc g++ make python3 sha256sum; do command -v "$NCCL_TOOL" >/dev/null; done
test -x "$NCCL_TOOLKIT/bin/nvcc"
umask 022
mkdir "$NCCL_TASK_DIR"
date -u +'%Y-%m-%dT%H:%M:%SZ Created dedicated NCCL native build directory. All output stays here; rollback is removal of this exact directory after preserving needed artifacts. No host packages, configuration, services, network or global libraries are changed.' > "$NCCL_TASK_DIR/changes.txt"
cp "$NCCL_PATCH" "$NCCL_TASK_DIR/nccl-2.30.7-dual-pci-domain.patch"
GIT_TERMINAL_PROMPT=0 git clone --quiet --depth 1 --branch v2.30.7-1 https://github.com/NVIDIA/nccl.git "$NCCL_TASK_DIR/src" > "$NCCL_TASK_DIR/prepare.log" 2>&1
cd "$NCCL_TASK_DIR/src"
test "$(git rev-parse HEAD)" = 73cf112295c33aee2b895f329f592f2a9b4b0f97
test "$(git write-tree)" = 3e7de6f92f0190d1afe9f05642e634cbf43ae4c9
git apply --check "$NCCL_TASK_DIR/nccl-2.30.7-dual-pci-domain.patch" >> "$NCCL_TASK_DIR/prepare.log" 2>&1
git apply --index "$NCCL_TASK_DIR/nccl-2.30.7-dual-pci-domain.patch" >> "$NCCL_TASK_DIR/prepare.log" 2>&1
test "$(git write-tree)" = 01e0e8a3a821ebc92a4d1333981d18ed4eeb96e4
g++ -std=c++11 -O2 -Wall -Wextra -Werror tests/routing_handle/compat.cc -o "$NCCL_TASK_DIR/routing-handle-test"
"$NCCL_TASK_DIR/routing-handle-test" > "$NCCL_TASK_DIR/cpu-tests.log" 2>&1
"$NCCL_TOOLKIT/bin/nvcc" --version > "$NCCL_TASK_DIR/toolchain.txt"
gcc --version >> "$NCCL_TASK_DIR/toolchain.txt"
ldd --version >> "$NCCL_TASK_DIR/toolchain.txt"
date -u +'%Y-%m-%dT%H:%M:%SZ Verified pinned source revision, base tree, cumulative patch hash and patched tree. CPU ABI/GID/PCI-root tests passed. Starting SM121 compile; no GPU job executed.' >> "$NCCL_TASK_DIR/changes.txt"
make -j"$NCCL_BUILD_JOBS" src.build CUDA_HOME="$NCCL_TOOLKIT" CUDA_LIB="$NCCL_TOOLKIT/lib64" NVCC_GENCODE='-gencode=arch=compute_121,code=sm_121' > "$NCCL_TASK_DIR/build.log" 2>&1
mkdir "$NCCL_TASK_DIR/lib"
install -m 0755 build/lib/libnccl.so.2.30.7 "$NCCL_TASK_DIR/lib/libnccl.so.2.30.7"
ln -s libnccl.so.2.30.7 "$NCCL_TASK_DIR/lib/libnccl.so.2"
ln -s libnccl.so.2 "$NCCL_TASK_DIR/lib/libnccl.so"
install -m 0644 LICENSE.txt ThirdPartyNotices.txt "$NCCL_TASK_DIR/lib/"
python3 - "$NCCL_TASK_DIR" <<'NCCL_RECEIPT'
from pathlib import Path
import ctypes,datetime,hashlib,json,sys
root=Path(sys.argv[1]); lib=root/'lib/libnccl.so.2.30.7'; version=ctypes.c_int()
assert ctypes.CDLL(str(lib)).ncclGetVersion(ctypes.byref(version))==0 and version.value==23007
receipt={'generated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'base_commit':'73cf112295c33aee2b895f329f592f2a9b4b0f97','base_tree':'3e7de6f92f0190d1afe9f05642e634cbf43ae4c9','patch_sha256':'8e2b8715d62d2b07a74caca3778da0eff2e7b54caf8a184bb728f179a5d1eba4','patched_tree':'01e0e8a3a821ebc92a4d1333981d18ed4eeb96e4','library':str(lib),'library_sha256':hashlib.sha256(lib.read_bytes()).hexdigest(),'nccl_get_version':version.value,'toolchain':root.joinpath('toolchain.txt').read_text(),'native_cpu_test_passed':True,'native_cpu_library_load_passed':True,'target_image_loader_qualified':False,'hardware_collective_qualified':False}
root.joinpath('build-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n'); print(json.dumps(receipt,indent=2))
NCCL_RECEIPT
date -u +'%Y-%m-%dT%H:%M:%SZ Build completed: local lib aliases, licenses and build-receipt.json created. No global installation. A rebuilt binary requires fresh target-image and four-rank collective qualification.' >> "$NCCL_TASK_DIR/changes.txt"
printf 'Library: %s\n' "$NCCL_TASK_DIR/lib/libnccl.so.2.30.7"

if [[ -n "$NCCL_PUBLISH_DIR" ]]; then
  mkdir -p "$NCCL_PUBLISH_DIR"
  install -m 0755 "$NCCL_TASK_DIR/lib/libnccl.so.2.30.7" "$NCCL_PUBLISH_DIR/libnccl.so.2.30.7"
  ln -s libnccl.so.2.30.7 "$NCCL_PUBLISH_DIR/libnccl.so.2"
  ln -s libnccl.so.2 "$NCCL_PUBLISH_DIR/libnccl.so"
  install -m 0644 "$NCCL_TASK_DIR/lib/LICENSE.txt" "$NCCL_TASK_DIR/lib/ThirdPartyNotices.txt" "$NCCL_TASK_DIR/build-receipt.json" "$NCCL_PUBLISH_DIR/"
  mkdir "$NCCL_PUBLISH_DIR/sparkring-notices"
  install -m 0644 "$NCCL_RECIPE_DIR/third_party/sparkring/LICENSE" "$NCCL_RECIPE_DIR/third_party/sparkring/NOTICE" "$NCCL_RECIPE_DIR/third_party/sparkring/THIRD_PARTY_NOTICES.md" "$NCCL_PUBLISH_DIR/sparkring-notices/"
  date -u +'%Y-%m-%dT%H:%M:%SZ Published library aliases, build receipt and upstream notices to the explicitly requested NEW directory. No existing library was overwritten.' >> "$NCCL_TASK_DIR/changes.txt"
  cp "$NCCL_TASK_DIR/changes.txt" "$NCCL_PUBLISH_DIR/build-changes.txt"
  printf 'Published library: %s\n' "$NCCL_PUBLISH_DIR/libnccl.so.2"
fi
