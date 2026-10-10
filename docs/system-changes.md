# System changes and rollback

The deployment and benchmark evidence were collected before preparing this
portable repository. Public packaging and RigMark did not restart ranks or
change OS configuration. The original image/model/drafter/NCCL pins remain in
[provenance.json](../provenance.json). This document describes both the changes
made for that working deployment and the effects of the portable commands.

## Working deployment changes

- Identified four physical ring edges and sixteen HCA functions. Created four
  dedicated MAC-bound NetworkManager/Netplan fabric profiles per host: static
  IPv4 /24, MTU9000, no gateway/DNS/manual routes, never-default. The original
  deployment disabled IPv6 only on these dedicated fabric profiles; the portable
  fresh-setup helper instead preserves IPv6 state with NM `ignore` and checks it.
- Saved original network configurations and interface/policy state in root-only
  host backups. Kept management/Wi-Fi default routes, forwarding/rp_filter values,
  firewall/NAT rules and original configuration files unchanged. Existing IPv4
  forwarding was already enabled; this recipe neither enables nor relies on it.
- Pulled the immutable Mia runtime image. Downloaded immutable model and drafter
  snapshots into task-owned local directories on all four hosts. Created local
  HF/JIT/Torch/Triton caches and temporary build/validation directories. No host
  packages, driver settings, global NCCL libraries, clocks or power limits changed.
- Built an isolated patched NCCL library for ARM64/SM121 and retained source
  pins, licences, logs and hash receipts. Selected it by both preload and the
  TensorFold library setting; actual process mappings were checked.
- Created one ownership-labelled model container per Spark using host networking,
  host IPC, NVIDIA GPU access, `/dev/infiniband`, IPC_LOCK and unlimited memlock.
  Model/library/source mounts are read-only; local cache mounts are writable.
  No automatic restart service or published-port NAT rule was installed.
- Restarted only those owned containers while moving from the initial 16K/P4
  profile to native1M/P4 and then the tested native1M/P8 profile. Earlier logs and
  validation receipts were retained. Final defaults are 8 lanes, a 64-row verify
  window, 32 kept states, 32K default reply and 24 GiB extra KV with 20 GiB reserve.
- Ran CPU/build checks, real four-rank collectives, model correctness/admission
  tests, host-memory RDMA link capacity and HTTP benchmarks. These write task
  logs/receipts and update runtime prompt caches; they do not change OS routes.
- SSH uses the existing keys. Earlier OpenSSH automatic host-key updates added
  four entries to the operator's trust file; subsequent commands disable updates.
  A per-command `IdentityAgent=none` bypassed a stalled agent, without changing
  keys, agent state or SSH configuration. The operator separately provisioned
  peer SSH keys; no private keys were copied by the recipe.
- Prepared this portable source/configuration/licence/result package. Created a
  clean pinned RigMark source checkout and benchmark outputs on shared storage.
  Ran the original RigMark client against the existing service. No engine upgrade,
  host package installation or live network change was made during this stage.
- Prepared the phase-one public release, including attributed Mia/Alex tables,
  a portable SparkDash client, offline checks and source manifests. Published
  only the reviewed source/configuration/notice/result tree to its dedicated
  public GitHub repository. Kept site-specific chronological records, operational
  receipts, keys, weights, binaries and backups outside that repository.

The complete site-specific chronological ledger, private runtime inventories,
real addresses/MACs and host trust material remain in the operator's task
workspace and are deliberately excluded from public Git. They are not necessary
to reproduce the source or the published benchmark protocol.

## Portable command effects

| Command | Effects |
| --- | --- |
| `cluster.py plan`, `fabric.py plan`, download/build `--plan` | Print an offline plan; no SSH/GPU/filesystem mutations |
| `fabric.py check/verify` | Read-only host inspection; write ignored operator audit receipts |
| `fabric.py apply` | Create root-only backups and new dedicated NM profiles; activate fabric IPv4/MTU; verify original policy/files |
| `fabric.py rollback` | Delete only owned profiles and restore recorded prior interface/profile state; retain private backups/audit |
| `build-nccl.sh` | Fetch/patch/compile in a new dedicated local task directory; optional publication to a new shared library directory |
| `download-models.py` | Download pinned snapshots and write receipts/HF cache to local model storage |
| `qualify.py` | Run temporary owned GPU collective jobs; write ignored qualification receipts/logs, remove owned temporary jobs |
| `cluster.py start` | Create local cache directory and four owned serving containers; write ignored events/readiness receipts |
| `cluster.py stop` | Archive logs and stop/remove only containers with this recipe's ownership label |
| `chat.py` | Send inference requests; memory-only client history |
| `run-rigmark.py` | Send inference/tokenizer requests; write benchmark JSON/log/card and aggregate request-count audit |
| `run-sparkdash.py` | Send warmup and fixed-length inference requests; write per-wave outputs, timing summaries and request-count audit in a new directory |

Fabric changes are not an atomic transaction across four machines. If an apply
fails, inspect all per-host audit records and the retained private backup before
retrying or rolling back. Helpers refuse conflicting profile/container ownership
and preserve unknown user changes rather than overwriting them.

## Rollback

Stop this recipe's model before network rollback:

```bash
python3 scripts/cluster.py stop --config local/config.json
python3 scripts/fabric.py rollback --config local/config.json
```

Private network baselines remain at
`/var/backups/spark-ring-public/<topology-sha256>/<node>/state.json` with their
original configuration copies and command audit. Rollback restores prior active
profiles, MTU, UP/DOWN and IPv6 address-generation mode and checks policy/routes.
It refuses profiles changed by someone else. Do not publish or delete private
backups until their value for recovery has been assessed.

Once no container references the artifacts, optional cleanup can remove the
exact task-owned build, validation, cache, model and shared-library directories
created during that installation. Preserve needed logs/receipts first. The
recipe does not perform broad deletion or modify global image/library state.

## Packaging qualification scope

The original engine/profile/transport and recorded inference were exercised on
real hardware. Portable configuration/SSH wrappers and fresh fabric setup were
adapted afterwards and checked offline. They have not been rerun as a fresh
installation on the serving cluster; doing so would unnecessarily displace the
working service. Every new cluster must pass its own current image/library/HCA
collective qualification before model startup.
