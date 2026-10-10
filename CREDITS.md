# Sources and licences

This repository contains orchestration and documentation for a tested four-Spark
TensorFold deployment. The orchestration is MIT licensed except files carrying an Apache-2.0 SPDX
header (the native builder, fabric/qualification helpers and SparkDash client); third-party material
retains its original licence. It contains no model weights, container layers,
private keys, host trust files, private network backups, or copied TensorFold
engine sources.

Retained third-party notice files refer to paths in their original upstream
repositories; those reference paths are not bundled runtime dependencies here.

- **MiaAI-Lab** supplies the GLM-5.3-Flash EXL3 recipe, 96-patch TensorFold v0.6
  image and optimisations. Reference recipe commit:
  `7d42f905388a8d7fa91e2dc9458e0a4a68001540`.
  [Source](https://github.com/MiaAI-Lab/GLM-5.3-Flash-EXL3-2x-DGX-Sparks-TensorFold).
- **TensorFold** supplies the inference engine. The pinned Mia image already
  supports TP4; this repository does not patch or distribute the engine.
  [Source](https://github.com/ashhart/TensorFold).
- **NVIDIA NCCL 2.30.7**: pristine commit
  `73cf112295c33aee2b895f329f592f2a9b4b0f97`.
  [Source](https://github.com/NVIDIA/nccl/tree/73cf112295c33aee2b895f329f592f2a9b4b0f97).
  Its licence and third-party notices are retained under `third_party/nccl/`.
- **FujitsuPolycom/SparkRing contributors** supply the cumulative switchless,
  IPv4-GID and PCI-domain patch at commit
  `6d27dd10a9f97da21278ae504ea6235f8634f303`. The patch is applied alone to
  pristine NCCL 2.30.7. Apache-2.0 licence/notices are under
  `third_party/sparkring/`.
  [Source](https://github.com/FujitsuPolycom/sparkring/tree/6d27dd10a9f97da21278ae504ea6235f8634f303).
  The custom SparkRing/SIRCL transport is not loaded.
- **Alex Ellis / OpenFaaS Ltd** supplies the public four-Spark vLLM recipe,
  switchless NCCL background and RigMark benchmark. Alex recipe reference:
  `e2d0839a92f118a0a874abcfc1fe8012ee69978e`.
  [Recipe](https://github.com/alexellis/glm-5.3-flash-4x-dgx-spark-switchless).
- **RigMark** uses clean commit
  `d8353e93b274e8d880ab14a5df2a55c87d7bee16`, protocol/prompts 1.0.0.
  [Source](https://github.com/alexellis/rigmark/tree/d8353e93b274e8d880ab14a5df2a55c87d7bee16).
  Benchmark baseline receipts and corresponding MIT licence are retained under
  `results/` and `third_party/rigmark/`. The benchmark source is fetched separately.
- **MiaAI-Lab / SparkDash** supplies the prose/counting/Python benchmark prompts
  and client decode timing definitions at commit
  `fab8dadff951d1c8cc1450a657805d8e3ea5c9f6`.
  [Source](https://github.com/MiaAI-Lab/sparkDash/tree/fab8dadff951d1c8cc1450a657805d8e3ea5c9f6).
  The portable SparkDash client adapts the harness used for our measurements;
  it is not a copy of the dashboard. The source is Apache-2.0 from v2 onward,
  with the MIT licence retained for its legacy code. Both licence texts and
  NOTICE are retained under `third_party/sparkdash/`. Mia's published
  measurements are attributed in [the comparison](docs/benchmark-comparison.md).
- **Mia-AiLab** provides the EXL3 checkpoint; **incoai** provides DFlash2.
  Download immutable model revisions listed in `provenance.json`; their upstream
  terms apply. Weights are not redistributed here. DFlash2 is CC BY-NC-ND 4.0
  according to the pinned Mia NOTICE; commercial licensing is a separate upstream
  dependency concern. This repository's MIT licence does not relicense it.

The dual-PCI-domain NCCL patch SHA256 is
`8e2b8715d62d2b07a74caca3778da0eff2e7b54caf8a184bb728f179a5d1eba4`.
Our native build's library SHA256 was
`aca99264fe2a474e0f7aa964a3c3ef65254ad8f8d375de76631a7eb973c15041`.
A rebuild may differ byte-for-byte and must receive fresh four-rank qualification.
