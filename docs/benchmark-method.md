# Benchmark method

The full pinned RigMark suite completed on October 9, 2026: all 15 decode requests passed their completion gates, and the receipt contains all 54 expected inference requests. The wrapper observed a server request-count increase of 54 and detected no additional requests. The public receipts are [our unchanged RigMark result](../results/rigmark-result.json) and [Alex's original RigMark result](../results/alex-rigmark-result.json). The pinned strict comparison accepts their client protocol/settings match; this document explains the measurement method and its limits without asserting a performance conclusion.

Our raw receipt reports the runner's Git identity as `unknown`. Git rejected the NFS checkout's ownership: the original wrapper's per-command `safe.directory` exception verified its clean source pin but did not reach upstream RigMark's child Git calls. The separate [source proof](../results/rigmark-source-proof.json) binds the unedited receipt, recorded preflight audit, preserved [executed wrapper](../results/rigmark-runner-executed.py), and all 27 tracked source-file hashes. It distinguishes the recorded preflight check from the independently verified clean source inventory after the run. The future helper supplies the exception only in the child environment; no global Git configuration or upstream source was changed, and this completed benchmark was not repeated.

## Metrics and workload scope

SparkDash and RigMark answer different questions. Our SparkDash reproduction uses the prompts and client timing formula from [sparkDash revision `fab8dadff951d1c8cc1450a657805d8e3ea5c9f6`](https://github.com/MiaAI-Lab/sparkDash/tree/fab8dadff951d1c8cc1450a657805d8e3ea5c9f6): 400 output tokens, temperature 0, top-p 1, thinking disabled, and a 32-token warmup. Its prose, counting, and Python workloads differ from RigMark's engineering memo, exact JSON array, and Go implementation. The original Mia table does not provide a raw run receipt establishing its output cap; 400 is the verified harness default, not a verified original run setting.

Let `n` be the server-reported completion token count, and `wall` the complete
concurrent round duration. For RigMark, `first` and `last` are the client
timestamps of the first and last nonempty content or reasoning events.
Our SparkDash harness uses visible content events and rejects any reasoning
output in its thinking-disabled profile.

| Metric | Definition | Includes |
| --- | --- | --- |
| Per-stream decode, both harnesses | `(n - 1) / (last - first)` | Streamed generation after the first measurable token |
| SparkDash aggregate decode | `sum(n_i - 1) / (max(last_i) - min(first_i))` | Combined generation interval |
| RigMark aggregate concurrency | `sum(n_i) / wall` | Prefill, queueing, scheduling, generation, and request completion |
| TTFT | First measured event minus request start, using each harness's event rule | Client/server overhead and prefill |

Never compare SparkDash aggregate decode directly with RigMark aggregate concurrency. RigMark's TTFT may be the first reasoning token rather than the first visible answer. Count tokens using final usage, not SSE events: speculative decoding can emit several tokens in one event. Both decode estimates depend on stream pacing and event batching. TensorFold's server `decode_s` is a separate diagnostic clock and must not replace the pinned client formula. Report distributions and the specified summaries, rather than choosing the fastest sample.

## Pinned Alex comparison

The primary reference is Alex's September 12 receipt, `data/rigmark/glm53-nccl-v0.0.1-tp4-20260912.json`, from the [four-Spark switchless recipe](https://github.com/alexellis/glm-5.3-flash-4x-dgx-spark-switchless). Its SHA256 is `7f9c9cc1f617f5e183950127f1e3b716edec4dc23c731876366fde64dea89b46`. Do not substitute the earlier Red Hat checkpoint receipt.

Use [RigMark commit `d8353e93b274e8d880ab14a5df2a55c87d7bee16`](https://github.com/alexellis/rigmark/tree/d8353e93b274e8d880ab14a5df2a55c87d7bee16), protocol and prompt version `1.0.0`. The pinned [prompt corpus](https://github.com/alexellis/rigmark/blob/d8353e93b274e8d880ab14a5df2a55c87d7bee16/prompts.json) SHA256 is `0c3ac4015813af6a1b967659bd2289990fde895a5e43ae065b894d1e1f909ecb`. Preserve the seed `20260912` and comparison ID `2026-09-12-nccl-patch-ab-v1`: deterministic per-workload/per-run nonces depend on the comparison ID, so changing it changes the prompts.

The exact additional chat request body is:

```json
{"chat_template_kwargs":{"reasoning_effort":"low"}}
```

This enables low-effort reasoning on our endpoint. Do not add thinking-off, `ignore_eos`, `min_tokens`, or an explicit draft override to this chat profile. The pinned runner defaults to temperature 0 and top-p 1; its default timeout is 1800 seconds. Drafting remains the engine's configured default. See the pinned [runner](https://github.com/alexellis/rigmark/blob/d8353e93b274e8d880ab14a5df2a55c87d7bee16/bench.py) and [protocol](https://github.com/alexellis/rigmark/blob/d8353e93b274e8d880ab14a5df2a55c87d7bee16/PROTOCOL.md).

Set `API_BASE_URL` to the serving origin, such as `http://HEAD_IP:8888`
(without `/v1`), and `RECIPE_ROOT` to the absolute public recipe directory.
Prepare a metadata JSON from `metadata.example.json`, recording the actual
appliance profile, and save it as `local/metadata.json`. Run against an idle
endpoint. The maintained wrapper records the exact command, source identity
and request-count audit; use a new output directory for every run:

```sh
mkdir -p "$RECIPE_ROOT/state"
git clone https://github.com/alexellis/rigmark.git "$RECIPE_ROOT/state/rigmark-source"
git -c safe.directory="$RECIPE_ROOT/state/rigmark-source" -C "$RECIPE_ROOT/state/rigmark-source" checkout --detach d8353e93b274e8d880ab14a5df2a55c87d7bee16
python3 "$RECIPE_ROOT/scripts/run-rigmark.py" \
  --source "$RECIPE_ROOT/state/rigmark-source" \
  --url "$API_BASE_URL" \
  --metadata "$RECIPE_ROOT/local/metadata.json" \
  --output "$RECIPE_ROOT/state/rigmark-run-01"
PYTHONDONTWRITEBYTECODE=1 python3 "$RECIPE_ROOT/state/rigmark-source/rigmark" compare --card \
  "$RECIPE_ROOT/results/alex-rigmark-result.json" \
  "$RECIPE_ROOT/state/rigmark-run-01/result.json"
```

`state/` and `local/` are ignored by Git. Keep command audits and unreviewed
metadata private. Existing NFS checkout owners may require a narrowly scoped
Git trust exception when cloning/checking out sources; the wrapper supplies
one only for its child process. The original published receipts are retained.

For reference, the equivalent unmodified upstream command below uses
`RIGMARK_METADATA` for the prepared metadata file and `RIGMARK_OUTPUT` for an
absolute, new output directory outside the source checkout:

```sh
cd "$RECIPE_ROOT/state/rigmark-source"
PYTHONDONTWRITEBYTECODE=1 ./rigmark run \
  --base-url "$API_BASE_URL" \
  --model GLM-5.3-Flash-EXL3 \
  --label glm53-exl3-tensorfold-tp4-low \
  --comparison-id 2026-09-12-nccl-patch-ab-v1 \
  --metadata "$RIGMARK_METADATA" \
  --seed 20260912 --runs 5 --decode-tokens 4096 \
  --prefill-depths 8192,32768,65536 --prefill-runs 3 \
  --concurrency 1,2,4 --concurrency-runs 3 \
  --concurrency-tokens 256 --concurrency-workload code \
  --extra-body '{"chat_template_kwargs":{"reasoning_effort":"low"}}' \
  --output "$RIGMARK_OUTPUT/result.json"
PYTHONDONTWRITEBYTECODE=1 ./rigmark compare --card \
  "$RECIPE_ROOT/results/alex-rigmark-result.json" \
  "$RIGMARK_OUTPUT/result.json"
```

The executed run writes its original receipt into the dated measurement directory; publication copies that receipt without changing measurement fields. This suite issues 54 inference requests: 15 decode requests, 18 cold/replay prefill requests, and 21 requests across concurrency rounds. Tokenizer calls are additional operations. TensorFold supports the required tokenization and raw-token completion APIs, so this run uses the unmodified runner without an adapter or omitted prefill stage.

The pinned comparator checks the protocol, prompt hash, comparison ID, output caps, run counts, sampling/body settings, prefill depths, and concurrency configuration. It does not independently enforce the Git revision or seed, or equal model/engine metadata. Verify those separately; do not use `--allow-mismatch` as evidence of a matched run. Keep the checkout clean and retain the exact command, original JSON, corpus hash, runner revision, and comparison output.

## Completion gates and cache limits

All 15 decode requests must pass their workload gates before their throughput is presented as completed work. Code and prose require nonempty visible output and a finish reason other than `length`; these are completion gates, not compilation or semantic correctness tests. Structured output must parse as exactly 50 JSON objects containing only integer `index` and `square`, with indices 1 through 50 and each square correct. Keep failed outputs and timings visible as diagnostics. The 256-token concurrency workload can intentionally truncate and has no equivalent decode completion gate.

For each prefill depth, RigMark constructs an exact-length token-ID prompt using a unique cold nonce and then immediately replays those same IDs. It sends raw completions with eight output tokens, `add_special_tokens=false`, and `ignore_eos=true`; chat `extra_body` and the chat seed are not applied to that stage. Effective prefill throughput is prompt tokens divided by TTFT, including fixed request overhead. “Cold” means a unique prompt prefix, not a newly started process or flushed GPU/cache. Warm replay measures that engine's prefix-cache behavior. It does not establish uncached bulk compute throughput or document-understanding quality.

The 8K/32K/64K suite fits our configured 1,048,576-token context. It does not test a full one-million-token prompt or eight simultaneous full-window requests. Chat templates and tokenization can also change the actual prompt token counts between engines; preserve reported usage rather than assuming equal rendered tokens.

## Public metadata and interpretation

Follow the pinned [metadata guidance](https://github.com/alexellis/rigmark/blob/d8353e93b274e8d880ab14a5df2a55c87d7bee16/METADATA.md). Publish exact model and drafter revisions, quantization and activation/KV formats, image digest, engine revision, speculative policy, TP size, scheduler parallelism, verify window, context/cache limits, transport/library hash, link topology, stream pacing, clock/power conditions, and competing-traffic observations. Record unknowns explicitly. Preserve the original reference receipt and disclose any separately redacted private metadata; exclude credentials, private endpoint addresses, and host identifiers from our public metadata.

Both configurations use four DGX Sparks and the GLM-5.3-Flash family, but this is a comparison of complete recipes. Alex's September 12 receipt uses LibertAIDAI NVFP4 W4A16, vLLM/DFlash2 static k=7, FP8 E4M3 KV, a 262,144-token context, and a six-sequence scheduler. Our profile uses Mia's EXL3 4-bpw checkpoint with Q4 dense kernels, TensorFold, adaptive DFlash2/copy drafting, FP8 latent KV, a 1,048,576-token context, and eight parallel streams with verify window 64. Model, drafter, kernels, cache, scheduler, and transport differ. Matched client prompts and settings do not isolate the effect of TensorFold, quantization, or the ring patch, and the workload gates do not establish equivalent model quality.

## Separate concurrency-eight extension

Keep concurrency `1,2,4` in the primary Alex comparison. Our eight-stream scheduler warrants a separate explicitly labeled C8 run with its own comparison ID, receipt, exact command, and metadata. It may use `--concurrency 1,2,4,8` with the same pinned runner and other stated settings, but Alex's reference contains no C8 result and will not be a strictly matched counterpart. No C8 RigMark measurement or conclusion is included in this document; existing SparkDash C8 measurements remain a different workload and metric.
