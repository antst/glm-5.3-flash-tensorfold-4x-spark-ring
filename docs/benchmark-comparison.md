# Benchmark comparisons

At eight concurrent requests, our current four-Spark TensorFold profile measured **215.7 tok/s for prose** and **290.7 tok/s for Python code**, as three-wave median aggregate decode rates. Relative to Mia's published eight-lane results, these are **1.65x / 1.74x** the two-Spark values and **1.30x / 1.37x** the three-Spark values. These compare complete configurations; they do not isolate the benefit of adding a Spark or changing topology.

The [Mia comparison data](../results/mia-comparison.json) contains every available row, all retained wave values and ranges, input/cache counts, provenance digests, and explicit nulls for missing measurements. The earlier [public SparkDash summary](../results/sparkdash-summary.json) and [recipe provenance](../provenance.json) provide additional context. Historical 16K-window measurements are excluded from the tables below.

## Definitions and sources

**TP2 / TP3 / TP4** count Sparks and tensor-parallel ranks. **P4 / P8** identify the server's four- or eight-lane decode limit. **C1 / C2 / C4 / C8** count concurrent client requests. Eight clients on P4 can queue; they do not become eight active decode lanes.

Mia's published numbers come from README revision `7d42f905388a8d7fa91e2dc9458e0a4a68001540`:

- [Two Sparks, P4](https://github.com/MiaAI-Lab/GLM-5.3-Flash-EXL3-2x-DGX-Sparks-TensorFold/blob/7d42f905388a8d7fa91e2dc9458e0a4a68001540/README.md#L33-L44).
- [Two Sparks, P8](https://github.com/MiaAI-Lab/GLM-5.3-Flash-EXL3-2x-DGX-Sparks-TensorFold/blob/7d42f905388a8d7fa91e2dc9458e0a4a68001540/README.md#L49-L61).
- [Three Sparks, P4](https://github.com/MiaAI-Lab/GLM-5.3-Flash-EXL3-2x-DGX-Sparks-TensorFold/blob/7d42f905388a8d7fa91e2dc9458e0a4a68001540/README.md#L410-L429).
- [Three Sparks, P8](https://github.com/MiaAI-Lab/GLM-5.3-Flash-EXL3-2x-DGX-Sparks-TensorFold/blob/7d42f905388a8d7fa91e2dc9458e0a4a68001540/README.md#L432-L445).

The protocol reconstruction uses [SparkDash revision `fab8dadff951d1c8cc1450a657805d8e3ea5c9f6`](https://github.com/MiaAI-Lab/sparkDash/tree/fab8dadff951d1c8cc1450a657805d8e3ea5c9f6), including its [prompts](https://github.com/MiaAI-Lab/sparkDash/blob/fab8dadff951d1c8cc1450a657805d8e3ea5c9f6/src/shared/llmPrompts.js), [decode harness](https://github.com/MiaAI-Lab/sparkDash/blob/fab8dadff951d1c8cc1450a657805d8e3ea5c9f6/server/collectors/DecodeBench.js), and [stream timing](https://github.com/MiaAI-Lab/sparkDash/blob/fab8dadff951d1c8cc1450a657805d8e3ea5c9f6/server/collectors/LlmStreaming.js). It establishes available source prompts and formulas, not the exact revision or flags used for Mia's original runs. No local TP2 or TP3 control was run.

All table values are **aggregate decode tokens/s**. Mia cells are published point values with unavailable repetition statistics. Our cells are medians of three retained waves, except the explicitly marked single probe. Ratios use unrounded values. **—** means the result was not published or not measured; it never means zero.

## Four-lane profiles, P4

These are historical four-lane results, including our native 1,048,576-token window. Our P4 profile used a 32-row verify window, eight kept prompt states, and a 4,096-token default reply allowance; every measured request explicitly asked for 400 output tokens.

| Workload | Clients | Mia TP2 | Mia TP3 | Our TP4 | Our / TP2 | Our / TP3 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Prose | C1 | 60.4 | 77.6 | 88.9 | 1.47x | 1.15x |
| Prose | C2 | 79.2 | 95.7 | 123.4 | 1.56x | 1.29x |
| Prose | C4 | 108.8 | 146.2 | 158.8 | 1.46x | 1.09x |
| Prose | C8 | — | — | 157.3 | — | — |
| Python code | C1 | — | 104.3 | — | — | — |
| Python code | C2 | — | 139.0 | — | — | — |
| Python code | C4 | — | 169.6 | — | — | — |
| Python code | C8 | — | — | — | — | — |
| Counting | C1 | 114.7 | — | 169.8 | 1.48x | — |
| Counting | C2 | 147.6 | — | 245.9 | 1.67x | — |
| Counting | C4 | 227.9 | — | 375.3 | 1.65x | — |
| Counting | C8 | — | — | 358.1 | — | — |

Our C8 rows here send eight clients through four lanes and include queueing between groups. They are not substituted into the P8 table. Python code was not measured on our P4 profile.

## Eight-lane profiles, P8

This is the current server lane configuration: TP4, native 1,048,576-token context, 64 shared verify rows, 32 kept prompt states, 32,768-token default reply allowance, 24 GiB extra KV per rank, and 20 GiB memory reserve. The observed shared pool was 4,569,088 tokens, replicated per rank. Mia's P8 tables use 64-row windows; its P4 and P8 measurements came from different boots.

| Workload | Clients | Mia TP2 | Mia TP3 | Our TP4 | Our / TP2 | Our / TP3 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Prose | C1 | — | 65.7 | 88.7† | — | 1.35x† |
| Prose | C2 | — | 93.8 | — | — | — |
| Prose | C4 | 103.2 | 121.8 | — | — | — |
| Prose | C8 | 130.8 | 166.0 | 215.7 | 1.65x | 1.30x |
| Python code | C1 | — | 100.4 | — | — | — |
| Python code | C2 | — | 129.5 | — | — | — |
| Python code | C4 | 126.7 | 165.3 | — | — | — |
| Python code | C8 | 167.0 | 211.5 | 290.7 | 1.74x | 1.37x |
| Counting | C1 | — | — | — | — | — |
| Counting | C2 | — | — | — | — | — |
| Counting | C4 | — | — | — | — | — |
| Counting | C8 | — | — | 537.5 | — | — |

† Our P8 C1 prose value is **one probe**, not a three-wave median. Its 88.7 tok/s is close to the earlier P4 C1 median of 88.9, but this does not establish statistical equivalence. Mia says one or two TP2 P8 requests are as fast as P4 without publishing numerical C1/C2 rows for that boot; those cells remain missing. Our P8 C2/C4 and code C1/C2/C4 also remain unmeasured.

Current C8 ranges across all three waves were 214.50–216.08 prose, 286.95–291.23 Python code, and 536.73–538.62 counting tok/s. The corresponding aggregate **end-to-end** medians were 214.3, 287.5, and 514.2 tok/s. These are separate metrics and are not the decode values in the tables.

## What the Mia comparison establishes

Our reconstruction used temperature 0, top-p 1, thinking disabled, configured drafting, and exactly 400 reported completion tokens per measured stream. It used the SparkDash hash-map explanation and counting prompts; concurrent requests append distinct stream suffixes. Python code uses eight distinct function/class tasks without that suffix. “Structured” in Mia/SparkDash means counting, not RigMark's JSON workload. The main matrix contains 162 measured streams; the separate P8 single probe adds one. All retained waves are included in their profile's median.

Aggregate decode is `sum(completion_tokens - 1) / (latest last content - earliest first content)`. Timestamps observe visible content; unexpected reasoning was rejected. Usage counts tokens, not SSE events. Speculative bursts and smoothing influence event timing. Our 32-token warmups force that length, whereas upstream warmups normally request 32 without force-fill fields; our stream smoothing was off, versus the pinned Mia default of on/400 ms. Normal prefix caching was preserved. Actual input counts were 27–40 tokens for counting/prose and 57–66 for Python code; some code responses reported 64 cached prompt tokens. A configured 1M window does not make these short decode prompts 1M long.

Mia does not preserve raw receipts proving the original input lengths, output cap, sampling/body settings, repeat counts, cache state, or exact harness revision. **400 is the verified harness default, not a verified original Mia output cap.** Its main TP2 P4 table explicitly states a 2,200 MHz GPU cap. The separate TP2 P8 boot and TP3 per-boot caps are not independently preserved. Our existing clocks/power settings were unchanged; an earlier loaded sample showed 2,476–2,541 MHz, with no synchronized GPU telemetry during the matrix. The ratios therefore compare configurations with potentially different clocks, transport, pool, scheduler, and exact runtime revisions.

Fixed-length counting checks establish correct sequential prefixes up to the reply budget, not completion of all 200 numbers. Python code throughput has no compilation or complete-solution quality gate. Our current context admission was checked with 26,333 input tokens plus a 32,768-token reply allowance and a short actual reply. A full 1M input and eight full-window requests were not tested; eight 1M contexts cannot all reside in the shared 4,569,088-token pool.

## Alex's matched RigMark comparison

The separate [RigMark comparison](../results/rigmark-comparison.json) uses the exact pinned client protocol, prompt corpus, seed, comparison ID, and low-effort reasoning body from Alex's September 12 run. Its [reference receipt](../results/alex-rigmark-result.json) and [our raw receipt](../results/rigmark-result.json) are unedited. See the [benchmark method](benchmark-method.md), [source proof](../results/rigmark-source-proof.json), and [runtime proof](../results/rigmark-runtime-proof.json). All 15 decode completion gates passed and all 54 expected inference requests were accounted for.

| RigMark metric, median tok/s | Alex TP4 | Our TP4 | Our / Alex |
| --- | ---: | ---: | ---: |
| Completed code decode | 71.3 | 111.7 | 1.57x |
| Completed prose decode | 31.2 | 62.9 | 2.02x |
| Structured JSON | 109.2 | 157.1 | 1.44x |
| 64K cold prefill | 1,964.7 | 1,816.4 | 0.925x |
| 64K cached replay | 36,074.9 | 983,832.0 | 27.27x |
| C1 short-code aggregate end-to-end | 43.0 | 82.7 | 1.92x |
| C2 short-code aggregate end-to-end | 72.5 | 118.9 | 1.64x |
| C4 short-code aggregate end-to-end | 110.8 | 162.3 | 1.46x |

RigMark's concurrency headline includes complete request wall time, unlike SparkDash aggregate decode. Its stream decode timing includes content or reasoning. Completed code/prose gates require nonempty visible output without length truncation; they do not prove code compilation or semantic correctness. The structured gate validates the exact 50-object JSON array. No RigMark C8 measurement was run.

The cold 64K rate is **7.54% lower**, with median TTFT **36.080 versus 33.358 seconds**, adding **2.722 seconds** for that uncached input. Our three 64K runs were tightly grouped at 1,816.2–1,817.2 tok/s. Faster short-prompt decode therefore does not guarantee lower first-token latency for a long uncached prompt. Cold means a unique prefix within the running engine, not a restarted process or flushed GPU. Immediate cached replay measures near-complete prefix reuse; the large cached rate is not new-token compute throughput.

The cause of the cold-prefill gap is unresolved. All 16 HCA functions carried substantial payload with zero new errors, but accumulated counters and a one-cable-at-a-time host-memory capacity test do not isolate prefill's NCCL/PCIe wait time or rule out a collective bottleneck. Alex uses NVFP4/vLLM and static DFlash2; we use EXL3/TensorFold with adaptive/copy drafting. Quantization, kernels, chunking, transport, scheduler, cache, and clocks require controlled profiling before attributing the difference to any one component.
