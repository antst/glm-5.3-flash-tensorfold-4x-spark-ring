# Phase one: serving and benchmark baseline

This release records the first working four-DGX-Spark GLM-5.3-Flash EXL3
deployment on a switchless RoCE ring, using the pinned Mia TensorFold image.
It preserves that engine version so subsequent upgrades can be compared with
an identified baseline.

## Delivered

- Four tensor-parallel ranks, one per Spark, using direct ring neighbors.
- Four HCA functions per host, exposing both PCIe paths to each neighbor.
- An isolated, source-pinned NCCL 2.30.7 build with the cumulative SparkRing
  dual-PCI-domain patch; no manual OS routes, forwarding changes or NIC hairpins.
- An OpenAI-compatible endpoint and a standard-library terminal client.
- A selected profile of 8 decode lanes, a 1,048,576-token per-request limit,
  a 64-row shared verify window and 32 kept prompt states. The observed shared
  pool was 4,569,088 tokens, replicated per rank.
- Portable topology configuration, model download/build helpers, mandatory
  fresh collective qualification, scoped startup/stop and fabric rollback.
- Published results comparing Mia's two/three-Spark configurations through
  SparkDash and Alex's four-Spark configuration through pinned RigMark.
- Dependency pins, source/binary hashes, licences, change documentation and
  a release file manifest. Local configuration and operating receipts stay private.

## Evidence and boundaries

The original serving deployment ran on four real DGX Sparks. Its collective
qualification exercised changing data, four dtypes, in-place/out-of-place
buffers and CUDA graph replay. The measured service used every configured HCA,
with zero new transport errors during the RigMark before/after counter window.

The full pinned RigMark run issued 54 inference requests, exactly matching the
server request-count increase. All 15 decode completion gates passed. Code and
prose gates establish nonempty output completed below the output ceiling;
they do not compile code or establish equivalent model quality. Structured
output passed the exact 50-object index/square check.

The native1M request limit passed admission with a 26,333-token prompt and a
32,768-token reply allowance; RigMark additionally processed raw prompts through
65,536 tokens. Neither a full 1M input nor eight simultaneous full 1M requests
was tested. Eight full windows cannot reside in the observed pool together.

The portable wrappers were prepared after the working deployment. Their source,
configuration guards and command plans were checked offline; they have not been
executed as a fresh installation on that serving cluster. Historical public
results cannot qualify another installation. The startup helper requires a
fresh receipt bound to that cluster's actual library, image, topology and HCAs.

The dual-path fabric reached 196.08 Gb/s one-way host-memory RDMA payload per
cable in separate tests, one edge at a time. This establishes neither sustained
200G inference traffic nor simultaneous saturation of the entire ring.

## Interpreting the baseline

See [benchmark comparisons](benchmark-comparison.md) and
[method](benchmark-method.md). SparkDash aggregate decode and RigMark
aggregate end-to-end throughput use different timing windows and workloads.
They cannot be merged into one performance table.

The 64K cold-prefill result was 7.54% lower throughput than Alex's median,
adding 2.72 seconds to first-token latency. Our completed code/prose decode
results were faster. Decode figures exclude first-token wait; end-to-end
figures include it. Cache replay is a separate prefix-reuse measurement.

The cause of the prefill difference remains unproven. The retained engine
source shows different prefill kernels, a default 2,048-row prompt capacity
and a four-rank whole-partial all-gather path with less fine-grained overlap
than its two-rank path. These are profiling candidates. Healthy links and
aggregate counters do not isolate collective wait time, and no clock/thermal
telemetry was collected during the compared runs.

Engine upgrades, prefill tuning, full-window stress tests and RigMark C8 are
outside this phase's measured scope. Each future result needs its own settings,
receipt and current hardware qualification where the transport changes.

## Changes and recovery

[System changes and rollback](system-changes.md) describes every category of
deployment change and the effects of each portable command. Exact site-specific
chronological records and root-only network backups remain in the operator's
workspace and on the hosts. They are excluded from this public source release.

Public documentation and source preparation did not restart the serving ranks
or change the cluster's OS/network configuration.
