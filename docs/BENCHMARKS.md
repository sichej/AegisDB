# AegisDB — Performance Benchmarks

> **Status:** Exploratory baseline, 2026-11-01. These results are not a production performance claim or a comparative database benchmark.

## Test environment

| Component | Configuration |
|---|---|
| Host | Apple M1, 8 CPU cores |
| Operating system | macOS Tahoe 26.6.2 |
| .NET | SDK 10.0.402; runtime 10.0.13 (Arm64 RyuJIT) |
| Microbenchmark harness | BenchmarkDotNet 0.15.8, Release |
| HTTP load generator | Grafana k6 2.3.0 |
| Deployment | Single local macOS host; loopback HTTP |
| Raft transport | In-process, in-memory transport; three logical Raft nodes on the same host |

All timings below are measurements of the specific workloads described, not general database latency or fault-tolerance guarantees. The underlying storage device, filesystem settings, power-management state, competing processes, and exact source revision were not captured in the supplied benchmark reports; reproduce and record these before treating results as regression gates.

## Summary

| Area | Scenario | Observed mean |
|---|---|---:|
| In-memory store | GET, 1 KiB value, 1,000 keys | 81.44 ns |
| WAL serialization | Serialize PUT record, 1 KiB value | 386.2 ns |
| WAL append | Append + individual durable flush, 1 KiB value | 3.905 ms |
| Raft log | Leader append with 1,000 existing entries | 6.527 ms |
| Raft replication | Append, replicate, commit/apply, three online nodes | 23.163 ms |
| Raft replication | Same, one follower unregistered | 11.777 ms |
| HTTP single-node | PUT, 25 virtual users | 82.76 ms |
| HTTP Raft | PUT, 25 virtual users | 337.77 ms |

**Important:** These benchmarks cover different code paths and test conditions; their latency figures are not additive. HTTP figures are particularly short-run and include setup traffic in the reported `http_req_duration` aggregate.

## 1. In-memory `KeyValueStore`

BenchmarkDotNet measured GET, PUT, DELETE-and-restore, and a mixed operation loop. The values below are **nanoseconds per benchmark operation**. They exclude WAL persistence, Raft consensus, HTTP, and disk I/O.

| Operation | Value size | 1,000 keys | 10,000 keys |
|---|---:|---:|---:|
| GET | 128 B | 28.33 ns | 30.07 ns |
| PUT | 128 B | 25.73 ns | 37.65 ns |
| DELETE + restore | 128 B | 42.67 ns | 54.31 ns |
| Mixed loop | 128 B | 28.94 ns | 34.13 ns |
| GET | 1 KiB | 81.44 ns | 88.88 ns |
| PUT | 1 KiB | 84.99 ns | 245.09 ns |
| DELETE + restore | 1 KiB | 104.16 ns | 267.52 ns |
| Mixed loop | 1 KiB | 86.59 ns | 161.8 ns |
| GET | 16 KiB | 955.19 ns | 979.29 ns |
| PUT | 16 KiB | 3.395 µs | 3.199 µs |
| DELETE + restore | 16 KiB | 3.392 µs | 3.199 µs |
| Mixed loop | 16 KiB | 1.664 µs | 1.590 µs |

The mixed workload's 100 iterations contain 80 GETs, 15 PUTs, and 5 DELETEs, followed by 5 restorative PUTs. It therefore must **not** be described as an exact 80/15/5 distribution of physical operations.

## 2. WAL serialization

`WalSerializationBenchmarks.Serialize` measures `WalSerializer.Serialize(record)` without file I/O.

| Value size | Mean | Managed allocation per call |
|---|---:|---:|
| 128 B | 117.6 ns | 752 B |
| 1 KiB | 386.2 ns | 5,752 B |
| 16 KiB | 3.665 µs | 82,552 B |

Allocation increases substantially with payload size, indicating serialization intermediates worth profiling. This measurement alone does not identify their source.

## 3. WAL durable append

`WalBatchBenchmarks.AppendBatch100` invokes `WriteAheadLog.AppendAsync` **100 times sequentially** per measured invocation; BenchmarkDotNet normalizes the result per append with `OperationsPerInvoke = 100`. Each append independently serializes, writes, and flushes. This is **not** a group-commit/batched WAL implementation.

| Value size | Mean per append | Standard deviation across normalized iterations | Managed allocation per append |
|---|---:|---:|---:|
| 128 B | 4.037 ms | 0.250 ms | 5.07 KiB |
| 1 KiB | 3.905 ms | 0.094 ms | 9.96 KiB |
| 16 KiB | 4.012 ms | 0.194 ms | 81.23 KiB |

Configuration: one launch, three warmup iterations, ten measurement iterations, 100 appends per iteration. The original single-append experiment observed higher variance and `MinIterationTime` warnings; it is retained only as exploratory data.

Because individual append flushes dominate these results, larger payloads did not produce a clear monotonic latency increase on this device. No claim is made about other disks or operating systems. The implied ~250 sequential appends/s is a **workload-specific estimate**, not whole-system throughput.

## 4. Raft log append and growth

`RaftLogAppendBenchmarks.Append` appends a PUT command with a 128 B value to an elected single-node leader configured with a prepopulated persistent Raft log. The state and log are restored to the specified size outside the measured region for every invocation.

| Existing log entries | Mean | Managed allocation per append |
|---|---:|---:|
| 0 | 4.227 ms | 7.38 KiB |
| 100 | 5.401 ms | 124.6 KiB |
| 1,000 | 6.527 ms | 1,212.69 KiB |
| 10,000 | 14.902 ms | 12,102.34 KiB |

The approximately 3.53× mean-latency increase from 0 to 10,000 entries and near-linear allocation growth with the number of existing entries are consistent with rewriting a complete JSON Raft log image per append. The total work to append an unbounded sequence of constant-sized entries may grow approximately quadratically in the absence of compaction.

The test has **one timed append per iteration** (10 measurement iterations) and BenchmarkDotNet reported `MinIterationTime` warnings. Treat the timings as exploratory; the strong allocation-growth signal motivates targeted profiling and an append-only/segmented-log design evaluation.

## 5. Raft replication and commit

A persistent leader and two persistent followers are created on the same host and communicate via `InMemoryRaftTransport`. Each measured invocation performs:

1. Leader append of a 1 KiB PUT record.
2. One `RaftReplicationManager.ReplicateAllOnceAsync()` attempt (waits for both peers to finish or fail).
3. `CommitAndApplyAsync()` on the leader.

| Configuration | Mean | Std. deviation | Managed allocation |
|---|---:|---:|---:|
| All three logical nodes online | 23.163 ms | 1.939 ms | 53.73 KiB |
| One follower unregistered | 11.777 ms | 1.918 ms | 38.41 KiB |

Each configuration used 20 measured iterations after three warmups. The faster degraded case has less persistence work because the unregistered follower fails quickly. **This does not establish better performance under a slow, stalled, partitioned, or non-cooperative peer.** The known all-peer wait can still delay a quorum-available leader (RAFT-002). Results do not measure network latency, process separation, or follower application of the committed entry.

## 6. HTTP workloads with k6

`benchmarks/http/load-test.js` exercises `PUT` and `GET` against:

- `/kv/{key}`: independent single-node engine with WAL.
- `/admin/kv/{key}`: locally hosted three-node Raft simulation, enabled in a Development environment and restricted to loopback access.

`per-vu-iterations` ran 5 writes or 20 reads per virtual user (VU), with 1, 5, 10, or 25 VUs. The write requests repeatedly updated a per-VU key. The read requests used a seeded key. The script checked expected HTTP status codes, not byte-for-byte data correctness after recovery.

### HTTP PUT

| VUs | Single-node mean | Single-node P95 | Raft mean | Raft P95 |
|---:|---:|---:|---:|---:|
| 1 | 16.51 ms | 59.84 ms | 19.19 ms | 35.12 ms |
| 5 | 18.84 ms | 24.96 ms | 63.31 ms | 73.81 ms |
| 10 | 35.82 ms | 40.13 ms | 122.62 ms | 150.88 ms |
| 25 | 82.76 ms | 111.91 ms | 337.77 ms | 393.31 ms |

At 25 VUs, the measured iteration rates were approximately 264.65 PUT iterations/s for the single-node route and 65.45 PUT iterations/s for Raft. These are **short-burst completion rates**, not sustained-capacity estimates.

### HTTP GET

| VUs | Single-node mean | Single-node P95 | Raft mean | Raft P95 |
|---:|---:|---:|---:|---:|
| 1 | 0.404 ms | 2.25 ms | 0.983 ms | 3.27 ms |
| 5 | 0.215 ms | 0.514 ms | 0.657 ms | 2.25 ms |
| 10 | 0.289 ms | 0.792 ms | 0.338 ms | 0.797 ms |
| 25 | 0.625 ms | 2.11 ms | 0.635 ms | 1.45 ms |

All 16 workload scenarios completed their measured checks with the expected HTTP statuses (0% observed status failures). This is **not** proof of linearizability, fault tolerance, or correctness under crashes.

### Critical HTTP methodology caveats

- k6 `setup()` sent a seed PUT. The reported `http_req_duration`, `http_reqs`, and percentiles aggregate setup requests together with workload requests. The script's custom check metrics count workload checks only. Subsequent benchmark revisions should tag setup versus workload and report filtered workload-only metrics.
- The samples were very small and runs were very short (several completed in milliseconds). P95 values and especially P99 would be unstable and are not presented as reliable tail-latency measurements.
- Repeated benchmark runs reused the server's persistent data unless explicitly reset. The Raft log grows, changing the work needed for later appends. A definitive run must begin from documented, equal, pre-seeded storage states.
- The client and server ran on the same machine; they share CPU and disk resources. These results cannot be generalized to a remote client, a multi-host cluster, or sustained traffic.
- Raft GET is **leader-local and explicitly non-linearizable**; it does not perform a quorum ReadIndex/barrier.

## Reproduce

From the repository root, use a dedicated, disposable data directory and ensure the configured server persistence paths point there. Never load-test a production instance or a dataset that must be retained.

```bash
# Microbenchmarks (BenchmarkDotNet)
dotnet run -c Release --project benchmarks/AegisDB.Benchmarks -- \
  --filter '*KeyValueStoreBenchmarks*'
dotnet run -c Release --project benchmarks/AegisDB.Benchmarks -- \
  --filter '*WalSerializationBenchmarks*'
dotnet run -c Release --project benchmarks/AegisDB.Benchmarks -- \
  --filter '*WalBatchBenchmarks*'
dotnet run -c Release --project benchmarks/AegisDB.Benchmarks -- \
  --filter '*RaftLogAppendBenchmarks*'
dotnet run -c Release --project benchmarks/AegisDB.Benchmarks -- \
  --filter '*RaftReplicationBenchmarks*'

# Start server with explicit Development configuration and the
# repository's actual Raft cluster-enable setting.
ASPNETCORE_ENVIRONMENT=Development \
ASPNETCORE_URLS=http://127.0.0.1:5050 \
Raft__Enabled=true \
  dotnet run -c Release --no-launch-profile \
  --project src/AegisDB.Server

# Example single-node HTTP smoke workload
k6 run -e MODE=single -e OPERATION=write -e VUS=1 \
  -e RUN_ID=smoke benchmarks/http/load-test.js
```

**Raft startup:** The local Raft simulation must be enabled using the actual configuration key defined by this repository revision; merely setting `ASPNETCORE_ENVIRONMENT=Development` is not sufficient. Check `/admin/kv` with one valid request before running any Raft load test. Do not assume a specific configuration variable name without verifying it.

## Known correctness and reliability limitations

The performance benchmarks do not remediate previously identified audit findings. Notably:

- **PERSIST-001 / PERSIST-002:** WAL framing corruption and uncertain append-failure handling can compromise acknowledged-write recovery; these must be addressed before durability claims.
- **PERSIST-003 / PERSIST-004:** Complete Raft-cluster restart does not autonomously restore applied key-value state; missing persisted election metadata can affect voting safety.
- **RAFT-002:** A stalled follower can delay submission despite a live quorum because the replication path awaits all peers.
- **CONC-001:** Leadership-transition races can affect runtime availability.
- **Raft GET:** Reads do not yet provide linearizable freshness guarantees.
- **Failure domains:** Three local Raft nodes do not provide independent process, machine, or storage failure isolation.

The project is a distributed-systems portfolio and learning implementation, **not a production-ready database**.

## Next measurements and engineering work

1. Repeat HTTP tests with tagged setup traffic excluded, fixed initial state, more samples, and realistic sustained throughput/latency distributions.
2. Capture source commit SHA, storage filesystem and device, CPU/power conditions, and all benchmark configuration in the run artifacts.
3. Profile Raft JSON serialization, full-file persistence, allocations, and redundant replication rounds independently.
4. Test nonresponding followers with bounded cancellation and controlled deterministic scheduling, outside BenchmarkDotNet throughput measurements.
5. Compare later performance revisions to this baseline **only after correctness and recovery fixes are validated**.
