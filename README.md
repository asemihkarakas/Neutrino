# Neutrino — Zero-Latency QoS Telemetry Ingestion Engine

> A lock-free, allocation-free C++20 engine that ingests UDP QoS telemetry from Radio Access Networks without a single mutex, heap allocation, or OS context switch on the data path.

**Status:** the foundation described in this README is implemented, tested, and benchmarked — everything below is real, runnable code, not a design doc. The project is under active development; see [What's Next](#whats-next) for the parts that are *not* built yet.

---

## What This Is

Radio Access Networks emit thousands of mobility-aware telemetry signals per second — RSRP, SINR, handover events, per-UE QoS measurements. If that stream feeds a downstream model or dashboard in real time, the ingestion layer cannot stall: a single `std::mutex`, a single heap allocation, a single OS context switch on the hot path is enough to introduce jitter that corrupts timing-sensitive downstream analysis.

This engine is a two-thread ingestion pipeline that receives UDP telemetry packets, classifies them by DSCP QoS class, and maintains live per-flow statistics — with:

- **No heap allocation on the data path.** A lock-free bump-pointer `ArenaAllocator` and a Treiber-stack `PacketPool` hand out pre-allocated buffers; nothing calls `new`/`delete` after startup.
- **No locks.** The `SpscQueue` between the receive thread and the process thread is built entirely from `std::atomic` with explicit `acquire`/`release` memory ordering — no mutex, no OS scheduler involvement.
- **No false sharing.** Every shared hot data structure is `alignas(64)`, so concurrent threads never contend over the same cacheline.
- **No exceptions.** The whole codebase builds with `-fno-exceptions`; errors propagate through a `Result<T,E>` type.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  UDP senders (packet_gen, or any RAN/IoT source)                │
└───────────────────────────┬─────────────────────────────────────┘
                            │  UDP packets (TelemetryHeader + payload)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Kernel UDP Socket  (SO_RCVBUF = 8 MiB, blocking + 200ms timeout) │
└───────────────────────────┬─────────────────────────────────────┘
                            │  recvfrom() into PacketPool slot
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  RECV THREAD                                                    │
│  PacketPool::acquire() → recvfrom() → SpscQueue::push()         │
│  (no allocation, no copy, no lock)                               │
└───────────────────────────┬─────────────────────────────────────┘
                            │  PacketDescriptor (ptr + size)
                            ▼  [SpscQueue — wait-free, cacheline-aligned]
┌─────────────────────────────────────────────────────────────────┐
│  PROCESS THREAD                                                 │
│  SpscQueue::pop() → validate header → update FlowStats (atomic) │
│  → PacketPool::release()                                         │
└───────────────────────────┬─────────────────────────────────────┘
                            │  atomic reads (relaxed)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  MAIN THREAD  (1 Hz)                                             │
│  flow_stats() → live table: packets, bytes, avg/min/max latency  │
└─────────────────────────────────────────────────────────────────┘
```

Nothing on the recv or process path allocates memory, acquires a lock, or throws an exception.

---

## What's Implemented

| Component | File | What it does |
|---|---|---|
| Error handling | `include/zlte/result.hpp` | `Result<T,E>` — exception-free error propagation |
| Arena allocator | `include/zlte/arena_allocator.hpp` | Lock-free bump-pointer allocator, CAS-based, O(1) alloc |
| Packet pool | `include/zlte/packet_pool.hpp` | ABA-safe Treiber stack of fixed-size UDP buffers |
| SPSC queue | `include/zlte/spsc_queue.hpp` | Wait-free ring buffer between recv and process threads |
| Wire format | `include/zlte/telemetry.hpp` | `TelemetryHeader` (24 B), DSCP classification, packet descriptor |
| Pipeline | `include/zlte/ingestion_pipeline.hpp` + `src/ingestion_pipeline.cpp` | Two-thread ingestion engine with per-flow atomic stats |
| Binary | `src/main.cpp` | Live stats display, `SIGINT`/`SIGTERM` handling, `--port` |
| Traffic generator | `tools/packet_gen.cpp` | Multi-flow UDP sender with nanosecond-precision pacing |
| Unit tests | `tests/arena_test.cpp` | 11 Google Test cases, incl. 8-thread concurrent stress |
| Benchmarks | `benchmarks/arena_bench.cpp` | Google Benchmark suite for arena/pool at 1–8 threads |

For a component-by-component deep dive — wire format, memory ordering rationale, build/run walkthrough — see [`tutorial.md`](tutorial.md).

---

## Build & Run

```bash
# Option A — CMake (installs cmake if missing, RelWithDebInfo build)
./scripts/build.sh

# Option B — g++ directly
g++ -std=c++20 -fno-exceptions -O2 -Iinclude \
    src/ingestion_pipeline.cpp src/main.cpp -lpthread -o zlte_ingest

g++ -std=c++20 -fno-exceptions -O2 -Iinclude \
    tools/packet_gen.cpp -o zlte_gen
```

```bash
# Terminal 1 — start the engine
./build/src/zlte_ingest --port 9000

# Terminal 2 — send 4 flows at 10,000 packets/second, 64-byte payload
./build/tools/zlte_gen --host 127.0.0.1 --port 9000 --flows 4 --rate 10000 --payload 64
```

---

## Ingestion Simulation

A real run — `zlte_gen` sending 4 flows at 5,000 pkt/s combined into `zlte_ingest`, captured live:

```
$ ./build/src/zlte_ingest --port 9123
ZLTE — Zero-Latency Telemetry Ingestion Engine  |  UDP :9123  |  Ctrl-C to quit

Flow      Class  DSCP  Packets       Bytes         AvgLat µs    MinLat µs    MaxLat µs
--------  -----  ----  ------------  ------------  ------------  ------------  ------------
0         EF     46    3747          329736        108           38            1239
1         AF4x   34    3747          329736        54            8             1947
2         AF3x   26    3747          329736        49            4             1876
3         AF2x   18    3747          329736        46            4             1870
```

```
$ ./build/tools/zlte_gen --host 127.0.0.1 --port 9123 --flows 4 --rate 5000 --payload 64
Sending to 127.0.0.1:9123  flows=4  rate=5000 pkt/s  payload=64 B
DSCP mapping:
  flow 0   → DSCP 46 (EF)
  flow 1   → DSCP 34 (AF4x)
  flow 2   → DSCP 26 (AF3x)
  flow 3   → DSCP 18 (AF2x)
Press Ctrl-C to stop.

Sent 14988 packets total.
```

Each flow is DSCP-classified into an RFC 4594 QoS class (EF, AF4x, AF3x, AF2x, AF1x, BE) and tracked independently with lock-free per-flow atomic counters — packets, bytes, and running min/avg/max latency — refreshed on screen once per second.

---

## Verifying It Works

### Unit tests
```bash
ctest --test-dir build --output-on-failure
```
11 Google Test cases covering `ArenaAllocator` (allocation, alignment, exhaustion, reset, non-overlap) and `PacketPool` (acquire/release correctness, exhaustion, data integrity, 8-thread concurrent stress).

### AddressSanitizer
```bash
cmake -S . -B build -DENABLE_ASAN=ON && cmake --build build
ctest --test-dir build --output-on-failure
```
`ENABLE_ASAN` defaults to **on** for a bare `cmake` configure (`scripts/build.sh` turns it off for performance runs — see below). The full test suite and a live ingest/generator run both pass clean under ASan; no leaks, no use-after-free, no data races.

### Benchmarks
```bash
cmake -S . -B build -DENABLE_ASAN=OFF -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --target zlte_benchmarks
./build/benchmarks/zlte_benchmarks
```
Measured on a 16-core host, `RelWithDebInfo`, ASan off (single-threaded numbers):

| Operation | Latency |
|---|---|
| `ArenaAllocator::allocate` (64 B) | ~23 ns |
| `ArenaAllocator::allocate` (1500 B) | ~23 ns |
| `PacketPool` acquire/release roundtrip | ~56 ns |

The benchmark suite also reports 2/4/8-thread contended numbers for both structures, so regressions in concurrent throughput are visible, not just single-thread speed.

---

## Project Constraints (`CLAUDE.md`)

| Rule | Enforcement |
|---|---|
| C++20 only | `set(CMAKE_CXX_STANDARD 20)`, `cxx_std_20` feature |
| No exceptions | `-fno-exceptions`, `Result<T,E>` everywhere |
| No mutexes on data path | `std::atomic` with explicit memory orders only |
| Cacheline alignment | `alignas(64)` on all shared hot variables |
| No `std::string` on hot path | `std::string_view`, raw `std::byte*`, `std::span` |

---

## What's Next

The pipeline above is a single blocking socket feeding one recv thread and one process thread — correct and fast, but not yet built for thousands of concurrent senders or production observability. In progress:

- **`epoll`-based event loop** with `recvmmsg` batching, replacing the single blocking socket so one thread can multiplex thousands of senders.
- **Multi-producer ingestion** (`MpscQueue`) for multiple recv threads feeding one process thread.
- **Runtime configuration & structured lock-free logging**, in place of compiled-in constants.
- **Prometheus/Grafana observability**, exported from a dedicated thread that never touches the data path.
- **GPU inference stage** — a TensorRT model consuming the live telemetry stream for QoS prediction.

None of this is wired up yet; treat anything not listed under [What's Implemented](#whats-implemented) as future work.

---

## References

- Alexandrescu, A. — *Modern C++ Design* (policy-based design, type lists)
- Meyers, S. — *Effective Modern C++* (move semantics, `std::atomic`)
- Williams, A. — *C++ Concurrency in Action* (lock-free data structures, memory ordering)
- Preshing, J. — [An Introduction to Lock-Free Programming](https://preshing.com/20120612/an-introduction-to-lock-free-programming/)
- Gregg, B. — *Systems Performance* (CPU cache hierarchy, flame graphs, `perf`)
- IETF RFC 4594 — *Configuration Guidelines for DiffServ Service Classes*

---

## License

MIT — see [`LICENSE`](LICENSE).
