# Zero-Latency QoS Telemetry Ingestion Engine

> A lock-free, allocation-free C++20 network engine that absorbs thousands of concurrent QoS telemetry streams from Radio Access Networks — without a single mutex, heap allocation, or OS context switch on the data path.

---

## Why This Exists

### 1. The Industry Problem: The Bridge Between Data and Learning

Radio Access Networks (RAN) emit thousands of mobility-aware telemetry signals every second — RSRP, SINR, handover events, per-UE QoS measurements, and dozens of other KPIs. If you want to feed this stream into a deep sequential learning model and predict the future in real time, the collection layer **cannot stall**.

A single `std::mutex` lock, a single `std::string` copy, a single OS-level context switch on the ingestion path is enough to introduce jitter that corrupts a 60-second sliding window and invalidates your model's temporal features before inference even begins.

This engine is the **asynchronous shock absorber** between the network and the model: it absorbs concurrent telemetry bursts from thousands of IoT devices or base stations, ingests them with zero-copy semantics, and delivers them to the processing stage without ever touching the OS scheduler on the critical path.

### 2. The Engineering Goal: Proof of Hardware Mastery

This project exists to demonstrate, in running code, that the author controls the machine rather than being controlled by it. Concretely:

- **Memory is owned, not borrowed.** A pre-allocated `ArenaAllocator` and `PacketPool` eliminate `new`/`delete` from the data path entirely. RAM usage is flat and deterministic.
- **The OS is a bystander, not a gatekeeper.** A non-blocking UDP socket with a tuned kernel receive buffer means the networking stack never puts our thread to sleep waiting for a system call.
- **Caches do not lie.** Every shared data structure is `alignas(64)` — occupying exactly one cacheline — so no two CPU cores ever fight over the same 64-byte block. False sharing is designed out, not debugged away.
- **Locks are replaced with physics.** The `SpscQueue` between the receive thread and the process thread uses only `std::atomic` with explicit `acquire`/`release` memory orders. The CPU's own Compare-And-Swap hardware instruction does the synchronization. The OS scheduler is never consulted.

### 3. The Career Signal: The Sniper Shot

Standard portfolios contain chat apps and to-do lists. This project sends a different message:

> *"The lock-free ring buffer and zero-copy ingestion architecture in this engine is built on the same engineering principles as the order-matching engines in HFT firms and the nanosecond-precision White Rabbit timing infrastructure at CERN."*

The techniques used here — SPSC queues, cacheline alignment, atomic memory ordering, arena allocation, non-blocking I/O — are not academic exercises. They are the exact primitives that appear in latency-sensitive systems across quantitative finance, particle physics, autonomous vehicles, and 5G core networks.

---

## The Five Pillars

Understanding this engine means internalizing five concepts that are physical laws, not textbook theory.

### 1. Blocking vs. Non-Blocking I/O

When a standard program calls `recv()` on a socket with no data, the OS freezes that thread. One thousand connections means one thousand sleeping threads — a RAM massacre.

With a non-blocking socket (`O_NONBLOCK`), the call returns immediately with `EAGAIN` if no data is ready. The CPU is never surrendered to the scheduler. One thread can serve thousands of connections without sleeping once.

### 2. Event Loop and epoll Multiplexing

Polling every socket in a tight loop burns the CPU to 100% for nothing.

`epoll` is the Linux kernel mechanism that lets you hand a list of sockets to the OS and say: *"Wake me only when one of these has data."* The kernel uses hardware interrupts from the NIC to deliver readiness notifications in O(1). The event loop thread sleeps with zero CPU cost and wakes exactly when work exists.

### 3. CPU Cache Hierarchy and False Sharing

A modern CPU never reads a single byte from RAM. It fetches 64-byte blocks called **cachelines**. If two CPU cores modify different variables that happen to share a cacheline, they constantly invalidate each other's L1 cache — a phenomenon called **false sharing** — and throughput collapses by up to 90%.

The fix is `alignas(64)`: every hot shared variable gets its own cacheline. This is not an optimization. It is a correctness requirement for concurrent data structures.

### 4. Lock-Free Programming and `std::atomic`

A mutex asks the OS to put one thread to sleep and wake another. That transition — a context switch — costs thousands of nanoseconds. In HFT order books and CERN timing systems, that is the entire budget.

`std::atomic` with explicit memory ordering (`memory_order_acquire`, `memory_order_release`, `memory_order_acq_rel`) maps directly to CPU hardware instructions (CMPXCHG, MFENCE). No OS involvement. No thread sleep. The synchronization happens in silicon, in nanoseconds.

### 5. Zero-Copy and RAII

Copying a packet from one buffer to another wastes CPU cycles and pollutes caches. Zero-copy means writing the packet **once** — into a pre-allocated arena — and then passing only a pointer and a length everywhere else. The bytes never move again.

RAII ensures that every packet buffer is returned to the pool exactly once, automatically, even on error paths — with no `delete` and no destructor call overhead on the hot path.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  RAN / IoT Senders                                              │
│  (base stations, sensors, UEs)                                  │
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
│  (no allocation, no copy, no lock)                              │
└───────────────────────────┬─────────────────────────────────────┘
                            │  PacketDescriptor (ptr + size)
                            ▼  [SpscQueue — wait-free, cacheline-aligned]
┌─────────────────────────────────────────────────────────────────┐
│  PROCESS THREAD                                                 │
│  SpscQueue::pop() → memcpy header → validate → update FlowStats │
│  → PacketPool::release()                                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │  atomic reads (relaxed)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  MAIN THREAD  (1 Hz)                                            │
│  flow_stats() → live table: packets, bytes, avg/min/max latency │
└─────────────────────────────────────────────────────────────────┘
```

Nothing on the recv or process path allocates memory, acquires a lock, or throws an exception.

---

## Current Implementation

| Component | File | What it does |
|---|---|---|
| Error handling | `include/zlte/result.hpp` | `Result<T,E>` — exception-free error propagation |
| Arena allocator | `include/zlte/arena_allocator.hpp` | Lock-free bump-pointer allocator, CAS-based, O(1) alloc |
| Packet pool | `include/zlte/packet_pool.hpp` | ABA-safe Treiber stack of fixed-size UDP buffers |
| SPSC queue | `include/zlte/spsc_queue.hpp` | Wait-free ring buffer between recv and process threads |
| Wire format | `include/zlte/telemetry.hpp` | `TelemetryHeader` (24 B), DSCP classification, packet descriptor |
| Pipeline | `include/zlte/ingestion_pipeline.hpp` + `src/ingestion_pipeline.cpp` | Two-thread ingestion engine with per-flow atomic stats |
| Binary | `src/main.cpp` | Live stats display, signal handling, CLI args |
| Generator | `tools/packet_gen.cpp` | Multi-flow UDP packet sender with nanosecond-precision pacing |

---

## Quick Start

```bash
# Build (cmake)
sudo apt install cmake
./scripts/build.sh

# Or build directly with g++
g++ -std=c++20 -fno-exceptions -O2 -Iinclude \
    src/ingestion_pipeline.cpp src/main.cpp -lpthread -o zlte_ingest

g++ -std=c++20 -fno-exceptions -O2 -Iinclude \
    tools/packet_gen.cpp -o zlte_gen
```

```bash
# Terminal 1 — start the engine
./build/src/zlte_ingest --port 9000

# Terminal 2 — send 4 flows at 10,000 packets/second with 64-byte payload
./build/tools/zlte_gen --flows 4 --rate 10000 --payload 64
```

The engine prints a live stats table updated every second:

```
ZLTE — Zero-Latency Telemetry Ingestion Engine  |  UDP :9000

Flow     Class  DSCP  Packets       Bytes         AvgLat µs    MinLat µs    MaxLat µs
--------  -----  ----  ...
0         EF     46    10000         880000        9            6            34
1         AF4x   34    10000         880000        8            5            31
2         AF3x   26    10000         880000        10           7            38
3         AF2x   18    10000         880000        9            6            33
```

---

## Testing Protocol

This project is tested at four levels of aggression.

### 1. AddressSanitizer — Memory Proof
```bash
cmake -S . -B build -DENABLE_ASAN=ON
cmake --build build
./build/src/zlte_ingest &
./build/tools/zlte_gen --rate 1000000  # 1M pkt/s stress
```
ASan instruments every pointer dereference and catches use-after-free, buffer overflows, and data races before they become production incidents. The target: zero ASan reports under maximum load.

### 2. Google Benchmark — Nanosecond Proof
```bash
cmake --build build --target zlte_benchmarks
./build/benchmarks/zlte_benchmarks
```
Reports per-operation latency for the arena allocator and packet pool at 1, 2, 4, and 8 threads. Target: sub-50 ns per acquire/release roundtrip.

### 3. Load Testing — Survival Proof
```bash
# Multiple sender instances in parallel
for i in {1..8}; do ./zlte_gen --flows 128 --rate 50000 & done
```
8 senders × 128 flows × 50,000 pkt/s = 51.2 million packets/second. The engine must not drop packets (pool not exhausted), not spin the CPU to 100% (SPSC queue keeps threads coordinated), and not crash (ASan confirms this).

### 4. perf + Flame Graph — Hardware Proof
```bash
perf record -g ./build/src/zlte_ingest --port 9000
perf script | stackcollapse-perf.pl | flamegraph.pl > flame.svg
```
A flame graph that shows the vast majority of CPU time in `recvfrom` and `memcpy` — not in lock contention, not in allocator overhead, not in OS scheduler code — is the hardware-level proof that the architecture is correct.

---

## Roadmap & Todo

The project is divided into four phases. Each phase is a vertical slice: it can be compiled, tested, and demonstrated independently before the next begins.

---

### Phase 0 — Foundation: Lock-Free Primitives & UDP Ingestion ✅ COMPLETE

> Goal: prove that the core data path is allocation-free, lock-free, and correct under concurrent stress.

**Core data structures**
- [x] `Result<T,E>` — exception-free error propagation (`include/zlte/result.hpp`)
- [x] `ArenaAllocator<Capacity>` — lock-free bump-pointer slab, CAS-based, O(1) alloc, `alignas(64)` (`include/zlte/arena_allocator.hpp`)
- [x] `PacketPool<Size,Depth>` — ABA-safe Treiber stack of pre-allocated UDP buffers, `alignas(64)` slots (`include/zlte/packet_pool.hpp`)
- [x] `SpscQueue<T,Capacity>` — wait-free SPSC ring buffer with `acquire`/`release` memory ordering, cacheline-separated head/tail (`include/zlte/spsc_queue.hpp`)

**Wire protocol**
- [x] `TelemetryHeader` — 24-byte naturally-packed on-wire format (`include/zlte/telemetry.hpp`)
- [x] `dscp_class()` — constexpr DSCP → PHB name mapping, zero allocation (`include/zlte/telemetry.hpp`)
- [x] `PacketDescriptor` — 24-byte token (pointer + size) passed through the SPSC queue

**Ingestion pipeline**
- [x] `IngestionPipeline` class — owns socket, pool, queue, both threads, and `FlowStats` array (`include/zlte/ingestion_pipeline.hpp`)
- [x] `recv_loop` — `recvfrom` into pool slot → SPSC push; blocking socket with `SO_RCVTIMEO = 200 ms` for clean shutdown (`src/ingestion_pipeline.cpp`)
- [x] `process_loop` — SPSC pop → header validate → per-flow atomic stat update → pool release (`src/ingestion_pipeline.cpp`)
- [x] `FlowStats` — per-flow `alignas(64)` atomic counters: packets, bytes, latency sum/min/max, DSCP, active flag
- [x] Pool-exhaustion drop counter (`dropped_` atomic, reported in dashboard)
- [x] Clean shutdown: `stop()` sets `running_` false, closes socket, joins both threads

**Application & tooling**
- [x] `main.cpp` — live ANSI stats table refreshed every second, `SIGINT`/`SIGTERM` handling, `--port` CLI arg (`src/main.cpp`)
- [x] `packet_gen` — multi-flow UDP sender with nanosecond-precision `busy_sleep_until` pacing, `--flows / --rate / --payload` args (`tools/packet_gen.cpp`)
- [x] `scripts/build.sh` — installs cmake if missing, `RelWithDebInfo` build, prints run commands

**Build system & quality**
- [x] CMake 3.20+, C++20, `-fno-exceptions`, `-Wall -Wextra -Werror -Wpedantic` (`CMakeLists.txt`)
- [x] `FetchContent` for Google Test v1.15.2 and Google Benchmark v1.9.1
- [x] AddressSanitizer support (`-DENABLE_ASAN=ON/OFF`) wired into CMake
- [x] Google Test suite — 9 test cases for `ArenaAllocator` and `PacketPool` including 8-thread concurrent stress (`tests/arena_test.cpp`)
- [x] Google Benchmark suite — ST/MT throughput for arena and pool, nanosecond-resolution (`benchmarks/arena_bench.cpp`)
- [x] `compile_commands.json` export for clangd LSP (`CMAKE_EXPORT_COMPILE_COMMANDS ON`)

**Documentation**
- [x] `README.md` — goals, architecture diagram, five pillars, testing protocol, roadmap
- [x] `tutorial.md` — component deep-dives, wire format table, build/run walkthrough, benchmark interpretation

---

### Phase 1 — epoll Event Loop & Multi-Source Ingestion

> Goal: one thread handles thousands of concurrent UDP senders with O(1) kernel dispatch instead of one blocking socket per thread.

**epoll core**
- [ ] `EventLoop` class (`include/zlte/event_loop.hpp`) wrapping `epoll_create1`, `epoll_ctl`, `epoll_wait`
- [ ] Non-blocking sockets (`SOCK_DGRAM | SOCK_NONBLOCK`) for all sender connections
- [ ] `epoll_wait` with hardware-interrupt-driven wakeup — zero CPU cost while idle
- [ ] Per-event `recvmmsg` (receive multiple datagrams per syscall) to amortise syscall overhead
- [ ] Replace `recv_loop` in `IngestionPipeline` with `EventLoop::run()`

**Multi-producer support**
- [ ] `MpscQueue<T, Capacity>` — multi-producer single-consumer ring buffer (`include/zlte/mpsc_queue.hpp`) to support multiple recv threads feeding one process thread
- [ ] Per-sender connection registry (map from fd → sender metadata)
- [ ] Idle sender timeout and fd cleanup

**Observability & hardening**
- [ ] Per-flow sequence-number gap detection (out-of-order and lost packet counters in `FlowStats`)
- [ ] Thread affinity / CPU pinning (`sched_setaffinity`) — recv thread pinned to core 0, process thread to core 1
- [ ] `scripts/flamegraph.sh` — `perf record` + stackcollapse + flamegraph.pl in one command
- [ ] Load test script (`scripts/loadtest.sh`) launching N parallel `zlte_gen` instances

**Tests & benchmarks**
- [ ] Unit tests for `EventLoop` (single sender, multi-sender, idle timeout)
- [ ] Unit tests for `MpscQueue` (multi-producer correctness under 8-thread stress)
- [ ] Benchmark: `epoll` dispatch latency vs. blocking socket at 1k / 10k / 100k senders
- [ ] Benchmark: `recvmmsg` batch size vs. per-call `recvfrom` throughput

---

### Phase 2 — Hardening, Configurability & CI

> Goal: production-grade reliability — no silent drops, no configuration baked into source, continuous verification on every commit.

**Runtime configurability**
- [ ] `Config` struct loaded from CLI flags and/or a TOML/INI file (no `std::string` in hot path; parse at startup only)
- [ ] Runtime-configurable pool depth, queue depth, max flows, listen port, CPU affinity map
- [ ] Graceful capacity overflow policy: configurable drop-tail vs. back-pressure vs. oldest-eviction

**Structured lock-free logging**
- [ ] Fixed-size log entry struct (no heap, no `std::string`)
- [ ] SPSC log ring from data-path threads → dedicated logger thread
- [ ] Logger thread writes to `stderr` or a memory-mapped log file

**Network robustness**
- [ ] Per-flow reorder buffer (small fixed-size window for out-of-order detection)
- [ ] Duplicate packet suppression via sequence number bitmap
- [ ] Malformed-packet fuzzing harness (libFuzzer target for `TelemetryHeader` parsing)

**CI pipeline**
- [ ] GitHub Actions workflow: build → ASan test run → benchmark regression check
- [ ] Benchmark baseline committed to repo; CI fails if p99 regresses by > 20%
- [ ] `clang-tidy` and `clang-format` checks in CI
- [ ] Valgrind `memcheck` nightly run on full load test

---

### Phase 3 — GPU Inference: From Raw Bytes to Predictions

> Goal: the process thread feeds a TensorRT inference engine so that QoS predictions exit the GPU within microseconds of packet arrival.

**Python model pipeline (offline)**
- [ ] Dataset: synthetic RAN telemetry generator (RSRP, SINR, throughput, DSCP sequences)
- [ ] Model: stacked LSTM + Temporal Convolutional Network (TCN) + self-attention head
- [ ] Training loop in PyTorch with per-UE 60-second sliding-window input
- [ ] ONNX export (`torch.onnx.export`) with dynamic batch dimension
- [ ] Validation: ONNX Runtime inference matches PyTorch output to float32 tolerance

**TensorRT engine (C++ side)**
- [ ] `TrtEngine` class (`include/zlte/trt_engine.hpp`) wrapping `nvinfer1::IRuntime`
- [ ] Engine compilation at startup: ONNX → TensorRT with FP16 + INT8 calibration for Ada Lovelace Tensor Cores
- [ ] Layer fusion and kernel auto-tuning (`BuilderFlag::kFP16`, `kOBEY_PRECISION_CONSTRAINTS`)
- [ ] Serialized engine cache: compile once, load from `.trt` file on subsequent runs

**Zero-copy CPU → GPU pipeline**
- [ ] Pinned memory pool: replace `std::byte` arena backing store with `cudaHostAlloc` page-locked pages
- [ ] CUDA stream per inference slot for overlap: while GPU runs inference on window *t*, CPU transfers window *t+1*
- [ ] Async `cudaMemcpyAsync` H2D via DMA — CPU not involved in the transfer

**In-GPU sliding window**
- [ ] VRAM-resident circular buffer holding the T=60 s feature matrix
- [ ] CUDA kernel: shift matrix left by one timestep, append new feature row to rightmost column
- [ ] Kernel launch from process thread via CUDA stream immediately after H2D completes

**Inference dispatch & output**
- [ ] TensorRT `enqueueV3` with async stream — non-blocking from the process thread's perspective
- [ ] D2H transfer of prediction vector (QoS class probabilities, handover score)
- [ ] Decision output: write prediction to a lock-free output ring, readable by the main thread
- [ ] Dashboard extended with predicted next-second QoS class per flow

**End-to-end validation**
- [ ] Latency histogram: NIC receive → GPU prediction output (target: < 1 ms p99 at 10k pkt/s)
- [ ] Accuracy benchmark against offline PyTorch reference on held-out telemetry traces
- [ ] Stress test: 30-minute continuous run under full load with ASan + CUDA-Memcheck

---

## Project Constraints (CLAUDE.md)

| Rule | Enforcement |
|---|---|
| C++20 only | `set(CMAKE_CXX_STANDARD 20)`, `cxx_std_20` feature |
| No exceptions | `-fno-exceptions`, `Result<T,E>` everywhere |
| No mutexes on data path | `std::atomic` with explicit memory orders only |
| Cacheline alignment | `alignas(64)` on all shared hot variables |
| No `std::string` on hot path | `std::string_view`, raw `std::byte*`, `std::span` |

---

## References

- Alexandrescu, A. — *Modern C++ Design* (policy-based design, type lists)
- Meyers, S. — *Effective Modern C++* (move semantics, `std::atomic`)
- Williams, A. — *C++ Concurrency in Action* (lock-free data structures, memory ordering)
- Preshing, J. — [An Introduction to Lock-Free Programming](https://preshing.com/20120612/an-introduction-to-lock-free-programming/)
- Gregg, B. — *Systems Performance* (CPU cache hierarchy, flame graphs, `perf`)
- IETF RFC 4594 — *Configuration Guidelines for DiffServ Service Classes*
- NVIDIA TensorRT Developer Guide — (Phase 3 reference)
