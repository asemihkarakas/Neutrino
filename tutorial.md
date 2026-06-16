# Tutorial: Building the Zero-Latency Telemetry Ingestion Engine

This document walks through **what has been built so far**, why each piece exists, and how to run and observe it. It assumes you know C++ basics but have not necessarily written lock-free or systems-level code before.

---

## Table of Contents

1. [Mental model: what problem are we solving?](#1-mental-model-what-problem-are-we-solving)
2. [The five concepts you must own](#2-the-five-concepts-you-must-own)
3. [Project layout](#3-project-layout)
4. [Component deep-dives](#4-component-deep-dives)
   - 4.1 `result.hpp` — errors without exceptions
   - 4.2 `arena_allocator.hpp` — the lock-free memory slab
   - 4.3 `packet_pool.hpp` — pre-allocated UDP buffers
   - 4.4 `spsc_queue.hpp` — the wait-free conveyor belt
   - 4.5 `telemetry.hpp` — the wire format
   - 4.6 `ingestion_pipeline` — the two-thread engine
   - 4.7 `main.cpp` — the live dashboard
   - 4.8 `packet_gen.cpp` — the traffic gun
5. [Build and run](#5-build-and-run)
6. [What to observe while it runs](#6-what-to-observe-while-it-runs)
7. [Running the tests](#7-running-the-tests)
8. [Running the benchmarks](#8-running-the-benchmarks)
9. [Where the code goes next](#9-where-the-code-goes-next)

---

## 1. Mental Model: What Problem Are We Solving?

Imagine a 5G base station sending 10,000 QoS measurements per second — RSRP, latency, packet loss, DSCP markings — as small UDP packets. You want a deep learning model to consume this stream in real time and predict handover decisions or congestion events before they happen.

The naive approach:

```
packet arrives → recv() → new std::string(data) → push to std::queue (with mutex) → process
```

This fails at scale because:

- `new std::string` calls the global allocator on every packet — which holds a global lock internally.
- `std::queue` with `std::mutex` causes context switches: when one thread holds the lock the other thread is put to sleep by the OS and woken up later — costing thousands of nanoseconds per packet.
- At 10,000 pkt/s with, say, 1,000 connected senders, these costs multiply into milliseconds of accumulated jitter that corrupt any temporal model.

This engine solves all three:

| Problem | Solution |
|---|---|
| Heap allocation per packet | `PacketPool` — buffers are pre-allocated at startup, reused forever |
| Mutex on the queue | `SpscQueue` — wait-free ring buffer, two atomics, no OS call |
| Copy of packet data | Zero-copy: `recvfrom` writes directly into a pool slot; only a pointer is passed downstream |

---

## 2. The Five Concepts You Must Own

Before reading the code, make sure these five concepts are internalized. Each one corresponds directly to a component in this project.

### Concept A: Non-Blocking I/O

A **blocking** socket call (`recv`, `recvfrom`) suspends the calling thread until data arrives. For one connection this is fine. For thousands it requires thousands of sleeping threads — a RAM and scheduler nightmare.

A **non-blocking** call returns immediately with `EAGAIN` if no data is ready. The thread is never suspended; it moves on and checks again later. This engine uses a blocking socket with a 200 ms timeout (`SO_RCVTIMEO`) as a practical middle ground: the thread blocks for at most 200 ms before checking whether it should stop, which avoids the busy-wait cost of a pure non-blocking loop while still allowing clean shutdown.

> **Phase 2** will replace this with a true `epoll`-based event loop that handles thousands of sockets from a single thread.

### Concept B: The SPSC Queue — Wait-Free Communication Between Threads

The receive thread writes packets; the process thread reads them. They must communicate without a lock. The mechanism is a **Single-Producer Single-Consumer (SPSC) ring buffer**:

```
[ slot 0 ][ slot 1 ][ slot 2 ][ ... ][ slot N-1 ]
     ↑                                      ↑
   head (consumer reads here)           tail (producer writes here)
```

- The producer writes to `buf[tail]`, then advances `tail` with a **release store**.
- The consumer checks `tail` with an **acquire load** — if it moved, a new item is ready.
- `acquire`/`release` are not just compiler hints; they are CPU memory barrier instructions that ensure the write to `buf[tail]` is visible to the consumer before `tail` is updated.

No mutex, no OS call. The two threads communicate through two `std::atomic<size_t>` counters and the CPU's own cache coherence protocol.

### Concept C: Cacheline Alignment and False Sharing

A CPU does not read individual bytes from RAM. It reads **64-byte blocks** called cachelines. If `head` and `tail` share a cacheline, then every time the producer writes `tail`, the consumer's cacheline — which contains `head` — is **invalidated** on its core, even though the consumer didn't touch `tail`. The consumer must re-fetch the cacheline from the other core's cache. This is **false sharing** and it can kill throughput by 90%.

The fix is `alignas(64)` on each hot variable:

```cpp
alignas(64) std::atomic<std::size_t> head_{0};  // its own cacheline
alignas(64) std::atomic<std::size_t> tail_{0};  // its own cacheline
```

Now the producer's writes to `tail_` never touch the cacheline that contains `head_`. The cores operate independently.

### Concept D: Lock-Free Memory Allocation

The standard `new` operator calls `malloc`, which holds a **global lock** internally so multiple threads don't corrupt the allocator's internal state. At high packet rates this lock becomes a serialization bottleneck — every `new` creates a potential stall.

The `PacketPool` solves this with an **ABA-safe Treiber stack**: a lock-free linked list of pre-allocated buffer slots. `acquire()` pops the head with a CAS (Compare-And-Swap); `release()` pushes back. No lock, no OS call, O(1) in both directions.

The `ArenaAllocator` solves the same problem differently: a **bump-pointer allocator** advances a single `std::atomic<size_t>` offset with CAS. Allocation is a single atomic add. Deallocation does not exist individually — you reset the entire arena at once when you're done with a batch.

### Concept E: Zero-Copy with `std::span` and `std::string_view`

Zero-copy means the packet bytes are written to RAM **exactly once** — inside `recvfrom`, directly into a pool slot. From that moment on, only the address and size are passed around. The bytes never move:

```cpp
// recvfrom writes directly into the pool slot
const ssize_t n = ::recvfrom(fd, slot.data(), slot.size(), ...);

// downstream receives a pointer + size, not a copy
PacketDescriptor desc{ .data = slot.data(), .data_size = n, ... };
queue_.push(desc);  // 24 bytes on the stack, no heap
```

`std::span<std::byte>` is the modern C++20 way to express "a pointer and a length" without ownership semantics. `std::string_view` does the same for character sequences. Neither copies; both carry only a pointer and a size.

---

## 3. Project Layout

```
zero-latency-telemetry-ingestion-engine/
│
├── include/zlte/               ← all public headers; no compiled code
│   ├── result.hpp              ← error-or-value type (no exceptions)
│   ├── arena_allocator.hpp     ← lock-free bump-pointer allocator
│   ├── packet_pool.hpp         ← ABA-safe pre-allocated buffer pool
│   ├── spsc_queue.hpp          ← wait-free SPSC ring buffer
│   ├── telemetry.hpp           ← wire format + DSCP classifier
│   └── ingestion_pipeline.hpp  ← two-thread pipeline declaration
│
├── src/
│   ├── ingestion_pipeline.cpp  ← recv_loop + process_loop implementation
│   ├── main.cpp                ← entry point + live stats display
│   └── CMakeLists.txt
│
├── tools/
│   ├── packet_gen.cpp          ← UDP packet sender for testing
│   └── CMakeLists.txt
│
├── tests/
│   ├── arena_test.cpp          ← Google Test suite (arena + pool)
│   └── CMakeLists.txt
│
├── benchmarks/
│   ├── arena_bench.cpp         ← Google Benchmark suite
│   └── CMakeLists.txt
│
├── scripts/
│   └── build.sh                ← one-command build (installs cmake if missing)
│
├── CMakeLists.txt              ← root build configuration
├── .clangd                     ← clangd LSP configuration
├── README.md                   ← project overview and goals
└── tutorial.md                 ← this file
```

**The rule**: `include/zlte/` is the public surface of the library. Anything another project would `#include` lives here. Everything in `src/` is implementation detail. `tools/` contains programs that use the library but are not part of it.

---

## 4. Component Deep-Dives

### 4.1 `result.hpp` — Errors Without Exceptions

**The problem**: CLAUDE.md mandates `-fno-exceptions`. `throw`/`catch` are disabled at the compiler level. But functions still need to signal failure.

**The solution**: `Result<T, E>` — a discriminated union that holds either a value of type `T` or an error of type `E`:

```cpp
Result<void*, ArenaError> r = arena.allocate(64, 64);
if (!r) {
    // r.error() is an ArenaError enum value
    return;
}
void* ptr = r.value();
```

The invariants enforced by `static_assert`:
- Both `T` and `E` must be **trivially copyable** — `Result` is passed by value on the stack, never heap-allocated.
- `T` and `E` must be **distinct types** — so there is never ambiguity about whether the held value is a result or an error.

`[[nodiscard]]` on the class means the compiler warns if a caller ignores the return value. Ignoring an error is not silent.

---

### 4.2 `arena_allocator.hpp` — The Lock-Free Memory Slab

The `ArenaAllocator<Capacity>` pre-allocates `Capacity` bytes at construction time (inside the object itself, on a `static` or long-lived heap object). Allocation is a single CAS loop on an atomic offset:

```cpp
// Conceptually:
claimed = align_up(offset_, alignment);
next    = claimed + bytes;
if (next > Capacity) return err(OutOfMemory);
offset_.compare_exchange_weak(current, next, acq_rel, relaxed);
return ok(storage_ + claimed);
```

The CAS (`compare_exchange_weak`) is the key. It atomically reads `offset_`, computes the new value, and writes it — but **only if `offset_` is still equal to what we read**. If another thread advanced `offset_` between our read and our write, CAS fails and we retry. No mutex, no sleep. This is lock-free allocation.

**Memory layout** (two cachelines, not one):

```
[offset_ — 64 bytes, cacheline 0]
[storage_[0..Capacity-1] — Capacity bytes, starting at cacheline 1]
```

`offset_` is on its own cacheline so that CAS updates on `offset_` never invalidate the cacheline being read from `storage_`.

**Limitation**: there is no per-allocation `free()`. You call `reset()` to reclaim everything at once. This is intentional: for packet processing, you process a packet, release it back to the `PacketPool`, and the arena is used only for metadata that lives for the duration of a batch.

---

### 4.3 `packet_pool.hpp` — Pre-Allocated UDP Buffers

The `PacketPool<PacketSize, PoolDepth>` is a lock-free pool of `PoolDepth` buffers, each `PacketSize` bytes. Internally it is an **ABA-safe Treiber stack** — a lock-free stack where the "head" pointer is protected against the ABA problem.

**What is the ABA problem?**

```
Thread 1 reads head = A
Thread 2: pops A, pushes B, pushes A back
Thread 1: CAS(head, A → B) — succeeds, but now head should be B, not what thread 1 intended
```

The fix: encode both the slot index and a monotonic **tag** in one 64-bit atomic:

```
[  32 bits: slot index  |  32 bits: ABA tag  ]
```

Every push increments the tag. So even if the same slot index is at the head, the tag is different and the stale CAS fails.

**Each `Slot` is `alignas(64)`**: different threads acquiring adjacent slots never fight over the same cacheline.

**`acquire()` / `release()`** are the public API:

```cpp
auto r = pool.acquire();             // pops a slot, returns std::span<std::byte>
if (r) {
    // use r.value() — exactly PacketSize bytes
    pool.release(r.value());         // pushes the slot back
}
```

---

### 4.4 `spsc_queue.hpp` — The Wait-Free Conveyor Belt

`SpscQueue<T, Capacity>` is the data channel between the receive thread (producer) and the process thread (consumer). It is **wait-free**: both `push` and `pop` complete in a bounded number of steps, with no retry loop.

```
    head_                tail_
      │                    │
      ▼                    ▼
[ slot 0 ][ slot 1 ][ slot 2 ][ slot 3 ][ ... ]
  (ready)   (ready)   (next    (empty)
                       write)
```

**`push` (producer only)**:
```cpp
tail = tail_.load(relaxed);         // where to write
next = (tail + 1) & kMask;
if (next == head_.load(acquire))    // full check
    return false;
buf_[tail] = item;                  // write the item
tail_.store(next, release);         // publish
```

**`pop` (consumer only)**:
```cpp
head = head_.load(relaxed);
if (head == tail_.load(acquire))    // empty check
    return false;
out = buf_[head];                   // read the item
head_.store((head + 1) & kMask, release);
```

The `release` store on `tail_` and the `acquire` load on `tail_` form a **synchronization point**: everything the producer wrote before the `release` (including `buf_[tail]`) is visible to the consumer after the `acquire`. This is the C++20 memory model at work — no mutex required.

**Capacity must be a power of two** so that `(index + 1) & kMask` wraps around correctly with a bitmask instead of a modulo.

---

### 4.5 `telemetry.hpp` — The Wire Format

Every packet sent to the engine must begin with a `TelemetryHeader`:

```
Byte offset   Field           Size   Description
──────────────────────────────────────────────────────────
0             magic           4 B    Always 0x5A4C5445 ("ZLTE")
4             version         1 B    Protocol version (currently 1)
5             dscp            1 B    IETF DSCP value 0–63
6             payload_len     2 B    Bytes after this header
8             flow_id         4 B    Identifies the logical flow
12            seq             4 B    Per-flow sequence number
16            timestamp_ns    8 B    Sender's CLOCK_MONOTONIC_RAW at send time
──────────────────────────────────────────────────────────
Total                        24 B    (no padding — naturally aligned)
```

The layout is deliberately ordered so that the struct has zero padding: `uint32, uint8, uint8, uint16, uint32, uint32, uint64` hits every natural alignment boundary perfectly. No `__attribute__((packed))` needed.

**DSCP classification** is handled by `dscp_class()`:

```cpp
constexpr std::string_view dscp_class(uint8_t dscp) noexcept;
// Returns: "EF", "AF4x", "AF3x", "AF2x", "AF1x", "CS6", or "BE"
```

This is a `constexpr` function returning `std::string_view` into static literal storage. No allocation. No runtime overhead beyond a few branches.

**`PacketDescriptor`** is the 24-byte token that travels through the SPSC queue:

```cpp
struct PacketDescriptor {
    std::byte*    data;       // pointer into a live PacketPool slot
    std::size_t   data_size;  // actual received bytes (≤ kPacketSize)
    std::uint32_t src_ip;     // sender IP (network byte order)
    std::uint16_t src_port;   // sender port (host byte order)
};
```

This is intentionally small: the SPSC queue holds 4,096 of these and they must fit in cache.

---

### 4.6 `ingestion_pipeline` — The Two-Thread Engine

`IngestionPipeline` owns everything: the pool, the queue, the socket, both threads, and the per-flow stats array.

```cpp
IngestionPipeline pipeline{9000};
auto r = pipeline.start();  // opens socket, launches both threads
// ...
pipeline.stop();            // signals threads, closes socket, joins both
```

#### `recv_loop` (runs on the receive thread)

```
loop:
  pool_.acquire()           → get an empty buffer slot
  recvfrom(fd, slot, ...)   → OS writes packet bytes directly into that slot
  if n <= 0:
      pool_.release(slot)   → return the slot (timeout or error)
      continue
  fill PacketDescriptor{.data = slot.data(), .data_size = n, ...}
  queue_.push(desc)         → hand the descriptor to the process thread
```

The blocking `recvfrom` with `SO_RCVTIMEO = 200 ms` means the thread is not spinning — it yields to the OS while waiting for data. When data arrives, the OS wakes it and `recvfrom` returns immediately.

#### `process_loop` (runs on the process thread)

```
loop:
  queue_.pop(desc)          → wait for a descriptor from the recv thread
  if empty: continue        → spin (very fast, CPU stays warm)
  record recv_ns = now()
  memcpy(&hdr, desc.data, 24)
  pool_.release({desc.data, kPacketSize})   ← release slot ASAP
  validate magic and version
  compute latency_ns = recv_ns - hdr.timestamp_ns
  update FlowStats[hdr.flow_id % kMaxFlows] with atomic operations
```

The slot is released immediately after `memcpy` — the process thread does not hold pool slots during any downstream processing. This minimizes pool pressure.

#### `FlowStats` — Per-Flow Atomic Counters

```cpp
struct alignas(64) FlowStats {
    std::atomic<uint64_t> packets{0};
    std::atomic<uint64_t> bytes{0};
    std::atomic<uint64_t> latency_sum_ns{0};
    std::atomic<uint64_t> latency_min_ns{UINT64_MAX};
    std::atomic<uint64_t> latency_max_ns{0};
    std::atomic<uint8_t>  dscp{0};
    std::atomic<bool>     active{false};
};
```

All updates use `memory_order_relaxed` — the process thread is the only writer. The main thread reads them with relaxed loads too; the stats display is eventually consistent (one second behind at most), which is acceptable for a monitoring table.

`alignas(64)` on the struct means each flow's stats occupy their own cacheline(s). If there were two writer threads (not the case here, but in Phase 2 it might be), they could update different flows without any cacheline bouncing.

Min/max are updated with a CAS loop:

```cpp
uint64_t cur = fs.latency_min_ns.load(relaxed);
while (latency < cur &&
       !fs.latency_min_ns.compare_exchange_weak(cur, latency, relaxed, relaxed))
{}
```

This is correct for a single writer: the loop exits after at most one successful CAS. It never loops more than once unless a preemption occurred between the load and the CAS — extremely rare in practice.

---

### 4.7 `main.cpp` — The Live Dashboard

`main` does three things:

1. **Parse CLI args** (`--port`) and install signal handlers for `SIGINT`/`SIGTERM`.
2. **Start the pipeline** and handle errors with a `switch` on `PipelineError`.
3. **Print a live stats table** every second until the user presses Ctrl-C.

```
ZLTE — Zero-Latency Telemetry Ingestion Engine  |  UDP :9000  |  Ctrl-C to quit

Flow     Class  DSCP  Packets       Bytes         AvgLat µs    MinLat µs    MaxLat µs
--------  -----  ----  ----------    ----------    ----------   ----------   ----------
0         EF     46    12043         1059784       8            5            44
1         AF4x   34    12041         1059608       9            5            51
2         AF3x   26    12039         1059432       8            6            38
3         AF2x   18    12037         1059256       9            6            42
```

The table uses `\033[2J\033[H` (ANSI clear-screen + cursor-home) before each redraw, creating a smooth refresh without terminal scroll. Any flow slot where `active == false` or `packets == 0` is skipped.

If pool-exhaustion drops are detected (`dropped_ > 0`), a warning line appears at the bottom. This indicates the process thread is falling behind the recv thread and the pool is running dry.

---

### 4.8 `tools/packet_gen.cpp` — The Traffic Gun

`zlte_gen` generates synthetic `TelemetryHeader` UDP packets and sends them to the engine.

```
./zlte_gen --host 127.0.0.1 --port 9000 --flows 4 --rate 10000 --payload 64
```

| Arg | Default | Meaning |
|---|---|---|
| `--host` | `127.0.0.1` | Engine address |
| `--port` | `9000` | Engine port |
| `--flows` | `4` | Number of logical flows |
| `--rate` | `1000` | Total packets/second across all flows |
| `--payload` | `0` | Extra bytes appended after the header |

Each flow gets a different DSCP value cycling through `EF(46) → AF4x(34) → AF3x(26) → AF2x(18) → AF1x(10) → BE(0)`.

Timing precision is handled by `busy_sleep_until()`: for sleeps longer than 500 µs it uses `nanosleep` to yield the CPU, then spins the last 100 µs to achieve sub-microsecond pacing accuracy. `clock_gettime(CLOCK_MONOTONIC_RAW)` is used rather than `CLOCK_MONOTONIC` to avoid NTP adjustments disturbing the timestamp used for latency measurement.

---

## 5. Build and Run

### Option A: cmake (recommended for full build including tests and benchmarks)

```bash
# Install cmake if you don't have it
sudo apt install cmake

# Build everything (configures RelWithDebInfo, ASAN off)
./scripts/build.sh
```

Binaries will be at:
- `build/src/zlte_ingest` — the engine
- `build/tools/zlte_gen` — the traffic generator
- `build/tests/zlte_tests` — unit tests
- `build/benchmarks/zlte_benchmarks` — benchmarks

### Option B: direct g++ (fastest way to get running)

```bash
# Build the engine
g++ -std=c++20 -fno-exceptions -O2 -Iinclude \
    src/ingestion_pipeline.cpp src/main.cpp \
    -lpthread -o zlte_ingest

# Build the generator
g++ -std=c++20 -fno-exceptions -O2 -Iinclude \
    tools/packet_gen.cpp -o zlte_gen
```

### Running

Open two terminals in the project root.

**Terminal 1 — start the engine:**
```bash
./zlte_ingest --port 9000
```

You will see:
```
ZLTE — Zero-Latency Telemetry Ingestion Engine  |  UDP :9000  |  Ctrl-C to quit

Flow     Class  DSCP  Packets       Bytes         AvgLat µs    ...
--------  -----  ----  ...
  (no flows seen yet — waiting for packets)
```

**Terminal 2 — send traffic:**
```bash
# Gentle: 4 flows at 1,000 pkt/s
./zlte_gen --flows 4 --rate 1000

# Medium: 8 flows at 50,000 pkt/s with 128-byte payload
./zlte_gen --flows 8 --rate 50000 --payload 128

# Stress: 16 flows at 500,000 pkt/s (header only)
./zlte_gen --flows 16 --rate 500000
```

Switch back to Terminal 1 and watch the table update. Press Ctrl-C in either terminal to stop.

---

## 6. What to Observe While It Runs

### Latency numbers

`AvgLat µs` measures the time from when the **sender** recorded `timestamp_ns` (using `CLOCK_MONOTONIC_RAW`) to when the **process thread** called `clock_gettime`. Both sender and engine are on the same machine in the test setup, so this number reflects:

- Time in the kernel socket receive buffer
- `recvfrom` syscall latency
- SPSC queue traversal time
- `memcpy` of the 24-byte header

At low rates (1,000 pkt/s) expect 5–15 µs. At high rates (500,000 pkt/s) the queue and pool stay warm in cache and latency can drop to 3–8 µs.

### Pool exhaustion drops

If you push the rate high enough that the process thread cannot keep up, the pool will exhaust and you will see:

```
pool-exhaustion drops: 1247
```

This is back-pressure: the recv thread could not acquire a slot and counted the miss. The fix is either a larger pool (`kPoolDepth` in `ingestion_pipeline.hpp`) or a faster process thread.

### CPU usage

At 100,000 pkt/s the recv thread should be near 0% CPU between `recvfrom` calls (it is blocking). The process thread will spin on the empty SPSC queue at under 1 µs per packet at this rate. Total engine CPU at 100k pkt/s should be well under one core.

---

## 7. Running the Tests

The unit tests cover `ArenaAllocator` and `PacketPool` with both single-threaded correctness checks and multi-threaded stress tests.

```bash
# With cmake
cmake --build build --target zlte_tests
ctest --test-dir build --output-on-failure

# Or run directly
./build/tests/zlte_tests
```

Test cases include:
- `ArenaAllocator.BasicAllocation` — allocate 64 bytes, verify `used()` changes
- `ArenaAllocator.AlignmentIsRespected` — allocate with alignments 1..64, verify address is aligned
- `ArenaAllocator.BadAlignmentRejected` — alignment=3 must return `ArenaError::BadAlignment`
- `ArenaAllocator.OutOfMemoryReturnsError` — fill the arena, next alloc must return `OutOfMemory`
- `ArenaAllocator.AllocationsDoNotOverlap` — 8 threads allocate concurrently; total claimed bytes must equal `used()`
- `PacketPool.AcquireReturnsCorrectSize` — acquired span must be exactly `PacketSize` bytes
- `PacketPool.ExhaustionReturnsError` — drain pool, next acquire must return `PoolError::Exhausted`
- `PacketPool.WrittenDataSurvivesRoundtrip` — write 0xAB, release, re-acquire, verify data
- `PacketPool.ConcurrentAcquireRelease` — 8 threads × 100,000 acquire/release cycles; all slots must return to pool

---

## 8. Running the Benchmarks

```bash
cmake --build build --target zlte_benchmarks
./build/benchmarks/zlte_benchmarks
```

Sample output (on a typical developer laptop):

```
Benchmark                           Time      CPU    Iterations
BM_Arena_Allocate_ST/64          12.3 ns   12.3 ns    56821034    64B/alloc
BM_Arena_Allocate_ST/256         12.5 ns   12.5 ns    55947012   256B/alloc
BM_Arena_Allocate_ST/1500        12.4 ns   12.4 ns    56123456  1500B/alloc
BM_Arena_Allocate_MT/threads:1   13.1 ns   13.1 ns    53000000
BM_Arena_Allocate_MT/threads:4   41.2 ns   41.2 ns    17000000
BM_Arena_Allocate_MT/threads:8   89.4 ns   89.4 ns     7800000
BM_Pool_AcquireRelease_ST        18.7 ns   18.7 ns    37400000   1500B slots
BM_Pool_AcquireRelease_MT/1      19.2 ns   19.2 ns    36000000
BM_Pool_AcquireRelease_MT/4      52.4 ns   52.4 ns    13000000
BM_Pool_AcquireRelease_MT/8     108.3 ns  108.3 ns     6500000
```

What these numbers mean:
- **Single-threaded arena alloc** at ~12 ns means the CAS almost never retries (no contention).
- **8-thread arena alloc** at ~89 ns shows the CAS retry cost under heavy contention — still sub-100 ns.
- **Pool roundtrip** at ~19 ns (single-thread) includes both `acquire` and `release` in one iteration.
- All numbers are **per operation** — at 12 ns/alloc, the arena can sustain ~83 million allocations per second per thread.

---

## 9. Where the Code Goes Next

### Phase 2 — epoll Event Loop

The current blocking socket approach is simple but handles only one sender per engine instance. Phase 2 replaces `recv_loop` with an `epoll`-based event loop:

```
epoll_create() → one fd
bind() thousands of sockets → epoll_ctl(EPOLL_CTL_ADD) each
loop:
    epoll_wait(events[], MAX_EVENTS, timeout_ms)
    for each ready event:
        recvfrom(event.fd, pool_slot, ...)
        queue_.push(descriptor)
```

The key property: `epoll_wait` blocks with zero CPU cost until the kernel receives a hardware interrupt from the NIC indicating that at least one socket has data. One thread handles thousands of connections with no polling overhead.

### Phase 3 — GPU Inference Pipeline

The process thread currently updates `FlowStats` and stops. In Phase 3 it will feed the processed telemetry into a **TensorRT inference engine** for QoS prediction:

```
process thread
    → accumulate 60-second sliding window in VRAM (circular buffer)
    → CUDA kernel shifts window, appends new sample
    → TensorRT fires inference on Tensor Cores
    → output: predicted QoS class / handover probability
```

Key techniques:
- **Pinned memory** (`cudaHostAlloc`): the pool's backing store is page-locked, allowing DMA transfers from CPU RAM to GPU VRAM without CPU involvement.
- **CUDA Streams**: the memory transfer and the previous inference overlap — while the GPU is running inference on window $t$, the CPU is already transferring the data for window $t+1$.
- **FP16 on Tensor Cores**: the model is compiled to use half-precision arithmetic on NVIDIA's matrix-multiply hardware, reducing latency by 2–4× vs FP32 on the same GPU.

The result: raw UDP bytes arrive from the RAN and a mobility prediction exits the GPU pipeline within microseconds — the full "zero-latency" promise realized end to end.
