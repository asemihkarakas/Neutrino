# Phase 0 — Deep Dive: Every Module, Every Decision

This document explains **why** every class, struct, constant, and line in Phase 0 exists the way it does. It is written for someone who can read C++ but has not written lock-free or systems-level code before. No assumption is made about prior knowledge of atomics, memory ordering, or network programming.

---

## Table of Contents

1. [The constraints that shaped everything](#1-the-constraints-that-shaped-everything)
2. [`result.hpp` — Error handling without exceptions](#2-resulthpp--error-handling-without-exceptions)
3. [`arena_allocator.hpp` — The lock-free memory slab](#3-arena_allocatorhpp--the-lock-free-memory-slab)
4. [`packet_pool.hpp` — Pre-allocated UDP buffers](#4-packet_poolhpp--pre-allocated-udp-buffers)
5. [`spsc_queue.hpp` — The wait-free conveyor belt](#5-spsc_queuehpp--the-wait-free-conveyor-belt)
6. [`telemetry.hpp` — The wire format](#6-telemetryhpp--the-wire-format)
7. [`ingestion_pipeline.hpp` — The pipeline class skeleton](#7-ingestion_pipelinehpp--the-pipeline-class-skeleton)
8. [`ingestion_pipeline.cpp` — The two threads](#8-ingestion_pipelinecpp--the-two-threads)
9. [`main.cpp` — The live dashboard](#9-maincpp--the-live-dashboard)
10. [`packet_gen.cpp` — The traffic gun](#10-packet_gencpp--the-traffic-gun)
11. [The build system](#11-the-build-system)
12. [How all the pieces fit together end-to-end](#12-how-all-the-pieces-fit-together-end-to-end)

---

## 1. The Constraints That Shaped Everything

Before any code was written, four hard rules were set in `CLAUDE.md`. Every design decision in Phase 0 traces back to one or more of these rules.

### Rule 1: C++20 only (`-std=c++20`)

C++20 introduced several features used directly in this codebase:
- **`requires` clauses** on templates — enforces compile-time preconditions instead of getting a cryptic linker error.
- **`std::span<T>`** — a non-owning view over a contiguous range. Used to represent packet buffer slices without copying.
- **`std::has_single_bit()`** — checks whether an integer is a power of two. Used to validate alignment arguments.
- **Designated initializers** (`PacketDescriptor{ .data = ptr, .data_size = n }`) — named struct initialization without a constructor.

### Rule 2: No exceptions (`-fno-exceptions`)

`throw` and `catch` are disabled at the compiler level. This means:
- No `std::bad_alloc` when memory runs out.
- No `std::runtime_error` from library calls.
- Every function that can fail **must communicate failure through its return type**.

This is not a restriction unique to this project. Virtually every real-time, embedded, and HFT codebase disables exceptions because:
1. Exception propagation requires the compiler to generate hidden stack-unwinding code at every call site, which bloats binary size.
2. Exception handling can stall a CPU pipeline at unpredictable times.
3. The cost model is invisible — you cannot easily reason about when a `throw` will occur.

### Rule 3: No `std::mutex` on the data path

`std::mutex::lock()` makes a system call (`futex`) that puts the calling thread to sleep and asks the OS scheduler to wake it later. On a Linux system, a context switch costs roughly 1,000–10,000 nanoseconds. At 100,000 packets per second, the inter-packet budget is 10,000 nanoseconds total. A single mutex acquisition consumes the entire budget.

The alternative is `std::atomic` — variables that map to single CPU instructions (`CMPXCHG`, `LOCK XADD`) that complete in 5–50 nanoseconds without involving the OS.

### Rule 4: `alignas(64)` on shared data structures

A CPU fetches data from RAM in 64-byte blocks called **cachelines**. If thread A writes to byte 0 and thread B writes to byte 8, those bytes may share a cacheline. Every time A writes, the CPU must invalidate B's copy of that cacheline across the core-to-core interconnect, even though B never touched byte 0. This is **false sharing** and it can reduce throughput by 90%.

`alignas(64)` on a variable or struct ensures it starts at a 64-byte boundary. Combined with padding or careful sizing, this guarantees that each hot variable lives on its own cacheline.

### Rule 5: No `std::string` on the hot path

`std::string` has three problems on a high-throughput data path:
1. It calls `new` for strings longer than the Small String Optimization (SSO) threshold (~15 bytes).
2. It owns its memory, so passing it between functions copies the bytes.
3. Its internal state (pointer + size + capacity) is 24–32 bytes, which pollutes the cacheline of any struct it lives in.

The replacements used here:
- `std::string_view` — a non-owning (pointer + length) pair. Zero allocation. Used for DSCP class names.
- `std::span<std::byte>` — same concept but for raw bytes. Used for packet buffers.
- `std::byte*` raw pointers — inside `PacketDescriptor`, where the smallest possible token is needed.

---

## 2. `result.hpp` — Error Handling Without Exceptions

### The problem

With exceptions disabled, a function like this cannot compile:

```cpp
void* allocate(size_t bytes) {
    if (bytes > capacity_)
        throw std::bad_alloc{};   // ERROR: exceptions are disabled
    return storage_ + offset_;
}
```

You need a way to return either a successful result or an error from the same function.

### Why not just return `nullptr` or `-1`?

Returning a sentinel value (`nullptr`, `-1`, `UINT64_MAX`) works, but has problems:
- The caller can ignore the return value. There is no compiler warning.
- There is no way to attach information about *which* error occurred.
- Different functions use different sentinel conventions, which is inconsistent.

### The `Result<T, E>` design

```cpp
template<typename T, typename E>
class [[nodiscard]] Result {
    static_assert(std::is_trivially_copyable_v<T>);
    static_assert(std::is_trivially_copyable_v<E>);
    static_assert(!std::is_same_v<T, E>);
    ...
};
```

`Result<T, E>` holds either a value of type `T` (success) or a value of type `E` (error). Never both, never neither.

**Why `static_assert(std::is_trivially_copyable_v<T>)`?**

`Result` is returned by value from functions and stored on the stack. If `T` or `E` had a non-trivial copy constructor (like `std::string` does), returning `Result` by value would invoke that constructor — potentially allocating heap memory. Restricting to trivially copyable types ensures the struct is just a raw bytes copy, with no hidden constructor calls.

**Why `static_assert(!std::is_same_v<T, E>)`?**

If `T` and `E` were the same type, `Result<int, int>` would be ambiguous: is the stored `int` the success value or the error code? Requiring distinct types makes the meaning unambiguous at the type level.

**Why `[[nodiscard]]` on the class?**

`[[nodiscard]]` tells the compiler to warn if the return value of a function is discarded without being checked. Since `Result` represents the outcome of a fallible operation, silently ignoring it is almost always a bug.

```cpp
arena.allocate(64, 64);   // WARNING: ignoring nodiscard return value
auto r = arena.allocate(64, 64);  // OK
if (!r) { /* handle error */ }
```

### The named constructor idiom

```cpp
[[nodiscard]] static constexpr Result ok(T v) noexcept {
    Result r;
    r.has_value_ = true;
    r.val_       = v;
    return r;
}

[[nodiscard]] static constexpr Result err(E e) noexcept {
    Result r;
    r.err_ = e;
    return r;
}
```

`ok()` and `err()` are static factory methods. You cannot construct `Result` directly — you must call one of these, which makes the intent explicit at the call site:

```cpp
return Result<void*, ArenaError>::ok(storage_ + claimed);
return Result<void*, ArenaError>::err(ArenaError::OutOfMemory);
```

### Why not use `std::expected<T, E>` (C++23)?

`std::expected` is the standard library equivalent of `Result<T, E>`, arriving in C++23. This project targets C++20. Rolling a minimal custom version keeps the dependency on the standard library minimal and gives complete control over the `static_assert` constraints.

---

## 3. `arena_allocator.hpp` — The Lock-Free Memory Slab

### What is a bump-pointer allocator?

The standard allocator (`new`/`malloc`) maintains a complex free-list data structure that must be protected by a global lock. Every `new` potentially acquires that lock.

A bump-pointer allocator is the opposite: it is a pre-allocated block of memory with a single integer `offset` that tracks how much has been used. Allocation is one operation: advance `offset` by the requested amount and return the old address.

```
Before alloc(64):                After alloc(64):
[used: 0..0][free: 0..Capacity]  [used: 0..64][free: 64..Capacity]
 ^offset=0                        ^offset=64
```

### The class template

```cpp
template<std::size_t Capacity>
    requires (Capacity > 0)
class ArenaAllocator {
```

`Capacity` is a **compile-time constant**. The storage array lives inside the object itself:

```cpp
alignas(64) std::byte storage_[Capacity];
```

This means the arena occupies exactly `Capacity` bytes of the object's footprint — no heap allocation. The trade-off is that you must declare large arenas as `static` or as members of long-lived heap-allocated objects, because putting a 64 MiB arena on the stack would overflow it.

### The atomic offset and the CAS loop

```cpp
alignas(64) std::atomic<std::size_t> offset_{0};
```

In a multi-threaded context, two threads could read `offset_` simultaneously and both think they own the same range:

```
Thread 1 reads offset=0, plans to claim [0..64)
Thread 2 reads offset=0, plans to claim [0..64)    ← same range — collision!
```

The solution is a **Compare-And-Swap (CAS)** loop:

```cpp
std::size_t current = offset_.load(std::memory_order_relaxed);
std::size_t claimed, next;

do {
    claimed = align_up(current, alignment);
    next    = claimed + bytes;
    if (next > Capacity)
        return Result<void*, ArenaError>::err(ArenaError::OutOfMemory);

} while (!offset_.compare_exchange_weak(
             current, next,
             std::memory_order_acq_rel,
             std::memory_order_relaxed));
```

`compare_exchange_weak(current, next, ...)` does this atomically at the hardware level:
- If `offset_` still equals `current`, set it to `next` and return `true` (success).
- If `offset_` was changed by another thread, load the new value into `current` and return `false` (retry).

`_weak` vs `_strong`: `compare_exchange_weak` is allowed to fail spuriously (even if the value matches) on some architectures. Inside a loop this is always correct and often faster because the compiler can avoid an extra memory barrier.

### Memory ordering on the CAS

`std::memory_order_acq_rel` on success means:
- **Acquire**: we see all writes by any thread that previously released on this atomic. So if another thread called `reset()` (which uses `memory_order_release`), we see the reset before we claim our range.
- **Release**: any thread that later acquires this atomic (another CAS, or a `load(acquire)`) will see the writes we make into `storage_[claimed..next)` after the CAS.

`std::memory_order_relaxed` on failure means: just reload `current` with the latest value of `offset_`. No fence needed since we're going to loop and try again.

### Alignment

```cpp
[[nodiscard]] static constexpr std::size_t
align_up(std::size_t v, std::size_t a) noexcept {
    return (v + a - 1u) & ~(a - 1u);
}
```

This rounds `v` up to the next multiple of `a`. It uses a bitmask trick: for power-of-two `a`, `~(a-1)` is a mask that zeroes the low bits. For example, with `a=64`: `~63 = 0xFFFFFFFFFFFFFFC0`, so `& ~63` clears the bottom 6 bits, rounding down. Adding `a-1=63` first ensures we round *up*.

Before performing alignment, we validate:

```cpp
if (!std::has_single_bit(alignment)) [[unlikely]]
    return Result<void*, ArenaError>::err(ArenaError::BadAlignment);
```

`std::has_single_bit(n)` returns true only if `n` is a non-zero power of two. The `align_up` bitmask trick only works for power-of-two alignments, so this check guards against incorrect usage.

### Memory layout — why two separate cachelines

```cpp
alignas(64) std::atomic<std::size_t> offset_{0};    // cacheline 0
alignas(64) std::byte storage_[Capacity];            // cacheline 1..N
```

`offset_` is written on every `allocate()` call. `storage_` is written by callers writing into their allocated ranges. If `offset_` and the start of `storage_` shared a cacheline, every CAS on `offset_` would invalidate the cacheline that a caller is writing into for a different thread's allocation. Separating them onto different cachelines eliminates this interference.

### The typed convenience overload

```cpp
template<typename T>
[[nodiscard]] Result<T*, ArenaError> allocate(std::size_t count = 1) noexcept {
    auto r = allocate(sizeof(T) * count, alignof(T));
    if (!r) return Result<T*, ArenaError>::err(r.error());
    return Result<T*, ArenaError>::ok(static_cast<T*>(r.value()));
}
```

This wraps the raw byte overload and deduces size and alignment from the type. `alignof(T)` is the natural alignment requirement of `T` as defined by the ABI.

---

## 4. `packet_pool.hpp` — Pre-Allocated UDP Buffers

### Why a separate pool from the arena?

The `ArenaAllocator` is designed for "allocate many, reset all at once" patterns. For packet buffers, we need individual `acquire` and `release` — take one buffer, use it, give it back, take another one. A reset-only allocator cannot model this lifecycle.

The `PacketPool` is designed specifically for this: a fixed set of identically-sized buffers that circulate between producer and consumer. No buffer is ever created or destroyed after startup.

### What is a Treiber stack?

A Treiber stack is a lock-free LIFO stack implemented with a single atomic head pointer and a CAS loop:

```
free_head → [slot 2] → [slot 1] → [slot 0] → null

acquire():
  head = free_head         // read head
  next = head->next        // peek at next
  CAS(free_head, head, next)  // try to pop head
  if success: return head

release(slot):
  slot->next = free_head   // point slot at current head
  CAS(free_head, old_head, slot)  // try to push slot
```

### The ABA problem and how the tag solves it

Consider this sequence:

```
Thread 1 reads free_head = slot_A
Thread 2: pops slot_A, pops slot_B, pushes slot_A back
Thread 1's CAS(free_head, slot_A, slot_B) succeeds
```

Thread 1 thinks slot_B is now head, but slot_B was already popped by thread 2 and may be in use. This is the ABA problem — the pointer looks the same (slot_A) but the state has changed.

The solution: pack a **monotonic tag** alongside the slot index in a single 64-bit integer.

```cpp
static constexpr std::uint64_t pack(std::uint32_t idx, std::uint32_t tag) noexcept {
    return (static_cast<std::uint64_t>(idx) << 32) |
           static_cast<std::uint64_t>(tag);
}
static constexpr std::uint32_t idx_of(std::uint64_t v) noexcept { return static_cast<std::uint32_t>(v >> 32); }
static constexpr std::uint32_t tag_of(std::uint64_t v) noexcept { return static_cast<std::uint32_t>(v); }
```

Every push increments the tag. Now the sequence becomes:

```
free_head = pack(slot_A, tag=5)
Thread 2 pops slot_A  → free_head = pack(slot_B, tag=6)
Thread 2 pops slot_B  → free_head = pack(null,   tag=7)
Thread 2 pushes slot_A → free_head = pack(slot_A, tag=8)
Thread 1's CAS(free_head, pack(slot_A,5), ...) FAILS — tag is now 8
```

Thread 1 retries with the fresh head `pack(slot_A, 8)` and reads the correct `next_idx`.

**Why `uint64_t` for the head?** A 64-bit atomic is always lock-free on any 64-bit platform. A 128-bit atomic (`__int128`) would be needed to store a full 64-bit pointer plus a 64-bit tag, but is not always lock-free. The `static_assert` at the top of the class verifies this:

```cpp
static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
              "64-bit atomic must be lock-free");
```

### The `Slot` struct

```cpp
struct alignas(64) Slot {
    std::byte              data[PacketSize];
    std::atomic<std::uint32_t> next_idx{kNull};
};
```

`alignas(64)` ensures each slot starts on a cacheline boundary. This is critical: when the recv thread writes into `slot[0].data` and the process thread reads `slot[1].data` (two different acquired slots), they must not fight over the same cacheline. Without `alignas(64)`, if `PacketSize` were not a multiple of 64, adjacent slots could share cachelines.

`next_idx` is `std::atomic<uint32_t>` rather than a plain `uint32_t`. During `acquire()`, the consumer reads `slots_[head_idx].next_idx`. Concurrently, a separate thread might be in the middle of a `release()` that is updating that same `next_idx`. Without atomics, this is a data race (undefined behaviour). The atomic ensures the read and write are coherent.

### Memory ordering in `acquire()`

```cpp
const std::uint32_t next_idx =
    slots_[head_idx].next_idx.load(std::memory_order_relaxed);
```

Why `relaxed` here? Because we already did an `acquire` load on `free_head_`. The `acquire` load on `free_head_` synchronises with the `release` store from the last `release()` call that pushed `head_idx` onto the stack. That synchronisation guarantees that the write to `slots_[head_idx].next_idx` (done before the `release` store in `release()`) is visible to us now.

On success:
```cpp
free_head_.compare_exchange_weak(curr, desired,
    std::memory_order_acq_rel,
    std::memory_order_acquire)
```

`acq_rel` on success: we acquire ownership of the slot, which means we see all prior writes to `slot.data` from previous owners. We release the head update so future acquirers see our write to `desired`.

`acquire` on failure: we need a fresh view of `free_head_` including whoever updated it, so we can correctly read their `next_idx`. A plain `relaxed` reload would not guarantee visibility.

### Memory ordering in `release()`

```cpp
slots_[slot_idx].next_idx.store(idx_of(curr), std::memory_order_relaxed);
desired = pack(slot_idx, tag_of(curr) + 1u);

while (!free_head_.compare_exchange_weak(
             curr, desired,
             std::memory_order_release,
             std::memory_order_relaxed));
```

`next_idx` is stored relaxed because it will only be read by `acquire()` after observing the `release` store on `free_head_`. The `release` store on `free_head_` is the "fence" that makes `next_idx` visible.

`release` on CAS success: any future `acquire()` that reads this head with `acquire` will see the `next_idx` store we did above, and will also see any writes we made to `slot.data` before calling `release()` on the pool. This is the mechanism that ensures packet data written by the process thread is visible to the next acquirer of that slot.

### Building the free list at startup

```cpp
void build_free_list() noexcept {
    for (std::uint32_t i = 0; i < static_cast<std::uint32_t>(PoolDepth) - 1u; ++i)
        slots_[i].next_idx.store(i + 1u, std::memory_order_relaxed);
    slots_[PoolDepth - 1u].next_idx.store(kNull, std::memory_order_relaxed);
    free_head_.store(pack(0u, 0u), std::memory_order_release);
}
```

This chains `slot[0] → slot[1] → slot[2] → ... → slot[N-1] → null`. The final `release` store on `free_head_` publishes the entire chain atomically. All the `relaxed` stores to `next_idx` are made visible by this single `release` fence.

---

## 5. `spsc_queue.hpp` — The Wait-Free Conveyor Belt

### SPSC vs MPMC

**SPSC (Single-Producer Single-Consumer)**: exactly one thread calls `push()`, exactly one calls `pop()`. This constraint allows a dramatically simpler implementation — no CAS loop, just two atomic loads and two atomic stores per operation.

**MPMC (Multi-Producer Multi-Consumer)**: any thread can call `push()` or `pop()`. Requires CAS loops or more complex coordination. Much more expensive.

The pipeline has exactly two threads involved in the queue: the recv thread produces, the process thread consumes. SPSC is the right choice.

### Power-of-two capacity

```cpp
template<typename T, std::size_t Capacity>
    requires (Capacity > 1 && (Capacity & (Capacity - 1)) == 0)
```

The `requires` clause enforces at compile time that `Capacity` is a non-zero power of two. This enables the wrap-around trick:

```cpp
static constexpr std::size_t kMask = Capacity - 1u;
// instead of: index = (index + 1) % Capacity;
//        use: index = (index + 1) & kMask;
```

For a power-of-two `N`, `x % N == x & (N-1)` because `N-1` is a bitmask of all 1s in the low bits. The bitmask version is one instruction on any CPU; modulo requires a division instruction which is 20-90× slower.

### Memory layout

```cpp
alignas(64) std::atomic<std::size_t> head_{0};   // consumer-owned, its own cacheline
alignas(64) std::atomic<std::size_t> tail_{0};   // producer-owned, its own cacheline
alignas(64) std::array<T, Capacity>  buf_{};      // data, its own cachelines
```

`head_` is written only by the consumer. `tail_` is written only by the producer. If they shared a cacheline, every write by the producer to `tail_` would invalidate the consumer's copy of `head_` (and vice versa), causing unnecessary cache coherence traffic.

`buf_` is on its own cacheline start. The producer writes `buf_[tail]` and the consumer reads `buf_[head]`. These are different indices most of the time, so they rarely share a cacheline — but starting `buf_` on its own cacheline prevents it from clashing with `tail_` or `head_`.

### `push` — producer side

```cpp
bool push(const T& item) noexcept {
    const std::size_t tail = tail_.load(std::memory_order_relaxed);
    const std::size_t next = (tail + 1u) & kMask;
    if (next == head_.load(std::memory_order_acquire))
        return false;   // full
    buf_[tail] = item;
    tail_.store(next, std::memory_order_release);
    return true;
}
```

Step by step:

1. **`tail_.load(relaxed)`**: only the producer writes `tail_`, so reading it with `relaxed` is safe — no other thread will have changed it.

2. **Full check: `next == head_.load(acquire)`**: the producer reads `head_` with `acquire` to synchronize with the consumer's last `head_.store(release)`. This ensures the producer sees the consumer's latest progress and doesn't overwrite a slot the consumer hasn't read yet. The "full" condition is `next == head` (the next write position equals the read position), which means the ring is full. One slot is always wasted: if `head == tail`, the ring is empty; if `(tail + 1) % N == head`, the ring is full.

3. **`buf_[tail] = item`**: write the item. This happens *before* the release store on `tail_`, so it will be visible to the consumer when the consumer's acquire load on `tail_` sees the new `next` value.

4. **`tail_.store(next, release)`**: publish. Any consumer that subsequently does `tail_.load(acquire)` will see `next` and will also see the write to `buf_[tail]`.

### `pop` — consumer side

```cpp
bool pop(T& out) noexcept {
    const std::size_t head = head_.load(std::memory_order_relaxed);
    if (head == tail_.load(std::memory_order_acquire))
        return false;   // empty
    out = buf_[head];
    head_.store((head + 1u) & kMask, std::memory_order_release);
    return true;
}
```

1. **`head_.load(relaxed)`**: only the consumer writes `head_`, safe.

2. **Empty check: `head == tail_.load(acquire)`**: the consumer reads `tail_` with `acquire` to synchronize with the producer's `tail_.store(release)`. If `tail_ > head`, the producer has published at least one new item. The `acquire` guarantees the write to `buf_[head]` (done before the producer's `release`) is now visible to us.

3. **`out = buf_[head]`**: read the item safely, because the acquire-release pair guarantees visibility.

4. **`head_.store(release)`**: advance head, publishing to the producer that this slot is now free. The producer's next `head_.load(acquire)` will see this.

### Why this is wait-free, not just lock-free

`push` and `pop` complete in a **bounded** number of steps regardless of what other threads do. There is no retry loop. Either the queue is full/empty (and we return false immediately), or we succeed in exactly four operations. This is the definition of wait-free.

The `PacketPool::acquire` and `ArenaAllocator::allocate` are lock-free (they have a CAS retry loop) but not wait-free (if contention is very high, a thread could theoretically spin for a long time). In practice, for the packet pool and arena with expected contention levels, the CAS succeeds on the first try almost always.

---

## 6. `telemetry.hpp` — The Wire Format

### The `TelemetryHeader` struct

```cpp
struct TelemetryHeader {
    std::uint32_t magic;         // 4 bytes, offset 0
    std::uint8_t  version;       // 1 byte,  offset 4
    std::uint8_t  dscp;          // 1 byte,  offset 5
    std::uint16_t payload_len;   // 2 bytes, offset 6
    std::uint32_t flow_id;       // 4 bytes, offset 8
    std::uint32_t seq;           // 4 bytes, offset 12
    std::uint64_t timestamp_ns;  // 8 bytes, offset 16
};                               // total:   24 bytes
```

**Why is there no padding?** The C++ compiler inserts padding between struct members to ensure each field is naturally aligned (a `uint32_t` at an offset divisible by 4, a `uint64_t` at an offset divisible by 8, etc.). The fields are ordered so that every field's offset is already a multiple of its size:

| Field | Size | Offset | Offset % Size |
|---|---|---|---|
| `magic` | 4 | 0 | 0 ✓ |
| `version` | 1 | 4 | 0 ✓ |
| `dscp` | 1 | 5 | 0 ✓ |
| `payload_len` | 2 | 6 | 0 ✓ |
| `flow_id` | 4 | 8 | 0 ✓ |
| `seq` | 4 | 12 | 0 ✓ |
| `timestamp_ns` | 8 | 16 | 0 ✓ |
| *total* | | 24 | |

No `__attribute__((packed))` or `#pragma pack` needed. The `static_assert` enforces this at compile time:

```cpp
static_assert(sizeof(TelemetryHeader) == 24, "TelemetryHeader must be 24 bytes");
```

### The magic number

```cpp
static constexpr std::uint32_t kTelemetryMagic = 0x5A4C5445u; // "ZLTE"
```

`0x5A = 'Z'`, `0x4C = 'L'`, `0x54 = 'T'`, `0x45 = 'E'`. The magic is checked in `process_loop` before parsing any other field. Any packet that does not start with these 4 bytes is silently discarded. This protects against accidental traffic from other applications using the same port.

### The version field

`version` allows future protocol changes without breaking existing engines. The engine currently only accepts `version == 1`. If a sender starts sending `version == 2` packets (with a different layout), the engine discards them gracefully until it is updated to understand v2.

### DSCP and `dscp_class()`

DSCP (Differentiated Services Code Point) is a 6-bit field in the IP header that encodes a packet's priority class according to IETF RFC 4594. The `dscp` field in the telemetry header carries the DSCP value the sender assigned to the flow. The engine uses it to classify flows into QoS buckets.

```cpp
constexpr std::string_view dscp_class(std::uint8_t dscp) noexcept {
    if (dscp == 46)               return "EF";    // Expedited Forwarding — voice, lowest jitter
    if (dscp >= 32u && dscp <= 39u) return "AF4x"; // Assured Forwarding class 4 — realtime video
    if (dscp >= 24u && dscp <= 31u) return "AF3x"; // Assured Forwarding class 3 — streaming
    if (dscp >= 16u && dscp <= 23u) return "AF2x"; // Assured Forwarding class 2 — transactional
    if (dscp >= 8u  && dscp <= 15u) return "AF1x"; // Assured Forwarding class 1 — bulk
    if (dscp == 48u)               return "CS6";   // Control traffic (routing protocols)
    return "BE";                                   // Best Effort — everything else
}
```

This is `constexpr` — the compiler can evaluate it at compile time if `dscp` is a constant expression. It returns `std::string_view` pointing into string literals (static storage), so there is no allocation.

### `PacketDescriptor`

```cpp
struct PacketDescriptor {
    std::byte*    data;       // pointer into a PacketPool slot
    std::size_t   data_size;  // actual received bytes
    std::uint32_t src_ip;     // sender IP (network byte order)
    std::uint16_t src_port;   // sender port (host byte order)
};
```

This is the token that travels through the `SpscQueue`. Its size:
- `std::byte*` — 8 bytes
- `std::size_t` — 8 bytes
- `std::uint32_t` — 4 bytes
- `std::uint16_t` — 2 bytes
- (2 bytes padding to align to 8) — implicit
- Total: 24 bytes

24 bytes fits in 1/2 a cacheline. The `SpscQueue<PacketDescriptor, 4096>` buffer holds 4096 × 24 = 96 KiB — fits entirely in L2 cache on most CPUs.

**Why `src_ip` in network byte order but `src_port` in host byte order?**

`src_ip` comes from `sockaddr_in.sin_addr.s_addr` which is stored in network byte order (big-endian) by the kernel. It is stored as-is to avoid a byte-swap that the process thread would need to undo. `src_port` is converted with `ntohs()` in `recv_loop` for human readability in the dashboard and future logging.

---

## 7. `ingestion_pipeline.hpp` — The Pipeline Class Skeleton

### Constants and their values

```cpp
static constexpr std::size_t kPacketSize  = 2048;
static constexpr std::size_t kPoolDepth   = 1024;
static constexpr std::size_t kQueueDepth  = 4096;
static constexpr std::size_t kMaxFlows    = 1024;
```

**`kPacketSize = 2048`**: Ethernet frames have a maximum payload (MTU) of 1500 bytes. A UDP datagram's payload can be up to 65,507 bytes, but in practice is always limited to the MTU to avoid IP fragmentation. 2048 bytes gives comfortable headroom above 1500 with room for the `TelemetryHeader` (24 bytes), application payload, and any future header growth. It is a power of two for alignment convenience.

**`kPoolDepth = 1024`**: at 100,000 packets/second, the process thread has 10 µs per packet. The recv thread can fill at most ~10 slots before the process thread catches up. 1024 slots is more than 100× the maximum in-flight count, ensuring the pool never exhausts under normal operation.

**`kQueueDepth = 4096`**: the SPSC queue must hold packets for the time it takes the process thread to drain them. At 100k pkt/s, a 4096-slot queue represents 40 ms of burst absorption. This is far larger than any realistic processing latency.

**`kMaxFlows = 1024`**: flow statistics are stored in a flat array indexed by `flow_id % kMaxFlows`. If more than 1024 distinct `flow_id` values arrive, different flows hash to the same slot (a collision). Increasing this constant reduces collisions at the cost of more memory (each `FlowStats` is ~128 bytes, so 1024 × 128 = 128 KiB — fits in L2 cache).

### `FlowStats`

```cpp
struct alignas(64) FlowStats {
    std::atomic<std::uint64_t> packets{0};
    std::atomic<std::uint64_t> bytes{0};
    std::atomic<std::uint64_t> latency_sum_ns{0};
    std::atomic<std::uint64_t> latency_min_ns{UINT64_MAX};
    std::atomic<std::uint64_t> latency_max_ns{0};
    std::atomic<std::uint8_t>  dscp{0};
    std::atomic<bool>          active{false};
};
```

**Why all fields are `std::atomic`**: the process thread writes these fields; the main thread reads them for display. Without atomics, concurrent read/write is a data race — undefined behaviour in C++. With `std::atomic` and `memory_order_relaxed`, both threads operate on the fields independently with no synchronization cost. The display is eventually consistent: it may show values that are 1 second old, which is acceptable for a monitoring table.

**Why `latency_min_ns` is initialized to `UINT64_MAX`**: the minimum is updated with "is the new value smaller than the current minimum?" When we haven't seen any packets yet, the minimum should be considered "infinity" so that the first real latency value always replaces it. `UINT64_MAX` is the largest possible `uint64_t`, so any real latency is smaller.

**`alignas(64)` on the struct**: with `kMaxFlows = 1024` flows, if the process thread updates `flow_stats_[0]` and `flow_stats_[1]` in two packets processed back-to-back, and those two structs share a cacheline, there is self-induced false sharing. `alignas(64)` ensures each `FlowStats` sits on its own cacheline boundary. `sizeof(FlowStats)` needs to be verified to be a multiple of 64 (it is: 7 × 8 bytes for the atomics + padding = 64 bytes).

### The `IngestionPipeline` member layout

```cpp
alignas(64) std::atomic<bool>          running_{false};
alignas(64) std::atomic<std::uint64_t> dropped_{0};

PacketPool<kPacketSize, kPoolDepth>       pool_;
SpscQueue<PacketDescriptor, kQueueDepth>  queue_;
std::array<FlowStats, kMaxFlows>          flow_stats_;

std::thread recv_thread_;
std::thread process_thread_;
```

`running_` and `dropped_` are each on their own cacheline. `running_` is read by both threads every loop iteration. `dropped_` is written by the recv thread on every pool-exhaustion event. If they shared a cacheline with each other or with other fields, every update to `dropped_` would invalidate the cacheline that both threads are reading for `running_`, introducing unnecessary cache traffic.

`pool_`, `queue_`, and `flow_stats_` are large objects whose internal layouts handle their own alignment. They do not need additional `alignas` here.

`std::thread` members are stored directly (not `std::unique_ptr<std::thread>`) because the class is already non-copyable (copy constructor and assignment are deleted), so there is no risk of accidentally copying the thread handles.

---

## 8. `ingestion_pipeline.cpp` — The Two Threads

### `now_ns()` — why `CLOCK_MONOTONIC_RAW`

```cpp
static std::uint64_t now_ns() noexcept {
    struct timespec ts{};
    ::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(ts.tv_nsec);
}
```

There are three relevant clocks on Linux:

| Clock | Characteristics |
|---|---|
| `CLOCK_REALTIME` | Wall-clock time, can jump backwards due to NTP or leap seconds |
| `CLOCK_MONOTONIC` | Monotonic, but subject to NTP frequency adjustments (slewing) |
| `CLOCK_MONOTONIC_RAW` | Monotonic, driven by the hardware oscillator, not adjusted by NTP |

For latency measurement (sender timestamp → receiver timestamp), any NTP adjustment on either machine would corrupt the measurement. `CLOCK_MONOTONIC_RAW` gives the rawest hardware time available. Both `packet_gen` (sender) and `ingestion_pipeline` (receiver) use this clock, so the latency calculation is consistent.

### `start()` — the single-shot CAS

```cpp
bool expected = false;
if (!running_.compare_exchange_strong(expected, true,
        std::memory_order_acq_rel, std::memory_order_relaxed))
    return Result<bool, PipelineError>::err(PipelineError::AlreadyRunning);
```

`compare_exchange_strong` (not `_weak`) is used here because this is not in a retry loop. We want a definitive answer: either `running_` was `false` and we set it to `true` (we own the start), or it was already `true` (someone else already started it). `_strong` does not have spurious failures.

**Why `acq_rel` on success?** The `acquire` part ensures we see all prior state (e.g., if `stop()` was called previously, we see the effects of that stop). The `release` part ensures the threads we are about to launch will see `running_ == true`.

### Socket setup

```cpp
const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
```

`SOCK_DGRAM` is UDP. No `SOCK_NONBLOCK` — the socket is blocking, paired with `SO_RCVTIMEO`:

```cpp
struct timeval tv{};
tv.tv_usec = 200'000;   // 200 ms
::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
```

With `SO_RCVTIMEO`, `recvfrom` blocks for at most 200 ms and then returns `EAGAIN`. This means the recv thread can check `running_` every 200 ms, which allows clean shutdown without burning 100% CPU in a spin loop. 200 ms was chosen as a balance: short enough for responsive shutdown, long enough to avoid frequent wakeups when the sender is quiet.

```cpp
const int rcvbuf = 8 << 20;  // 8 MiB
::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
```

The kernel's socket receive buffer absorbs packets that arrive faster than `recvfrom` can drain them. At 500,000 pkt/s × 2048 bytes/pkt = ~1 GB/s, the kernel buffer can save packets during the brief delays when the recv thread is processing the SPSC push. 8 MiB holds roughly 4096 full-size packets — enough to absorb a 8 ms burst at 500k pkt/s.

### `stop()` — shutdown order matters

```cpp
void IngestionPipeline::stop() noexcept {
    if (!running_.exchange(false, std::memory_order_acq_rel))
        return;  // already stopped

    if (socket_fd_ >= 0) {
        ::close(socket_fd_);
        socket_fd_ = -1;
    }

    if (recv_thread_.joinable())    recv_thread_.join();
    if (process_thread_.joinable()) process_thread_.join();
}
```

The order is: set `running_` false → close socket → join recv → join process.

**Why close the socket before joining?** The recv thread may be blocked inside `recvfrom`. Closing the socket file descriptor causes the `recvfrom` call to return with an error (typically `EBADF` or `EINTR`). Without closing the socket, the recv thread would wait up to 200 ms for the `SO_RCVTIMEO` timeout before checking `running_`. Closing the socket makes the shutdown immediate.

**Why join recv before joining process?** The recv thread pushes to the queue; the process thread pops. If we join the process thread first while the recv thread is still running, the recv thread could spin on `queue_.push` forever (if the queue is full with nobody popping). Joining recv first ensures the queue producer stops before we stop the consumer.

**`exchange(false, acq_rel)` vs `store(false, release)`**: `exchange` returns the old value, which lets us detect if `stop()` was already called (`if (!running_.exchange(false))` — if it returned false, it was already stopped). This makes `stop()` idempotent: calling it twice is safe.

### `recv_loop(int fd)` — why `fd` is passed by value

```cpp
void IngestionPipeline::recv_loop(int fd) noexcept {
```

The socket file descriptor is passed as a function argument to the thread, not read from `socket_fd_` inside the loop. This avoids a data race: `stop()` sets `socket_fd_ = -1` concurrently with the recv thread running. If the recv thread read `socket_fd_` in a loop, it could observe the `-1` value at any moment and crash. By capturing `fd` once at thread start (when `socket_fd_` is valid), the recv thread operates on its own stable copy of the file descriptor for its entire lifetime.

### Pool exhaustion back-pressure

```cpp
auto slot_r = pool_.acquire();
if (!slot_r) [[unlikely]] {
    dropped_.fetch_add(1u, std::memory_order_relaxed);
    continue;
}
```

If the pool is empty, we count the drop and immediately retry. This is intentional back-pressure: if the process thread is too slow to release buffers, the recv thread stops accepting packets from the kernel. The kernel's `SO_RCVBUF` (8 MiB) acts as the next level of buffering, absorbing packets during this stall.

The alternative — blocking until a pool slot is available — would mean holding no slot while blocking, but then we would lose the position in the receive queue. The current approach (count and retry) is transparent: you can see drops in the dashboard.

### The queue push spin

```cpp
while (!queue_.push(desc)) {
    if (!running_.load(std::memory_order_relaxed)) {
        pool_.release(slot);
        return;
    }
}
```

If the SPSC queue is full (4096 packets are waiting to be processed), the recv thread spins. The `running_` check inside the spin ensures we don't spin forever if `stop()` is called while the queue is full. When `running_` becomes false, we release the slot back to the pool (to avoid a leak) and return.

### `process_loop()` — release the slot immediately

```cpp
std::memcpy(&hdr, desc.data, sizeof(TelemetryHeader));
pool_.release(std::span<std::byte>{desc.data, kPacketSize});  // ← immediately after memcpy
```

The pool slot is released as soon as the 24-byte header is copied into the stack-local `hdr`. The rest of the processing (stat updates) works from `hdr`, not from the pool slot. This minimizes the time any slot is "in-flight" in the process thread, keeping pool pressure low and maximizing the number of free slots available to the recv thread.

### CAS loops for latency min/max

```cpp
std::uint64_t cur = fs.latency_min_ns.load(std::memory_order_relaxed);
while (latency_ns < cur &&
       !fs.latency_min_ns.compare_exchange_weak(
           cur, latency_ns, std::memory_order_relaxed, std::memory_order_relaxed))
{}
```

This is a read-modify-write operation on an atomic without a lock. The pattern:
1. Load current minimum.
2. If the new value is smaller, try to CAS it in.
3. If CAS fails (another writer — though in this design there is only one writer, the process thread — changed the value), reload and retry.

In this codebase, `process_loop` is the only writer to `FlowStats`, so the CAS will always succeed on the first try in practice. The CAS is still needed because without it, the load-compare-store is not atomic and is technically a data race with the main thread's read.

---

## 9. `main.cpp` — The Live Dashboard

### `volatile bool g_running`

```cpp
static volatile bool g_running = true;
static void on_signal(int) noexcept { g_running = false; }
```

Signal handlers run in an asynchronous context — they can interrupt any instruction in the main thread. `volatile` tells the compiler not to cache `g_running` in a register; it must re-read it from memory on every access. Without `volatile`, the compiler might optimize `while (g_running)` into an infinite loop because it cannot see where `g_running` changes.

Note: `volatile` alone is not sufficient for synchronization between threads. Here it is correct because signal handlers and the main thread are not truly concurrent — a signal handler interrupts the main thread rather than running simultaneously. For cross-thread flags, `std::atomic<bool>` (as used in `IngestionPipeline::running_`) is the correct tool.

### ANSI terminal clear

```cpp
std::printf("\033[2J\033[H");
```

- `\033[2J` — erase the entire screen.
- `\033[H` — move the cursor to row 1, column 1 (top-left).

This pair gives a flicker-free refresh: the terminal replaces the previous content in-place rather than scrolling. The output appears as a live dashboard updating every second.

### Error switch

```cpp
switch (r.error()) {
    case zlte::PipelineError::SocketFailed:   reason = "socket() failed";  break;
    case zlte::PipelineError::BindFailed:     reason = "bind() failed — port in use?"; break;
    case zlte::PipelineError::AlreadyRunning: reason = "already running";  break;
}
```

`switch` on an enum class generates a compile warning if a case is missing. This ensures that if a new `PipelineError` value is added later, the compiler reminds every switch statement to handle it. This is the C++20-idiomatic way to handle exhaustive matching without pattern matching.

---

## 10. `packet_gen.cpp` — The Traffic Gun

### `busy_sleep_until()` — nanosecond-precision pacing

```cpp
static void busy_sleep_until(std::uint64_t target_ns) noexcept {
    const std::uint64_t now = now_ns();
    if (target_ns <= now) return;
    const std::uint64_t rem = target_ns - now;
    if (rem > 500'000u) {
        const std::uint64_t sleep_ns = rem - 100'000u;
        struct timespec ts{ ... };
        ::nanosleep(&ts, nullptr);
    }
    while (now_ns() < target_ns) {}  // precision spin
}
```

`nanosleep` is accurate to roughly 100–500 µs (it depends on the kernel tick rate, usually 250 Hz or 1000 Hz). For sub-100 µs precision, the only option is a CPU spin loop. The hybrid approach:
1. Sleep for most of the wait time using `nanosleep` to yield the CPU to other processes.
2. Spin the last 100 µs for precision.

This keeps CPU usage low at moderate rates while maintaining accurate packet pacing at high rates.

### Batch interval calculation

```cpp
const std::uint64_t batch_ns = (rate > 0u)
    ? (1'000'000'000ULL * static_cast<std::uint64_t>(flows)) / rate
    : 0u;
```

One iteration of the outer loop sends `flows` packets (one per flow). The time budget for one iteration is:

```
batch_ns = 1 second / (rate / flows)
         = 1,000,000,000 ns × flows / rate
```

For `flows=4, rate=10000`: `batch_ns = 1e9 × 4 / 10000 = 400,000 ns = 400 µs`. The generator sends 4 packets then sleeps 400 µs, repeating 2500 times per second = 10,000 packets/second.

### DSCP table cycling

```cpp
static constexpr std::uint8_t kDscpTable[] = {46u, 34u, 26u, 18u, 10u, 0u};
hdr.dscp = kDscpTable[f % std::size(kDscpTable)];
```

`std::size(kDscpTable)` is a C++17/20 idiom for the array length. Flow 0 gets DSCP 46 (EF), flow 1 gets 34 (AF41), flow 2 gets 26 (AF31), and so on. With 6 entries in the table and any number of flows, the pattern repeats cyclically. This produces varied DSCP values in the dashboard without needing explicit per-flow configuration.

---

## 11. The Build System

### Why FetchContent and not system packages

`apt install libgtest-dev` installs whatever version Ubuntu ships, which may not match the version the code was written against. `FetchContent` pins an exact version (`v1.15.2` for GTest, `v1.9.1` for Google Benchmark), so the build is reproducible on any machine with internet access.

### Why compile flags are declared after FetchContent

```cmake
# FetchContent first:
FetchContent_MakeAvailable(googlebenchmark)
FetchContent_MakeAvailable(googletest)

# Then our strict flags:
add_compile_options(-Wall -Wextra -Werror -Wpedantic ...)
```

`add_compile_options` applies to all targets defined in the current `CMakeLists.txt` and all subsequent `add_subdirectory` calls. If it were placed before `FetchContent_MakeAvailable`, the strict `-Werror` would apply to the Google Benchmark and Google Test source code — which don't build cleanly under `-Wsign-conversion` and `-Wconversion`. Placing our flags after the dependencies means the dependencies build with the compiler's defaults and only our own code gets the strict warnings.

### The `ENABLE_ASAN` option

```cmake
option(ENABLE_ASAN "Enable AddressSanitizer" ON)
if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(   -fsanitize=address)
endif()
```

ASan is ON by default so that running `cmake -S . -B build && cmake --build build` gives you an instrumented binary automatically — making it harder to accidentally ship un-instrumented code during development. Production builds use `-DENABLE_ASAN=OFF` (which `scripts/build.sh` does by default in its `RelWithDebInfo` configuration).

`-fno-omit-frame-pointer` preserves the frame pointer register so that ASan's stack trace tool can walk the call stack and show you exactly which line caused a memory error.

---

## 12. How All the Pieces Fit Together End-to-End

Here is the exact sequence of events from the moment a packet leaves `packet_gen` to the moment its latency appears in the dashboard.

```
packet_gen (sender process)
│
│  1. hdr.timestamp_ns = clock_gettime(CLOCK_MONOTONIC_RAW)
│  2. sendto(fd, &hdr, 24, ...)
│
└─→ [Linux kernel UDP stack]
       └─→ [SO_RCVBUF ring buffer, 8 MiB]
              └─→ recv thread (blocked in recvfrom)

recv thread
│
│  3. slot = pool_.acquire()                 // pop a free buffer from the Treiber stack
│  4. n = recvfrom(fd, slot.data, 2048)      // kernel copies packet bytes into pool slot
│  5. desc = {.data = slot.data, .data_size = n, .src_ip = ..., .src_port = ...}
│  6. queue_.push(desc)                      // release-store on tail_ publishes to process thread
│
└─→ SpscQueue (in-memory, 4096 slots × 24 bytes)
       └─→ process thread (spinning on queue_.pop)

process thread
│
│  7. queue_.pop(desc)                       // acquire-load on tail_ sees release from step 6
│  8. recv_ns = clock_gettime(CLOCK_MONOTONIC_RAW)
│  9. memcpy(&hdr, desc.data, 24)            // copy 24-byte header off the pool slot
│  10. pool_.release({desc.data, 2048})      // push slot back onto Treiber stack
│  11. validate hdr.magic and hdr.version
│  12. latency_ns = recv_ns - hdr.timestamp_ns
│  13. fs = flow_stats_[hdr.flow_id % 1024]
│  14. fs.packets.fetch_add(1, relaxed)
│  15. fs.bytes.fetch_add(n, relaxed)
│  16. fs.latency_sum_ns.fetch_add(latency_ns, relaxed)
│  17. CAS loop: update fs.latency_min_ns if latency_ns < current min
│  18. CAS loop: update fs.latency_max_ns if latency_ns > current max
│
└─→ flow_stats_[] (in-memory, 1024 × alignas(64) atomic structs)
       └─→ main thread (sleeping in sleep_for(1s))

main thread  [every 1 second]
│
│  19. printf("\033[2J\033[H")              // clear screen
│  20. for each flow_stats_[i] where active == true:
│        packets  = fs.packets.load(relaxed)
│        avg_us   = (fs.latency_sum_ns.load(relaxed) / packets) / 1000
│        max_us   = fs.latency_max_ns.load(relaxed) / 1000
│        printf(table row)
```

**Steps 6 → 7** are the SPSC synchronization point. The `release` store in step 6 and the `acquire` load in step 7 form a happens-before edge: everything done in steps 1–5 is visible to the process thread after step 7.

**Step 10 happens before steps 11–18**. The pool slot is returned as soon as the header is copied. This minimises the "pool pressure window" — the amount of time a slot is unavailable to the recv thread.

**Steps 14–18 use `relaxed` ordering** because the process thread is the only writer. The main thread reads them with `relaxed` too. There is no synchronization requirement between the write and the display — stale-by-one-second is fine for a monitoring dashboard.

The entire path from UDP arrival (step 4) to stat update (step 18) involves:
- 1 `recvfrom` syscall
- 1 `pool_.acquire` (one 64-bit CAS)
- 1 `queue_.push` (one atomic store)
- 1 `queue_.pop` (one atomic load)
- 1 `pool_.release` (one 64-bit CAS)
- 1 `memcpy` of 24 bytes
- 5 `fetch_add` operations
- 2 CAS loops (min/max, nearly always one iteration)

No heap allocation, no mutex, no OS call after `recvfrom`. This is the zero-latency data path.
