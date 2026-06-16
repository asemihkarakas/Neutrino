#include <zlte/arena_allocator.hpp>
#include <zlte/packet_pool.hpp>

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

// ---------------------------------------------------------------------------
// ArenaAllocator benchmarks
// ---------------------------------------------------------------------------

// Single-threaded throughput: how fast can we bump-allocate?
static void BM_Arena_Allocate_ST(benchmark::State& state) {
    static zlte::ArenaAllocator<64u << 20u> arena; // 64 MiB static
    const auto bytes = static_cast<std::size_t>(state.range(0));

    for (auto _ : state) {
        auto r = arena.allocate(bytes, 64u);
        if (!r) [[unlikely]] {
            arena.reset();
            benchmark::DoNotOptimize(arena.allocate(bytes, 64u));
        } else {
            benchmark::DoNotOptimize(r.value());
        }
    }

    state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                             static_cast<std::int64_t>(bytes));
    state.SetLabel(std::to_string(bytes) + "B/alloc");
}
BENCHMARK(BM_Arena_Allocate_ST)
    ->Arg(64)->Arg(256)->Arg(1500)
    ->Unit(benchmark::kNanosecond);

// Multi-threaded: all threads contend on the same arena.
static void BM_Arena_Allocate_MT(benchmark::State& state) {
    static zlte::ArenaAllocator<256u << 20u> arena; // 256 MiB static
    static std::atomic<bool> reset_flag{false};

    for (auto _ : state) {
        auto r = arena.allocate(64u, 64u);
        if (!r) [[unlikely]] {
            // Only one thread resets; others spin briefly until offset_ drops.
            bool expected = false;
            if (reset_flag.compare_exchange_strong(expected, true,
                    std::memory_order_acq_rel)) {
                arena.reset();
                reset_flag.store(false, std::memory_order_release);
            }
        } else {
            benchmark::DoNotOptimize(r.value());
        }
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Arena_Allocate_MT)
    ->Threads(1)->Threads(2)->Threads(4)->Threads(8)
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// PacketPool benchmarks
// ---------------------------------------------------------------------------

// Single-threaded acquire + release roundtrip.
static void BM_Pool_AcquireRelease_ST(benchmark::State& state) {
    zlte::PacketPool<1500u, 512u> pool;

    for (auto _ : state) {
        auto r = pool.acquire();
        benchmark::DoNotOptimize(r);
        if (r) [[likely]] pool.release(r.value());
    }

    state.SetItemsProcessed(state.iterations());
    state.SetLabel("1500B slots");
}
BENCHMARK(BM_Pool_AcquireRelease_ST)->Unit(benchmark::kNanosecond);

// Multi-threaded: each thread independently acquires and immediately releases.
// Models the steady-state producer/consumer pattern.
static void BM_Pool_AcquireRelease_MT(benchmark::State& state) {
    static zlte::PacketPool<1500u, 1024u> pool;

    for (auto _ : state) {
        auto r = pool.acquire();
        benchmark::DoNotOptimize(r);
        if (r) [[likely]] pool.release(r.value());
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Pool_AcquireRelease_MT)
    ->Threads(1)->Threads(2)->Threads(4)->Threads(8)
    ->Unit(benchmark::kNanosecond);

// Burst: fill the pool then drain it — measures worst-case contention.
static void BM_Pool_Burst_MT(benchmark::State& state) {
    static zlte::PacketPool<1500u, 1024u> pool;

    for (auto _ : state) {
        // Acquire without immediately releasing (burst pattern).
        auto r = pool.acquire();
        benchmark::DoNotOptimize(r);
        if (r) [[likely]] {
            benchmark::ClobberMemory();
            pool.release(r.value());
        }
    }

    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Pool_Burst_MT)
    ->Threads(1)->Threads(4)->Threads(8)
    ->Unit(benchmark::kNanosecond);

BENCHMARK_MAIN();
