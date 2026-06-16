#include <zlte/arena_allocator.hpp>
#include <zlte/packet_pool.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <thread>
#include <vector>

// ---------------------------------------------------------------------------
// ArenaAllocator
// ---------------------------------------------------------------------------

TEST(ArenaAllocator, BasicAllocation) {
    zlte::ArenaAllocator<1024> arena;
    auto r = arena.allocate(64, 64);
    ASSERT_TRUE(r);
    EXPECT_NE(r.value(), nullptr);
    EXPECT_EQ(arena.used(), 64u);
}

TEST(ArenaAllocator, AlignmentIsRespected) {
    zlte::ArenaAllocator<4096> arena;
    for (std::size_t align : {1u, 2u, 4u, 8u, 16u, 32u, 64u}) {
        auto r = arena.allocate(1, align);
        ASSERT_TRUE(r) << "alignment=" << align;
        auto addr = reinterpret_cast<std::uintptr_t>(r.value());
        EXPECT_EQ(addr % align, 0u) << "alignment=" << align;
    }
}

TEST(ArenaAllocator, BadAlignmentRejected) {
    zlte::ArenaAllocator<256> arena;
    auto r = arena.allocate(8, 3); // 3 is not a power-of-two
    EXPECT_FALSE(r);
    EXPECT_EQ(r.error(), zlte::ArenaError::BadAlignment);
}

TEST(ArenaAllocator, OutOfMemoryReturnsError) {
    zlte::ArenaAllocator<64> arena;
    ASSERT_TRUE(arena.allocate(64, 1));
    auto r = arena.allocate(1, 1);
    EXPECT_FALSE(r);
    EXPECT_EQ(r.error(), zlte::ArenaError::OutOfMemory);
}

TEST(ArenaAllocator, ResetReclaims) {
    zlte::ArenaAllocator<64> arena;
    ASSERT_TRUE(arena.allocate(64, 1));
    EXPECT_FALSE(arena.allocate(1, 1));

    arena.reset();

    EXPECT_EQ(arena.used(), 0u);
    EXPECT_TRUE(arena.allocate(64, 1));
}

TEST(ArenaAllocator, AllocationsDoNotOverlap) {
    // Each alloc is 64 bytes; total must fit in Capacity.
    static constexpr std::size_t kCapacity  = 1u << 20; // 1 MiB
    static constexpr std::size_t kAllocSize = 64u;
    static constexpr int         kThreads   = 8;

    static zlte::ArenaAllocator<kCapacity> arena; // static: avoid 1 MiB on stack
    arena.reset();

    std::atomic<std::size_t> total_allocs{0};

    auto worker = [&] {
        while (true) {
            auto r = arena.allocate(kAllocSize, kAllocSize);
            if (!r) break;
            total_allocs.fetch_add(1u, std::memory_order_relaxed);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i)
        threads.emplace_back(worker);
    for (auto& t : threads)
        t.join();

    // Every successful allocation must be accounted for in used().
    EXPECT_EQ(total_allocs.load() * kAllocSize, arena.used());
    EXPECT_LE(arena.used(), kCapacity);
}

// ---------------------------------------------------------------------------
// PacketPool
// ---------------------------------------------------------------------------

TEST(PacketPool, AcquireReturnsCorrectSize) {
    zlte::PacketPool<1500, 64> pool;
    auto r = pool.acquire();
    ASSERT_TRUE(r);
    EXPECT_EQ(r.value().size(), 1500u);
    pool.release(r.value());
}

TEST(PacketPool, ReleaseRestoresAvailability) {
    zlte::PacketPool<1500, 4> pool;
    EXPECT_EQ(pool.available(), 4u);

    auto r = pool.acquire();
    ASSERT_TRUE(r);
    EXPECT_EQ(pool.available(), 3u);
    EXPECT_EQ(pool.in_use(), 1u);

    pool.release(r.value());
    EXPECT_EQ(pool.available(), 4u);
    EXPECT_EQ(pool.in_use(), 0u);
}

TEST(PacketPool, ExhaustionReturnsError) {
    zlte::PacketPool<256, 4> pool;

    std::array<std::span<std::byte>, 4> held;
    for (auto& slot : held) {
        auto r = pool.acquire();
        ASSERT_TRUE(r);
        slot = r.value();
    }

    auto r = pool.acquire();
    EXPECT_FALSE(r);
    EXPECT_EQ(r.error(), zlte::PoolError::Exhausted);

    for (auto slot : held)
        pool.release(slot);
    EXPECT_EQ(pool.available(), 4u);
}

TEST(PacketPool, WrittenDataSurvivesRoundtrip) {
    zlte::PacketPool<256, 2> pool;

    auto r = pool.acquire();
    ASSERT_TRUE(r);

    std::memset(r.value().data(), 0xAB, 256);
    pool.release(r.value());

    // Re-acquire the same slot (LIFO, only one was released).
    auto r2 = pool.acquire();
    ASSERT_TRUE(r2);
    EXPECT_EQ(r2.value().data()[0],   static_cast<std::byte>(0xAB));
    EXPECT_EQ(r2.value().data()[255], static_cast<std::byte>(0xAB));
    pool.release(r2.value());
}

TEST(PacketPool, ConcurrentAcquireRelease) {
    static constexpr int kThreads = 8;
    static constexpr int kOps     = 100'000;

    zlte::PacketPool<1500, 256> pool;

    auto worker = [&] {
        for (int i = 0; i < kOps; ++i) {
            auto r = pool.acquire();
            if (r) pool.release(r.value());
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i)
        threads.emplace_back(worker);
    for (auto& t : threads)
        t.join();

    // All packets must be back in the pool after all threads finish.
    EXPECT_EQ(pool.available(), pool.capacity());
    EXPECT_EQ(pool.in_use(), 0u);
}
