#pragma once

#include "config.hpp"
#include "result.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>

namespace zlte {

enum class PoolError : std::uint8_t {
    Exhausted   = 1,
    InvalidSlot = 2,
};

// Lock-free fixed-size packet pool implemented as an ABA-safe Treiber stack.
//
// Pre-allocates PoolDepth slots of PacketSize bytes at construction.
// acquire() pops a slot from the free list in O(1); release() pushes it back.
// No new/delete is used after construction.
//
// ABA prevention: the free-list head is a uint64_t encoding a 32-bit slot
// index and a 32-bit monotonic tag. This fits in a single 64-bit atomic —
// always lock-free on any 64-bit platform, no platform-specific 16-byte CAS
// required.
//
// PacketSize : byte capacity of each individual packet buffer.
// PoolDepth  : number of packet buffers; must be < UINT32_MAX.
template<std::size_t PacketSize, std::size_t PoolDepth>
    requires (PacketSize > 0 && PoolDepth > 0 &&
              PoolDepth < std::numeric_limits<std::uint32_t>::max())
class PacketPool {
    // When a slot is on the free list its first sizeof(void*) bytes are unused
    // packet data — no extra pointer needed; instead we keep next_idx as a
    // dedicated atomic field outside the data buffer.
    static constexpr std::uint32_t kNull = std::numeric_limits<std::uint32_t>::max();

    // Each slot is cacheline-aligned to prevent false sharing between
    // concurrently acquired adjacent slots.
    struct alignas(kCacheLineSize) Slot {
        std::byte              data[PacketSize];
        // next_idx is std::atomic to eliminate data races between an acquire()
        // reader that has a stale head pointer and a concurrent release() writer
        // updating that same slot's next link.
        std::atomic<std::uint32_t> next_idx{kNull};
    };

    static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                  "64-bit atomic must be lock-free");

    // Pack/unpack slot index (high 32 bits) and ABA tag (low 32 bits).
    static constexpr std::uint64_t pack(std::uint32_t idx,
                                        std::uint32_t tag) noexcept {
        return (static_cast<std::uint64_t>(idx) << 32) |
               static_cast<std::uint64_t>(tag);
    }
    static constexpr std::uint32_t idx_of(std::uint64_t v) noexcept {
        return static_cast<std::uint32_t>(v >> 32);
    }
    static constexpr std::uint32_t tag_of(std::uint64_t v) noexcept {
        return static_cast<std::uint32_t>(v);
    }

public:
    PacketPool() noexcept { build_free_list(); }

    PacketPool(const PacketPool&)            = delete;
    PacketPool& operator=(const PacketPool&) = delete;

    // Pop one slot from the free list. Lock-free.
    // Returns a span of exactly PacketSize bytes on success.
    [[nodiscard]] Result<std::span<std::byte>, PoolError> acquire() noexcept {
        std::uint64_t curr = free_head_.load(std::memory_order_acquire);

        while (true) {
            const std::uint32_t head_idx = idx_of(curr);
            if (head_idx == kNull) [[unlikely]]
                return Result<std::span<std::byte>, PoolError>::err(
                    PoolError::Exhausted);

            // Safe to read next_idx because we hold an acquire view of free_head_:
            // whoever last pushed head_idx did a release store that includes their
            // write to slots_[head_idx].next_idx.
            const std::uint32_t next_idx =
                slots_[head_idx].next_idx.load(std::memory_order_relaxed);

            const std::uint64_t desired = pack(next_idx, tag_of(curr) + 1u);

            if (free_head_.compare_exchange_weak(
                    curr, desired,
                    std::memory_order_acq_rel,  // own the slot; see prior writes to data
                    std::memory_order_acquire)) // failure: resync head, re-read next_idx safely
            {
                in_use_.fetch_add(1u, std::memory_order_relaxed);
                return Result<std::span<std::byte>, PoolError>::ok(
                    std::span<std::byte>{slots_[head_idx].data, PacketSize});
            }
        }
    }

    // Push a slot back onto the free list. Lock-free.
    // `packet` must be a span previously returned by acquire() from this pool.
    void release(std::span<std::byte> packet) noexcept {
        // Recover the slot index from the span. data is the first member of Slot
        // (standard-layout), so &slot.data[0] == reinterpret_cast<byte*>(&slot).
        const auto* slot_ptr = reinterpret_cast<const Slot*>(packet.data());
        assert(slot_ptr >= slots_.data() &&
               slot_ptr <  slots_.data() + PoolDepth &&
               "released span does not belong to this pool");

        const auto slot_idx = static_cast<std::uint32_t>(slot_ptr - slots_.data());

        std::uint64_t curr = free_head_.load(std::memory_order_relaxed);
        std::uint64_t desired;

        do {
            // Store next link before the CAS so the release fence below publishes it.
            slots_[slot_idx].next_idx.store(idx_of(curr),
                                            std::memory_order_relaxed);
            desired = pack(slot_idx, tag_of(curr) + 1u);

            // release on success: acquirers will see the next_idx write above.
        } while (!free_head_.compare_exchange_weak(
                     curr, desired,
                     std::memory_order_release,
                     std::memory_order_relaxed));

        in_use_.fetch_sub(1u, std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t in_use()    const noexcept {
        return in_use_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::size_t available() const noexcept {
        return PoolDepth - in_use();
    }
    [[nodiscard]] static constexpr std::size_t capacity()    noexcept { return PoolDepth; }
    [[nodiscard]] static constexpr std::size_t packet_size() noexcept { return PacketSize; }

private:
    void build_free_list() noexcept {
        for (std::uint32_t i = 0; i < static_cast<std::uint32_t>(PoolDepth) - 1u; ++i)
            slots_[i].next_idx.store(i + 1u, std::memory_order_relaxed);
        slots_[PoolDepth - 1u].next_idx.store(kNull, std::memory_order_relaxed);
        free_head_.store(pack(0u, 0u), std::memory_order_release);
    }

    // free_head_ and in_use_ each get their own cacheline; the hot read/CAS path
    // on free_head_ must never bounce off in_use_ writes from other threads.
    alignas(kCacheLineSize) std::atomic<std::uint64_t> free_head_{pack(kNull, 0u)};
    alignas(kCacheLineSize) std::atomic<std::size_t>   in_use_{0};

    alignas(kCacheLineSize) std::array<Slot, PoolDepth> slots_;
};

} // namespace zlte
