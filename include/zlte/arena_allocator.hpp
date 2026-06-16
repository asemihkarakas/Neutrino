#pragma once

#include "result.hpp"

#include <atomic>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>

namespace zlte {

enum class ArenaError : std::uint8_t {
    OutOfMemory  = 1,
    BadAlignment = 2,
};

// Lock-free bump-pointer arena.
//
// Pre-allocates Capacity bytes at construction (inside the object itself — use
// as a static/global or embed in a long-lived heap object to avoid stack overflow
// for large capacities).
//
// allocate() is lock-free: a CAS loop advances the offset and returns a pointer
// to the exclusive [claimed, claimed+bytes) region. No new/delete is used after
// construction. reset() is not safe to call concurrently with allocate().
template<std::size_t Capacity>
    requires (Capacity > 0)
class ArenaAllocator {
public:
    static constexpr std::size_t kCapacity = Capacity;

    ArenaAllocator()  noexcept = default;
    ~ArenaAllocator() noexcept = default;

    ArenaAllocator(const ArenaAllocator&)            = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;

    // Allocate `bytes` bytes aligned to `alignment` (must be a power of two).
    // Lock-free. Returns a non-null pointer on success.
    [[nodiscard]] Result<void*, ArenaError>
    allocate(std::size_t bytes,
             std::size_t alignment = alignof(std::max_align_t)) noexcept {
        if (!std::has_single_bit(alignment)) [[unlikely]]
            return Result<void*, ArenaError>::err(ArenaError::BadAlignment);
        if (bytes == 0) [[unlikely]]
            return Result<void*, ArenaError>::ok(storage_);

        std::size_t current = offset_.load(std::memory_order_relaxed);
        std::size_t claimed;
        std::size_t next;

        do {
            claimed = align_up(current, alignment);
            next    = claimed + bytes;
            if (next > Capacity) [[unlikely]]
                return Result<void*, ArenaError>::err(ArenaError::OutOfMemory);

            // acq_rel on success: the claimed range is visible before caller writes
            // into it, and prior resets (release) are observed before we claim.
        } while (!offset_.compare_exchange_weak(
                     current, next,
                     std::memory_order_acq_rel,
                     std::memory_order_relaxed));

        return Result<void*, ArenaError>::ok(storage_ + claimed);
    }

    // Typed convenience overload. Deduces size and alignment from T.
    template<typename T>
    [[nodiscard]] Result<T*, ArenaError> allocate(std::size_t count = 1) noexcept {
        auto r = allocate(sizeof(T) * count, alignof(T));
        if (!r) return Result<T*, ArenaError>::err(r.error());
        return Result<T*, ArenaError>::ok(static_cast<T*>(r.value()));
    }

    // Reset the bump pointer to zero. Logically frees all prior allocations.
    // Must not be called while other threads are calling allocate().
    void reset() noexcept {
        offset_.store(0, std::memory_order_release);
    }

    [[nodiscard]] std::size_t used()      const noexcept { return offset_.load(std::memory_order_acquire); }
    [[nodiscard]] std::size_t available() const noexcept { return Capacity - used(); }
    [[nodiscard]] static constexpr std::size_t capacity() noexcept { return Capacity; }

private:
    [[nodiscard]] static constexpr std::size_t
    align_up(std::size_t v, std::size_t a) noexcept {
        return (v + a - 1u) & ~(a - 1u);
    }

    // The atomic bump pointer sits on its own cacheline to prevent false sharing
    // with the storage or with other hot variables in the same struct.
    alignas(64) std::atomic<std::size_t> offset_{0};

    // Backing store — 64-byte aligned so allocations promoted to cacheline
    // alignment never require a second level of indirection.
    alignas(64) std::byte storage_[Capacity];
};

} // namespace zlte
