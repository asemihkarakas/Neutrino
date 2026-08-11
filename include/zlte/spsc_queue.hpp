#pragma once

#include "config.hpp"

#include <array>
#include <atomic>
#include <cstddef>

namespace zlte {

// Wait-free single-producer / single-consumer ring buffer.
// Capacity must be a non-zero power of two.
// Only one thread may call push(); only one thread may call pop().
template<typename T, std::size_t Capacity>
    requires (Capacity > 1 && (Capacity & (Capacity - 1)) == 0)
class SpscQueue {
public:
    // Returns false if the queue is full (producer side only).
    [[nodiscard]] bool push(const T& item) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next = (tail + 1u) & kMask;
        if (next == head_.load(std::memory_order_acquire))
            return false;
        buf_[tail] = item;
        tail_.store(next, std::memory_order_release);
        return true;
    }

    // Returns false if the queue is empty (consumer side only).
    [[nodiscard]] bool pop(T& out) noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire))
            return false;
        out = buf_[head];
        head_.store((head + 1u) & kMask, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }

    // Usable slots = Capacity - 1 (one slot is sacrificed to distinguish full/empty).
    [[nodiscard]] static constexpr std::size_t capacity() noexcept { return Capacity - 1u; }

private:
    static constexpr std::size_t kMask = Capacity - 1u;

    alignas(kCacheLineSize) std::atomic<std::size_t> head_{0};
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_{0};
    alignas(kCacheLineSize) std::array<T, Capacity>  buf_{};
};

} // namespace zlte
