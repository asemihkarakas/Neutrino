#pragma once

#include "packet_pool.hpp"
#include "result.hpp"
#include "spsc_queue.hpp"
#include "telemetry.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>

namespace zlte {

enum class PipelineError : std::uint8_t {
    SocketFailed   = 1,
    BindFailed     = 2,
    AlreadyRunning = 3,
};

// Tunables — all compile-time to keep the data path allocation-free.
static constexpr std::size_t kPacketSize  = 2048;   // bytes per pool slot (≥ max UDP payload)
static constexpr std::size_t kPoolDepth   = 1024;   // number of pre-allocated packet buffers
static constexpr std::size_t kQueueDepth  = 4096;   // SPSC ring capacity (power of two)
static constexpr std::size_t kMaxFlows    = 1024;   // hash-table size for per-flow stats

// Per-flow counters updated from the process thread.
// alignas(64) ensures each FlowStats occupies its own cacheline(s) so
// different flows' writes never bounce the same cacheline across threads.
struct alignas(64) FlowStats {
    std::atomic<std::uint64_t> packets{0};
    std::atomic<std::uint64_t> bytes{0};
    std::atomic<std::uint64_t> latency_sum_ns{0};
    std::atomic<std::uint64_t> latency_min_ns{UINT64_MAX};
    std::atomic<std::uint64_t> latency_max_ns{0};
    std::atomic<std::uint8_t>  dscp{0};
    std::atomic<bool>          active{false};
};

// Two-thread ingestion pipeline:
//   recv thread   — recvfrom() → PacketPool slot → SpscQueue push
//   process thread — SpscQueue pop → parse TelemetryHeader → update FlowStats
//
// The main thread can read flow_stats() at any time (relaxed loads; data is
// eventually consistent — no locking required for a stats display).
class IngestionPipeline {
public:
    explicit IngestionPipeline(std::uint16_t port) noexcept;
    ~IngestionPipeline() noexcept;

    IngestionPipeline(const IngestionPipeline&)            = delete;
    IngestionPipeline& operator=(const IngestionPipeline&) = delete;

    // Open the UDP socket and launch the recv and process threads.
    [[nodiscard]] Result<bool, PipelineError> start() noexcept;

    // Signal threads to stop, close the socket, and join both threads.
    void stop() noexcept;

    [[nodiscard]] const std::array<FlowStats, kMaxFlows>& flow_stats() const noexcept {
        return flow_stats_;
    }

    [[nodiscard]] std::uint64_t dropped() const noexcept {
        return dropped_.load(std::memory_order_relaxed);
    }

private:
    void recv_loop(int fd) noexcept;
    void process_loop() noexcept;

    std::uint16_t port_;
    int           socket_fd_{-1};

    alignas(64) std::atomic<bool>          running_{false};
    alignas(64) std::atomic<std::uint64_t> dropped_{0};

    PacketPool<kPacketSize, kPoolDepth>       pool_;
    SpscQueue<PacketDescriptor, kQueueDepth>  queue_;
    std::array<FlowStats, kMaxFlows>          flow_stats_;

    std::thread recv_thread_;
    std::thread process_thread_;
};

} // namespace zlte
