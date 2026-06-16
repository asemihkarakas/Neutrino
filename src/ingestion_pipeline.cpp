#include <zlte/ingestion_pipeline.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <cstring>
#include <span>

namespace zlte {

static std::uint64_t now_ns() noexcept {
    struct timespec ts{};
    ::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(ts.tv_nsec);
}

// ── ctor / dtor ──────────────────────────────────────────────────────────────

IngestionPipeline::IngestionPipeline(std::uint16_t port) noexcept
    : port_{port} {}

IngestionPipeline::~IngestionPipeline() noexcept { stop(); }

// ── start ────────────────────────────────────────────────────────────────────

Result<bool, PipelineError> IngestionPipeline::start() noexcept {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel, std::memory_order_relaxed))
        return Result<bool, PipelineError>::err(PipelineError::AlreadyRunning);

    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        running_.store(false, std::memory_order_release);
        return Result<bool, PipelineError>::err(PipelineError::SocketFailed);
    }

    // Allow rapid re-bind after restart.
    const int reuse = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    // 8 MiB kernel receive buffer to absorb bursts.
    const int rcvbuf = 8 << 20;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    // 200 ms receive timeout so the recv thread can check running_ periodically.
    struct timeval tv{};
    tv.tv_usec = 200'000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port_);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        running_.store(false, std::memory_order_release);
        return Result<bool, PipelineError>::err(PipelineError::BindFailed);
    }

    socket_fd_      = fd;
    recv_thread_    = std::thread(&IngestionPipeline::recv_loop,    this, fd);
    process_thread_ = std::thread(&IngestionPipeline::process_loop, this);

    return Result<bool, PipelineError>::ok(true);
}

// ── stop ─────────────────────────────────────────────────────────────────────

void IngestionPipeline::stop() noexcept {
    if (!running_.exchange(false, std::memory_order_acq_rel))
        return; // already stopped

    // Close the socket to unblock a pending recvfrom.
    if (socket_fd_ >= 0) {
        ::close(socket_fd_);
        socket_fd_ = -1;
    }

    if (recv_thread_.joinable())    recv_thread_.join();
    if (process_thread_.joinable()) process_thread_.join();
}

// ── recv thread ──────────────────────────────────────────────────────────────

void IngestionPipeline::recv_loop(int fd) noexcept {
    sockaddr_in src{};
    socklen_t   src_len = sizeof(src);

    while (running_.load(std::memory_order_acquire)) {
        auto slot_r = pool_.acquire();
        if (!slot_r) [[unlikely]] {
            // Pool exhausted — count drop and retry; processor will catch up.
            dropped_.fetch_add(1u, std::memory_order_relaxed);
            continue;
        }
        auto slot = slot_r.value();

        const ssize_t n = ::recvfrom(
            fd, slot.data(), slot.size(), 0,
            reinterpret_cast<sockaddr*>(&src), &src_len);

        if (n <= 0) [[unlikely]] {
            // Timeout (EAGAIN) or socket closed by stop() — release and loop.
            pool_.release(slot);
            continue;
        }

        const PacketDescriptor desc{
            .data      = slot.data(),
            .data_size = static_cast<std::size_t>(n),
            .src_ip    = src.sin_addr.s_addr,
            .src_port  = ntohs(src.sin_port),
        };

        // Spin until the process thread drains a slot (bounded by kQueueDepth).
        while (!queue_.push(desc)) {
            if (!running_.load(std::memory_order_relaxed)) {
                pool_.release(slot);
                return;
            }
        }
    }
}

// ── process thread ────────────────────────────────────────────────────────────

void IngestionPipeline::process_loop() noexcept {
    PacketDescriptor desc{};

    while (running_.load(std::memory_order_acquire)) {
        if (!queue_.pop(desc))
            continue;

        const std::uint64_t recv_ns = now_ns();

        // Return the slot as soon as we're done with the data.
        auto release_slot = [&] {
            pool_.release(std::span<std::byte>{desc.data, kPacketSize});
        };

        if (desc.data_size < sizeof(TelemetryHeader)) [[unlikely]] {
            release_slot();
            continue;
        }

        TelemetryHeader hdr{};
        std::memcpy(&hdr, desc.data, sizeof(TelemetryHeader));
        release_slot();

        if (hdr.magic != kTelemetryMagic || hdr.version != kTelemetryVersion) [[unlikely]]
            continue;

        const std::uint64_t latency_ns = (recv_ns > hdr.timestamp_ns)
                                         ? recv_ns - hdr.timestamp_ns : 0u;

        FlowStats& fs = flow_stats_[hdr.flow_id % kMaxFlows];
        fs.active.store(true,     std::memory_order_relaxed);
        fs.dscp.store(hdr.dscp,   std::memory_order_relaxed);
        fs.packets.fetch_add(1u,  std::memory_order_relaxed);
        fs.bytes.fetch_add(desc.data_size, std::memory_order_relaxed);
        fs.latency_sum_ns.fetch_add(latency_ns, std::memory_order_relaxed);

        // CAS loops for running min / max — best-effort, relaxed ordering.
        std::uint64_t cur = fs.latency_min_ns.load(std::memory_order_relaxed);
        while (latency_ns < cur &&
               !fs.latency_min_ns.compare_exchange_weak(
                   cur, latency_ns, std::memory_order_relaxed, std::memory_order_relaxed))
        {}

        cur = fs.latency_max_ns.load(std::memory_order_relaxed);
        while (latency_ns > cur &&
               !fs.latency_max_ns.compare_exchange_weak(
                   cur, latency_ns, std::memory_order_relaxed, std::memory_order_relaxed))
        {}
    }
}

} // namespace zlte
