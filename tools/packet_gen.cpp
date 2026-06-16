#include <zlte/telemetry.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>

static volatile bool g_running = true; // NOLINT

static void on_signal(int) noexcept { g_running = false; }

static std::uint64_t now_ns() noexcept {
    struct timespec ts{};
    ::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(ts.tv_nsec);
}

static void busy_sleep_until(std::uint64_t target_ns) noexcept {
    const std::uint64_t now = now_ns();
    if (target_ns <= now) return;
    const std::uint64_t rem = target_ns - now;
    // For waits ≥ 500 µs use nanosleep; spin the last 100 µs for precision.
    if (rem > 500'000u) {
        const std::uint64_t sleep_ns = rem - 100'000u;
        struct timespec ts{
            .tv_sec  = static_cast<time_t>(sleep_ns / 1'000'000'000ULL),
            .tv_nsec = static_cast<long>(sleep_ns % 1'000'000'000ULL),
        };
        ::nanosleep(&ts, nullptr);
    }
    while (now_ns() < target_ns) {} // precision spin
}

int main(int argc, char** argv) {
    const char*   host    = "127.0.0.1";
    std::uint16_t port    = 9000;
    std::uint32_t flows   = 4;
    std::uint64_t rate    = 1'000;   // total packets/second across all flows
    std::size_t   payload = 0;       // extra payload bytes after the header

    for (int i = 1; i < argc; ++i) {
        if      (std::strcmp(argv[i], "--host")    == 0 && i+1 < argc) host    = argv[++i];
        else if (std::strcmp(argv[i], "--port")    == 0 && i+1 < argc) port    = static_cast<std::uint16_t>(std::atoi(argv[++i]));
        else if (std::strcmp(argv[i], "--flows")   == 0 && i+1 < argc) flows   = static_cast<std::uint32_t>(std::atoi(argv[++i]));
        else if (std::strcmp(argv[i], "--rate")    == 0 && i+1 < argc) rate    = static_cast<std::uint64_t>(std::atoi(argv[++i]));
        else if (std::strcmp(argv[i], "--payload") == 0 && i+1 < argc) payload = static_cast<std::size_t>(std::atoi(argv[++i]));
        else if (std::strcmp(argv[i], "--help")    == 0) {
            std::printf("Usage: zlte_gen [--host H] [--port P] [--flows N]"
                        " [--rate N] [--payload N]\n");
            return 0;
        }
    }

    // One DSCP value per flow, cycling through EF → AF4x → AF3x → AF2x → AF1x → BE.
    static constexpr std::uint8_t kDscpTable[] = {46u, 34u, 26u, 18u, 10u, 0u};

    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) { std::perror("socket"); return 1; }

    sockaddr_in dst{};
    dst.sin_family = AF_INET;
    dst.sin_port   = htons(port);
    if (::inet_pton(AF_INET, host, &dst.sin_addr) != 1) {
        std::fprintf(stderr, "invalid host: %s\n", host);
        ::close(fd);
        return 1;
    }

    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);

    std::printf("Sending to %s:%u  flows=%u  rate=%llu pkt/s  payload=%zu B\n",
                host, port, flows,
                static_cast<unsigned long long>(rate), payload);
    std::printf("DSCP mapping:\n");
    for (std::uint32_t f = 0; f < flows; ++f) {
        const std::uint8_t dscp = kDscpTable[f % std::size(kDscpTable)];
        std::printf("  flow %-3u → DSCP %-2u (%.*s)\n", f, dscp,
                    static_cast<int>(zlte::dscp_class(dscp).size()),
                    zlte::dscp_class(dscp).data());
    }
    std::printf("Press Ctrl-C to stop.\n\n");

    // Build a reusable packet buffer (header + optional payload).
    // We use a fixed-size stack buffer; payload is capped at 1400 B here.
    static constexpr std::size_t kMaxPayload = 1400u;
    const std::size_t safe_payload = (payload > kMaxPayload) ? kMaxPayload : payload;
    const std::size_t safe_pkt_size = sizeof(zlte::TelemetryHeader) + safe_payload;
    static std::byte buf[sizeof(zlte::TelemetryHeader) + kMaxPayload]{};
    std::memset(buf + sizeof(zlte::TelemetryHeader), 0xAA, safe_payload);

    // Inter-batch interval: send `flows` packets per batch at target rate.
    const std::uint64_t batch_ns = (rate > 0u)
        ? (1'000'000'000ULL * static_cast<std::uint64_t>(flows)) / rate
        : 0u;

    std::uint64_t total_sent = 0;
    std::uint64_t next_batch = now_ns();

    // Per-flow sequence numbers.
    static std::uint32_t seq[1024]{};

    while (g_running) {
        for (std::uint32_t f = 0; f < flows && g_running; ++f) {
            zlte::TelemetryHeader hdr{};
            hdr.magic        = zlte::kTelemetryMagic;
            hdr.version      = zlte::kTelemetryVersion;
            hdr.dscp         = kDscpTable[f % std::size(kDscpTable)];
            hdr.payload_len  = static_cast<std::uint16_t>(safe_payload);
            hdr.flow_id      = f;
            hdr.seq          = seq[f]++;
            hdr.timestamp_ns = now_ns();

            std::memcpy(buf, &hdr, sizeof(hdr));
            ::sendto(fd, buf, safe_pkt_size, 0,
                     reinterpret_cast<const sockaddr*>(&dst), sizeof(dst));
            ++total_sent;
        }

        if (batch_ns > 0u) {
            next_batch += batch_ns;
            busy_sleep_until(next_batch);
        }
    }

    ::close(fd);
    std::printf("\nSent %llu packets total.\n",
                static_cast<unsigned long long>(total_sent));
    return 0;
}
