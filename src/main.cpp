#include <zlte/ingestion_pipeline.hpp>
#include <zlte/telemetry.hpp>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <thread>

static volatile bool g_running = true; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

static void on_signal(int) noexcept { g_running = false; }

static void print_stats(const zlte::IngestionPipeline& pipeline,
                        std::uint16_t port) noexcept {
    // ANSI: clear screen + move cursor home.
    std::printf("\033[2J\033[H");
    std::printf("ZLTE — Zero-Latency Telemetry Ingestion Engine"
                "  |  UDP :%u  |  Ctrl-C to quit\n\n", port);

    std::printf("%-8s  %-5s  %-4s  %-12s  %-12s  %-12s  %-12s  %-12s\n",
                "Flow", "Class", "DSCP",
                "Packets", "Bytes",
                "AvgLat µs", "MinLat µs", "MaxLat µs");
    std::printf("%.8s  %.5s  %.4s  %.12s  %.12s  %.12s  %.12s  %.12s\n",
                "--------", "-----", "----",
                "------------", "------------",
                "------------", "------------", "------------");

    bool any = false;
    const auto& stats = pipeline.flow_stats();

    for (std::size_t i = 0; i < zlte::kMaxFlows; ++i) {
        const auto& fs = stats[i];
        if (!fs.active.load(std::memory_order_relaxed)) continue;

        const std::uint64_t pkts    = fs.packets.load(std::memory_order_relaxed);
        if (pkts == 0) continue;

        const std::uint64_t bytes   = fs.bytes.load(std::memory_order_relaxed);
        const std::uint64_t sum_ns  = fs.latency_sum_ns.load(std::memory_order_relaxed);
        const std::uint64_t min_ns  = fs.latency_min_ns.load(std::memory_order_relaxed);
        const std::uint64_t max_ns  = fs.latency_max_ns.load(std::memory_order_relaxed);
        const std::uint8_t  dscp    = fs.dscp.load(std::memory_order_relaxed);

        const std::uint64_t avg_us  = (sum_ns / pkts) / 1'000u;
        const std::uint64_t min_us  = (min_ns == UINT64_MAX) ? 0u : min_ns / 1'000u;
        const std::uint64_t max_us  = max_ns / 1'000u;
        const auto          cls     = zlte::dscp_class(dscp);

        std::printf("%-8zu  %-5.*s  %-4u  %-12llu  %-12llu  %-12llu  %-12llu  %-12llu\n",
                    i,
                    static_cast<int>(cls.size()), cls.data(),
                    static_cast<unsigned>(dscp),
                    static_cast<unsigned long long>(pkts),
                    static_cast<unsigned long long>(bytes),
                    static_cast<unsigned long long>(avg_us),
                    static_cast<unsigned long long>(min_us),
                    static_cast<unsigned long long>(max_us));
        any = true;
    }

    if (!any)
        std::printf("  (no flows seen yet — waiting for packets)\n");

    const std::uint64_t dropped = pipeline.dropped();
    if (dropped > 0)
        std::printf("\n  pool-exhaustion drops: %llu\n",
                    static_cast<unsigned long long>(dropped));
}

int main(int argc, char** argv) {
    std::uint16_t port = 9000;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc)
            port = static_cast<std::uint16_t>(std::atoi(argv[++i]));
        else if (std::strcmp(argv[i], "--help") == 0) {
            std::printf("Usage: zlte_ingest [--port PORT]\n");
            return 0;
        }
    }

    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);

    zlte::IngestionPipeline pipeline{port};
    const auto r = pipeline.start();
    if (!r) {
        const char* reason = "unknown";
        switch (r.error()) {
            case zlte::PipelineError::SocketFailed:   reason = "socket() failed";  break;
            case zlte::PipelineError::BindFailed:     reason = "bind() failed — port in use?"; break;
            case zlte::PipelineError::AlreadyRunning: reason = "already running";  break;
        }
        std::fprintf(stderr, "error: %s\n", reason);
        return 1;
    }

    while (g_running) {
        print_stats(pipeline, port);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    pipeline.stop();
    std::printf("\nStopped.\n");
    return 0;
}
