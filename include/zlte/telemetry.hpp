#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace zlte {

static constexpr std::uint32_t kTelemetryMagic   = 0x5A4C5445u; // "ZLTE"
static constexpr std::uint8_t  kTelemetryVersion = 1;

// On-wire telemetry packet header — naturally packed (no padding).
// Layout:
//   [0..3]   magic        (4 B)
//   [4]      version      (1 B)
//   [5]      dscp         (1 B)
//   [6..7]   payload_len  (2 B)
//   [8..11]  flow_id      (4 B)
//   [12..15] seq          (4 B)
//   [16..23] timestamp_ns (8 B) — sender CLOCK_MONOTONIC_RAW
struct TelemetryHeader {
    std::uint32_t magic;
    std::uint8_t  version;
    std::uint8_t  dscp;          // IETF DSCP value 0–63
    std::uint16_t payload_len;   // bytes of application payload after this header
    std::uint32_t flow_id;       // flow identifier (arbitrary)
    std::uint32_t seq;           // per-flow sequence number
    std::uint64_t timestamp_ns;  // sender's CLOCK_MONOTONIC_RAW at send time
};

static_assert(sizeof(TelemetryHeader) == 24, "TelemetryHeader must be 24 bytes");

// Map a DSCP value to its RFC 4594 per-hop behaviour name.
// Returns a string_view into static storage — no allocation.
[[nodiscard]] constexpr std::string_view dscp_class(std::uint8_t dscp) noexcept {
    if (dscp == 46)                         return "EF";
    if (dscp >= 32u && dscp <= 39u)         return "AF4x";
    if (dscp >= 24u && dscp <= 31u)         return "AF3x";
    if (dscp >= 16u && dscp <= 23u)         return "AF2x";
    if (dscp >= 8u  && dscp <= 15u)         return "AF1x";
    if (dscp == 48u)                        return "CS6";
    return "BE";
}

// Lightweight packet descriptor passed through the SPSC queue.
// Holds a pointer into a live PacketPool slot — ownership transfers
// to the process thread, which must call pool_.release() when done.
struct PacketDescriptor {
    std::byte*    data;       // base of PacketPool slot
    std::size_t   data_size;  // actual received bytes (≤ slot size)
    std::uint32_t src_ip;     // network byte order
    std::uint16_t src_port;   // host byte order
};

} // namespace zlte
