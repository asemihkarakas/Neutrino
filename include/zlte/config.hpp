#pragma once

#include <cstddef>

namespace zlte {

// Assumed cacheline size for false-sharing avoidance (alignas placement of
// hot atomics/slots). Not sourced from std::hardware_destructive_interference_size:
// that value is allowed to vary by compiler flags/version, which risks silent
// ODR/ABI mismatches across translation units. A fixed project-wide constant
// avoids that hazard; adjust here if targeting a platform with a different
// cacheline size (e.g. 128 on some ARM cores).
inline constexpr std::size_t kCacheLineSize = 64;

} // namespace zlte
