cmake_minimum_required(VERSION 3.20)

project(ZeroLatencyTelemetryEngine
    VERSION 0.1.0
    DESCRIPTION "Zero-Latency QoS Telemetry Ingestion Engine"
    LANGUAGES CXX
)

# ---------------------------------------------------------------------------
# C++20 — no GNU extensions
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD          20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS        OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ---------------------------------------------------------------------------
# AddressSanitizer (opt-out with -DENABLE_ASAN=OFF)
# ---------------------------------------------------------------------------
option(ENABLE_ASAN "Enable AddressSanitizer" ON)
if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(   -fsanitize=address)
endif()

# ---------------------------------------------------------------------------
# pthread
# ---------------------------------------------------------------------------
find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# Google Benchmark
# ---------------------------------------------------------------------------
include(FetchContent)

FetchContent_Declare(
    googlebenchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG        v1.9.1
    GIT_SHALLOW    TRUE
)
set(BENCHMARK_ENABLE_TESTING     OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL     OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
# Google Benchmark builds its own sources at -O2 with -Werror by default. On
# GCC 15 that turns a known <regex> -Wmaybe-uninitialized false positive into a
# fatal error, so disable the dependency's -Werror (our own code stays strict).
set(BENCHMARK_ENABLE_WERROR      OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googlebenchmark)

# GCC 15 emits a spurious -Wmaybe-uninitialized inside libstdc++ <regex> when
# compiling Google Benchmark's registration code. It is a false positive in a
# third-party dependency, so quiet it to keep our own build output readable.
if(TARGET benchmark AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(benchmark PRIVATE -Wno-maybe-uninitialized)
endif()

# ---------------------------------------------------------------------------
# Google Test
# ---------------------------------------------------------------------------
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.15.2
    GIT_SHALLOW    TRUE
)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(CTest)
enable_testing()

# ---------------------------------------------------------------------------
# Compile flags (project code only)
#
# Declared AFTER the FetchContent dependencies are pulled in so the strict
# -Werror warning set is NOT applied to vendored third-party sources
# (googlebenchmark / googletest), which do not build cleanly under
# -Wsign-conversion. add_compile_options only affects directories added below.
# ---------------------------------------------------------------------------
add_compile_options(
    -Wall
    -Wextra
    -Werror
    -Wpedantic
    -Wshadow
    -Wconversion
    -Wsign-conversion
    -fno-exceptions          # no exceptions per project policy
)

# ---------------------------------------------------------------------------
# Subdirectories
# ---------------------------------------------------------------------------
add_subdirectory(src)
add_subdirectory(tools)
add_subdirectory(tests)
add_subdirectory(benchmarks)
