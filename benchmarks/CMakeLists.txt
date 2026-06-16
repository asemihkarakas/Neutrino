add_executable(zlte_benchmarks
    arena_bench.cpp
)

target_link_libraries(zlte_benchmarks PRIVATE
    zlte
    benchmark::benchmark_main
    Threads::Threads
)

target_compile_features(zlte_benchmarks PRIVATE cxx_std_20)
