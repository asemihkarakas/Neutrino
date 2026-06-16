add_executable(zlte_tests
    arena_test.cpp
)

target_link_libraries(zlte_tests PRIVATE
    zlte
    GTest::gtest_main
    Threads::Threads
)

target_compile_features(zlte_tests PRIVATE cxx_std_20)

include(GoogleTest)
gtest_discover_tests(zlte_tests)
