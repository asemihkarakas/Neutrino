add_executable(zlte_gen packet_gen.cpp)

target_include_directories(zlte_gen PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(zlte_gen PRIVATE cxx_std_20)
