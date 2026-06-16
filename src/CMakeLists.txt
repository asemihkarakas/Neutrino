add_library(zlte STATIC
    ingestion_pipeline.cpp
)

target_include_directories(zlte PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(zlte PUBLIC
    Threads::Threads
)

target_compile_features(zlte PUBLIC cxx_std_20)

# ── main application ──────────────────────────────────────────────────────────

add_executable(zlte_ingest main.cpp)

target_link_libraries(zlte_ingest PRIVATE zlte)

target_compile_features(zlte_ingest PRIVATE cxx_std_20)
