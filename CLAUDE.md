"Strictly use C++20 standard."

"No exceptions (-fno-exceptions). Return std::expected or custom error codes."

"Absolutely no std::mutex or locking mechanisms in the data path. Use std::atomic with explicit memory orders (acquire/release)."

"Data structures must be cacheline aligned (alignas(64)) to prevent false sharing."

"Avoid std::string in the fast path; use std::string_view and custom memory arenas."