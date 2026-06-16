#pragma once

#include <type_traits>

namespace zlte {

// Lightweight discriminated union for no-exception error paths.
// Both T and E must be trivially copyable. Stores both fields separately
// (no union) to stay fully well-defined across all C++20 implementations.
template<typename T, typename E>
class [[nodiscard]] Result {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    static_assert(std::is_trivially_copyable_v<E>, "E must be trivially copyable");
    static_assert(!std::is_same_v<T, E>,            "T and E must be distinct types");

public:
    using value_type = T;
    using error_type = E;

    [[nodiscard]] static constexpr Result ok(T v) noexcept {
        Result r;
        r.has_value_ = true;
        r.val_       = v;
        return r;
    }

    [[nodiscard]] static constexpr Result err(E e) noexcept {
        Result r;
        r.err_ = e;
        return r;
    }

    [[nodiscard]] constexpr bool     has_value()     const noexcept { return has_value_; }
    [[nodiscard]] constexpr explicit operator bool() const noexcept { return has_value_; }

    // Call only when has_value() == true.
    [[nodiscard]] constexpr T value() const noexcept { return val_; }

    // Call only when has_value() == false.
    [[nodiscard]] constexpr E error() const noexcept { return err_; }

private:
    bool has_value_{false};
    T    val_{};
    E    err_{};
};

} // namespace zlte
