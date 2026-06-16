#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build"

# ── install cmake if missing ──────────────────────────────────────────────────
if ! command -v cmake &>/dev/null; then
    echo "cmake not found — installing..."
    sudo apt-get update -qq
    sudo apt-get install -y cmake
fi

# ── configure ─────────────────────────────────────────────────────────────────
cmake -S "$ROOT" -B "$BUILD" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DENABLE_ASAN=OFF

# ── build ─────────────────────────────────────────────────────────────────────
cmake --build "$BUILD" --parallel "$(nproc)"

# ── usage ─────────────────────────────────────────────────────────────────────
cat <<EOF

Build complete.

  Terminal 1 — start the ingestion engine:
    $BUILD/src/zlte_ingest [--port 9000]

  Terminal 2 — send test traffic:
    $BUILD/tools/zlte_gen [--host 127.0.0.1] [--port 9000] [--flows 4] [--rate 1000] [--payload 64]

  Run unit tests:
    ctest --test-dir $BUILD --output-on-failure

EOF
