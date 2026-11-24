#!/bin/bash
# Builds Shadow 3.3.0 with shadow-mysql.patch into <dir>/install (default
# ~/ws/shadow), without root: rustup, the glib headers and networkx go into
# <dir> as well. Needs git, curl, python3, pkg-config, cmake, a C compiler
# and libclang (LIBCLANG_PATH, default /usr/lib/llvm-20/lib).
#
# usage: build_shadow.sh [dir]
set -euo pipefail
S=${1:-$HOME/ws/shadow}
PATCH=$(cd "$(dirname "$0")" && pwd)/shadow-mysql.patch
mkdir -p "$S"; cd "$S"
export RUSTUP_HOME=$S/rustup CARGO_HOME=$S/cargo PATH=$S/cargo/bin:$PATH
echo "== rustup"
[ -x "$S/cargo/bin/cargo" ] || curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs |
  sh -s -- -y --no-modify-path --profile minimal
cargo --version
echo "== glib headers"
mkdir -p "$S/deb" "$S/sysroot"
if [ ! -f "$S/sysroot/usr/lib/x86_64-linux-gnu/pkgconfig/glib-2.0.pc" ]; then
  (cd "$S/deb" && apt-get download libglib2.0-dev && for d in *.deb; do dpkg -x "$d" "$S/sysroot"; done)
fi
echo "== networkx"
python3 -m pip install --quiet --target "$S/py" networkx
echo "== shadow"
[ -d "$S/src" ] || git clone -q --depth 1 --branch v3.3.0 https://github.com/shadow/shadow.git "$S/src"
cd "$S/src"
if git apply --check "$PATCH" 2>/dev/null; then
  git apply "$PATCH"
elif git apply --check --reverse "$PATCH" 2>/dev/null; then
  echo "shadow-mysql.patch is already applied"
else
  echo "shadow-mysql.patch does not apply to $S/src"; exit 1
fi
export PKG_CONFIG_PATH=$S/sysroot/usr/lib/x86_64-linux-gnu/pkgconfig:$S/sysroot/usr/share/pkgconfig
export LIBRARY_PATH=$S/sysroot/usr/lib/x86_64-linux-gnu
export LIBCLANG_PATH=${LIBCLANG_PATH:-/usr/lib/llvm-20/lib} PYTHONPATH=$S/py
./setup build --prefix "$S/install" -j "$(nproc)"
./setup install
"$S/install/bin/shadow" --version
