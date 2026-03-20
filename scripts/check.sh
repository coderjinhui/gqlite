#!/usr/bin/env bash
# gqlite 统一验收脚本
# 用法: ./scripts/check.sh [--strict]
#   --strict: 使用 -D warnings（clippy 零警告模式）
#   默认: 使用 -W warnings（允许警告但仍报告）

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

CLIPPY_LEVEL="-W warnings"
if [[ "${1:-}" == "--strict" ]]; then
    CLIPPY_LEVEL="-D warnings"
    echo "=== 严格模式：clippy 零警告 ==="
fi

FAILED=0

echo ""
echo "========================================="
echo "  gqlite 验收检查"
echo "========================================="

# 1. cargo fmt
echo ""
echo "--- [1/3] cargo fmt --check ---"
if cargo fmt --all -- --check; then
    echo "✓ fmt 通过"
else
    echo "✗ fmt 失败 — 请运行 cargo fmt --all"
    FAILED=1
fi

# 2. cargo clippy
echo ""
echo "--- [2/3] cargo clippy ($CLIPPY_LEVEL) ---"
if cargo clippy --all-targets --all-features -- $CLIPPY_LEVEL 2>&1; then
    echo "✓ clippy 通过"
else
    if [[ "$CLIPPY_LEVEL" == "-D warnings" ]]; then
        echo "✗ clippy 失败 — 请修复所有警告"
        FAILED=1
    else
        echo "⚠ clippy 有警告（非严格模式下不阻塞）"
    fi
fi

# 3. cargo test
echo ""
echo "--- [3/3] cargo test ---"
if cargo test --workspace 2>&1; then
    echo "✓ 测试通过"
else
    echo "✗ 测试失败"
    FAILED=1
fi

# 结果
echo ""
echo "========================================="
if [[ $FAILED -eq 0 ]]; then
    echo "  ✓ 所有检查通过"
else
    echo "  ✗ 存在失败项，请修复后重试"
    exit 1
fi
echo "========================================="
