#!/usr/bin/env bash
#
# doctor.sh —— 跑工具的接口自检（把两个端点串起来）
#
# 依据：percona-server 的 Docs/raft_replication/perf_tool_plan.md §3.3（7 条检查）与 §8.4（golden vector）。
# 为什么需要 wrapper：Lua 层一次进程只能连一个端点。这里两条命令各连一头：
#   1) doctor        → 对**目标**（活集群/原生实例）跑检查 1/2/4/5/6/7
#   2) golden-check  → 对**预演实例**（Raft 关闭、可建表）跑检查 3
#
# 用法：
#   percona/doctor.sh --sysbench=./src/sysbench \
#     --target-host=127.0.0.1 --target-port=46101 --target-user=root \
#     --preview-host=127.0.0.1 --preview-port=46104 --preview-user=root \
#     --require-raft --golden-dir=./schemas/golden
#
# 退出码：0 = 全部 PASS（SKIP 不算失败）；非 0 = 有 FAIL
#
set -euo pipefail

SYSBENCH=./src/sysbench
LUA=
TARGET_HOST=127.0.0.1; TARGET_PORT=; TARGET_USER=root
PREVIEW_HOST=127.0.0.1; PREVIEW_PORT=; PREVIEW_USER=root
GOLDEN_DIR=./schemas/golden
REQUIRE_RAFT=0

here=$(cd "$(dirname "$0")" && pwd)
LUA=${here}/../src/lua/raft_doctor.lua

normalized=()
for arg in "$@"; do
  case "$arg" in
    --*=*) normalized+=("${arg%%=*}" "${arg#*=}");;
    *)     normalized+=("$arg");;
  esac
done
set -- "${normalized[@]}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sysbench) SYSBENCH=$2; shift 2;;
    --lua) LUA=$2; shift 2;;
    --target-host) TARGET_HOST=$2; shift 2;;
    --target-port) TARGET_PORT=$2; shift 2;;
    --target-user) TARGET_USER=$2; shift 2;;
    --preview-host) PREVIEW_HOST=$2; shift 2;;
    --preview-port) PREVIEW_PORT=$2; shift 2;;
    --preview-user) PREVIEW_USER=$2; shift 2;;
    --golden-dir) GOLDEN_DIR=$2; shift 2;;
    --require-raft) REQUIRE_RAFT=1; shift;;
    *) echo "未知参数：$1" >&2; exit 2;;
  esac
done

[[ -n "$TARGET_PORT" ]] || { echo "必须给 --target-port" >&2; exit 2; }

raft_arg=()
[[ $REQUIRE_RAFT -eq 1 ]] && raft_arg+=(--require-raft)

target_out=/tmp/doctor-target.out
echo "=== 1/2 doctor：对目标 ${TARGET_HOST}:${TARGET_PORT} 跑检查 1/2/4/5/6/7 ==="
set +e
"$SYSBENCH" "$LUA" --mysql-host="$TARGET_HOST" --mysql-port="$TARGET_PORT" \
  --mysql-user="$TARGET_USER" --mysql-db=mysql \
  --golden-dir="$GOLDEN_DIR" "${raft_arg[@]}" doctor > "$target_out" 2>&1
target_rc=$?
set -e
grep -vE "^sysbench 1" "$target_out" | grep -v '^$' || true

golden_rc=0
if [[ -n "$PREVIEW_PORT" ]]; then
  golden_out=/tmp/doctor-golden.out
  echo "=== 2/2 golden-check：对预演实例 ${PREVIEW_HOST}:${PREVIEW_PORT} 跑检查 3 ==="
  set +e
  "$SYSBENCH" "$LUA" --mysql-host="$PREVIEW_HOST" --mysql-port="$PREVIEW_PORT" \
    --mysql-user="$PREVIEW_USER" --mysql-db=mysql \
    --golden-dir="$GOLDEN_DIR" golden-check > "$golden_out" 2>&1
  golden_rc=$?
  set -e
  grep -vE "^sysbench 1" "$golden_out" | grep -v '^$' || true
else
  echo "=== 2/2 golden-check：SKIP（未给 --preview-port）==="
  echo "    第 3 条必须在 Raft 关闭的实例上跑（它要建表，受管集群上 DDL 被拒）。"
fi

echo
if [[ $target_rc -eq 0 && $golden_rc -eq 0 ]]; then
  echo "=== doctor: PASS（目标 rc=0，golden rc=0）==="
  exit 0
fi
echo "=== doctor: FAIL（目标 rc=${target_rc}，golden rc=${golden_rc}）===" >&2
exit 1
