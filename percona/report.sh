#!/usr/bin/env bash
#
# percona/report.sh —— 跑一次受管场景并归档 report.json（§8.2 / §8.3）
#
# 为什么需要 wrapper：
#   · 状态量差值必须**跨两个时间点**采（跑前/跑后），中间要真把负载跑完；
#   · 环境段要 `uname`/CPU/内存，这些 Lua 读不到；
#   · sysbench 的 stdout 要**逐字**留在 report.json 里（第一段口径不加工）。
# 三件事各归各位：采样与合并在 Lua（src/lua/raft_report.lua），跑负载与收集环境在 shell。
#
# 用法：
#   percona/report.sh --scenario=src/lua/oltp_read_write.lua \
#     --mysql-host=127.0.0.1 --mysql-port=48102 --mysql-user=root --mysql-db=perf \
#     --target=raft --threads=16 --time=60 \
#     --tables=1 --table-size=12910 --row-length=400 --types=builtin \
#     --index-profile=pk --seed=42 \
#     --out-dir=/private/tmp/raft-reports [--sample-port=48102] [--extra "…额外 sysbench 参数…"]
#
set -uo pipefail

SYSBENCH=./src/sysbench
SCENARIO=""
HOST=127.0.0.1
PORT=""
USER=root
DB=""
TARGET=""
READ_CONSISTENCY=""
THREADS=""
TIME_S=""
EVENTS=""
TABLES=""
TABLE_SIZE=""
ROW_LENGTH=""
TYPES=""
INDEX_PROFILE=""
SEED=""
ROWLEN_AVG=""
ROWLEN_DEV=""
OUT_DIR=""
SAMPLE_PORT=""
EXTRA=""

while [ $# -gt 0 ]; do
  case "$1" in
    --sysbench=*) SYSBENCH=${1#*=}; shift;;
    --scenario=*) SCENARIO=${1#*=}; shift;;
    --mysql-host=*) HOST=${1#*=}; shift;;
    --mysql-port=*) PORT=${1#*=}; shift;;
    --mysql-user=*) USER=${1#*=}; shift;;
    --mysql-db=*) DB=${1#*=}; shift;;
    --target=*) TARGET=${1#*=}; shift;;
    --read-consistency=*) READ_CONSISTENCY=${1#*=}; shift;;
    --threads=*) THREADS=${1#*=}; shift;;
    --time=*) TIME_S=${1#*=}; shift;;
    --events=*) EVENTS=${1#*=}; shift;;
    --tables=*) TABLES=${1#*=}; shift;;
    --table-size=*) TABLE_SIZE=${1#*=}; shift;;
    --row-length=*) ROW_LENGTH=${1#*=}; shift;;
    --types=*) TYPES=${1#*=}; shift;;
    --index-profile=*) INDEX_PROFILE=${1#*=}; shift;;
    --seed=*) SEED=${1#*=}; shift;;
    --rowlen-measured-avg=*) ROWLEN_AVG=${1#*=}; shift;;
    --rowlen-measured-deviation=*) ROWLEN_DEV=${1#*=}; shift;;
    --out-dir=*) OUT_DIR=${1#*=}; shift;;
    --sample-port=*) SAMPLE_PORT=${1#*=}; shift;;
    --extra=*) EXTRA=${1#*=}; shift;;
    *) echo "未知参数：$1" >&2; exit 2;;
  esac
done

[ -n "$SCENARIO" ] || { echo "必须给 --scenario=<lua 脚本>" >&2; exit 2; }
[ -n "$PORT" ] || { echo "必须给 --mysql-port" >&2; exit 2; }
[ -n "$DB" ] || { echo "必须给 --mysql-db" >&2; exit 2; }
[ -n "$OUT_DIR" ] || { echo "必须给 --out-dir（归档目录）" >&2; exit 2; }
SAMPLE_PORT=${SAMPLE_PORT:-$PORT}

mkdir -p "$OUT_DIR"
STAMP=$(date +%Y%m%d-%H%M%S)
NAME="${TARGET:-auto}-$(basename "$SCENARIO" .lua)-t${THREADS:-?}-${STAMP}"
BEFORE="$OUT_DIR/$NAME.before.json"
AFTER="$OUT_DIR/$NAME.after.json"
SB_OUT="$OUT_DIR/$NAME.sysbench.txt"
REPORT="$OUT_DIR/$NAME.report.json"

echo "report.name=$NAME"
echo "report.out=$REPORT"

# 采样：状态量的差值以 --sample-port 那一台为准（写场景下就是 leader）
LUA_REPORT=./src/lua/raft_report.lua
sample() { # <file>
  "$SYSBENCH" "$LUA_REPORT" --mysql-host="$HOST" --mysql-port="$SAMPLE_PORT" \
     --mysql-user="$USER" --mysql-db="${DB:-mysql}" --out="$1" sample > /dev/null 2>&1 \
     || echo "  （采样失败：$1）"
}

echo "--- 跑前采样（端口 ${SAMPLE_PORT}）---"
sample "$BEFORE"

echo "--- 跑负载：$SCENARIO ---"
SB_ARGS=(--mysql-host="$HOST" --mysql-port="$PORT" --mysql-user="$USER" --mysql-db="$DB")
[ -n "$TARGET" ] && SB_ARGS+=(--target="$TARGET")
[ -n "$READ_CONSISTENCY" ] && SB_ARGS+=(--read-consistency="$READ_CONSISTENCY")
[ -n "$THREADS" ] && SB_ARGS+=(--threads="$THREADS")
[ -n "$TIME_S" ] && SB_ARGS+=(--time="$TIME_S")
[ -n "$EVENTS" ] && SB_ARGS+=(--events="$EVENTS")
[ -n "$TABLES" ] && SB_ARGS+=(--tables="$TABLES")
[ -n "$TABLE_SIZE" ] && SB_ARGS+=(--table-size="$TABLE_SIZE")
# shellcheck disable=SC2086
"$SYSBENCH" "$SCENARIO" "${SB_ARGS[@]}" $EXTRA run 2>&1 | tee "$SB_OUT"
rc=${PIPESTATUS[0]}
echo "sysbench.exit=$rc"

echo "--- 跑后采样 ---"
sample "$AFTER"

# 环境段：一条命令能读到的就够（§8.3）
ENV_OS=$(uname -a 2>/dev/null || echo "")
if [ -r /proc/cpuinfo ]; then
  ENV_CPU=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')
elif command -v sysctl > /dev/null 2>&1; then
  ENV_CPU=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "")
fi
ENV_CPU=${ENV_CPU:-unknown}
if [ -r /proc/meminfo ]; then
  ENV_MEM=$(grep -m1 MemTotal /proc/meminfo | awk '{print $2" kB"}')
elif command -v sysctl > /dev/null 2>&1; then
  ENV_MEM=$(sysctl -n hw.memsize 2>/dev/null | awk '{printf "%.1f GB", $1/1024/1024/1024}')
fi
ENV_MEM=${ENV_MEM:-unknown}

echo "--- 合成 report.json ---"
"$SYSBENCH" "$LUA_REPORT" \
   --mysql-host="$HOST" --mysql-port="$SAMPLE_PORT" --mysql-user="$USER" --mysql-db="${DB:-mysql}" \
   --before="$BEFORE" --after="$AFTER" --sysbench-out="$SB_OUT" --report="$REPORT" \
   --scenario="$(basename "$SCENARIO")" --meta-target="$TARGET" --meta-read-consistency="$READ_CONSISTENCY" \
   --meta-threads="$THREADS" --meta-time="$TIME_S" --meta-events="$EVENTS" \
   --meta-host="$HOST" --meta-port="$PORT" --meta-db="$DB" \
   --tables="$TABLES" --table-size="$TABLE_SIZE" --row-length="$ROW_LENGTH" \
   --types="$TYPES" --index-profile="$INDEX_PROFILE" --seed="$SEED" \
   --rowlen-measured-avg="$ROWLEN_AVG" --rowlen-measured-deviation="$ROWLEN_DEV" \
   --env-os="$ENV_OS" --env-cpu="$ENV_CPU" --env-mem="$ENV_MEM" \
   combine 2>&1 | tail -2

echo "report.path=$REPORT"
exit $rc
