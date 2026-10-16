#!/usr/bin/env bash
#
# monitor.sh —— 边跑一条命令，边在三台上采 Percona Raft 指标，跑完自动判读"慢在哪条腿"
#
# 为什么需要：灌数/压测变慢时，服务端的 recent_events 只给每笔事务的分阶段耗时；"提交完成 →
# 本机开始 apply"这段空档只能靠**状态量在时间轴上的形状**才能定性（apply 滞后 / 本机日志持久化
# 滞后 / commit gate 排队 / 许可排队 / 冻结推迟 / 装快照）。所以把采样放在 wrapper 里，
# 与被测命令并行跑，结束时给每个成员出一份汇总 + 一份三台并排的短表。
#
# 用法（一般直接包住 prepare.sh，或任何一条命令）：
#   bash percona/monitor.sh \
#     --sysbench=./src/sysbench \
#     --member 10.188.62.79:6606 --member 10.214.38.209:6606 --member 10.188.48.140:6606 \
#     --user=root --password=root --interval-ms=1000 --out-dir=/tmp/raft-mon [--print-full] \
#     -- bash percona/prepare.sh --sysbench=./src/sysbench --schema-db=test ... --skip-ddl
#
# 退出码 = 被包命令的退出码（采样/汇总自身出错只打印警告，不改判据）。
#
set -uo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SYSBENCH=./src/sysbench
LUA=${here}/../src/lua/raft_monitor.lua
USER=root
PASSWORD=${MYSQL_PWD:-}
INTERVAL_MS=1000
OUT_DIR=
TAG=mon
PRINT_FULL=0
MEMBERS=()
CMD=()

while [[ $# -gt 0 ]]; do
  if [[ "$1" == --*=* ]]; then set -- "${1%%=*}" "${1#*=}" "${@:2}"; fi
  case "$1" in
    --sysbench) SYSBENCH=$2; shift 2;;
    --lua) LUA=$2; shift 2;;
    --member) MEMBERS+=("$2"); shift 2;;
    --user) USER=$2; shift 2;;
    --password) PASSWORD=$2; shift 2;;
    --interval-ms) INTERVAL_MS=$2; shift 2;;
    --out-dir) OUT_DIR=$2; shift 2;;
    --tag) TAG=$2; shift 2;;
    --print-full) PRINT_FULL=1; shift;;
    --) shift; CMD=("$@"); break;;
    *) echo "未知参数：$1（-- 之后是要包住的命令）" >&2; exit 2;;
  esac
done

[[ ${#MEMBERS[@]} -gt 0 ]] || { echo "至少给一个 --member host:port" >&2; exit 2; }
[[ ${#CMD[@]} -gt 0 ]] || { echo "要用 -- 给出被包住的命令（例：-- bash percona/prepare.sh ...）" >&2; exit 2; }
[[ -n "$OUT_DIR" ]] || OUT_DIR=/tmp/raft-mon.$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT_DIR"
[[ -x "$SYSBENCH" || -n "$(command -v "$SYSBENCH" 2>/dev/null || true)" ]] || { echo "找不到 sysbench：$SYSBENCH" >&2; exit 2; }

conn_args=(--mysql-user="$USER" --mysql-db=mysql)   # 采样不依赖业务库
[[ -n "$PASSWORD" ]] && conn_args+=(--mysql-password="$PASSWORD")

# 文件名必须**每台唯一**：三台常常用同一个端口（例如都用 6606），只按端口命名会让三个采样进程
# 写同一个文件（实测：两份数据交错 + 表头被覆盖 ⇒ summarize 直接解析不出来）。
sanitize() { printf '%s' "$1" | tr '.:/' '___'; }
base_of() { # <host:port> -> <tag>-<host>_<port>
  local m=$1
  printf '%s-%s_%s' "$TAG" "$(sanitize "${m%%:*}")" "${m##*:}"
}

# 同一 host:port 重复给会互相覆盖 —— 直接拒绝
seen=""
for m in "${MEMBERS[@]}"; do
  case " $seen " in *" $m "*) echo "重复的 --member：$m" >&2; exit 2;; esac
  seen="$seen $m"
done

sampler_pids=()
start_samplers() {
  local m host port tsv i=0
  for m in "${MEMBERS[@]}"; do
    host=${m%%:*}; port=${m##*:}; i=$((i + 1))
    tsv="$OUT_DIR/$(base_of "$m").tsv"
    "$SYSBENCH" "$LUA" --mysql-host="$host" --mysql-port="$port" "${conn_args[@]}" \
      sample --out="$tsv" --interval-ms="$INTERVAL_MS" > "$OUT_DIR/$(base_of "$m").sample.log" 2>&1 &
    sampler_pids+=("$!")
    echo "  [monitor] 采样启动：$host:$port → $tsv"
  done
  # 自检：文件得出现，且头里的 host/port 必须就是这台（串台当场发现，别等跑完才发现）
  local m2 host2 port2 tsv2 hdr
  for m2 in "${MEMBERS[@]}"; do
    host2=${m2%%:*}; port2=${m2##*:}; tsv2="$OUT_DIR/$(base_of "$m2").tsv"
    for _ in $(seq 1 20); do [[ -s "$tsv2" ]] && break; sleep 0.3; done
    if [[ ! -s "$tsv2" ]]; then
      echo "  [monitor] 采样没起来：$host2:$port2（见 $OUT_DIR/$(base_of "$m2").sample.log）" >&2
      exit 2
    fi
    hdr=$(head -1 "$tsv2" | cut -f3,4)
    if [[ "$hdr" != *"$host2"* && "$hdr" != *"$port2"* ]]; then
      echo "  [monitor] 警告：$tsv2 的表头与 $host2:$port2 不匹配（$hdr）" >&2
    fi
  done
}
stop_samplers() {
  local m tsv
  for m in "${MEMBERS[@]}"; do
    tsv="$OUT_DIR/$(base_of "$m").tsv"
    : > "$tsv.stop"
  done
  local pid
  for pid in "${sampler_pids[@]:-}"; do [[ -n "$pid" ]] && wait "$pid" 2>/dev/null; done
  sampler_pids=()
}
trap 'stop_samplers' EXIT

echo "=== monitor：$(date +%T) 开始，输出目录 $OUT_DIR ==="
start_samplers
sleep 1

echo "=== 跑被包的命令：${CMD[*]} ==="
set -o pipefail
"${CMD[@]}" > >(tee "$OUT_DIR/command.log") 2>&1
rc=$?
echo "=== 命令结束（rc=${rc}），停止采样并汇总 ==="
stop_samplers

for m in "${MEMBERS[@]}"; do
  host=${m%%:*}; port=${m##*:}
  ts="$OUT_DIR/$(base_of "$m")"
  "$SYSBENCH" "$LUA" --mysql-host="$host" --mysql-port="$port" "${conn_args[@]}" ring --out="$ts.ring.json" >/dev/null 2>&1 || echo "  [monitor] 警告：$host:$port 的 ring 抓取失败" >&2
  "$SYSBENCH" "$LUA" --mysql-host="$host" --mysql-port="$port" "${conn_args[@]}" summarize \
    --in-file="$ts.tsv" --ring-file="$ts.ring.json" > "$ts.summary.txt" 2>&1 || echo "  [monitor] 警告：$host:$port 的汇总失败" >&2
done

echo
echo "################ 三台并排 ################"
for m in "${MEMBERS[@]}"; do
  f="$OUT_DIR/$(base_of "$m").summary.txt"
  grep -h "^raft_monitor.result" "$f" 2>/dev/null || echo "raft_monitor.result member=$m （缺：$f）"
done
echo
echo "################ 逐台判读（完整版见 <out-dir>/*.summary.txt）################"
for m in "${MEMBERS[@]}"; do
  host=${m%%:*}; port=${m##*:}
  echo; echo "----- $host:$port -----"
  sed -n '/=== 判读/,$p' "$OUT_DIR/$(base_of "$m").summary.txt" 2>/dev/null | head -20
done

# 合成一份 ALL-in-one：发给别人看只需要这一个文件
COMBINED="$OUT_DIR/${TAG}-combined.txt"
{
  echo "# raft monitor 合并报告  $(date '+%F %T')"
  echo "# 被包命令：${CMD[*]}"
  echo
  echo "## 三台并排"
  for m in "${MEMBERS[@]}"; do
    grep -h "^raft_monitor.result" "$OUT_DIR/$(base_of "$m").summary.txt" 2>/dev/null || echo "raft_monitor.result member=$m （缺）"
  done
  echo
  echo "## 逐台完整汇总"
  for m in "${MEMBERS[@]}"; do
    echo; echo "----- $m -----"
    cat "$OUT_DIR/$(base_of "$m").summary.txt" 2>/dev/null
  done
} > "$COMBINED"
echo
echo "monitor.workdir=$OUT_DIR"
echo "monitor.combined=$COMBINED   ← 要发给别人时，这一个文件就够"
[[ "${PRINT_FULL:-0}" -eq 1 ]] && { echo; cat "$COMBINED"; }
exit "$rc"
