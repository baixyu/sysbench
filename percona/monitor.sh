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
RING_INTERVAL_MS=250
LOCK_WAITS=0
LOCK_INTERVAL_MS=250
HOST_METRICS=0
HOST_INTERVAL_MS=1000
NODE_EXPORTER_PORT=9100
PROMETHEUS_URL=
NODE_EXPORTER_URLS=()
PROMETHEUS_INSTANCES=()
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
    --ring-interval-ms) RING_INTERVAL_MS=$2; shift 2;;
    --lock-waits) LOCK_WAITS=1; shift;;
    --lock-interval-ms) LOCK_INTERVAL_MS=$2; shift 2;;
    --host-metrics) HOST_METRICS=1; shift;;
    --no-host-metrics) HOST_METRICS=0; shift;;
    --host-interval-ms) HOST_INTERVAL_MS=$2; shift 2;;
    --node-exporter-port) NODE_EXPORTER_PORT=$2; shift 2;;
    --node-exporter-url) NODE_EXPORTER_URLS+=("$2"); shift 2;;
    --prometheus-url) PROMETHEUS_URL=$2; shift 2;;
    --prometheus-instance) PROMETHEUS_INSTANCES+=("$2"); shift 2;;
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
if [[ "$LOCK_WAITS" -eq 1 && ( ! "$LOCK_INTERVAL_MS" =~ ^[0-9]+$ || "$LOCK_INTERVAL_MS" -lt 100 ) ]]; then
  echo "--lock-interval-ms 必须是 >= 100 的整数" >&2
  exit 2
fi

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
ring_pids=()
lock_pids=()
host_pids=()
host_files=()
start_samplers() {
  local m host port tsv lock_file suffix i=0
  for m in "${MEMBERS[@]}"; do
    host=${m%%:*}; port=${m##*:}; i=$((i + 1))
    tsv="$OUT_DIR/$(base_of "$m").tsv"
    rm -f "$tsv.stop" "$OUT_DIR/$(base_of "$m").events.jsonl" "$OUT_DIR/$(base_of "$m").events.jsonl.stop"
    "$SYSBENCH" "$LUA" --mysql-host="$host" --mysql-port="$port" "${conn_args[@]}" \
      sample --out="$tsv" --interval-ms="$INTERVAL_MS" > "$OUT_DIR/$(base_of "$m").sample.log" 2>&1 &
    sampler_pids+=("$!")
    "$SYSBENCH" "$LUA" --mysql-host="$host" --mysql-port="$port" "${conn_args[@]}" \
      ring-watch --out="$OUT_DIR/$(base_of "$m").events.jsonl" --interval-ms="$RING_INTERVAL_MS" \
      > "$OUT_DIR/$(base_of "$m").ring-watch.log" 2>&1 &
    ring_pids+=("$!")
    if [[ "$LOCK_WAITS" -eq 1 ]]; then
      lock_file="$OUT_DIR/$(base_of "$m").lock-waits.jsonl"
      rm -f "$lock_file" "$lock_file.stop"
      "$SYSBENCH" "$LUA" --mysql-host="$host" --mysql-port="$port" "${conn_args[@]}" \
        lock-watch --out="$lock_file" --lock-interval-ms="$LOCK_INTERVAL_MS" \
        > "$OUT_DIR/$(base_of "$m").lock-watch.log" 2>&1 &
      lock_pids+=("$!")
    fi
    suffix=""
    [[ "$LOCK_WAITS" -eq 0 ]] || suffix=" + lock-waits.jsonl"
    echo "  [monitor] 采样启动：$host:$port → $tsv + events.jsonl$suffix"
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
    if [[ "$hdr" != *"port=$port2"* || "$hdr" != *"host="* ]]; then
      echo "  [monitor] 警告：$tsv2 的表头缺少目标端口或主机名（$hdr）" >&2
    fi
  done
}
start_host_monitors() {
  [[ "$HOST_METRICS" -eq 1 ]] || return 0
  command -v python3 >/dev/null 2>&1 || { echo "[monitor] --host-metrics 需要压测机上的 python3，主机指标未启动" >&2; return 0; }
  [[ "$HOST_INTERVAL_MS" =~ ^[0-9]+$ && "$NODE_EXPORTER_PORT" =~ ^[0-9]+$ ]] || {
    echo "[monitor] host interval/Node Exporter port 必须是整数，主机指标未启动" >&2; return 0;
  }
  local m host endpoint source instance map key file already
  local seen_hosts=()
  for m in "${MEMBERS[@]}"; do
    host=${m%%:*}
    already=0
    for key in "${seen_hosts[@]}"; do [[ "$key" == "$host" ]] && already=1; done
    [[ "$already" -eq 0 ]] || continue
    seen_hosts+=("$host")
    if [[ -n "$PROMETHEUS_URL" ]]; then
      source=prometheus; endpoint=$PROMETHEUS_URL; instance="${host}:${NODE_EXPORTER_PORT}"
      for map in "${PROMETHEUS_INSTANCES[@]}"; do
        key=${map%%=*}; [[ "$key" == "$host" ]] && instance=${map#*=}
      done
    else
      source=node-exporter; endpoint="http://${host}:${NODE_EXPORTER_PORT}"
      for map in "${NODE_EXPORTER_URLS[@]}"; do
        key=${map%%=*}; [[ "$key" == "$host" ]] && endpoint=${map#*=}
      done
    fi
    file="$OUT_DIR/${TAG}-host-$(sanitize "$host").tsv"
    rm -f "$file.stop"
    host_files+=("$file")
    local args=(--source "$source" --url "$endpoint" --out "$file" --interval-ms "$HOST_INTERVAL_MS")
    [[ "$source" != prometheus ]] || args+=(--instance "$instance")
    python3 "$here/host_monitor.py" "${args[@]}" > "$file.log" 2>&1 &
    host_pids+=("$!")
    echo "  [monitor] 主机指标启动：$host → $file ($source)"
  done
}

stop_samplers() {
  local m tsv
  for m in "${MEMBERS[@]}"; do
    tsv="$OUT_DIR/$(base_of "$m").tsv"
    : > "$tsv.stop"
    : > "$OUT_DIR/$(base_of "$m").events.jsonl.stop"
    [[ "$LOCK_WAITS" -eq 0 ]] || : > "$OUT_DIR/$(base_of "$m").lock-waits.jsonl.stop"
  done
  local f
  for f in "${host_files[@]:-}"; do : > "$f.stop"; done
  local pid
  for pid in "${sampler_pids[@]:-}"; do [[ -n "$pid" ]] && wait "$pid" 2>/dev/null; done
  for pid in "${ring_pids[@]:-}"; do [[ -n "$pid" ]] && wait "$pid" 2>/dev/null; done
  for pid in "${lock_pids[@]:-}"; do [[ -n "$pid" ]] && wait "$pid" 2>/dev/null; done
  for pid in "${host_pids[@]:-}"; do [[ -n "$pid" ]] && wait "$pid" 2>/dev/null; done
  sampler_pids=()
  ring_pids=()
  lock_pids=()
  host_pids=()
}
trap 'stop_samplers' EXIT

echo "=== monitor：$(date +%T) 开始，输出目录 $OUT_DIR ==="
start_samplers
start_host_monitors
sleep 1

echo "=== 跑被包的命令：${CMD[*]} ==="
set -o pipefail
"${CMD[@]}" 2>&1 | tee "$OUT_DIR/command.log" | while IFS= read -r line; do
  printf '%s\n' "$line"
  ts_now=$(date +%s)
  if [[ "$line" == ===* ]]; then printf '%s\t%s\n' "$ts_now" "$line" >> "$OUT_DIR/phase-markers.tsv"; fi
  if [[ "$line" == *tps:* && "$line" == *lat* ]]; then printf '%s\t%s\n' "$ts_now" "$line" >> "$OUT_DIR/client-intervals.tsv"; fi
done
rc=${PIPESTATUS[0]}
echo "=== 命令结束（rc=${rc}），停止采样并汇总 ==="
stop_samplers

for m in "${MEMBERS[@]}"; do
  host=${m%%:*}; port=${m##*:}
  ts="$OUT_DIR/$(base_of "$m")"
  summary_args=(--in-file="$ts.tsv" --ring-file="$ts.events.jsonl")
  [[ -s "$OUT_DIR/client-intervals.tsv" ]] && summary_args+=(--client-file="$OUT_DIR/client-intervals.tsv")
  "$SYSBENCH" "$LUA" --mysql-host="$host" --mysql-port="$port" "${conn_args[@]}" summarize \
    "${summary_args[@]}" > "$ts.summary.txt" 2>&1 || echo "  [monitor] 警告：$host:$port 的汇总失败" >&2
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
  echo "# 客户端区间输出：$OUT_DIR/client-intervals.tsv（需被包命令启用 sysbench --report-interval）"
  echo "# 阶段标记：$OUT_DIR/phase-markers.tsv"
  for m in "${MEMBERS[@]}"; do
    echo "# 采样文件：$OUT_DIR/$(base_of "$m").tsv；事件：$OUT_DIR/$(base_of "$m").events.jsonl"
    if [[ "$LOCK_WAITS" -eq 1 ]]; then
      echo "# 锁等待：$OUT_DIR/$(base_of "$m").lock-waits.jsonl；日志：$OUT_DIR/$(base_of "$m").lock-watch.log"
    fi
  done
  for f in "${host_files[@]:-}"; do echo "# 主机指标：$f；日志：$f.log"; done
  echo
  echo "## 三台并排"
  for m in "${MEMBERS[@]}"; do
    grep -h "^raft_monitor.result" "$OUT_DIR/$(base_of "$m").summary.txt" 2>/dev/null || echo "raft_monitor.result member=$m （缺）"
  done
  echo
  if [[ ${#host_files[@]} -gt 0 ]]; then
    echo "## 主机指标窗口摘要"
    for f in "${host_files[@]}"; do
      echo "### $f"
      awk -F '\t' 'NR > 1 && $3 != "" { n++; cpu += $3; if ($4 > iowait) iowait = $4; if (!have_mem || $5 < min_mem) { min_mem = $5; have_mem = 1 } if ($10 > disk_util) disk_util = $10; if ($11 > disk_await) disk_await = $11; drops += $15; retrans += $16; if ($17 != "") { load += $17; if ($17 > load_max) load_max = $17; } if ($18 > procs_running) procs_running = $18; if ($19 > procs_blocked) procs_blocked = $19; ctxt += $20; sched_wait += $21; if ($22 > cpu_psi) cpu_psi = $22 } END { if (!n) print "samples=0（尚无完整计数器区间；检查同名 .log）"; else printf "samples=%d cpu_busy_avg=%.2f%% iowait_max=%.2f%% memavailable_min=%.0f disk_util_max=%.2f%% disk_await_max=%.2fms net_drops_delta=%.0f tcp_retrans_delta=%.0f load1_avg=%.2f load1_max=%.2f procs_running_max=%.0f procs_blocked_max=%.0f context_switches_avg_per_s=%.0f sched_wait_cpu_avg_core_s_per_s=%.3f cpu_psi_some_max=%.2f%%\n", n, cpu/n, iowait, min_mem, disk_util, disk_await, drops, retrans, load/n, load_max, procs_running, procs_blocked, ctxt/n, sched_wait/n, cpu_psi }' "$f"
    done
    echo
  fi
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
