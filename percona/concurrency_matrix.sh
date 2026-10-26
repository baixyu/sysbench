#!/usr/bin/env bash
#
# percona/concurrency_matrix.sh —— 并发梯度矩阵：4 个场景 × 一组线程数
#
# 每个"场景 × 线程数"组合都单独用 percona/monitor.sh 包住跑一次（所以每次都有
# 三台状态量采样、事件环 JSONL、客户端区间、主机指标），产物与结果分开落盘：
#
#   <out-root>/<时间戳>/
#     matrix.tsv / matrix.md                 ← 全矩阵总表（每个组合一行）
#     <scenario>/<scenario>-summary.tsv      ← 该场景的梯度表
#     <scenario>/t<NNN>/                     ← 该组合的全部原始数据（monitor 产物）
#         <scenario>-t<NNN>-combined.txt     ← 三台并排 + 逐台汇总
#         *.tsv / *.events.jsonl / client-intervals.tsv / command.log
#         run.txt                            ← 命令行、退出码、耗时
#
# 用法：
#   bash percona/concurrency_matrix.sh \
#     --sysbench=./src/sysbench \
#     --leader-host=10.188.62.79 --leader-port=6606 --leader-user=root --leader-password=root \
#     --member 10.188.62.79:6606 --member 10.214.38.209:6606 --member 10.188.48.140:6606 \
#     --mysql-db=test --tables=64 --table-size=200000 \
#     --threads=4,8,16,32,64,128 --time=60 \
#     --scenarios=ro,linro,wo,rw \
#     --host-metrics --node-exporter-port=9100 \
#     --out-root=/tmp/raft-conc
#
# 场景：
#   ro     oltp_point_select（默认读契约）
#   linro  oltp_point_select --read-consistency=linearizable（强读）
#   wo     oltp_write_only
#   rw     oltp_read_write
#
# 选项：
#   --threads=4,8,16,32,64,128  并发梯度（也接受 4-128:2 这种写法）
#   --time=60                   每个组合跑多少秒
#   --scenarios=ro,linro,wo,rw  只跑其中几个
#   --out-root=/tmp/raft-conc   产物根目录（自动按时间戳建子目录）
#   --settle=5                  组合之间等待秒数（让集群回稳）
#   --skip-preflight            跳过"表是否齐全/行数是否匹配"的预检
#   --no-leader-check           不在每次跑前检查目标是不是当前 Leader（写场景否则会 3098）
#   --mysql-cli=mysql           预检用的客户端（缺省 mysql，可用 MYSQL_CLI 覆盖）
#   --dry-run                   只打印将执行的命令，不跑
#   --interval-ms=500 --ring-interval-ms=250 --host-metrics --node-exporter-port=9100
#   --prometheus-url=URL        （透传给 monitor.sh）
#
# 退出码：0 = 所有组合都 rc==0；非 0 = 有组合失败（明细在 run.txt / matrix.tsv）
#
set -uo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MONITOR=${here}/monitor.sh

SYSBENCH=./src/sysbench
USER=root; PASSWORD=${MYSQL_PWD:-}
LEADER_HOST=127.0.0.1; LEADER_PORT=; LEADER_USER=; LEADER_PASSWORD=
DB=; TABLES=; TABLE_SIZE=
THREADS_SPEC="4,8,16,32,64,128"
TIME_S=60
SCENARIO_SPEC="ro,linro,wo,rw"
OUT_ROOT=/tmp/raft-conc
INTERVAL_MS=500; RING_INTERVAL_MS=250
HOST_METRICS=0; NODE_EXPORTER_PORT=9100; PROMETHEUS_URL=
SETTLE_S=5; DRY_RUN=0; SKIP_PREFLIGHT=0; REQUIRE_LEADER=1
MYSQL_CLI=${MYSQL_CLI:-mysql}
MEMBERS=()

while [[ $# -gt 0 ]]; do
  if [[ "$1" == --*=* ]]; then set -- "${1%%=*}" "${1#*=}" "${@:2}"; fi
  case "$1" in
    --sysbench) SYSBENCH=$2; shift 2;;
    --user) USER=$2; shift 2;;
    --password) PASSWORD=$2; shift 2;;
    --leader-host) LEADER_HOST=$2; shift 2;;
    --leader-port) LEADER_PORT=$2; shift 2;;
    --leader-user) LEADER_USER=$2; shift 2;;
    --leader-password) LEADER_PASSWORD=$2; shift 2;;
    --mysql-db) DB=$2; shift 2;;
    --tables) TABLES=$2; shift 2;;
    --table-size) TABLE_SIZE=$2; shift 2;;
    --threads) THREADS_SPEC=$2; shift 2;;
    --time) TIME_S=$2; shift 2;;
    --scenarios) SCENARIO_SPEC=$2; shift 2;;
    --out-root) OUT_ROOT=$2; shift 2;;
    --interval-ms) INTERVAL_MS=$2; shift 2;;
    --ring-interval-ms) RING_INTERVAL_MS=$2; shift 2;;
    --host-metrics) HOST_METRICS=1; shift;;
    --no-host-metrics) HOST_METRICS=0; shift;;
    --node-exporter-port) NODE_EXPORTER_PORT=$2; shift 2;;
    --prometheus-url) PROMETHEUS_URL=$2; shift 2;;
    --settle) SETTLE_S=$2; shift 2;;
    --mysql-cli) MYSQL_CLI=$2; shift 2;;
    --skip-preflight) SKIP_PREFLIGHT=1; shift;;
    --no-leader-check) REQUIRE_LEADER=0; shift;;
    --dry-run) DRY_RUN=1; shift;;
    --member) MEMBERS+=("$2"); shift 2;;
    *) echo "未知参数：$1（见文件头用法）" >&2; exit 2;;
  esac
done

[[ -n "$LEADER_PORT" ]] || { echo "必须给 --leader-port" >&2; exit 2; }
[[ -n "$DB" ]]         || { echo "必须给 --mysql-db" >&2; exit 2; }
[[ -n "$TABLES" ]]     || { echo "必须给 --tables" >&2; exit 2; }
[[ -n "$TABLE_SIZE" ]] || { echo "必须给 --table-size（与灌数时一致）" >&2; exit 2; }
LEADER_USER=${LEADER_USER:-$USER}
LEADER_PASSWORD=${LEADER_PASSWORD:-$PASSWORD}

# ── 并发梯度：接受 "4,8,16" 或 "4-128:2"（两端含，步长倍率）──────────────
expand_threads() {
  if [[ "$THREADS_SPEC" == *:* ]]; then
    local range=${THREADS_SPEC%%:*} step=${THREADS_SPEC##*:}
    local lo=${range%%-*} hi=${range##*-}
    [[ "$step" =~ ^[0-9]+$ && "$step" -ge 2 ]] || { echo "--threads 的步长必须是 >=2 的整数" >&2; exit 2; }
    local out="" t=$lo
    while [[ $t -le $hi ]]; do out="${out:+$out,}$t"; t=$((t * step)); done
    echo "$out"
  else
    echo "$THREADS_SPEC"
  fi
}
THREADS=$(expand_threads)

scenario_lua() {
  case "$1" in
    ro|linro) echo "src/lua/oltp_point_select.lua";;
    wo)       echo "src/lua/oltp_write_only.lua";;
    rw)       echo "src/lua/oltp_read_write.lua";;
    *) return 1;;
  esac
}
scenario_extra() {
  case "$1" in
    linro) echo "--read-consistency=linearizable";;
    *)     echo "";;
  esac
}
scenario_desc() {
  case "$1" in
    ro)    echo "只读点查";;
    linro) echo "强读点查（LINEARIZABLE）";;
    wo)    echo "只写";;
    rw)    echo "读写混合";;
  esac
}

for sc in ${SCENARIO_SPEC//,/ }; do
  scenario_lua "$sc" >/dev/null || { echo "未知场景：${sc}（可用 ro,linro,wo,rw）" >&2; exit 2; }
done

STAMP=$(date +%Y%m%d-%H%M%S)
ROOT="$OUT_ROOT/$STAMP"
mkdir -p "$ROOT"

conn_args=(--mysql-host="$LEADER_HOST" --mysql-port="$LEADER_PORT" --mysql-user="$LEADER_USER")
[[ -n "$LEADER_PASSWORD" ]] && conn_args+=(--mysql-password="$LEADER_PASSWORD")

echo "=== 并发矩阵：$(date '+%F %T') ==="
echo "    产物根目录：$ROOT"
echo "    场景：$SCENARIO_SPEC   并发：$THREADS   每组合 ${TIME_S}s"
echo "    Leader：$LEADER_HOST:$LEADER_PORT  库：$DB  表：$TABLES 张 × $TABLE_SIZE 行"
[[ ${#MEMBERS[@]} -gt 0 ]] && echo "    监控成员：${MEMBERS[*]}"

# ── 预检：表是否齐全、行数是否与 --table-size 相符 ─────────────────────
if [[ "$SKIP_PREFLIGHT" -eq 0 && "$DRY_RUN" -eq 0 ]]; then
  if command -v "$MYSQL_CLI" >/dev/null 2>&1; then
    cli=("$MYSQL_CLI" --connect-timeout=5 -h"$LEADER_HOST" -P"$LEADER_PORT" -u"$LEADER_USER")
    # 密码走环境变量（只对本次调用生效、不进 argv），与工具其它脚本的口径一致
    missing=$(MYSQL_PWD="$LEADER_PASSWORD" "${cli[@]}" -Nse "
      WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<$TABLES)
      SELECT GROUP_CONCAT(n ORDER BY n) FROM seq
       WHERE NOT EXISTS (SELECT 1 FROM information_schema.tables t
              WHERE t.table_schema='$DB' AND t.table_name=CONCAT('sbtest',n));" 2>/dev/null)
    pre_rc=$?
    if [[ $pre_rc -ne 0 ]]; then
      echo "    预检：$MYSQL_CLI 查询失败（rc=$pre_rc），跳过表齐全性检查" >&2
    elif [[ -n "$missing" ]]; then
      echo "预检失败：${DB} 下缺少 sbtest 表编号 [$missing]（oltp 脚本按 1..--tables 逐表 prepare，缺一张就 FATAL）" >&2
      echo "  修法：先 bash percona/prepare.sh … --skip-load 补齐，或把 --tables 降到连续前缀上限。" >&2
      exit 1
    fi
    # 写许可容量：--percona-raft-write-concurrency 是"同时持有写许可的受管事务上限"，
    # 超过就排队 --percona-raft-write-permit-wait-us（默认 1s），排不到时服务端拒，
    # 客户端只看到通用的 3098（ER_BEFORE_DML_VALIDATION_ERROR，"...external plugin"）。
    PERMIT_BOUND=$(MYSQL_PWD="$LEADER_PASSWORD" "${cli[@]}" -Nse \
      "SELECT @@percona_raft_write_concurrency" 2>/dev/null)
    PERMIT_WAIT=$(MYSQL_PWD="$LEADER_PASSWORD" "${cli[@]}" -Nse \
      "SELECT @@percona_raft_write_permit_wait_us" 2>/dev/null)
    if [[ "$PERMIT_BOUND" =~ ^[0-9]+$ ]]; then
      echo "    预检：percona-raft-write-concurrency=$PERMIT_BOUND（写许可上限），permit-wait-us=${PERMIT_WAIT:-?}"
      for sc0 in ${SCENARIO_SPEC//,/ }; do
        case "$sc0" in wo|rw) ;; *) continue;; esac
        for t0 in ${THREADS//,/ }; do
          if [[ "$t0" -gt "$PERMIT_BOUND" ]]; then
            echo "    预检警告：$sc0 的 threads=$t0 超过写许可上限 $PERMIT_BOUND ⇒ 服务端会排队 ${PERMIT_WAIT:-?}us 后拒绝，客户端看到 3098。" >&2
            echo "              要么把该场景的并发降到 <=$PERMIT_BOUND，要么调高服务端 --percona-raft-write-concurrency / --percona-raft-write-permit-wait-us。" >&2
          fi
        done
      done
    fi

    # 行数抽样（第 1 张表）：oltp 用 --table-size 算主键范围，给大了只会打空、不会报错
    cnt=$(MYSQL_PWD="$LEADER_PASSWORD" "${cli[@]}" -Nse "SELECT COUNT(*) FROM \`$DB\`.sbtest1" 2>/dev/null)
    if [[ "$cnt" =~ ^[0-9]+$ ]]; then
      if [[ "$cnt" != "$TABLE_SIZE" ]]; then
        echo "预检警告：sbtest1 实有 $cnt 行，而 --table-size=${TABLE_SIZE}；主键范围按后者算，可能打空。" >&2
      else
        echo "    预检：$TABLES 张表齐全，sbtest1=$cnt 行 ✓"
      fi
    fi
  else
    echo "    预检：找不到 mysql 客户端（${MYSQL_CLI}），跳过表齐全性检查" >&2
  fi
fi

# ── 从 sysbench 输出与 monitor 报告里抽指标 ────────────────────────────
grab() { # grab <file> <regex>
  grep -E "$2" "$1" 2>/dev/null | tail -1
}
num_after_colon() { awk -F: '{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2}' ; }
per_sec() { sed -E 's/.*\(([0-9.]+) per sec\..*/\1/'; }
parse_run() { # parse_run <run-dir> -> 写一行到 stdout（TSV）
  local dir=$1
  local log="$dir/command.log"
  local tps="" qps="" lavg="" lp95="" lp99="" lmax="" ierr="" leader="" note=""
  if [[ -f "$log" ]]; then
    tps=$(grep -E '^ *transactions: +[0-9]+ +\([0-9.]+ per sec\.\)' "$log" | tail -1 | per_sec)
    qps=$(grep -E '^ *queries: +[0-9]+ +\([0-9.]+ per sec\.\)' "$log" | tail -1 | per_sec)
    lavg=$(grep -E '^ *avg:' "$log" | tail -1 | awk '{print $2}')
    lmax=$(grep -E '^ *max:' "$log" | tail -1 | awk '{print $2}')
    lp95=$(grep -E '^ *95th percentile:' "$log" | tail -1 | awk '{print $3}')
    lp99=$(grep -E '^ *99th percentile:' "$log" | tail -1 | awk '{print $3}')
    ierr=$(grep -E '^ *ignored errors:' "$log" | tail -1 | awk '{print $3}')
    grep -q 'FATAL' "$log" && note="FATAL"
    grep -q '3197\|ER_XA_RETRY' "$log" && note="${note:+$note,}3197"
    if grep -q '3098\|external plugin' "$log"; then
      note="${note:+$note,}3098-permit-bound"
    fi
  fi
  local combined
  combined=$(ls "$dir"/*-combined.txt 2>/dev/null | head -1)
  if [[ -n "$combined" ]]; then
    leader=$(grep -h '^raft_monitor.result' "$combined" | grep "host=.*${LEADER_HOST}" | head -1)
    [[ -z "$leader" ]] && leader=$(grep -h '^raft_monitor.result' "$combined" | head -1)
  fi
  local ud="" al="" gap=""
  if [[ -n "$leader" ]]; then
    ud=$(sed -E 's/.*unknown_delta=([^ ]+).*/\1/' <<<"$leader")
    al=$(sed -E 's/.*max_apply_lag=([^ ]+).*/\1/' <<<"$leader")
    gap=$(sed -E 's/.*post_commit_gap_p95_us=([^ ]+).*/\1/' <<<"$leader")
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${tps:-n/a}" "${qps:-n/a}" "${lavg:-n/a}" "${lp95:-n/a}" "${lp99:-n/a}" "${lmax:-n/a}" \
    "${ierr:-n/a}" "${ud:-n/a}" "${al:-n/a}" "${gap:-n/a}" "${note:-ok}"
}

HDR='scenario\tthreads\ttps\tqps\tlat_avg_ms\tlat_p95_ms\tlat_p99_ms\tlat_max_ms\tignored_errors\tunknown_delta\tmax_apply_lag\tpost_commit_gap_p95_us\tnote'
printf '%b\n' "$HDR" > "$ROOT/matrix.tsv"

overall_rc=0
first=1
for sc in ${SCENARIO_SPEC//,/ }; do
  scdir="$ROOT/$sc"
  mkdir -p "$scdir"
  printf '%b\n' "$HDR" > "$scdir/$sc-summary.tsv"
  echo
  echo "########## 场景 ${sc}（$(scenario_desc "$sc")）→ $scdir"
  for t in ${THREADS//,/ }; do
    [[ "$t" =~ ^[0-9]+$ ]] || { echo "非法线程数：$t" >&2; exit 2; }
    run="$scdir/t$(printf '%03d' "$t")"
    mkdir -p "$run"
    extra=$(scenario_extra "$sc")
    sb_cmd=("$SYSBENCH" "$(scenario_lua "$sc")" --target=raft)
    [[ -n "$extra" ]] && sb_cmd+=($extra)
    sb_cmd+=(--mysql-host="$LEADER_HOST" --mysql-port="$LEADER_PORT"
             --mysql-user="$LEADER_USER")
    [[ -n "$LEADER_PASSWORD" ]] && sb_cmd+=(--mysql-password="$LEADER_PASSWORD")
    sb_cmd+=(--mysql-db="$DB" --tables="$TABLES" --table-size="$TABLE_SIZE"
             --threads="$t" --time="$TIME_S"
             --report-interval=1 --percentile=99 run)

    mon=(bash "$MONITOR" --sysbench="$SYSBENCH" "${MEMBERS[@]}" --user="$USER")
    [[ -n "$PASSWORD" ]] && mon+=(--password="$PASSWORD")
    mon+=(--interval-ms="$INTERVAL_MS" --ring-interval-ms="$RING_INTERVAL_MS")
    [[ "$HOST_METRICS" -eq 1 ]] && mon+=(--host-metrics --node-exporter-port="$NODE_EXPORTER_PORT")
    [[ -n "$PROMETHEUS_URL" ]] && mon+=(--prometheus-url="$PROMETHEUS_URL")
    mon+=(--out-dir="$run" --tag="${sc}-t${t}" -- "${sb_cmd[@]}")

    printf '%s\n' "${mon[*]}" > "$run/run.txt"

    # 3098（ER_TABLE_NEEDS_REBUILD）在本 fork 里是"Raft before-DML 准入门拒绝"：
    # 角色/服务态/binlog/协调态任一不满足就会在执行第一条 DML 时拒。跑前把这几项
    # 记下来，失败时能立刻区分是"连错成员"还是"成员本身没在服务"。
    if command -v "$MYSQL_CLI" >/dev/null 2>&1; then
      MYSQL_PWD="$LEADER_PASSWORD" "$MYSQL_CLI" --connect-timeout=5 -h"$LEADER_HOST" -P"$LEADER_PORT" \
        -u"$LEADER_USER" -Nse "
          SELECT CONCAT_WS('|',
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Percona_raft_state'),
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Percona_raft_serving'),
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Percona_raft_reconciliation_required'),
            (SELECT VARIABLE_VALUE FROM performance_schema.global_variables WHERE VARIABLE_NAME='log_bin'),
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Percona_raft_schema_epoch'))" \
        > "$run/status-before.txt" 2>/dev/null
      state_before=$(cat "$run/status-before.txt" 2>/dev/null)
      printf 'status_before=%s (state|serving|reconciliation|log_bin|epoch)\n' "${state_before:-unknown}" >> "$run/run.txt"
      if [[ "$REQUIRE_LEADER" -eq 1 && "$DRY_RUN" -eq 0 ]]; then
        case "$state_before" in
          LEADER*)
            if [[ "$state_before" == *"|OFF|"* ]]; then
              echo "     警告：$LEADER_HOST:$LEADER_PORT 是 LEADER 但 serving=OFF（$state_before）" >&2
            fi;;
          ""|unknown) echo "     警告：读不到 $LEADER_HOST:$LEADER_PORT 的角色/服务态，继续" >&2;;
          *)
            echo "     警告：$LEADER_HOST:$LEADER_PORT 当前不是 LEADER（$state_before）；写场景会 3098。" >&2
            echo "           用 SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Percona_raft_state' 找当前 Leader。" >&2;;
        esac
      fi
    fi
    echo "   ▶ $sc threads=$t → $run"
    if [[ "$DRY_RUN" -eq 1 ]]; then
      echo "     (dry-run) ${mon[*]}"
      rc=0
    else
      started=$(date +%s)
      "${mon[@]}" > "$run/monitor.stdout" 2>&1
      rc=$?
      printf 'rc=%d elapsed_s=%d\n' "$rc" "$(( $(date +%s) - started ))" >> "$run/run.txt"
    fi
    if [[ $rc -ne 0 ]]; then
      overall_rc=1
      if command -v "$MYSQL_CLI" >/dev/null 2>&1 && [[ "$DRY_RUN" -eq 0 ]]; then
        MYSQL_PWD="$LEADER_PASSWORD" "$MYSQL_CLI" --connect-timeout=5 -h"$LEADER_HOST" -P"$LEADER_PORT" \
          -u"$LEADER_USER" -e "SHOW GLOBAL STATUS LIKE 'Percona_raft_%'" \
          > "$run/status-after.txt" 2>/dev/null
      fi
      {
        echo
        echo "# rc=$rc"
        echo "# 若 command.log 里是 3098 'The table does not comply with the requirements by an external plugin.'："
        echo "#   那是 Raft before-DML 准入门拒绝，真正原因在 **服务端错误日志**（不是客户端）："
        echo "#   grep -n 'first-DML admission refused' <leader 的 error.log> | tail"
        echo "#   3098 = ER_BEFORE_DML_VALIDATION_ERROR（这个 fork 的 before-DML 校验，不是数据问题）："
        echo "#     最常见的直接原因是写许可上限 --percona-raft-write-concurrency（同时持有写许可的事务数）"
        echo "#     被 --threads 超过，排队 --percona-raft-write-permit-wait-us 后仍拿不到 ⇒ 拒绝。"
        echo "#     看 Percona_raft_write_permit_waits / write_permit_wait_timeouts 的增量即可确认。"
        echo "#   常见：no fresh write-quorum proof (not Leader, or peers stale) / write permit unavailable (bound reached …)"
        echo "#         local apply cursor is behind the committed prefix / 该成员没有 binlog"
        echo "#   对应调参：--percona-raft-write-permit-wait-us、admission 上限；或把并发降下来重试。"
      } >> "$run/run.txt"
    fi
    row=$(parse_run "$run")
    if [[ "$DRY_RUN" -eq 1 ]]; then
      row=$(printf 'n/a\tn/a\tn/a\tn/a\tn/a\tn/a\tn/a\tn/a\tn/a\tn/a\tdry-run')
    fi
    printf '%s\t%s\t%s\n' "$sc" "$t" "$row" >> "$scdir/$sc-summary.tsv"
    printf '%s\t%s\t%s\n' "$sc" "$t" "$row" >> "$ROOT/matrix.tsv"
    echo "     rc=$rc  $(cut -f1-4 <<<"$row" | tr '\t' ' ')"
    if [[ "$first" -eq 1 ]]; then first=0; else
      [[ "$DRY_RUN" -eq 0 && "$SETTLE_S" -gt 0 ]] && { echo "     settle ${SETTLE_S}s…"; sleep "$SETTLE_S"; }
    fi
  done
done

# ── 总表（markdown）────────────────────────────────────────────────────
{
  echo "# 并发矩阵 $(date '+%F %T')"
  echo
  echo "- Leader：$LEADER_HOST:$LEADER_PORT  库：$DB  表：$TABLES × $TABLE_SIZE 行"
  echo "- 每组合 ${TIME_S}s；并发：${THREADS}；产物：$ROOT"
  echo
  echo "| 场景 | 并发 | tps | qps | lat_avg | p95 | p99 | lat_max | err | unknown | apply_lag | gap_p95_us |"
  echo "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
  awk -F'\t' 'NR>1 && $1!="scenario" {printf "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13}' "$ROOT/matrix.tsv"
} > "$ROOT/matrix.md"

echo
echo "=== 完成：$(date '+%F %T') ==="
echo "    总表：$ROOT/matrix.tsv  /  $ROOT/matrix.md"
for sc in ${SCENARIO_SPEC//,/ }; do echo "    ${sc}：$ROOT/$sc/$sc-summary.tsv"; done
exit "$overall_rc"
