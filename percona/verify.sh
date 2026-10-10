#!/usr/bin/env bash
#
# percona/verify.sh —— 跨成员数据一致性核对（§6.4）
#
# 为什么需要 wrapper：Lua 层的 drv:connect() 不接受连接参数，一次进程只能连**一个**端点。
# 这里对每个成员各跑一次 src/lua/raft_verify.lua，再把结果比对：
#   1) aggregate：每表 行数 + SUM(CRC32) + BIT_XOR(CRC32)，逐成员必须逐字相同（快速失败信号）
#   2) dump：每表导出 (主键, 摘要) 有序清单，以第一个成员为基准 diff 其余成员
#      ⇒ 差异会**指出第一处不同的行**（§6.4 要的就是这个）
#
# 退出码：
#   0 = PASS（所有成员的聚合与逐行摘要逐字一致）
#   1 = 数据不一致（打印第一处差异）
#   2 = 检查无法完成（连接失败/查询报错/表不存在）
#   注：与 §6.4 提到的 harness 脚本（`run_all_types_bulk_check.sh`）口径略有不同——
#   那个脚本的 2 表示"数据一致但某成员无法分配新 id"。本脚本**不插入**任何行，
#   所以没有这个语义，2 一律表示"这次检查没能跑完"。别把两者混着读。
#
# 用法：
#   percona/verify.sh --sysbench=./src/sysbench \
#     --schema-db=perf --schema-tables=t,sbtest1 \
#     --member 127.0.0.1:48101 --member 127.0.0.1:48102 --member 127.0.0.1:48103 \
#     [--user=root] [--limit=N] [--skip-dump] [--tmpdir=$TMPDIR/raft-verify]
#
set -uo pipefail

SYSBENCH=./src/sysbench
SCHEMA_DB=""
SCHEMA_TABLES=""
USER=root
LIMIT=0
PK_COLUMN=""
SKIP_DUMP=0
SKIP_AGGREGATE=0
TMPDIR_BASE=""
MEMBERS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --sysbench=*) SYSBENCH=${1#*=}; shift;;
    --schema-db=*) SCHEMA_DB=${1#*=}; shift;;
    --schema-tables=*) SCHEMA_TABLES=${1#*=}; shift;;
    --member) MEMBERS+=("$2"); shift 2;;
    --member=*) MEMBERS+=("${1#*=}"); shift;;
    --user=*) USER=${1#*=}; shift;;
    --limit=*) LIMIT=${1#*=}; shift;;
    --pk-column=*) PK_COLUMN=${1#*=}; shift;;   # 没有主键的表（例如只有 UNIQUE 键）逐行 dump 时用它定序
    --skip-dump) SKIP_DUMP=1; shift;;
    --skip-aggregate) SKIP_AGGREGATE=1; shift;;
    --tmpdir=*) TMPDIR_BASE=${1#*=}; shift;;
    *) echo "未知参数：$1" >&2; exit 2;;
  esac
done

[ -n "$SCHEMA_DB" ] || { echo "必须给 --schema-db" >&2; exit 2; }
[ -n "$SCHEMA_TABLES" ] || { echo "必须给 --schema-tables" >&2; exit 2; }
[ ${#MEMBERS[@]} -ge 2 ] || { echo "至少给两个 --member host:port（一个成员无从比对）" >&2; exit 2; }

WORK=${TMPDIR_BASE:-$(mktemp -d "${TMPDIR:-/tmp}/raft-verify.XXXXXX")}
mkdir -p "$WORK"
echo "verify.workdir=$WORK"
echo "verify.members=${MEMBERS[*]}"
echo "verify.schema=$SCHEMA_DB tables=$SCHEMA_TABLES"

# 基准成员（第一个 --member）：aggregate 与 dump 都以它为对照
base=${MEMBERS[0]##*:}

run_verify() { # <host> <port> <command>
  local host=$1 port=$2 cmd=$3
  "$SYSBENCH" ./src/lua/raft_verify.lua \
     --mysql-host="$host" --mysql-port="$port" --mysql-user="$USER" --mysql-db="$SCHEMA_DB" \
     --schema-db="$SCHEMA_DB" --schema-tables="$SCHEMA_TABLES" --limit="$LIMIT" \
     ${PK_COLUMN:+--pk-column="$PK_COLUMN"} "$cmd"
}

########## 1) aggregate：快速失败信号 ##########
# 注意 aggregate 是**快速失败**信号：SUM/BIT_XOR 相等**不保证**逐行相等
# （两行的摘要互换后两个聚合值都不变），所以 dump 那一关不能省——除非你只想快速筛一遍。
if [ $SKIP_AGGREGATE -eq 1 ]; then
  echo "--- aggregate 跳过（--skip-aggregate）---"
else
echo "--- aggregate ---"
rc_all=0
for m in "${MEMBERS[@]}"; do
  host=${m%:*}; port=${m##*:}
  out=$(run_verify "$host" "$port" aggregate 2>&1)
  rc=$?
  printf '%s\n' "$out" > "$WORK/aggregate-$port.txt"
  if [ $rc -ne 0 ]; then
    echo "  成员 ${m}：aggregate 失败（rc=${rc}）"
    printf '%s\n' "$out" | tail -3 | sed 's/^/    /'
    rc_all=2
  else
    echo "  成员 ${m}：$(grep -c '^raft_verify.aggregate=' "$WORK/aggregate-$port.txt") 张表有聚合值"
  fi
done
[ $rc_all -eq 2 ] && { echo "verify.result=INCOMPLETE"; exit 2; }

first=1
for m in "${MEMBERS[@]}"; do
  if [ $first -eq 1 ]; then first=0; continue; fi   # 跳过基准成员（bash 3.2 不支持 ${arr[@]:1} + set -u）
  port=${m##*:}
  if ! diff -q "$WORK/aggregate-$base.txt" "$WORK/aggregate-$port.txt" > /dev/null; then
    echo "  ✗ 聚合不一致：${base} vs ${port}"
    diff -u "$WORK/aggregate-$base.txt" "$WORK/aggregate-$port.txt" | sed -n '1,12p' | sed 's/^/    /'
    echo "verify.result=AGGREGATE_MISMATCH"
    exit 1
  fi
done
echo "  ✓ 所有成员的聚合值逐字相同"
fi

########## 2) dump：逐行摘要 diff（指出第一处差异）##########
if [ $SKIP_DUMP -eq 1 ]; then
  echo "--- dump 跳过（--skip-dump）---"
  echo "verify.result=PASS (aggregate only)"
  exit 0
fi

echo "--- dump ---"
for m in "${MEMBERS[@]}"; do
  host=${m%:*}; port=${m##*:}
  out=$(run_verify "$host" "$port" dump 2>&1)
  rc=$?
  printf '%s\n' "$out" > "$WORK/dump-$port.txt"
  if [ $rc -ne 0 ]; then
    echo "  成员 ${m}：dump 失败（rc=${rc}）"
    printf '%s\n' "$out" | tail -3 | sed 's/^/    /'
    echo "verify.result=INCOMPLETE"
    exit 2
  fi
  echo "  成员 ${m}：$(wc -l < "$WORK/dump-$port.txt" | tr -d ' ') 行（含标记行）"
done

first=1
for m in "${MEMBERS[@]}"; do
  if [ $first -eq 1 ]; then first=0; continue; fi
  port=${m##*:}
  if diff -q "$WORK/dump-$base.txt" "$WORK/dump-$port.txt" > /dev/null; then
    echo "  ✓ ${base} 与 ${port} 的逐行摘要一致"
  else
    echo "  ✗ 逐行摘要不一致：${base} vs ${port}（下面是第一处差异）"
    diff "$WORK/dump-$base.txt" "$WORK/dump-$port.txt" | sed -n '1,10p' | sed 's/^/    /'
    echo "verify.result=ROW_MISMATCH (第一处差异见上；基准文件 ${WORK}/dump-${base}.txt，对照 ${WORK}/dump-${port}.txt)"
    exit 1
  fi
done

echo "verify.result=PASS"
exit 0
