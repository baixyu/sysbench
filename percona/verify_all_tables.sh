#!/usr/bin/env bash
#
# percona/verify_all_tables.sh —— 一次核对"所有表"的跨成员数据一致性
#
# 复用 percona/verify.sh（aggregate 口径：每表 COUNT(*) + SUM(CRC32(行摘要)) + BIT_XOR(...)），
# 但把结果整理成**逐表结论**：哪张表一致、哪张表不一致、差在哪个数。
#
#   · 默认只跑 aggregate（快：每表 3 条聚合查询/成员），给出逐表 PASS/FAIL；
#   · --dump 再加一关逐行摘要 diff（慢，会指出第一处不同的行；表多时建议配合 --limit）；
#   · 发现某张表不一致后，用 --only=<表> 只看那张（会自动切到 dump 口径定位行）。
#
# 用法：
#   bash percona/verify_all_tables.sh \
#     --sysbench=./src/sysbench --schema-db=test \
#     --table-prefix=sbtest --tables=64 \
#     --member 10.188.62.79:6606 --member 10.214.38.209:6606 --member 10.188.48.140:6606 \
#     --user=root --password=root
#
# 选项：
#   --tables=N             表名 = <prefix>1..N（默认 64）
#   --table-prefix=P       默认 sbtest
#   --discover             用 mysql 客户端从 information_schema 发现 ^P[0-9]+$ 表（忽略 --tables）
#   --only=a,b             只核对这几张（隐含 --dump，用于定位不一致）
#   --dump                 额外做逐行摘要 diff（默认只 aggregate）
#   --limit=N              dump 的最大行数（0=不限）
#   --pk-column=COL        无主键表（只有 UNIQUE）时指定定序列
#   --mysql-cli=mysql      --discover/预检用的客户端
#   --out-dir=DIR          保存 verify.sh 原始输出（默认 /tmp/raft-verify-all.<时间戳>）
#   --quiet                只打印结论汇总（不打印 verify.sh 全文）
#
# 退出码：0 = 全部一致；1 = 有表不一致（逐表列出）；2 = 检查无法完成（连接/查询失败）
#
set -uo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
VERIFY=${here}/verify.sh

SYSBENCH=./src/sysbench
DB=""; PREFIX=sbtest; NTABLES=64; DISCOVER=0; ONLY=""
MEMBERS=(); USER=root; PASSWORD=${MYSQL_PWD:-}
DO_DUMP=0; LIMIT=0; PK_COL=0; PK_COLUMN=""
OUT_DIR=""; QUIET=0; MYSQL_CLI=${MYSQL_CLI:-mysql}

while [ $# -gt 0 ]; do
  case "$1" in
    --sysbench=*) SYSBENCH=${1#*=}; shift;;
    --schema-db=*) DB=${1#*=}; shift;;
    --table-prefix=*) PREFIX=${1#*=}; shift;;
    --tables=*) NTABLES=${1#*=}; shift;;
    --discover) DISCOVER=1; shift;;
    --only=*) ONLY=${1#*=}; DO_DUMP=1; shift;;
    --dump) DO_DUMP=1; shift;;
    --limit=*) LIMIT=${1#*=}; shift;;
    --pk-column=*) PK_COLUMN=${1#*=}; shift;;
    --member) MEMBERS+=("$2"); shift 2;;
    --member=*) MEMBERS+=("${1#*=}"); shift;;
    --user=*) USER=${1#*=}; shift;;
    --password=*) PASSWORD=${1#*=}; shift;;
    --password) PASSWORD=$2; shift 2;;
    --mysql-cli=*) MYSQL_CLI=${1#*=}; shift;;
    --out-dir=*) OUT_DIR=${1#*=}; shift;;
    --quiet) QUIET=1; shift;;
    *) echo "未知参数：$1（见文件头用法）" >&2; exit 2;;
  esac
done

[ -n "$DB" ] || { echo "必须给 --schema-db" >&2; exit 2; }
[ ${#MEMBERS[@]} -ge 2 ] || { echo "至少给两个 --member host:port" >&2; exit 2; }

# ── 表清单 ────────────────────────────────────────────────────────────────
if [ -n "$ONLY" ]; then
  TABLE_LIST=$ONLY
elif [ "$DISCOVER" -eq 1 ]; then
  first=${MEMBERS[0]}; host=${first%%:*}; port=${first##*:}
  TABLE_LIST=$(MYSQL_PWD="$PASSWORD" "$MYSQL_CLI" --connect-timeout=5 -h"$host" -P"$port" -u"$USER" -Nse "
    SELECT GROUP_CONCAT(table_name ORDER BY CAST(SUBSTRING(table_name, LENGTH('$PREFIX')+1) AS UNSIGNED))
      FROM information_schema.tables
     WHERE table_schema='$DB' AND table_name REGEXP '^${PREFIX}[0-9]+$';" 2>/dev/null)
  [ -n "${TABLE_LIST:-}" ] || { echo "从 $first 发现不到 ${PREFIX}N 形式的表" >&2; exit 2; }
else
  TABLE_LIST=$(printf "${PREFIX}%d," $(seq 1 "$NTABLES")); TABLE_LIST=${TABLE_LIST%,}
fi
NFTABLES=$(awk -F, '{print NF}' <<<"$TABLE_LIST")

STAMP=$(date +%Y%m%d-%H%M%S)
OUT=${OUT_DIR:-/tmp/raft-verify-all.$STAMP}
mkdir -p "$OUT"

echo "=== 跨成员数据一致性核对：$DB 的 $NFTABLES 张表 ==="
echo "    成员：${MEMBERS[*]}"
echo "    口径：aggregate${DO_DUMP:+$( [ "$DO_DUMP" -eq 1 ] && echo ' + dump' )}"
echo "    输出：$OUT"

# ── 调 verify.sh（aggregate 一次覆盖所有表）──────────────────────────────
vargs=(--sysbench="$SYSBENCH" --schema-db="$DB" --schema-tables="$TABLE_LIST" --user="$USER")
[ -n "$PASSWORD" ] && vargs+=(--password="$PASSWORD")
[ "$LIMIT" -gt 0 ] && vargs+=(--limit="$LIMIT")
[ -n "$PK_COLUMN" ] && vargs+=(--pk-column="$PK_COLUMN")
[ "$DO_DUMP" -eq 0 ] && vargs+=(--skip-dump)
for m in "${MEMBERS[@]}"; do vargs+=(--member "$m"); done

bash "$VERIFY" "${vargs[@]}" > "$OUT/verify.out" 2>&1
vrc=$?
[ "$QUIET" -eq 0 ] && { cat "$OUT/verify.out"; echo; }

WORK=$(sed -n 's/^verify.workdir=//p' "$OUT/verify.out" | head -1)

# ── 逐表比较 aggregate 文件 ───────────────────────────────────────────────
# 每成员一份 aggregate-<port>.txt，行格式：
#   raft_verify.aggregate=<表> rows=<n> sum_crc32=<s> bit_xor_crc32=<x>
printf '%-12s' "表"; for m in "${MEMBERS[@]}"; do printf '%-10s' "${m##*:}"; done; echo "  结论"
printf '%-12s' "----"; for m in "${MEMBERS[@]}"; do printf '%-10s' "----"; done; echo "  ----"

mismatch=()
if [ -n "$WORK" ] && [ -d "$WORK" ]; then
  for t in ${TABLE_LIST//,/ }; do
    line_base=""
    ok=1
    vals=()
    for m in "${MEMBERS[@]}"; do
      port=${m##*:}
      f="$WORK/aggregate-$port.txt"
      if [ -f "$f" ]; then
        cur=$(grep -E "^raft_verify\.aggregate=${t}( |$)" "$f" | head -1 | sed 's/^raft_verify\.aggregate=[^ ]* //')
      else
        cur="NOFILE"
      fi
      vals+=("$cur")
      if [ -z "$line_base" ]; then line_base="$cur"; elif [ "$cur" != "$line_base" ]; then ok=0; fi
    done
    printf '%-12s' "$t"
    for v in "${vals[@]}"; do
      # 只显示 rows 数，其余差异在下面点出来
      r=$(sed -n 's/^rows=\([0-9]*\).*/\1/p' <<<"$v"); printf '%-10s' "${r:-?}"
    done
    if [ "$ok" -eq 1 ]; then echo "  一致"; else echo "  ✗ 不一致"; mismatch+=("$t"); fi
  done
else
  echo "（拿不到 aggregate 明细，workdir=${WORK:-?}；见 $OUT/verify.out）" >&2
fi

echo
echo "=== 结论 ==="
if [ ${#mismatch[@]} -eq 0 ] && [ "$vrc" -eq 0 ]; then
  echo "  ✓ $NFTABLES/$NFTABLES 张表的聚合值在三台逐字相同（aggregate 口径）"
  [ "$DO_DUMP" -eq 0 ] && echo "  提示：aggregate 相等不保证逐行相等（两行摘要互换时聚合不变）；要逐行确认加 --dump 或 --only=<表>。"
  exit 0
fi
if [ ${#mismatch[@]} -gt 0 ]; then
  echo "  ✗ ${#mismatch[@]} 张表不一致：${mismatch[*]}"
  echo "  定位第一处不同的行：bash percona/verify_all_tables.sh ... --only=${mismatch[0]}"
  exit 1
fi
echo "  ! 检查未完成（verify.sh rc=$vrc）——见 $OUT/verify.out"
exit 2
