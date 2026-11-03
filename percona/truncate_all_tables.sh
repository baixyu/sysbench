#!/usr/bin/env bash
#
# percona/truncate_all_tables.sh —— 逐表走受管 transition 把表清空
#
# 等价于对每张表执行：
#   SELECT PERCONA_RAFT_SCHEMA_TRANSITION('TRUNCATE TABLE `db`.`tbl`');
#
# 为什么要走 transition 而不是直接 TRUNCATE：受管集群的用户 DDL 必须经
# PERCONA_RAFT_SCHEMA_TRANSITION() 这一条通道（TRUNCATE TABLE 在允许的语句集合内），
# 每条 SQL 都是一次 schema transition ⇒ epoch +1、并复制到所有成员。
#
# 用法：
#   bash percona/truncate_all_tables.sh \
#     --schema-db=test --table-prefix=sbtest --tables=64 \
#     --member 10.188.62.79:6606 --member 10.214.38.209:6606 --member 10.188.48.140:6606 \
#     --user=root --password=root \
#     --dry-run            # 先看清单；确认后把 --dry-run 换成 --yes 才会真的执行
#
# 选项：
#   --tables=N              表名 = <prefix>1..N（默认 64）
#   --table-prefix=P       默认 sbtest
#   --discover              从 information_schema 发现 ^P[0-9]+$ 的表（忽略 --tables）
#   --only=a,b              只处理这几张
#   --leader-host=H --leader-port=P  显式指定 Leader（不给则从 --member 自动探测）
#   --member h:p            成员列表（可重复；用于探测 Leader 与执行后核对 epoch）
#   --wait-members=SEC      执行后等每个成员 schema_epoch 追到最终值（默认 120；0=不等）
#   --keep-going            某张失败后继续下一条（默认失败即停）
#   --dry-run               只打印将执行的 SQL，不执行
#   --yes                   确认执行（非 --dry-run 时必须给）
#   --mysql-cli=mysql       mysql 客户端
#   --mysql-cli-args="..."  追加给 mysql 客户端（如 --get-server-public-key）
#
# 退出码：0 = 全部 APPLIED 且（可选的）各成员 epoch 已追平；1 = 有表失败；2 = 前置条件不满足
#
set -uo pipefail

DB=""; PREFIX=sbtest; NTABLES=64; DISCOVER=0; ONLY=""
LEADER_HOST=""; LEADER_PORT=""; MEMBERS=()
USER=root; PASSWORD=${MYSQL_PWD:-}
WAIT_MEMBERS=120; KEEP_GOING=0; DRY_RUN=0; YES=0
MYSQL_CLI=${MYSQL_CLI:-mysql}; CLI_EXTRA=()

while [ $# -gt 0 ]; do
  case "$1" in
    --schema-db=*) DB=${1#*=}; shift;;
    --table-prefix=*) PREFIX=${1#*=}; shift;;
    --tables=*) NTABLES=${1#*=}; shift;;
    --discover) DISCOVER=1; shift;;
    --only=*) ONLY=${1#*=}; shift;;
    --leader-host=*) LEADER_HOST=${1#*=}; shift;;
    --leader-port=*) LEADER_PORT=${1#*=}; shift;;
    --member) MEMBERS+=("$2"); shift 2;;
    --member=*) MEMBERS+=("${1#*=}"); shift;;
    --user=*) USER=${1#*=}; shift;;
    --password=*) PASSWORD=${1#*=}; shift;;
    --password) PASSWORD=$2; shift 2;;
    --wait-members=*) WAIT_MEMBERS=${1#*=}; shift;;
    --keep-going) KEEP_GOING=1; shift;;
    --dry-run) DRY_RUN=1; shift;;
    --yes) YES=1; shift;;
    --mysql-cli=*) MYSQL_CLI=${1#*=}; shift;;
    --mysql-cli-args) read -r -a CLI_EXTRA <<<"$2"; shift 2;;
    --mysql-cli-args=*) read -r -a CLI_EXTRA <<<"${1#*=}"; shift;;
    *) echo "未知参数：$1（见文件头用法）" >&2; exit 2;;
  esac
done

[ -n "$DB" ] || { echo "必须给 --schema-db" >&2; exit 2; }
if [ "$DRY_RUN" -eq 0 ] && [ "$YES" -eq 0 ]; then
  echo "拒绝执行：这会逐表清空数据（每张表一次 schema transition，epoch 会 +N）。" >&2
  echo "  先看清单：加 --dry-run；确认后改成 --yes。" >&2
  exit 2
fi

q() { # q <host> <port> <sql>  -> 单值输出
  MYSQL_PWD="$PASSWORD" "$MYSQL_CLI" --connect-timeout=5 "${CLI_EXTRA[@]}" \
    -h"$1" -P"$2" -u"$USER" -Nse "$3" 2>/dev/null
}

# ── 表清单 ────────────────────────────────────────────────────────────────
if [ -n "$ONLY" ]; then
  TABLES=$ONLY
elif [ "$DISCOVER" -eq 1 ]; then
  first=${MEMBERS[0]:-}; [ -n "$first" ] || { echo "--discover 需要至少一个 --member" >&2; exit 2; }
  TABLES=$(q "${first%%:*}" "${first##*:}" "
    SELECT GROUP_CONCAT(table_name ORDER BY CAST(SUBSTRING(table_name, LENGTH('$PREFIX')+1) AS UNSIGNED))
      FROM information_schema.tables
     WHERE table_schema='$DB' AND table_name REGEXP '^${PREFIX}[0-9]+$';")
  [ -n "${TABLES:-}" ] || { echo "发现不到 ${PREFIX}N 形式的表（在 ${first} 上查 $DB）" >&2; exit 2; }
else
  TABLES=$(printf "${PREFIX}%d," $(seq 1 "$NTABLES")); TABLES=${TABLES%,}
fi
N=$(awk -F, '{print NF}' <<<"$TABLES")

# ── 定位 Leader ───────────────────────────────────────────────────────────
if [ -z "$LEADER_PORT" ]; then
  for m in "${MEMBERS[@]}"; do
    st=$(q "${m%%:*}" "${m##*:}" "SHOW STATUS LIKE 'Percona_raft_state'" | awk '{print $2}')
    if [ "$st" = "LEADER" ]; then LEADER_HOST=${m%%:*}; LEADER_PORT=${m##*:}; fi
  done
  [ -n "$LEADER_PORT" ] || { echo "没有成员报 LEADER（选举中？）——用 --leader-host/--leader-port 显式指定" >&2; exit 2; }
else
  LEADER_HOST=${LEADER_HOST:-${MEMBERS[0]%%:*}}
fi
echo "=== 清空 $DB 的 $N 张表（受管 transition）==="
echo "    Leader：$LEADER_HOST:$LEADER_PORT"
echo "    表：${TABLES:0:120}$([ ${#TABLES} -gt 120 ] && echo '…')"

epoch0=$(q "$LEADER_HOST" "$LEADER_PORT" "SHOW STATUS LIKE 'Percona_raft_schema_epoch'" | awk '{print $2}')
echo "    schema_epoch 起始：${epoch0:-?}"

# ── 逐表执行 ──────────────────────────────────────────────────────────────
ok=0; failed=(); i=0
for t in ${TABLES//,/ }; do
  i=$((i + 1))
  sql="TRUNCATE TABLE \`$DB\`.\`$t\`"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '  [%d/%d] %s\n' "$i" "$N" "$sql"
    continue
  fi
  out=$(q "$LEADER_HOST" "$LEADER_PORT" "SELECT PERCONA_RAFT_SCHEMA_TRANSITION('$sql')")
  res=$(grep -oE '"result"[[:space:]]*:[[:space:]]*"[A-Z_]+"' <<<"$out" | sed 's/.*"\([A-Z_]*\)"$/\1/' | head -1)
  to_e=$(grep -oE '"to_epoch"[[:space:]]*:[[:space:]]*[0-9]+' <<<"$out" | grep -oE '[0-9]+$' | head -1)
  if [ "$res" = "APPLIED" ]; then
    ok=$((ok + 1))
    printf '  [%d/%d] %-12s APPLIED  epoch→%s\n' "$i" "$N" "$t" "${to_e:-?}"
  else
    failed+=("$t")
    printf '  [%d/%d] %-12s ✗ %s\n' "$i" "$N" "$t" "${res:-无返回}"
    printf '          返回：%s\n' "${out:0:300}"
    [ "$KEEP_GOING" -eq 1 ] || { echo "  停止（未加 --keep-going）" >&2; break; }
  fi
done

[ "$DRY_RUN" -eq 1 ] && { echo "(dry-run) 以上为将执行的 SQL；确认后加 --yes 执行"; exit 0; }

# ── 等各成员追平 epoch ────────────────────────────────────────────────────
final_epoch=$(q "$LEADER_HOST" "$LEADER_PORT" "SHOW STATUS LIKE 'Percona_raft_schema_epoch'" | awk '{print $2}')
echo
echo "=== 结果：APPLIED ${ok}/${N}，失败 ${#failed[@]}，Leader epoch ${epoch0:-?} → ${final_epoch:-?} ==="
if [ ${#failed[@]} -gt 0 ]; then
  echo "    失败：${failed[*]}"
fi

lag=0
if [ "$WAIT_MEMBERS" -gt 0 ] && [ ${#MEMBERS[@]} -gt 0 ] && [ -n "${final_epoch:-}" ]; then
  echo "--- 等各成员 schema_epoch 追到 $final_epoch（最多 ${WAIT_MEMBERS}s）---"
  deadline=$(( $(date +%s) + WAIT_MEMBERS ))
  while :; do
    pending=()
    for m in "${MEMBERS[@]}"; do
      e=$(q "${m%%:*}" "${m##*:}" "SHOW STATUS LIKE 'Percona_raft_schema_epoch'" | awk '{print $2}')
      [ "${e:-0}" = "$final_epoch" ] || pending+=("${m}:${e:-?}")
    done
    [ ${#pending[@]} -eq 0 ] && { echo "    ✓ 所有成员 epoch=$final_epoch"; break; }
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "    ✗ 未追平：${pending[*]}" >&2; lag=1; break
    fi
    sleep 2
  done
fi

[ "$lag" -eq 0 ] && [ ${#failed[@]} -eq 0 ] && exit 0
[ ${#failed[@]} -gt 0 ] && exit 1
exit 1
