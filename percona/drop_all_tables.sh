#!/usr/bin/env bash
#
# percona/drop_all_tables.sh —— 逐表走受管 transition 把表 DROP 掉
#
# 等价于对每张表执行：
#   SELECT PERCONA_RAFT_SCHEMA_TRANSITION('DROP TABLE `db`.`tbl`');
#
# 为什么要走 transition 而不是直接 DROP：受管集群的用户 DDL 必须经
# PERCONA_RAFT_SCHEMA_TRANSITION() 这一条通道（DROP TABLE 在允许的语句集合内），
# 每次 DROP 都是一次 schema transition ⇒ manifest 少一张表、epoch +1、并复制到所有成员。
#
# 与 truncate 脚本的区别：DROP 只对**存在的表**有意义，所以默认 --discover 列出当前真实
# 存在的表；显式给 --tables=N 时也会先过滤掉不存在的表（否则那条 transition 会被拒）。
# 全删完后 manifest 为空，要重新建表就再跑 percona/prepare.sh。
#
# 用法：
#   bash percona/drop_all_tables.sh \
#     --schema-db=test --table-prefix=sbtest --tables=64 \
#     --member 10.188.62.79:6606 --member 10.214.38.209:6606 --member 10.188.48.140:6606 \
#     --user=root --password=root \
#     --dry-run            # 先看清单；确认后把 --dry-run 换成 --yes 才会真的执行
#
# 选项：
#   --tables=N              表名 = <prefix>1..N（不给则用 --discover）
#   --table-prefix=P       默认 sbtest
#   --discover             从 information_schema 发现 ^P[0-9]+$ 的表（**默认就开**）
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

DB=""; PREFIX=sbtest; NTABLES=64; DISCOVER=1; ONLY=""
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

# 客户端可用性：不同发行版/安装方式的 mysql 客户端能力不同（TLS、caching_sha2 插件的
# 支持），脚本默认用 PATH 里的 mysql；需要时用 --mysql-cli 指定完整路径。
if ! command -v "$MYSQL_CLI" >/dev/null 2>&1 && [ ! -x "$MYSQL_CLI" ]; then
  echo "✗ 找不到 mysql 客户端：$MYSQL_CLI" >&2
  echo "    用 --mysql-cli=<完整路径> 指定，例如：--mysql-cli=/usr/local/mysql/bin/mysql" >&2
  echo "    或设环境变量：MYSQL_CLI=/usr/local/mysql/bin/mysql bash percona/truncate_all_tables.sh ..." >&2
  exit 2
fi
if [ "$DRY_RUN" -eq 0 ] && [ "$YES" -eq 0 ]; then
  echo "拒绝执行：这会逐表 DROP（每张表一次 schema transition，manifest 收缩、epoch +N）。" >&2
  echo "  先看清单：加 --dry-run；确认后改成 --yes。" >&2
  exit 2
fi

q() { # q <host> <port> <sql>  -> 单值输出（错误写到 stderr，便于诊断）
  MYSQL_PWD="$PASSWORD" "$MYSQL_CLI" --connect-timeout=5 "${CLI_EXTRA[@]}" \
    -h"$1" -P"$2" -u"$USER" -Nse "$3"
}
q_quiet() { q "$@" 2>/dev/null; }
probe_state() { # probe_state <host> <port> -> 打印 LEADER/FOLLOWER/''，错误走 stderr
  local out rc
  out=$(q "$1" "$2" "SHOW STATUS LIKE 'Percona_raft_state'" 2>&1); rc=$?
  if [ $rc -ne 0 ]; then
    printf 'PROBE_ERROR\t%s\n' "$(tr '\n' ' ' <<<"$out" | cut -c1-160)"
    return 0
  fi
  awk '{print $2}' <<<"$out"
}

# ── 表清单 ────────────────────────────────────────────────────────────────
if [ -n "$ONLY" ]; then
  TABLES=$ONLY
elif [ "$DISCOVER" -eq 1 ]; then
  first=${MEMBERS[0]:-}; [ -n "$first" ] || { echo "--discover 需要至少一个 --member" >&2; exit 2; }
  TABLES=$(q_quiet "${first%%:*}" "${first##*:}" "
    SELECT COALESCE(GROUP_CONCAT(table_name ORDER BY CAST(SUBSTRING(table_name, LENGTH('$PREFIX')+1) AS UNSIGNED)), '')
      FROM information_schema.tables
     WHERE table_schema='$DB' AND table_name REGEXP '^${PREFIX}[0-9]+$';")
  [ -n "${TABLES:-}" ] || { echo "发现不到 ${PREFIX}N 形式的表（在 ${first} 上查 $DB）" >&2; exit 2; }
else
  # DROP 只对存在的表有意义：把 --tables=N 展开后过滤掉实际不存在的
  cand=$(printf "${PREFIX}%d," $(seq 1 "$NTABLES")); cand=${cand%,}
  first=${MEMBERS[0]:-}
  if [ -n "$first" ]; then
    exists=$(q_quiet "${first%%:*}" "${first##*:}" "
      SELECT COALESCE(GROUP_CONCAT(table_name), '') FROM information_schema.tables
       WHERE table_schema='$DB' AND table_name IN ($(sed "s/[^,]*/'&'/g" <<<"$cand"));")
    [ "$exists" = "NULL" ] && exists=""
    TABLES=""
    for t in ${cand//,/ }; do
      case ",$exists," in *",$t,"*) TABLES="${TABLES:+$TABLES,}$t";; esac
    done
    [ -n "$TABLES" ] || { echo "给定的 ${PREFIX}1..$NTABLES 在 $DB 里一张都不存在，无需 DROP" >&2; exit 0; }
    echo "    （已过滤不存在的表；实际待 DROP：$TABLES）"
  else
    TABLES=$cand
  fi
fi
N=$(awk -F, '{print NF}' <<<"$TABLES")

# ── 定位 Leader ───────────────────────────────────────────────────────────
if [ -z "$LEADER_PORT" ]; then
  [ ${#MEMBERS[@]} -gt 0 ] || { echo "必须给 --member（用于探测 Leader）或 --leader-host/--leader-port" >&2; exit 2; }
  echo "--- 探测 Leader（扫每个成员的 Percona_raft_state）---"
  probe_errors=0
  for m in "${MEMBERS[@]}"; do
    st=$(probe_state "${m%%:*}" "${m##*:}")
    case "$st" in
      PROBE_ERROR*) probe_errors=$((probe_errors + 1))
                    printf '    %-28s ✗ 查询失败：%s\n' "$m" "${st#PROBE_ERROR$'\t'}" >&2;;
      LEADER)       printf '    %-28s LEADER\n' "$m"; LEADER_HOST=${m%%:*}; LEADER_PORT=${m##*:};;
      '')           printf '    %-28s （没有该状态量——这台不是 Raft 成员？）\n' "$m" >&2;;
      *)            printf '    %-28s %s\n' "$m" "$st";;
    esac
  done
  if [ -z "$LEADER_PORT" ]; then
    if [ "$probe_errors" -eq "${#MEMBERS[@]}" ]; then
      echo "  ✗ 所有成员的探测查询都失败了 ⇒ 不是选举问题，而是**连不上/认证失败**。" >&2
      echo "    先手工验证一次：mysql -h<host> -P<port> -u$USER -p -e \"SHOW STATUS LIKE 'Percona_raft_state';\"" >&2
      echo "    常见修法：--mysql-cli-args=\"--get-server-public-key\"（caching_sha2_password 非 TLS）或 \"--ssl-mode=REQUIRED\"。" >&2
      exit 2
    fi
    echo "  ✗ 没有成员报 LEADER（都连上了，但状态里没有 LEADER）⇒ 可能正在选举，或集群未就绪。" >&2
    echo "    看一下：SHOW STATUS LIKE 'Percona_raft_%'; 或 percona/monitor.sh 的 Percona_raft_state 列。" >&2
    echo "    确认谁是 Leader 后可显式指定：--leader-host=<host> --leader-port=<port>" >&2
    exit 2
  fi
else
  LEADER_HOST=${LEADER_HOST:-${MEMBERS[0]%%:*}}
  [ -n "$LEADER_HOST" ] || { echo "给了 --leader-port 但没给 --leader-host（也没有 --member 可推断）" >&2; exit 2; }
fi
# ── 连通性 + 身份预检（关键：q_quiet 会吞 stderr，这里必须显式报错）────────
preflight_out=$(q "$LEADER_HOST" "$LEADER_PORT" "SELECT 1" 2>&1); preflight_rc=$?
if [ $preflight_rc -ne 0 ] || [ "$preflight_out" != "1" ]; then
  echo "✗ 连不上 Leader ${LEADER_HOST}:${LEADER_PORT}（或查询被拒），无法开始：" >&2
  echo "    ${preflight_out:0:300}" >&2
  echo "  常见原因与修法：" >&2
  echo "    · 密码不对：核对 --password（或 export MYSQL_PWD=...）" >&2
  echo "    · caching_sha2_password + 非 TLS：加 --mysql-cli-args=\"--get-server-public-key\"" >&2
  echo "    · 要求 TLS：--mysql-cli-args=\"--ssl-mode=REQUIRED\"" >&2
  echo "    · 端口/主机写错：mysql -h${LEADER_HOST} -P${LEADER_PORT} -u${USER} -p -e 'SELECT 1'" >&2
  echo "    · 客户端太旧/不支持 TLS 或 caching_sha2：换 MySQL 自带客户端试" >&2
  echo "      --mysql-cli=/usr/local/mysql/bin/mysql" >&2
  exit 2
fi

echo "=== DROP $DB 的 $N 张表（受管 transition）==="
echo "    Leader：$LEADER_HOST:$LEADER_PORT"
echo "    表：${TABLES:0:120}$([ ${#TABLES} -gt 120 ] && echo '…')"

epoch0=$(q_quiet "$LEADER_HOST" "$LEADER_PORT" "SHOW STATUS LIKE 'Percona_raft_schema_epoch'" | awk '{print $2}')
echo "    schema_epoch 起始：${epoch0:-?}"

# ── 逐表执行 ──────────────────────────────────────────────────────────────
ok=0; failed=(); i=0
for t in ${TABLES//,/ }; do
  i=$((i + 1))
  sql="DROP TABLE \`$DB\`.\`$t\`"
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '  [%d/%d] %s\n' "$i" "$N" "$sql"
    continue
  fi
  out=$(q_quiet "$LEADER_HOST" "$LEADER_PORT" "SELECT PERCONA_RAFT_SCHEMA_TRANSITION('$sql')")
  if [ -z "$out" ]; then
    # q_quiet 丢了 stderr；空返回几乎总是连接/认证/权限问题，重跑一次把原文拿出来
    err=$(q "$LEADER_HOST" "$LEADER_PORT" "SELECT PERCONA_RAFT_SCHEMA_TRANSITION('$sql')" 2>&1)
    printf '  [%d/%d] %-12s ✗ 无返回（查询失败）\n' "$i" "$N" "$t"
    printf '          错误：%s\n' "$(tr '\n' ' ' <<<"$err" | cut -c1-300)"
    printf "          语句：SELECT PERCONA_RAFT_SCHEMA_TRANSITION('%s')\n" "$sql"
    failed+=("$t")
    [ "$KEEP_GOING" -eq 1 ] || { echo "  停止（未加 --keep-going）" >&2; break; }
    continue
  fi
  res=$(grep -oE '"result"[[:space:]]*:[[:space:]]*"[A-Z_]+"' <<<"$out" | sed 's/.*"\([A-Z_]*\)"$/\1/' | head -1)
  to_e=$(grep -oE '"to_epoch"[[:space:]]*:[[:space:]]*[0-9]+' <<<"$out" | grep -oE '[0-9]+$' | head -1)
  if [ "$res" = "APPLIED" ]; then
    ok=$((ok + 1))
    printf '  [%d/%d] %-12s DROPPED  epoch→%s\n' "$i" "$N" "$t" "${to_e:-?}"
  else
    failed+=("$t")
    printf '  [%d/%d] %-12s ✗ %s\n' "$i" "$N" "$t" "${res:-无返回}"
    printf '          返回：%s\n' "${out:0:300}"
    [ "$KEEP_GOING" -eq 1 ] || { echo "  停止（未加 --keep-going）" >&2; break; }
  fi
done

[ "$DRY_RUN" -eq 1 ] && { echo "(dry-run) 以上为将执行的 SQL；确认后加 --yes 执行"; exit 0; }

# ── 等各成员追平 epoch ────────────────────────────────────────────────────
final_epoch=$(q_quiet "$LEADER_HOST" "$LEADER_PORT" "SHOW STATUS LIKE 'Percona_raft_schema_epoch'" | awk '{print $2}')
echo
echo "=== 结果：DROPPED ${ok}/${N}，失败 ${#failed[@]}，Leader epoch ${epoch0:-?} → ${final_epoch:-?} ==="
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
      e=$(q_quiet "${m%%:*}" "${m##*:}" "SHOW STATUS LIKE 'Percona_raft_schema_epoch'" | awk '{print $2}')
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
