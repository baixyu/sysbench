#!/usr/bin/env bash
#
# raft_schema.sh —— 在活着的 Percona Raft 集群上加/改表（把三个端点串起来）
#
# 依据：percona-server 的 Docs/raft_replication/perf_tool_plan.md §6.5。
# 为什么需要这个 wrapper：Lua 层的 drv:connect() 不接受连接参数，
# 一次 sysbench 进程只能连一个端点，所以：
#
#   0) 预检（Leader）  → "在 force 的表集合"能在 Leader 上派生（提前暴露清单/形状问题）
#   1) propose（Leader）→ 单参数 PERCONA_RAFT_SCHEMA_TRANSITION(ddl)
#   2) verify（每个成员）→ epoch + 本机自派生指纹（以 Leader 的派生指纹为参照）
#
# 不再需要预演实例（preview）：目标 manifest 由服务端从 DDL 和 in-force schema 推导，
# 每个成员 apply 时各自派生并比对；wrapper 只负责把 DDL 发到 Leader、再逐成员核对。
# 目标 manifest 与成员派生不一致时，成员会 fail closed（隔离）——这是提案后的最后一道闸。
#
# 用法：
#   percona/raft_schema.sh \
#     --sysbench=./src/sysbench \
#     --schema-db=perf --schema-tables=t,t2 \
#     --ddl="CREATE TABLE perf.t2 (id INT PRIMARY KEY, v INT NOT NULL)" \
#     --leader-host=127.0.0.1  --leader-port=46102  --leader-user=root \
#     --member 127.0.0.1:46101 --member 127.0.0.1:46102 --member 127.0.0.1:46103 \
#     --wait-applied
#
# 退出码：0 = APPLIED 且所有成员校验通过；非 0 = 任一步失败（具名 detail 会打出来）
#
set -euo pipefail

SYSBENCH=./src/sysbench
LUA=
SCHEMA_DB=
SCHEMA_TABLES=
EXISTING_TABLES=
DDL=
DDL_FILE=
LEADER_HOST=127.0.0.1;  LEADER_PORT=;  LEADER_USER=root
LEADER_PASSWORD=${MYSQL_PWD:-}
SB_EXTRA=()
MEMBERS=()
WAIT_APPLIED=0

here=$(cd "$(dirname "$0")" && pwd)
LUA=${here}/../src/lua/raft_schema.lua

# 允许 --opt=value 与 --opt value 两种写法：先把前者规范成后者
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
    --schema-db) SCHEMA_DB=$2; shift 2;;
    --schema-tables) SCHEMA_TABLES=$2; shift 2;;
    --existing-tables) EXISTING_TABLES=$2; shift 2;;
    --ddl) DDL=$2; shift 2;;
    --ddl-file) DDL_FILE=$2; shift 2;;
    --leader-host) LEADER_HOST=$2; shift 2;;
    --leader-port) LEADER_PORT=$2; shift 2;;
    --leader-user) LEADER_USER=$2; shift 2;;
    --leader-password) LEADER_PASSWORD=$2; shift 2;;
    --sysbench-args) read -r -a SB_EXTRA <<<"$2"; shift 2;;
    --member) MEMBERS+=("$2"); shift 2;;
    --wait-applied) WAIT_APPLIED=1; shift;;
    --preview-host|--preview-port|--preview-user|--preview-password|--skip-ddl)
      echo "已移除 --preview-* / --skip-ddl：建表不再需要预演实例（preview）" >&2; exit 2;;
    *) echo "未知参数：$1" >&2; exit 2;;
  esac
done

[[ -n "$SCHEMA_DB"     ]] || { echo "必须给 --schema-db" >&2; exit 2; }
[[ -n "$SCHEMA_TABLES" ]] || { echo "必须给 --schema-tables（目标顺序）" >&2; exit 2; }
[[ -n "$DDL$DDL_FILE"  ]] || { echo "必须给 --ddl 或 --ddl-file" >&2; exit 2; }
[[ -n "$LEADER_PORT"   ]] || { echo "必须给 --leader-port（Leader）" >&2; exit 2; }
command -v "$SYSBENCH" >/dev/null 2>&1 || [[ -x "$SYSBENCH" ]] || \
  { echo "找不到 sysbench：$SYSBENCH" >&2; exit 2; }

ddl_args=()
[[ -n "$DDL"      ]] && ddl_args+=(--ddl="$DDL")
[[ -n "$DDL_FILE" ]] && ddl_args+=(--ddl-file="$DDL_FILE")

common=(--schema-db="$SCHEMA_DB" --schema-tables="$SCHEMA_TABLES")
leader_conn=(--mysql-host="$LEADER_HOST" --mysql-port="$LEADER_PORT" --mysql-user="$LEADER_USER" --mysql-db="$SCHEMA_DB")
[[ -n "$LEADER_PASSWORD" ]]  && leader_conn+=(--mysql-password="$LEADER_PASSWORD")
[[ ${#SB_EXTRA[@]} -gt 0 ]] && leader_conn+=(${SB_EXTRA[@]+"${SB_EXTRA[@]}"})

# DDL 类型（只用于把"在 force 的表集合"推出来；推不出来就要求 --existing-tables）
ddl_head="$DDL"
[[ -z "$ddl_head" ]] && ddl_head=$(head -c 200 "$DDL_FILE" 2>/dev/null || true)
ddl_kind=$(tr 'A-Z' 'a-z' <<<"$ddl_head" | tr -s ' \t\n' ' ' | sed 's/^ *//')
case "$ddl_kind" in
  create\ table*)   ddl_kind=create;;
  alter\ table*)    ddl_kind=alter;;
  rename\ table*)   ddl_kind=rename;;
  drop\ table*)     ddl_kind=drop;;
  truncate\ table*) ddl_kind=truncate;;
  *)                ddl_kind=unknown;;
esac

echo "=== 0/2 预检：Leader 上在 force 的表集合可派生（DDL 类型：${ddl_kind}）==="
if [[ -n "$EXISTING_TABLES" ]]; then
  existing="$EXISTING_TABLES"
elif [[ "$ddl_kind" == "create" ]]; then
  # CREATE TABLE = 末尾追加一张 ⇒ 在 force 的表 = 目标去掉最后一张
  existing="${SCHEMA_TABLES%,*}"
  [[ "$existing" == "$SCHEMA_TABLES" ]] && existing=""
elif [[ "$ddl_kind" == "alter" || "$ddl_kind" == "truncate" ]]; then
  existing="$SCHEMA_TABLES"     # 表集合不变
else
  existing=""
fi

if [[ -z "$existing" ]]; then
  echo "⚠️  跳过预检：无法从 DDL 推出在 force 的表集合（${ddl_kind} 这类）。" >&2
  echo "    请用 --existing-tables=t,t2 显式给出，以便核对正确的 in-force 指纹。" >&2
else
  echo "   在 force 的表集合（预检用）：${existing}"
  # 派生失败（表缺失/形状不符/连不上）时 sysbench 是**非 0 退出**的：必须自己接住并说出原因，
  # 否则 `set -e`+`pipefail` 会在这里静默 exit 1（实测：只打一行"在 force 的表集合"就没了）。
  leader_out=$("$SYSBENCH" "$LUA" "${leader_conn[@]}" --schema-db="$SCHEMA_DB" \
               --schema-tables="$existing" fingerprint 2>&1) || true
  leader_fp=$(sed -n 's/^raft_schema\.fingerprint=//p' <<<"$leader_out")
  echo "   Leader 在 force 指纹 : ${leader_fp:-（派生失败）}"
  if [[ -z "$leader_fp" ]]; then
    echo "   Leader（${LEADER_HOST}:${LEADER_PORT}）派生不出在 force 的指纹，原始输出：" >&2
    sed 's/^/    /' <<<"$leader_out" >&2
    echo "预检失败：请检查 --existing-tables/--schema-tables 与集群在册表是否一致。" >&2
    exit 1
  fi
  echo "   预检通过"
fi

echo "=== 1/2 propose：对 Leader ${LEADER_HOST}:${LEADER_PORT} 提交单参数 DDL ==="
propose_args=("${common[@]}" "${ddl_args[@]}")
[[ $WAIT_APPLIED -eq 1 ]] && propose_args+=(--wait-applied)
propose_rc=0
propose_out=$("$SYSBENCH" "$LUA" "${leader_conn[@]}" "${propose_args[@]}" propose 2>&1) || propose_rc=$?
echo "$propose_out"
if [[ $propose_rc -ne 0 ]]; then
  echo "propose 步骤失败（rc=${propose_rc}）：对 Leader ${LEADER_HOST}:${LEADER_PORT} 的提案没成功（原因见上面 raft_schema.result / detail）。" >&2
  if grep -q '本机派生指纹与目标不一致' <<<"$propose_out"; then
    echo "  · 这是**客户端核对**失败，不是提案失败：上面若已有 result=APPLIED，集群侧的 schema 变更已经落地。" >&2
    echo "    核对差异（把 表名 换成报错里那张）：" >&2
    echo "      SHOW CREATE TABLE ${SCHEMA_DB}.表名   # 在 Leader ${LEADER_HOST}:${LEADER_PORT} 上" >&2
    echo "    修法：确认三成员数据字典一致；成员派生与声明不符时会自行隔离（fail closed）。" >&2
  fi
  exit 1
fi

fingerprint=$(sed -n 's/^raft_schema\.local_fingerprint=//p' <<<"$propose_out" | tail -1)
[[ -n "$fingerprint" ]] || { echo "propose 没给出 local_fingerprint（无法逐成员核对）" >&2; exit 1; }

echo "=== 2/2 verify：逐成员核对 epoch 与本机自派生指纹（参照 Leader 的派生指纹）==="
if [[ ${#MEMBERS[@]} -eq 0 ]]; then
  echo "（未给 --member，跳过逐成员校验；建议至少把三台都列上）"
else
  failed_members=""
  for m in "${MEMBERS[@]}"; do
    host=${m%%:*}; port=${m##*:}
    member_conn=(--mysql-host="$host" --mysql-port="$port" --mysql-user="$LEADER_USER" --mysql-db="$SCHEMA_DB")
    [[ -n "$LEADER_PASSWORD" ]] && member_conn+=(--mysql-password="$LEADER_PASSWORD")
    [[ ${#SB_EXTRA[@]} -gt 0 ]] && member_conn+=(${SB_EXTRA[@]+"${SB_EXTRA[@]}"})
    mrc=0
    out=$("$SYSBENCH" "$LUA" "${member_conn[@]}" \
            "${common[@]}" --expect-fingerprint="$fingerprint" verify 2>&1) || mrc=$?
    echo "member ${host}:${port}: $(tr '\n' ' ' <<<"$out" | sed 's/sysbench 1[^)]*)//')"
    [[ $mrc -eq 0 ]] || failed_members="${failed_members:+$failed_members }${host}:${port}"
  done
  [[ -z "${failed_members:-}" ]] || {
    echo "verify 失败：提案**已经 APPLIED**（上面 propose=PASS，epoch 已推进），但这些成员的现派生指纹没核对上：$failed_members" >&2
    echo "  ⇒ 表和声明都已落地，别当成"建表失败"重来；先单独核对这几台（例如用 raft_schema.sh verify 或手跑 PERCONA_RAFT_SCHEMA_MANIFEST 比指纹）。" >&2
    exit 1; }
fi

echo "=== raft_schema: PASS（指纹 ${fingerprint}，epoch 见上）==="
