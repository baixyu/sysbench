#!/usr/bin/env bash
#
# raft_schema.sh —— 在活着的 Percona Raft 集群上加/改表（把三个端点串起来）
#
# 依据：percona-server 的 Docs/raft_replication/perf_tool_plan.md §6.5。
# 为什么需要这个 wrapper：Lua 层的 drv:connect() 不接受连接参数，
# 一次 sysbench 进程只能连一个端点，所以：
#
#   0) 预检  预演实例上"在 force 的那几张表"的指纹 == 集群在 force 的指纹
#   1) preview（预演实例，Raft 关闭、schema 与集群一致）→ 执行 DDL + 派生目标 manifest
#   2) propose（Leader）                                → PERCONA_RAFT_SCHEMA_TRANSITION()
#   3) verify （每个成员各一次）                        → epoch + 本机自派生指纹
#
# 预检为什么必须在最前面：预演实例派生的是"目标 manifest"；若预演实例与集群在 force 的
# 形状不同，目标就是错的，而成员派生后与目标不符会 **fail closed**（手册 §4.2.5 明确写了
# 这条路径会把整个集群挡住）。所以先比指纹，再动预演实例。
#
# 用法：
#   percona/raft_schema.sh \
#     --sysbench=./src/sysbench \
#     --schema-db=perf --schema-tables=t,t2 \
#     --ddl="CREATE TABLE perf.t2 (id INT PRIMARY KEY, v INT NOT NULL)" \
#     --preview-host=127.0.0.1 --preview-port=46104 --preview-user=root \
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
PREVIEW_HOST=127.0.0.1; PREVIEW_PORT=; PREVIEW_USER=root
LEADER_HOST=127.0.0.1;  LEADER_PORT=;  LEADER_USER=root
PREVIEW_PASSWORD=${MYSQL_PWD:-}
LEADER_PASSWORD=${MYSQL_PWD:-}
SB_EXTRA=()
MEMBERS=()
WAIT_APPLIED=0
SKIP_DDL=0

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
    --preview-host) PREVIEW_HOST=$2; shift 2;;
    --preview-port) PREVIEW_PORT=$2; shift 2;;
    --preview-user) PREVIEW_USER=$2; shift 2;;
    --preview-password) PREVIEW_PASSWORD=$2; shift 2;;
    --leader-host) LEADER_HOST=$2; shift 2;;
    --leader-port) LEADER_PORT=$2; shift 2;;
    --leader-user) LEADER_USER=$2; shift 2;;
    --leader-password) LEADER_PASSWORD=$2; shift 2;;
    --sysbench-args) read -r -a SB_EXTRA <<<"$2"; shift 2;;
    --member) MEMBERS+=("$2"); shift 2;;
    --wait-applied) WAIT_APPLIED=1; shift;;
    --skip-ddl) SKIP_DDL=1; shift;;
    *) echo "未知参数：$1" >&2; exit 2;;
  esac
done

[[ -n "$SCHEMA_DB"     ]] || { echo "必须给 --schema-db" >&2; exit 2; }
[[ -n "$SCHEMA_TABLES" ]] || { echo "必须给 --schema-tables（目标顺序）" >&2; exit 2; }
[[ -n "$DDL$DDL_FILE"  ]] || { echo "必须给 --ddl 或 --ddl-file" >&2; exit 2; }
[[ -n "$PREVIEW_PORT"  ]] || { echo "必须给 --preview-port（Raft 关闭的预演实例）" >&2; exit 2; }
[[ -n "$LEADER_PORT"   ]] || { echo "必须给 --leader-port（Leader）" >&2; exit 2; }
command -v "$SYSBENCH" >/dev/null 2>&1 || [[ -x "$SYSBENCH" ]] || \
  { echo "找不到 sysbench：$SYSBENCH" >&2; exit 2; }

ddl_args=()
[[ -n "$DDL"      ]] && ddl_args+=(--ddl="$DDL")
[[ -n "$DDL_FILE" ]] && ddl_args+=(--ddl-file="$DDL_FILE")

common=(--schema-db="$SCHEMA_DB" --schema-tables="$SCHEMA_TABLES")
preview_conn=(--mysql-host="$PREVIEW_HOST" --mysql-port="$PREVIEW_PORT" --mysql-user="$PREVIEW_USER" --mysql-db="$SCHEMA_DB")
leader_conn=(--mysql-host="$LEADER_HOST" --mysql-port="$LEADER_PORT" --mysql-user="$LEADER_USER" --mysql-db="$SCHEMA_DB")
[[ -n "$PREVIEW_PASSWORD" ]] && preview_conn+=(--mysql-password="$PREVIEW_PASSWORD")
[[ ${#SB_EXTRA[@]} -gt 0 ]] && preview_conn+=(${SB_EXTRA[@]+"${SB_EXTRA[@]}"})
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

echo "=== 0/3 预检：预演实例与集群在 force 的 schema 必须一致（DDL 类型：${ddl_kind}）==="
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
  echo "    请用 --existing-tables=t,t2 显式给出（不给就可能把一个错的目标 manifest 提上去）。" >&2
else
  echo "   在 force 的表集合（预检用）：${existing}"
  # 派生失败（表缺失/形状不符/连不上）时 sysbench 是**非 0 退出**的：必须自己接住并说出原因，
  # 否则 `set -e`+`pipefail` 会在这里静默 exit 1（实测：只打一行"在 force 的表集合"就没了）。
  leader_out=$("$SYSBENCH" "$LUA" "${leader_conn[@]}" --schema-db="$SCHEMA_DB" \
               --schema-tables="$existing" fingerprint 2>&1) || true
  leader_fp=$(sed -n 's/^raft_schema\.fingerprint=//p' <<<"$leader_out")
  preview_out=$("$SYSBENCH" "$LUA" "${preview_conn[@]}" --schema-db="$SCHEMA_DB" \
                --schema-tables="$existing" fingerprint 2>&1) || true
  preview_fp=$(sed -n 's/^raft_schema\.fingerprint=//p' <<<"$preview_out")
  echo "   集群在 force 指纹  : ${leader_fp:-（派生失败）}"
  echo "   预演实例同表集指纹 : ${preview_fp:-（派生失败）}"
  if [[ -z "$leader_fp" || -z "$preview_fp" || "$leader_fp" != "$preview_fp" ]]; then
    if [[ -z "$leader_fp" ]]; then
      echo "  集群侧（${LEADER_HOST}:${LEADER_PORT}）派生不出在 force 的指纹，原始输出：" >&2
      sed 's/^/    /' <<<"$leader_out" >&2
    fi
    if [[ -z "$preview_fp" ]]; then
      echo "  预演实例侧（${PREVIEW_HOST}:${PREVIEW_PORT}）派生不出在 force 的指纹，原始输出：" >&2
      sed 's/^/    /' <<<"$preview_out" >&2
      echo "  —— 预演实例必须与集群**同一份 schema**：在 force 的那几张表要先在它上面按同样形状建出来；" >&2
      echo "     数据可以是空的，形状（列类型/可空/排序规则/默认值/索引/外键）要一致，用 mysqldump --no-data 搬最稳。" >&2
    fi
    echo "预检失败：预演实例与集群在 force 的 schema 不一致 ⇒ 拒绝提案（否则成员会 fail closed）。" >&2
    exit 1
  fi
  echo "   预检通过"
fi

echo "=== 1/3 preview：在预演实例 ${PREVIEW_HOST}:${PREVIEW_PORT} 上执行 DDL 并派生目标 manifest ==="
# 注意：`preview_out=$(...)` 在 set -e 下**失败即退出**，连子进程的 FATAL 都打不出来
# （实测：用户那边只看到 prepare.sh 的"transition 失败：sbtest1"，没有任何原因）⇒ 必须接住再打。
preview_rc=0
if [[ $SKIP_DDL -eq 1 ]]; then
  preview_out=$("$SYSBENCH" "$LUA" "${preview_conn[@]}" "${common[@]}" --skip-ddl "${ddl_args[@]}" preview 2>&1) || preview_rc=$?
else
  preview_out=$("$SYSBENCH" "$LUA" "${preview_conn[@]}" "${common[@]}" "${ddl_args[@]}" preview 2>&1) || preview_rc=$?
fi
echo "$preview_out"
if [[ $preview_rc -ne 0 ]]; then
  echo "preview 步骤失败（rc=${preview_rc}）：在预演实例 ${PREVIEW_HOST}:${PREVIEW_PORT} 上执行 DDL / 派生目标 manifest 没成功。常见原因：" >&2
  grep -qE "Unknown database" <<<"$preview_out" && echo "  · 预演实例上没有库 \`$SCHEMA_DB\`（先用 Raft 关闭的实例把库和**在 force 的那几张表**建出来）" >&2
  grep -q "1050\|already exists" <<<"$preview_out" && echo "  · 目标表在预演实例上已存在（脚本本应自动加 --skip-ddl；可显式传 --skip-ddl）" >&2
  grep -q "MISSING_TABLE" <<<"$preview_out" && echo "  · 在 force 的表在预演实例上不存在/形状不符 ⇒ 用 mysqldump --no-data 从 Leader 搬一份结构过来" >&2
  grep -q "1045" <<<"$preview_out" && echo "  · 认证失败（--preview-user/--preview-password，或 2061 见文档 §0）" >&2
  grep -q "2003\|2002\|Can't connect" <<<"$preview_out" && echo "  · 连不上预演实例 ${PREVIEW_HOST}:${PREVIEW_PORT}" >&2
  exit 1
fi

manifest_hex=$(sed -n 's/^raft_schema\.manifest_hex=//p' <<<"$preview_out")
fingerprint=$(sed -n 's/^raft_schema\.fingerprint=//p' <<<"$preview_out")
[[ -n "$manifest_hex" ]] || { echo "preview 没给出 manifest_hex" >&2; exit 1; }
[[ -n "$fingerprint"  ]] || { echo "preview 没给出 fingerprint" >&2; exit 1; }

echo "=== 2/3 propose：对 Leader ${LEADER_HOST}:${LEADER_PORT} 提案 ==="
propose_args=("${common[@]}" --manifest-hex="$manifest_hex" --expect-fingerprint="$fingerprint" "${ddl_args[@]}")
[[ $WAIT_APPLIED -eq 1 ]] && propose_args+=(--wait-applied)
propose_rc=0
"$SYSBENCH" "$LUA" "${leader_conn[@]}" "${propose_args[@]}" propose || propose_rc=$?
[[ $propose_rc -eq 0 ]] || { echo "propose 步骤失败（rc=${propose_rc}）：对 Leader ${LEADER_HOST}:${LEADER_PORT} 的提案没成功（原因见上面 raft_schema.result / detail）。" >&2; exit 1; }

echo "=== 3/3 verify：逐成员核对 epoch 与本机自派生指纹 ==="
if [[ ${#MEMBERS[@]} -eq 0 ]]; then
  echo "（未给 --member，跳过逐成员校验；建议至少把三台都列上）"
else
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
  [[ -z "$failed_members" ]] || { echo "verify 失败：这些成员没核对通过：$failed_members" >&2; exit 1; }
fi

echo "=== raft_schema: PASS（指纹 ${fingerprint}，epoch 见上）==="
