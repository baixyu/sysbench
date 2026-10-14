#!/usr/bin/env bash
#
# prepare.sh —— 一条命令把"建表（受管 transition）"和"灌数"合并：
#   ① `raft_prepare.lua ddl` 现算目标形状（与灌数用同一套参数，不重复实现形状逻辑）
#   ② 对集群上**还不存在**的表，按声明顺序逐张走 PERCONA_RAFT_SCHEMA_TRANSITION
#      （复用 raft_schema.sh：预检 → 预演实例派生目标 manifest → 对 Leader 提案 → 逐成员核对）
#   ③ 全部就位后跑 `raft_prepare.lua prepare --skip-ddl`（灌数 + 行长回读）
#
# 依据：percona-server 的 Docs/raft_replication/perf_tool_plan.md §6.5（活集群加/改表；一次只加一张表，
#       目标清单的顺序就是声明顺序）与 §4（形状 / 行长口径）。
#
# 为什么是 wrapper 而不是改 Lua：Lua 层一次进程只能连**一个**端点，而目标 manifest 必须在
# **预演实例**（Raft 关闭）上派生、提案必须打在 **Leader** 上 —— 与 raft_schema.sh 同一个理由。
#   另：服务端没有"列出已声明表"的 SQL 接口（`PERCONA_RAFT_SCHEMA_MANIFEST(db,list)` 是"按清单派生"），
#   所以完整目标清单必须由调用方给（`--schema-tables`），与 raft_schema.sh 的契约一致。
#
# 用法：
#   percona/prepare.sh --sysbench=./src/sysbench \
#     --schema-db=perf --mysql-db=perf \
#     --schema-tables=t,sbtest1,prep1,prep2 \        # 完整目标清单（= 在册的现有基表 + 待建的表）；缺省= 自动发现现有基表
#     --table-prefix=prep --tables=2 --table-size=2000 --row-length=400 --seed=42 \
#     --preview-host=127.0.0.1 --preview-port=48104 --preview-user=root \
#     --member 127.0.0.1:48101 --member 127.0.0.1:48102 --member 127.0.0.1:48103 \
#     --verify
#
# 三条硬校验（都会具名失败，不会静默做错事）：
#   · **目标清单尾部必须正好是要建的表**：`ddl` 生成的表名（`<prefix>1..N`）必须等于清单最后 N 项。
#     （服务端本身只管集合语义、顺序自由；这条是脚本自己的约定，好让"尾部 N 项 = 待建"成立。）
#   · **已存在的必须是前缀**：清单里"中间缺、后面在"的形态直接拒绝。
#   · **只灌空表**：`prepare` 对表做普通 INSERT（id=1..N），重复灌会撞主键 —— 待灌的 N 张表只要有
#     一张非空就拒绝灌数（--allow-nonempty 可强行灌；只建表用 --skip-load）。
#   · **schema 里不许有"清单外的表"**：声明集只能**追加**（看不见后文 §2 的规则），目标 manifest 是
#     按清单派生的，清单缺项会把在册的表从声明里挤掉 —— 只要该 schema 下还有不在清单里的基表就拒绝，
#     并把"按创建顺序补齐"的清单原样打出来（不给 --schema-tables 时本来就是自动发现的）。
#
# 选项：
#   --mysql-cli-args="..."  原样追加给 mysql 客户端（例：--get-server-public-key / --ssl-mode=REQUIRED）
#   --sysbench-args="..."   原样追加给所有 sysbench 调用（例：--mysql-ssl=on；也会透传给 raft_schema.sh/verify.sh）
#   --preview-password=PW   预演实例的密码（缺省取 MYSQL_PWD）
#   --leader-password=PW    Leader/成员 的密码（缺省取 MYSQL_PWD）
#   --existing-tables=a,b   显式给出"在 force"的表集合（预检用；缺省= 清单里已存在的前缀）
#   --skip-ddl              只灌数（等价于直接跑 raft_prepare --skip-ddl）
#   --skip-load             只建表不灌数
#   --verify                结束时做跨成员聚合核对（percona/verify.sh --skip-dump）
#   --allow-nonempty        允许对非空表灌数（默认拒绝）
#   --no-wait-applied       提案后不等本机应用完成（默认等待）
#
# 退出码：0 = 全部成功；非 0 = 任一步失败（具名原因会打出来）
#
set -uo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SYSBENCH=./src/sysbench
LUA_PREPARE=${here}/../src/lua/raft_prepare.lua
SCHEMA_SCRIPT=${here}/raft_schema.sh
VERIFY_SCRIPT=${here}/verify.sh

SCHEMA_DB=
MYSQL_DB=
SCHEMA_TABLES=
EXISTING_TABLES=
EXISTING_GIVEN=0
TABLE_PREFIX=sbtest
TABLES=
SHAPE_ARGS=()          # 原样转发给 raft_prepare.lua 的参数
PREVIEW_HOST=127.0.0.1; PREVIEW_PORT=; PREVIEW_USER=root
LEADER_HOST=127.0.0.1;  LEADER_PORT=;  LEADER_USER=root
# 密码：显式 --preview-password/--leader-password，或用 MYSQL_PWD 环境变量一次性给两边
PREVIEW_PASSWORD=${MYSQL_PWD:-}
LEADER_PASSWORD=${MYSQL_PWD:-}
# 逃生口：原样追加给 mysql 客户端 / sysbench 的额外参数（分词规则同 shell）
# 例：MYSQL_CLI_ARGS='--get-server-public-key' 或 '--ssl-mode=REQUIRED'
CLI_EXTRA=(); SB_EXTRA=(); SB_EXTRA_STR=()
[[ -n "${MYSQL_CLI_ARGS:-}" ]] && read -r -a CLI_EXTRA <<<"$MYSQL_CLI_ARGS"
if [[ -n "${SYSBENCH_ARGS:-}" ]]; then SB_EXTRA_STR+=("$SYSBENCH_ARGS"); read -r -a SB_EXTRA <<<"$SYSBENCH_ARGS"; fi
MEMBERS=()
SKIP_DDL=0; SKIP_LOAD=0; DO_VERIFY=0; WAIT_APPLIED=1; ALLOW_NONEMPTY=0

while [[ $# -gt 0 ]]; do
  # 同时接受 `--opt=value` 与 `--opt value`：把前者就地拆成两个位置参数，下面只写一种形态
  if [[ "$1" == --*=* ]]; then
    set -- "${1%%=*}" "${1#*=}" "${@:2}"
  fi
  case "$1" in
    --sysbench) SYSBENCH=$2; shift 2;;
    --schema-db) SCHEMA_DB=$2; SHAPE_ARGS+=(--schema-db="$2"); shift 2;;
    --mysql-db) MYSQL_DB=$2; shift 2;;
    --schema-tables) SCHEMA_TABLES=$2; shift 2;;
    --existing-tables) EXISTING_TABLES=$2; EXISTING_GIVEN=1; shift 2;;
    --table-prefix) TABLE_PREFIX=$2; SHAPE_ARGS+=(--table-prefix="$2"); shift 2;;
    --tables) TABLES=$2; SHAPE_ARGS+=(--tables="$2"); shift 2;;
    --table-size|--row-length|--types|--index-profile|--seed|--batch|--rowlen-tolerance)
      SHAPE_ARGS+=("$1=$2"); shift 2;;
    --preview-host) PREVIEW_HOST=$2; shift 2;;
    --preview-port) PREVIEW_PORT=$2; shift 2;;
    --preview-user) PREVIEW_USER=$2; shift 2;;
    --preview-password) PREVIEW_PASSWORD=$2; shift 2;;
    --leader-host) LEADER_HOST=$2; shift 2;;
    --leader-port) LEADER_PORT=$2; shift 2;;
    --leader-user) LEADER_USER=$2; shift 2;;
    --leader-password) LEADER_PASSWORD=$2; shift 2;;
    --mysql-cli-args) read -r -a CLI_EXTRA <<<"$2"; shift 2;;
    --sysbench-args) SB_EXTRA_STR+=("$2"); read -r -a SB_EXTRA <<<"$2"; shift 2;;
    --member) MEMBERS+=("$2"); shift 2;;
    --skip-ddl) SKIP_DDL=1; shift;;
    --skip-load) SKIP_LOAD=1; shift;;
    --verify) DO_VERIFY=1; shift;;
    --allow-nonempty) ALLOW_NONEMPTY=1; shift;;
    --no-wait-applied) WAIT_APPLIED=0; shift;;
    *) echo "未知参数：$1（见文件头用法）" >&2; exit 2;;
  esac
done

[[ -n "$SCHEMA_DB" ]] || { echo "必须给 --schema-db" >&2; exit 2; }
[[ -n "$MYSQL_DB" ]] || MYSQL_DB=$SCHEMA_DB
[[ -n "$PREVIEW_PORT" ]] || { echo "必须给 --preview-port（Raft 关闭、与集群同 schema 的预演实例）" >&2; exit 2; }
[[ -x "$SYSBENCH" || -n "$(command -v "$SYSBENCH" 2>/dev/null || true)" ]] || \
  { echo "找不到 sysbench：$SYSBENCH" >&2; exit 2; }
[[ -r "$LUA_PREPARE" ]] || { echo "找不到 $LUA_PREPARE" >&2; exit 2; }

# 需要一个 mysql 客户端来读 information_schema 与 Percona_raft_* 状态量。
# 优先 MYSQL_CLI=/path/to/mysql，其次 PATH 里的 mysql（构建产物里的 mysql 一般在
# <build>/runtime_output_directory/ 或 <build>/bin/ 下）。
MYSQL_CLI=${MYSQL_CLI:-$(command -v mysql || true)}
if [[ -z "$MYSQL_CLI" || ! -x "$MYSQL_CLI" ]]; then
  echo "找不到 mysql 客户端。" >&2
  if [[ -z "$MYSQL_CLI" ]]; then
    echo "  MYSQL_CLI 没设置，PATH 里也没有 mysql。" >&2
  elif [[ ! -e "$MYSQL_CLI" ]]; then
    echo "  MYSQL_CLI=$MYSQL_CLI —— 这个路径在本进程里不存在。" >&2
  else
    echo "  MYSQL_CLI=$MYSQL_CLI —— 这个文件不可执行（脚本要求 -x）。" >&2
  fi
  echo "  注意它必须是**这个脚本（子进程）能看到**的环境变量：" >&2
  echo "    export MYSQL_CLI=<build>/runtime_output_directory/mysql     # 少了 export 就只有当前 shell 看得到" >&2
  echo "    MYSQL_CLI=<build>/runtime_output_directory/mysql bash percona/prepare.sh ...   # 或者用前缀形式" >&2
  echo "    sudo -E ... / 别的 shell / 容器里跑时，变量和这个路径都要在那一侧存在" >&2
  echo "  没有客户端也可以装一个（这些查询是普通 SQL）：apt-get install -y mysql-client" >&2
  exit 2
fi

# 对某个端点执行一条语句（--protocol=tcp + -Nse）。密码走 MYSQL_PWD 环境变量，
# 只对这一次调用生效：不写进命令行，也就不会出现在 ps 的 argv 里。
cli() { # <host> <port> <user> <password> <mysql 参数…>
  local h=$1 p=$2 u=$3 pw=$4; shift 4
  if [[ -n "$pw" ]]; then
    MYSQL_PWD="$pw" "$MYSQL_CLI" --protocol=tcp -h"$h" -P"$p" -u"$u" ${CLI_EXTRA[@]+"${CLI_EXTRA[@]}"} "$@"
  else
    "$MYSQL_CLI" --protocol=tcp -h"$h" -P"$p" -u"$u" ${CLI_EXTRA[@]+"${CLI_EXTRA[@]}"} "$@"
  fi
}

# 对 Leader 执行一条语句（返回制表符分隔的行）
q() { # <port> <sql>
  cli "$LEADER_HOST" "$1" "$LEADER_USER" "$LEADER_PASSWORD" -Nse "$2"
}

# 预演实例上是否已经有这张表：上一次失败的尝试可能已经把 DDL 在预演实例上执行过了，
# 再执行一次会撞 1050（表已存在）⇒ 这种情况给 raft_schema.sh 传 --skip-ddl（它就是为这个准备的）。
preview_has_table() { # <table>
  local n
  n=$(cli "$PREVIEW_HOST" "$PREVIEW_PORT" "$PREVIEW_USER" "$PREVIEW_PASSWORD" -Nse \
      "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$SCHEMA_DB' AND table_name='$1'" 2>/dev/null)
  [[ "$n" == "1" ]]
}

leader_port_probe() { # 从 --member 里挑 state=LEADER 的那台
  local m host port st found=""
  for m in "${MEMBERS[@]:-}"; do
    [[ -n "$m" ]] || continue
    host=${m%%:*}; port=${m##*:}
    st=$(cli "$host" "$port" "$LEADER_USER" "$LEADER_PASSWORD" -Nse \
         "SHOW STATUS LIKE 'Percona_raft_state'" 2>/dev/null | awk '{print $2}')
    if [[ "$st" == "LEADER" ]]; then found="$port"; fi
  done
  echo "$found"
}

if [[ -z "$LEADER_PORT" ]]; then
  [[ ${#MEMBERS[@]} -gt 0 ]] || { echo "必须给 --leader-port（或用 --member 列出成员以便自动探测）" >&2; exit 2; }
  echo "=== 0) 探测 Leader（扫 --member 里的 Percona_raft_state）==="
  LEADER_PORT=$(leader_port_probe)
  [[ -n "$LEADER_PORT" ]] || { echo "没有成员报 LEADER（选举中？）——请稍后重试或显式给 --leader-port" >&2; exit 1; }
  echo "   Leader 端口：$LEADER_PORT"
fi

echo "=== 0) 连接：Leader ${LEADER_HOST}:${LEADER_PORT}（用户 ${LEADER_USER}）、预演实例 ${PREVIEW_HOST}:${PREVIEW_PORT}（用户 ${PREVIEW_USER}）、成员 ${#MEMBERS[@]} 个 ==="
echo "    步骤顺序：① 算 DDL（不连库）→ ② 读 Leader 的 information_schema 发现已有基表 → ③ 逐张 transition（这里先用预演实例派生目标 manifest）→ ④ 灌数 + 行长回读（Leader）→ ⑤ 跨成员核对"

# ── 1) 目标形状（DDL）────────────────────────────────────────────────────────
echo "=== 1) 现算目标 DDL（raft_prepare.lua ddl）==="
ddl_out=$("$SYSBENCH" "$LUA_PREPARE" "${SHAPE_ARGS[@]}" --mysql-db="$MYSQL_DB" ddl 2>&1)
rc=$?
echo "$ddl_out" | grep -E '^raft_prepare\.(ddl|types|fill_column|avg_row_bytes_estimate)=' | sed 's/^/   /'
[[ $rc -eq 0 ]] || { echo "取 DDL 失败（见上）" >&2; exit 1; }

DDL_NAMES=(); DDL_TEXTS=()
while IFS= read -r line; do
  ddl_text=${line#raft_prepare.ddl=}
  [[ -n "$ddl_text" ]] || continue
  tname=$(sed -E 's/^[Cc][Rr][Ee][Aa][Tt][Ee] +[Tt][Aa][Bb][Ll][Ee] +([^ (]+).*/\1/' <<<"$ddl_text")
  DDL_NAMES+=("${tname##*.}"); DDL_TEXTS+=("$ddl_text")
done < <(grep '^raft_prepare\.ddl=' <<<"$ddl_out")
NEW_COUNT=${#DDL_NAMES[@]}
[[ $NEW_COUNT -gt 0 ]] || { echo "没有从 ddl 输出解析出 CREATE TABLE" >&2; exit 1; }
# 灌数阶段跑的是 `prepare --skip-ddl`，它按 --tables=N **全部**灌一遍；所以空表检查必须看
# 这 N 张（ALL_LOAD_NAMES），不能只看"这次新建的那几张"（§2 过滤后 DDL_NAMES 可能为空）。
ALL_LOAD_NAMES=("${DDL_NAMES[@]}")

# ── 2) 目标清单（顺序 = 声明顺序）────────────────────────────────────────────
# 服务端对 CREATE 类提案的判定（`schema_transition_policy.cc:493-513`，逐条实测过）：
#   · 目标 manifest 的**表数**必须正好是在册 + 1（`:326` 的 appended 分支），少一张或多一张都被拒，
#     原文 "the target schema manifest describes a different number of tables than the one in force"；
#   · 在册的每一张都必须仍被描述（missing == 0），且**恰好一张**是新的（added == 1）——
#     所以一次 transition 只能加一张表，我的循环就是一张一次；
#   · 加表只能由 `CREATE TABLE` 完成（`a table may only be added by a CREATE TABLE`）；
#   · **顺序自由**：manifest 是序列，新表放哪儿由该条目自己声明（注释原文
#     "Where it appears is the entry's declaration to make"）。同表数的分支同样是集合比较
#     （`target_names_match`，`used[]` 逐名抵消）⇒ 重排不会被拒。
# 而"在册"就是该 schema 里现有的基表（每个成员在 apply 前拿 manifest 逐表校验本地 DD）。
# 所以：**清单 = 现有基表 + 待建的表**，现有表必须原样带上（否则就是"removes N table(s)"）。
# 缺省时这里自动发现它们；顺序取 information_schema 的 create_time（并列时按表名）——
# 纯粹是好让声明顺序看起来像创建顺序，功能上不敏感（见上面"顺序自由"）。
SUGGESTED_LIST=""
existing_rows=$(q "$LEADER_PORT" "SELECT table_name FROM information_schema.tables WHERE table_schema='$SCHEMA_DB' AND table_type='BASE TABLE' ORDER BY create_time IS NULL, create_time, table_name")
rc=$?
[[ $rc -eq 0 ]] || {
  echo "读 ${SCHEMA_DB} 的现有基表失败（Leader ${LEADER_HOST}:${LEADER_PORT}，用户 ${LEADER_USER}）：" >&2
  echo "$existing_rows" >&2
  echo "  —— 这一步连的是 **Leader**（用 --leader-host/--leader-port 指定，或用 --member 自动探测）；" >&2
  echo "     预演实例（--preview-host/--preview-port）要到逐张 transition 时才用。" >&2
  exit 1; }
EXISTING_DISCOVERED=""
for e in $existing_rows; do EXISTING_DISCOVERED="${EXISTING_DISCOVERED:+$EXISTING_DISCOVERED,}$e"; done

# 待建 = ddl 生成的表里，schema 里还没有的那些；已经存在的当作"在册"原样带上（不再 CREATE），
# 所以"补建到 N 张"可以重复跑（--tables=8 而 sbtest1..3 已在 ⇒ 只建 4..8）。
MISSING_CSV=""; keep_names=(); keep_texts=()
j=0
while [[ $j -lt $NEW_COUNT ]]; do
  n=${DDL_NAMES[$j]}
  already=0
  for e in $existing_rows; do [[ "$e" == "$n" ]] && already=1; done
  if [[ $already -eq 0 ]]; then
    MISSING_CSV="${MISSING_CSV:+$MISSING_CSV,}$n"
    keep_names+=("$n"); keep_texts+=("${DDL_TEXTS[$j]}")
  fi
  j=$((j + 1))
done
SUGGESTED_LIST="${EXISTING_DISCOVERED}${EXISTING_DISCOVERED:+${MISSING_CSV:+,}}$MISSING_CSV"
if [[ -z "$EXISTING_DISCOVERED" && $EXISTING_GIVEN -eq 0 ]]; then
  echo "拒绝：从 Leader ${LEADER_HOST}:${LEADER_PORT} 的 information_schema 看，库 ${SCHEMA_DB} 里**一张基表都没有**。" >&2
  echo "  而集群的在册集不可能为空（服务端要求 manifest 至少 1 张表）⇒ 大概率是这三件事之一：" >&2
  echo "    ① --schema-db 不是集群的**声明库**：在 Leader 上列出所有非系统库里的表，看哪个库有表；" >&2
  echo "    ② --leader-host/--leader-port 指到了别的实例（不是集群的 Leader）：查 Percona_raft_state；" >&2
  echo "    ③ 账号看不到 information_schema（权限不足）。" >&2
  echo "  确认过确实要往一个空库加表时，显式声明在 force 集合（可空）： --existing-tables=" >&2
  exit 1
fi
if [[ -n "$SCHEMA_TABLES" ]]; then
  echo "   （--schema-tables 显式给定：${SCHEMA_TABLES}）"
else
  if [[ -n "$EXISTING_DISCOVERED" && -n "$MISSING_CSV" ]]; then
    SCHEMA_TABLES="$EXISTING_DISCOVERED,$MISSING_CSV"
  else
    SCHEMA_TABLES="$EXISTING_DISCOVERED$MISSING_CSV"
  fi
  echo "   （未给 --schema-tables ⇒ 自动发现现有基表并按创建顺序前插：${SCHEMA_TABLES:-（无）}）"
  if [[ -z "$MISSING_CSV" ]]; then
    echo "   目标表 ${#DDL_NAMES[@]} 张在 $SCHEMA_DB 里都已存在 ⇒ 只灌数/核对，不建表"
  fi
  if [[ ${#keep_names[@]} -gt 0 ]]; then
    DDL_NAMES=("${keep_names[@]}"); DDL_TEXTS=("${keep_texts[@]}"); NEW_COUNT=${#DDL_NAMES[@]}
  else
    DDL_NAMES=(); DDL_TEXTS=(); NEW_COUNT=0
  fi
fi
IFS=',' read -r -a TARGETS <<<"$SCHEMA_TABLES"
TOTAL=${#TARGETS[@]}
[[ $TOTAL -ge $NEW_COUNT ]] || {
  echo "目标清单只有 $TOTAL 项，而要建 $NEW_COUNT 张表：$SCHEMA_TABLES" >&2; exit 2; }
i=0
while [[ $i -lt $NEW_COUNT ]]; do
  t=${TARGETS[$((TOTAL - NEW_COUNT + i))]}
  [[ "$t" == "${DDL_NAMES[$i]}" ]] || {
    echo "目标清单尾部与要建的表不一致：清单尾部含 '$t'，第 $((i + 1)) 张要建 '${DDL_NAMES[$i]}'。" >&2
    echo "  新增表必须是清单的**尾部**（transition 是追加语义）：请调整 --schema-tables，或让 --table-prefix/--tables 生成的名字对上。" >&2
    exit 2; }
  i=$((i + 1))
done

# ── 3) 集群上哪些表已存在 ────────────────────────────────────────────────────
in_list=""
for t in "${TARGETS[@]}"; do in_list="${in_list:+$in_list,}'$t'"; done
exists_raw=$(q "$LEADER_PORT" "SELECT table_name FROM information_schema.tables WHERE table_schema='$SCHEMA_DB' AND table_name IN (${in_list})")
rc=$?
[[ $rc -eq 0 ]] || { echo "读 information_schema 失败（$LEADER_HOST:${LEADER_PORT}）：$exists_raw" >&2; exit 1; }
FIRST_MISSING=-1
i=0
while [[ $i -lt $TOTAL ]]; do
  hit=0
  for e in $exists_raw; do [[ "$e" == "${TARGETS[$i]}" ]] && hit=1; done
  if [[ $hit -eq 0 && $FIRST_MISSING -lt 0 ]]; then FIRST_MISSING=$i; fi
  if [[ $hit -eq 1 && $FIRST_MISSING -ge 0 ]]; then
    echo "目标清单里 '${TARGETS[$i]}' 已存在，但它前面有表不存在（${TARGETS[$FIRST_MISSING]}）——不是追加形态，拒绝。" >&2
    exit 1
  fi
  i=$((i + 1))
done

EXISTING_PREFIX=""
if [[ $FIRST_MISSING -lt 0 ]]; then
  EXISTING_PREFIX=$(IFS=,; echo "${TARGETS[*]}")            # 清单里的表全都在
elif [[ $FIRST_MISSING -gt 0 ]]; then
  EXISTING_PREFIX=$(IFS=,; echo "${TARGETS[*]:0:$FIRST_MISSING}")
fi
EXISTING_PREFIX=${EXISTING_TABLES:-$EXISTING_PREFIX}
TO_CREATE=0
[[ $FIRST_MISSING -ge 0 ]] && TO_CREATE=$((TOTAL - FIRST_MISSING))
echo "=== 2) 集群现状：已声明（存在）= ${EXISTING_PREFIX:-（无）}；待建 = $TO_CREATE 张 ==="

# 保护：该 schema 下若有"不在目标清单里"的基表，目标 manifest 会把它们挤出声明 → 拒绝
extra=""
for t in $existing_rows; do
  in_target=0
  for x in "${TARGETS[@]}"; do [[ "$x" == "$t" ]] && in_target=1; done
  [[ $in_target -eq 0 ]] && extra="${extra:+$extra,}$t"
done
if [[ -n "$extra" ]]; then
  echo "拒绝：schema $SCHEMA_DB 里还有不在目标清单里的基表：$extra" >&2
  echo "  声明集只能追加（见 §2）：这些表若在册，按现在的清单提案会把它们从声明里挤掉，服务端必定以" >&2
  echo "  \"different number of tables than the one in force\" 拒掉——所以这里先拒，不浪费一次提案。" >&2
  echo "  按现有基表的创建顺序补齐（也就是省掉 --schema-tables 让脚本自动发现）：" >&2
  echo "    --schema-tables=$SUGGESTED_LIST" >&2
  exit 1
fi

# ── 4) 逐表 transition ──────────────────────────────────────────────────────
if [[ "$SKIP_DDL" -eq 1 ]]; then
  echo "=== 3) --skip-ddl：跳过建表 ==="
else
  FINAL_LOCAL=$EXISTING_PREFIX
  if [[ $FIRST_MISSING -lt 0 ]]; then
    echo "   目标表都已存在，无需 transition"
  else
    i=$FIRST_MISSING
    while [[ $i -lt $TOTAL ]]; do
      t=${TARGETS[$i]}
      ddl=""
      j=0
      while [[ $j -lt $NEW_COUNT ]]; do
        [[ "${DDL_NAMES[$j]}" == "$t" ]] && ddl="${DDL_TEXTS[$j]}"
        j=$((j + 1))
      done
      [[ -n "$ddl" ]] || { echo "找不到表 $t 的 DDL（清单尾部不一致？）" >&2; exit 1; }
      echo "=== 3.$((i - FIRST_MISSING + 1)) transition：$t ==="
      rs_args=(--sysbench="$SYSBENCH" --schema-db="$SCHEMA_DB" \
               --schema-tables="${FINAL_LOCAL:+$FINAL_LOCAL,}$t" \
               --ddl="$ddl" \
               --preview-host="$PREVIEW_HOST" --preview-port="$PREVIEW_PORT" --preview-user="$PREVIEW_USER" \
               --leader-host="$LEADER_HOST" --leader-port="$LEADER_PORT" --leader-user="$LEADER_USER")
      [[ -n "$FINAL_LOCAL" ]] && rs_args+=(--existing-tables="$FINAL_LOCAL")
      [[ -n "$PREVIEW_PASSWORD" ]] && rs_args+=(--preview-password="$PREVIEW_PASSWORD")
      [[ -n "$LEADER_PASSWORD" ]] && rs_args+=(--leader-password="$LEADER_PASSWORD")
      [[ ${#SB_EXTRA_STR[@]} -gt 0 ]] && rs_args+=(--sysbench-args="${SB_EXTRA_STR[*]}")
      if preview_has_table "$t"; then
        echo "   （预演实例上已有 $t ⇒ 传 --skip-ddl，避免重复执行 DDL）"
        rs_args+=(--skip-ddl)
      fi
      [[ $WAIT_APPLIED -eq 1 ]] && rs_args+=(--wait-applied)
      for m in "${MEMBERS[@]}"; do rs_args+=(--member "$m"); done
      "$SCHEMA_SCRIPT" "${rs_args[@]}" || { echo "transition 失败：$t" >&2; exit 1; }
      FINAL_LOCAL="${FINAL_LOCAL:+$FINAL_LOCAL,}$t"
      i=$((i + 1))
    done
  fi
fi

# ── 5) 灌数（只灌空表）──────────────────────────────────────────────────────
if [[ "$SKIP_LOAD" -eq 1 ]]; then
  echo "=== 4) --skip-load：不灌数 ==="
else
  if [[ "$ALLOW_NONEMPTY" -eq 0 ]]; then
    for t in "${ALL_LOAD_NAMES[@]}"; do
      cnt=$(q "$LEADER_PORT" "SELECT COUNT(*) FROM \`$SCHEMA_DB\`.\`$t\`")
      [[ "$cnt" =~ ^[0-9]+$ ]] || { echo "读 $t 行数失败：$cnt" >&2; exit 1; }
      if [[ "$cnt" -gt 0 ]]; then
        echo "拒绝灌数：$SCHEMA_DB.$t 已有 $cnt 行，而 prepare 是普通 INSERT（id=1..N）会撞主键。" >&2
        echo "  （同一批表重复跑就会走到这里——表已经建好、也已经灌过数了。）" >&2
        echo "  只建表用 --skip-load；只要核对用 --skip-load --verify；确实要再灌一次用 --allow-nonempty（会撞主键，除非换 --table-prefix）。" >&2
        exit 1
      fi
    done
  fi
  echo "=== 4) 灌数 + 行长回读（prepare --skip-ddl）==="
  load_args=(--mysql-host="$LEADER_HOST" --mysql-port="$LEADER_PORT" --mysql-user="$LEADER_USER")
  [[ -n "$LEADER_PASSWORD" ]] && load_args+=(--mysql-password="$LEADER_PASSWORD")
  [[ ${#SB_EXTRA[@]} -gt 0 ]] && load_args+=(${SB_EXTRA[@]+"${SB_EXTRA[@]}"})
  "$SYSBENCH" "$LUA_PREPARE" "${SHAPE_ARGS[@]}" --mysql-db="$MYSQL_DB" \
    "${load_args[@]}" --skip-ddl prepare
  rc=$?
  [[ $rc -eq 0 ]] || { echo "灌数/行长回读失败（rc=${rc}）" >&2; exit $rc; }
fi

# ── 6) 可选：跨成员核对 ─────────────────────────────────────────────────────
if [[ "$DO_VERIFY" -eq 1 ]]; then
  if [[ ${#MEMBERS[@]} -eq 0 ]]; then
    echo "（--verify 需要 --member，跳过）" >&2
  else
    echo "=== 5) 跨成员聚合核对 ==="
    v_args=(--sysbench="$SYSBENCH" --schema-db="$SCHEMA_DB" --schema-tables="$SCHEMA_TABLES" \
            --user="$LEADER_USER" --skip-dump)
    [[ -n "$LEADER_PASSWORD" ]] && v_args+=(--password="$LEADER_PASSWORD")
    [[ ${#SB_EXTRA_STR[@]} -gt 0 ]] && v_args+=(--sysbench-args="${SB_EXTRA_STR[*]}")
    for m in "${MEMBERS[@]}"; do v_args+=(--member "$m"); done
    "$VERIFY_SCRIPT" "${v_args[@]}" || { echo "跨成员核对失败" >&2; exit 1; }
  fi
fi

echo "=== prepare: PASS（声明清单：${SCHEMA_TABLES}）==="
