# percona-raft-perf 命令清单（实测过的行）

本文件是**这个 fork 上跑过的命令**的汇总：构建、起集群、建库建表、灌数、跨成员核对、性能验收、
schema transition、受管写探针、集群侧状态检查，以及服务端侧的验收命令。
每一行都来自实际执行（脚本原型在服务端仓库外的工作目录里，见文末"出处"）；命令里只保留占位符，
不写死某一台机器的路径。

---

## 0. 记号与前置

```bash
# 先设这四个变量，下面的命令就可以直接抄
SB=./src/sysbench          # 必须在 fork 仓库根目录执行（Lua 包路径依赖 cwd）
MYSQL=<build>/bin/mysql    # 你自己的构建产物；下面统一用 $MYSQL
MYSQLD=<build>/bin/mysqld
PORT=43703                 # 当前 leader 的端口（用 §8 的方法探）
LEADER=$PORT
```

四条**必须先知道**的规矩：

1. **cwd 必须是仓库根**：`$SB src/lua/xxx.lua`，否则报找不到 Lua 模块。
2. **两个库名参数**：`--mysql-db`（连哪个库，sysbench 默认 `sbtest`）必须与 `--schema-db`（DDL/DML 里的库名）
   给**同一个值**，否则直接 `Unknown database 'sbtest'`。
3. **库必须提前建好**：本工具**不建库**（`raft_prepare.lua` 只发 `CREATE TABLE`）。受管 DDL 也不允许
   `CREATE DATABASE`（服务端把它归为"未复制的元数据语句"而拒绝，且 transition 只能带
   `ALTER TABLE` / `CREATE TABLE` / `RENAME TABLE` / `DROP TABLE`）⇒ 库要在 **Raft 关闭时**每个成员各建一次
   （见 §3）。只有 `raft_doctor.lua` 例外，它自建自己的 `perf_doctor_golden`。
4. **`--target` 三档**：`raft`（受管集群；探不到身份变量就 FATAL，故意不静默降级）、`mysql`（原生 MySQL）、
   `auto`（默认）。对原生实例用 `raft` 一定失败，这是设计。

**账号密码**（2026-10-14 补，之前只有 `--*-user`，默认 `root` 无密码）：

| 脚本 | 参数 | 默认 |
| --- | --- | --- |
| `prepare.sh`、`raft_schema.sh` | `--preview-user` / `--preview-password`（预演实例）、`--leader-user` / `--leader-password`（Leader 与各成员） | `root`，密码取 `MYSQL_PWD` |
| `verify.sh` | `--user=` / `--password=` | 同上 |
| `doctor.sh` | `--target-user/--target-password`、`--preview-user/--preview-password` | 同上 |
| `report.sh` | `--mysql-user=` / `--mysql-password=` | 同上 |

也可以不写任何密码参数，直接 `export MYSQL_PWD=<密码>` 一把梭（所有脚本都会读它当默认值）。
客户端查询走 `MYSQL_PWD`（只对单次调用生效，**不进 `ps` 的 argv**）；转发给 sysbench 的那一份是
`--mysql-password=`，它会出现在 `ps` 里 —— 在意的话就用 `MYSQL_PWD`。

实测（2026-10-14，一个 `rafter`/`pw123` 的实例）：不给密码 ⇒ `ERROR 1045 Access denied`（客户端与
sysbench 两条路都会拒）；`--leader-password/--preview-password` 与 `MYSQL_PWD` 两种给法都能过
（自动发现、预检、预演实例执行 DDL 并派生指纹、`verify.sh` 聚合 `verify.result=PASS`、
`doctor.sh` `check1_connect=PASS`）。

**逃生口**（连接层以外的参数原样往下传，分词规则同 shell）：

| 参数 / 环境变量 | 作用 |
| --- | --- |
| `--mysql-cli-args="..."`（env `MYSQL_CLI_ARGS`） | 追加给 **mysql 客户端**的每次调用。例：`--get-server-public-key`、`--ssl-mode=REQUIRED` |
| `--sysbench-args="..."`（env `SYSBENCH_ARGS`） | 追加给**所有 sysbench 调用**；`prepare.sh` 会透传给 `raft_schema.sh` / `verify.sh`。例：`--mysql-ssl=on --mysql-ssl-cipher=...` |

**`caching_sha2_password` 的坑（实测）**：账号有密码时，**非 TLS 且客户端没要服务端公钥**的连接会被服务端
直接拒：`ERROR 2061 (HY000): Authentication plugin 'caching_sha2_password' reported error: Authentication
requires secure connection.`（认证**冷缓存**时必现；成功认证一次后走 fast path，`FLUSH PRIVILEGES` /
重启会清掉缓存。）四种出路，都实测过：

- `--mysql-cli-args="--get-server-public-key"` —— 不用 TLS 也能过（RSA 交换）；
- `--mysql-cli-args="--ssl-mode=REQUIRED"` —— 走 TLS（实测 `Ssl_cipher=TLS_AES_128_GCM_SHA256`），
  需要服务端有证书、客户端编译了 SSL 支持；
- 把缓存喂热：先用能过的客户端认证一次，之后的明文连接走 fast path（一次性，别指望它稳）；
- 换 `mysql_native_password`：**8.4 默认没加载该插件**（实测 `ERROR 1524 Plugin
  'mysql_native_password' is not loaded`），要用得在服务端加 `--mysql-native-password=ON`，不建议。

sysbench 那条路默认就会协商 TLS（libmysqlclient 默认 PREFERRED，而 `--mysql-ssl` 只能强制打开、不能关），
所以一般不会撞 2061 —— 撞 2061 基本就是 **mysql 客户端那一条**：客户端没编译 SSL，或服务端 TLS 没开。

---

## 1. 构建（Ubuntu 24.04）

依赖（`Containerfile.sysbench-build-env` 里的清单，三个坑都固化了）：

```bash
sudo apt-get update
sudo apt-get install -y build-essential autoconf automake libtool pkg-config m4 \
  git ca-certificates libaio-dev libluajit-5.1-dev libmysqlclient-dev mysql-client \
  libssl-dev zlib1g-dev
```

三条构建命令：

```bash
./autogen.sh
./configure CFLAGS="-std=gnu17 -O2 -ggdb3" --with-system-luajit \
  --with-mysql-includes=/usr/include/mysql \
  --with-mysql-libs=/usr/lib/x86_64-linux-gnu
make -j"$(nproc)" && $SB --version      # → sysbench 1.0.20-<git hash>
```

* `-std=gnu17` 不能省：autoconf 2.72+ 会注入 `-std=gnu23`，而本仓库自带的 zlib 副本用 K&R 函数定义（C23 已删除）。
* `--with-system-luajit`：否则去构建自带 LuaJIT（慢，且在部分工具链下产物不可用）。
* `--with-mysql-libs` 随发行版/架构变（aarch64 是 `/usr/lib/aarch64-linux-gnu`），用 `mysql_config --libs` 对一下。

容器里构建（源码只读挂载、clone 到容器内，避免带进宿主机构建产物）：

```bash
podman build -t localhost/sysbench-build-env:ubuntu24.04 -f Containerfile.sysbench-build-env .
podman run --rm -v <fork 路径>:/src:ro -v <脚本目录>:/scripts:ro \
  localhost/sysbench-build-env:ubuntu24.04 bash -lc 'bash /scripts/build-linux.sh'
```

---

## 2. 起一套三成员集群（参数模板）

三台只差 `datadir` / `port` / `socket` / `server-id` / `member-id` / `listen-port`：**整集群一致的是
`cluster-uuid`、`schema-manifest`、`schema-fingerprint`、`members`**（不一致会被启动期身份校验拒绝）。

```bash
$MYSQLD --no-defaults \
  --datadir=<node3 datadir> --socket=<node3>/mysql.sock --port=43703 --user=root \
  --pid-file=<node3>/mysqld.pid --log-error=<node3>/error.log \
  --default-time-zone=+00:00 --max-allowed-packet=64M \
  --log-bin=<node3>/binlog --server-id=3 --binlog-format=ROW --binlog-row-image=FULL \
  --percona-raft-enabled \
  --percona-raft-member-id=3 \
  --percona-raft-listen-port=43713 \
  --percona-raft-cluster-uuid=<32 hex，整集群一个新随机值> \
  --percona-raft-schema-manifest=<manifest hex> \
  --percona-raft-schema-fingerprint=<64 hex> \
  --percona-raft-members=1@127.0.0.1:43711,2@127.0.0.1:43712,3@127.0.0.1:43713 \
  --percona-raft-empty-data-baseline-verified \
  --percona-raft-schema-cutover-wait-ms=3000
```

`cluster-uuid` 是**整集群共用**的身份（事务身份、收据主键、派生 GTID 的 source_id 都含它），
写进 datadir 后不可改，改身份要重建基线。`manifest hex` / `fingerprint` 的产法见 §7 的第 ① 步
（在 Raft 关闭的实例上用 `SELECT PERCONA_RAFT_SCHEMA_MANIFEST('<库>','<表清单>')` 派生，见 §7）。

---

## 3. 建库 + 起空集群（必须，且在 Raft 关闭时）

本集群**只声明一个库**（manifest 里 `tables.front().schema` 就是声明库，`static_cluster_runtime.cc:4488`），
库必须在 Raft 关闭时建好，且**每个成员各建一遍**（DDL 不会被复制）。2026-10-14 用这套流程起过一套
全新 lab（v8，端口 48201-48204）：init → raft-off 建库建表 → 派生 genesis → raft-on ⇒
`leader=node1 serving=ON schema_epoch=1`。

```bash
# ① 初始化四个 datadir（三个成员 + 一个预演实例；预演实例不需要 raft/不需要 binlog）
$MYSQLD --no-defaults --initialize-insecure --user=root --datadir=<node1>

# ② Raft 关闭状态下启动（**去掉所有 --percona-raft-* 参数**），在每个成员上建同一个库和
#    **至少一张 genesis 表**：在册表数不能是 0（schema_manifest.cc:207），所以集群起点至少一张。
#    这张表的形状会被写进 genesis manifest，之后所有成员都必须与它逐列一致。
#    ★ 这张表是**集群自己的**，与 sysbench 无关：它不必是测试表，也不必给测试腾位置——
#      集群起来之后，`sbtest1..N` **全部**由 transition 建（见 §5），测试表一张都不用预建。
$MYSQL -h127.0.0.1 -P48201 -uroot -e "
  CREATE DATABASE IF NOT EXISTS perf DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
  CREATE TABLE IF NOT EXISTS perf.t (id INT NOT NULL, k INT NOT NULL DEFAULT 7,
    note VARCHAR(64) NOT NULL DEFAULT 'hello', PRIMARY KEY (id)) ENGINE=InnoDB"

# ③ 在预演实例上派生 genesis manifest（两参函数，返回 JSON）
JSON=$($MYSQL -h127.0.0.1 -P48204 -uroot -Nse "SELECT PERCONA_RAFT_SCHEMA_MANIFEST('perf','t')")
HEX=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['manifest_hex'])" "$JSON")
FP=$(python3  -c "import json,sys; print(json.loads(sys.argv[1])['fingerprint'])"  "$JSON")
#    实测：fingerprint=a14326e1fa673e4182451fbad3cd54e7bf0bffd3f454f180c560ecf0cd7c1bae、manifest_hex 724 字符

# ④ 带 Raft 参数启动（§2 的模板，--percona-raft-schema-manifest="$HEX" / -fingerprint="$FP"）
#    等 serving leader：SHOW STATUS LIKE 'Percona_raft_state' / 'Percona_raft_serving'

# ⑤ 测试表**不用手工建**：一条命令让工具从这张 genesis 表涨到 N 张（见 §5）
bash percona/prepare.sh --sysbench=$SB --schema-db=perf --mysql-db=perf \
  --table-prefix=sbtest --tables=4 --table-size=500 --row-length=400 \
  --preview-host=127.0.0.1 --preview-port=48204 \
  --member 127.0.0.1:48201 --member 127.0.0.1:48202 --member 127.0.0.1:48203 --verify
```

---

## 4. 工具自带命令（`src/lua/`）

### 4.1 `raft_prepare.lua`：建表 / 灌数 / 行长回读 / 类型面

```bash
# 只打印将执行的 DDL（不连库）
$SB src/lua/raft_prepare.lua --schema-db=perf --mysql-db=perf --tables=2 ddl

# 建表 + 灌数 + 自动行长回读（偏差超 ±10% 直接 FAIL，不静默接受）
$SB src/lua/raft_prepare.lua --schema-db=perf --mysql-db=perf \
  --mysql-host=127.0.0.1 --mysql-port=$LEADER --mysql-user=root \
  --table-prefix=sbtest --tables=2 --table-size=500 --row-length=400 --seed=42 prepare

# 只回读（对已灌好的表核对）
$SB src/lua/raft_prepare.lua --schema-db=perf --mysql-db=perf \
  --mysql-host=127.0.0.1 --mysql-port=$LEADER --mysql-user=root \
  --tables=8 --row-length=400 rowlen

# 全类型矩阵（31 列）并断言不出现 UNSUPPORTED_COLUMN
$SB src/lua/raft_prepare.lua --schema-db=perf --mysql-db=perf \
  --mysql-host=127.0.0.1 --mysql-port=$LEADER --mysql-user=root --types=all typescheck
```

形状相关开关：`--types=builtin|all`、`--index-profile=pk|pk_secondary`（默认 `pk`）、
`--row-length=B`（`B>0` 时会加宽承载列 `c` 并让 `pad` 恒 60 字节参与口径）。

### 4.2 `raft_verify.lua`：跨成员核对（一般用 §5 的包装脚本）

```bash
$SB src/lua/raft_verify.lua --schema-db=perf --mysql-db=perf \
  --mysql-host=127.0.0.1 --mysql-port=$LEADER --mysql-user=root \
  --schema-tables=sbtest1 aggregate      # 或 dump / columns（位置子命令）
```
选项：`--pk-column`（默认自动推导）、`--digest-cols`、`--where`、`--limit`（dump 行数上限，0=不限）、`--quiet`。

### 4.3 `raft_report.lua`：出报告（一般用 `percona/report.sh`）

子命令是**位置参数**（`sysbench.cmdline.commands`）：`sample` / `combine` / `compare` / `show`。

```bash
# 采样（跑前/跑后各一次，采样只读服务器状态量与参数，不跑负载）
$SB src/lua/raft_report.lua sample  --out=before.json
$SB src/lua/raft_report.lua sample  --out=after.json

# 合并成 report.json（元数据用 --meta-* 传；形状参数照抄本次负载）
$SB src/lua/raft_report.lua combine \
  --before=before.json --after=after.json --sysbench-out=sysbench.out \
  --report=run.report.json --scenario=oltp_read_write.lua \
  --meta-host=127.0.0.1 --meta-port=$LEADER --meta-db=perf --meta-target=raft \
  --meta-read-consistency=default --meta-threads=16 --meta-time=60 \
  --tables=8 --table-size=1000000 --row-length=400 --types=builtin \
  --index-profile=pk --seed=42 --rowlen-measured-avg=400.0 --rowlen-measured-deviation=0.00 \
  --env-os="$(uname -a)" --env-cpu=<cpu> --env-mem=<mem>

# 两臂对比 / 看一份
$SB src/lua/raft_report.lua compare --a=default.report.json --b=linearizable.report.json
$SB src/lua/raft_report.lua show    --in-file=run.report.json
```

> **选项名不能与驱动/全局选项重名**：脚本里若声明 `mysql_host` / `target` / `threads` 这类名字，
> sysbench 会**段错误退出（rc=139）**，所以元数据统一带 `meta_` 前缀（源码里有这条实测注释）。

### 4.4 `raft_doctor.lua`：环境自检（目标差异不当失败）

```bash
$SB src/lua/raft_doctor.lua --mysql-host=127.0.0.1 --mysql-port=$LEADER \
  --mysql-user=root --mysql-db=mysql
# 原生 MySQL 上也应 PASS：session_vars=SKIP、status_vars=0/22、PERCONA_RAFT* 函数 0 个
```

---

### 4.5 `raft_monitor.lua`：边跑边采 + 自动判读"慢在哪条腿"

```bash
# 采一段（默认 1s 一次；同名的 <out>.stop 文件出现即停，或用 --duration-s=N）
$SB src/lua/raft_monitor.lua --mysql-host=127.0.0.1 --mysql-port=$LEADER --mysql-user=root \
  --mysql-db=mysql sample --out=/tmp/m.tsv --interval-ms=1000
# 抓一份 recent_events（给 summarize 算分阶段分布）
$SB src/lua/raft_monitor.lua --mysql-host=127.0.0.1 --mysql-port=$LEADER --mysql-user=root \
  --mysql-db=mysql ring --out=/tmp/m.ring.json
# 汇总 + 判读（人看的报告，最后一行是机器可读的 raft_monitor.result）
$SB src/lua/raft_monitor.lua --mysql-host=127.0.0.1 --mysql-port=$LEADER --mysql-user=root \
  --mysql-db=mysql summarize --in-file=/tmp/m.tsv --ring-file=/tmp/m.ring.json
```

- **TSV 形状**：头一行 `#\traft_monitor.v1\thost=…\tport=…\tinterval_ms=…\tts_epoch\t<状态量名列…>`，
  每行一个采样点；时间戳取**服务端时钟**（`UNIX_TIMESTAMP(NOW(6))`）⇒ 三个成员的样本可以直接对齐。
- **采什么**（一次往返取回全部）：位点（commit / applier_progress / applying / local_durable /
  durable_checkpoint / durable_quorum）、等待类（apply_barrier_entries、apply_dependency_waits、
  write_permit_waits、caught_up_waits、admission_hold_timeouts）、结果未定（client_delivery_unknown 等 6 个）、
  本机日志（journal_write_us / fsync_us / applied_batch_fsync_us / scan_us）、引擎 apply
  （apply_count / apply_engine_us / slow_applies）、冻结与快照（freeze_* / binlog_freeze_deferrals /
  wal_snapshot_copy_*）、捕获编码（captured_cache / envelope_sha256）、MySQL 侧旁证
  （Threads_running / Innodb_data_fsyncs / Innodb_os_log_written / Queries）。**缺的状态量留空，不编 0**。
- **判读（verdict）**按优先级给：`UNKNOWN_OUTCOMES`（客户端拿到 3197 ⇒ 反查那几笔的 local_apply）、
  `COMMIT_GATE_QUEUE`、`APPLY_DEPENDENCY_WAIT`、`APPLIER_LAG`、`DURABLE_LAG`、`QUORUM_LAG`（**只在 leader 上判**）、
  `PERMIT_QUEUE`、`POST_COMMIT_WAIT`（ring 里 `proposal→local_apply` 空档）、`COMMIT_STALLED`（只在窗口内确实
  提交过时判）；无异常则 `NOTHING_SEEN` 并给出下一步。快照拷贝/回收推迟属于"提示项"，不算判据。
- `--mysql-db` 随便给一个存在的库即可（采样不碰业务库；驱动默认 `sbtest`，不存在的库会连不上）。

---

## 5. 包装脚本（`percona/*.sh`）

```bash
# 跨成员一致性：8 表聚合（不逐行 dump）+ 单表逐行摘要 dump+diff
bash percona/verify.sh --sysbench=$SB --schema-db=perf --schema-tables=sbtest1,sbtest8 \
  --user=root --skip-dump --member 127.0.0.1:43701 --member 127.0.0.1:43702 --member 127.0.0.1:43703
bash percona/verify.sh --sysbench=$SB --schema-db=perf --schema-tables=sbtest1 \
  --user=root --member 127.0.0.1:43701 --member 127.0.0.1:43702 --member 127.0.0.1:43703
# 退出码：0 一致 / 1 不一致（会打第一处差异）/ 2 成员不通

# 出报告（归档 report.json；可选 --sample-port（采样连哪台，默认与 --mysql-port 同）与 --extra "…额外参数…"）
bash percona/report.sh --scenario=src/lua/oltp_read_write.lua --mysql-port=$LEADER --target=raft \
  --threads=16 --time=60 --mysql-db=perf --tables=8 --table-size=1000000 --row-length=400 \
  --types=builtin --index-profile=pk --seed=42 --out-dir=<out>

# 自检（两个端点：目标跑检查 1/2/4/5/6/7，预演实例跑 golden 检查 3）
bash percona/doctor.sh --sysbench=$SB \
  --target-host=127.0.0.1 --target-port=$LEADER --target-user=root \
  --preview-host=127.0.0.1 --preview-port=43704 --preview-user=root \
  --require-raft --golden-dir=./schemas/golden

# 活集群加/改表（预演实例派生目标 manifest → 提案 → 三台核对）
bash percona/raft_schema.sh --sysbench=$SB --schema-db=perf --schema-tables=t,t2 \
  --ddl="CREATE TABLE perf.t2 (id INT PRIMARY KEY, v INT NOT NULL)" \
  --preview-host=127.0.0.1 --preview-port=43704 --preview-user=root \
  --leader-host=127.0.0.1 --leader-port=$LEADER --leader-user=root \
  --member 127.0.0.1:43701 --member 127.0.0.1:43702 --member 127.0.0.1:43703 --wait-applied
```

**边跑边采 + 自动判读：`percona/monitor.sh`**（把任意一条命令包起来，三台同时采，结束自动汇总）

```bash
bash percona/monitor.sh --sysbench=$SB \
  --member 10.188.62.79:6606 --member 10.214.38.209:6606 --member 10.188.48.140:6606 \
  --user=root --password='root' --interval-ms=1000 --out-dir=/tmp/raft-mon --tag=load \
  -- bash percona/prepare.sh --sysbench=$SB --schema-db=test --mysql-db=test ...
```

- `--` 之后是要包住的命令（`prepare.sh`、原生 sysbench 负载、任何命令都行）；**退出码 = 被包命令的退出码**。
- 产物：`<out-dir>/<tag>-<port>.tsv`（原始采样）、`.ring.json`、`.summary.txt`（逐台完整判读）、
  `command.log`（被包命令原样输出）；屏幕上给"三台并排"的 `raft_monitor.result` 行 + 逐台判读。
- 看什么：`post_commit_gap_p95_us`（提交完成→本机 apply 的空档）、`max_apply_lag` / `max_durable_lag`
  （本机游标落后 commit 的峰值）、`max_quorum_lag`（leader 侧）、`unknown_delta`（结果未定）。
  三台并排时，**谁的 lag 大就是那条腿**。

`prepare.sh` 接了同一套：加 `--monitor`（可配 `--monitor-interval-ms=`、`--monitor-out-dir=`）就会自动用
`monitor.sh` 把整条流程包住，不必手写 wrapper。

**合并「建表 + 灌数」：`percona/prepare.sh`**（缺的表逐张走 transition，随后灌数 + 行长回读）。

**集群已经在跑时（最常见）：测试表一张都不用预建，一条命令把 `sbtest1..N` 全建出来并灌数** ——
在册的是集群自己的表，脚本从 leader 的 `information_schema` 自动把它们前插进声明清单
（不给 `--schema-tables` 就是这个"自动收敛到 N 张"模式），然后逐张 transition 补缺的表：

`--schema-db` = 集群的**声明库**；`--mysql-db` = 灌数连的库（缺省与 `schema-db` 相同）。

```bash
MYSQL_CLI=<build>/runtime_output_directory/mysql \
bash percona/prepare.sh --sysbench=$SB \
  --schema-db=perf --mysql-db=perf \
  --table-prefix=sbtest --tables=8 --table-size=1000000 --row-length=400 --seed=42 \
  --preview-host=127.0.0.1 --preview-port=48104 --preview-user=root \
  --member 127.0.0.1:48101 --member 127.0.0.1:48102 --member 127.0.0.1:48103 \
  --verify                                      # 结束时做跨成员聚合核对
```

已经存在的表不会被重复 CREATE（想从 4 张长到 8 张就再跑一次 `--tables=8`）；
想显式给清单（表名不按 `<prefix>N`、或要自己写全）就加 `--schema-tables=t,sbtest1,prep1,prep2`，
此时尾部 N 项必须正好是 `--tables=N` 生成的那 N 张。

实测（2026-10-14，新起的 v8 lab：genesis 只声明 `t`，`--table-size=500 --row-length=400`）：

| 场景 | 结果 |
| --- | --- |
| 空集群一条命令：`--table-prefix=sbtest --tables=4`（不给 `--schema-tables`） | 自动发现 `t` ⇒ 清单 `t,sbtest1..4`；4 次 transition（**epoch 1→5**）、4×500 行、行长 `avg=399.8`（目标 400，偏差 0.05%）、三台聚合逐字相同、**退出码 0（4.5 s）** |
| 从 4 张长到 6 张：`--tables=6 --skip-load` | 只建缺的 `sbtest5`/`sbtest6`（epoch 5→7），已在的 4 张不重复 CREATE |
| 同一批重跑（4 张都已建好且已灌数） | **提前拒绝灌数**（"perf.sbtest1 已有 500 行…"），不会打到 `INSERT` 才撞主键；只核对用 `--skip-load --verify` |

四条硬校验（都具名失败，不静默做错事）：

| 校验 | 行为 |
| --- | --- |
| 目标清单尾部必须正好是要建的表 | `--schema-tables=t,sbtest1,prep2`（要建 prep1）⇒ 拒绝 |
| 已存在的必须是前缀 | `--schema-tables=t,ghost1,sbtest1,prep1` ⇒ 拒绝："不是追加形态" |
| 只灌空表 | 表里已有数据时**拒绝灌数**（`prepare` 是普通 `INSERT` id=1..N，重复灌必撞主键）；`--allow-nonempty` 可强行灌 |
| schema 里不许有"清单外的表" | 显式给了 `--schema-tables` 却漏掉在册的表 ⇒ 拒绝，并把"按创建顺序补齐"的清单原样打出来 |

只建表用 `--skip-load`，只灌数用 `--skip-ddl`。`--schema-tables` 的**顺序不影响服务端判定**
（见 §7 的集合语义），它只决定目标 manifest 里表的排列。

---

## 6. 验收命令集

### 6.1 M1 收尾（Linux 上的正式口径，leader 直跑）

```bash
# 只读基线（同口径、无写）
$SB src/lua/oltp_point_select.lua --target=raft --mysql-host=127.0.0.1 --mysql-port=$LEADER \
  --mysql-user=root --mysql-db=perf --tables=1 --table-size=12910 --threads=16 --time=10 run

# 正式场景：受管写，16 线程 60 秒
$SB src/lua/oltp_read_write.lua --target=raft --mysql-host=127.0.0.1 --mysql-port=$LEADER \
  --mysql-user=root --mysql-db=perf --tables=1 --table-size=12910 --threads=16 --time=60 run

# 强读对照（LINEARIZABLE）
$SB src/lua/oltp_point_select.lua --target=raft --read-consistency=linearizable \
  --mysql-host=127.0.0.1 --mysql-port=$LEADER --mysql-user=root --mysql-db=perf \
  --tables=1 --table-size=12910 --threads=16 --time=10 run
```

### 6.2 M2-5 验收（8 表 × 100 万行、行长 400；三臂都经 report.sh 归档）

```bash
# 先探 leader（状态量 leader_id ⇒ 端口 = 端口基址 + leader_id）
LID=$($MYSQL -h127.0.0.1 -P43701 -uroot -Nse "SHOW GLOBAL STATUS LIKE 'Percona_raft_leader_id'" | awk '{print $2}')
LEADER=$((43700 + LID))

COMMON="--mysql-host=127.0.0.1 --mysql-user=root --mysql-db=perf \
  --tables=8 --table-size=1000000 --row-length=400 --types=builtin --index-profile=pk --seed=42"

bash percona/report.sh --scenario=src/lua/oltp_read_write.lua  --mysql-port=$LEADER --target=raft \
  --threads=16 --time=60 $COMMON --out-dir=<out>
bash percona/report.sh --scenario=src/lua/oltp_point_select.lua --mysql-port=$LEADER --target=raft \
  --threads=16 --time=30 $COMMON --out-dir=<out>
bash percona/report.sh --scenario=src/lua/oltp_point_select.lua --mysql-port=$LEADER --target=raft \
  --read-consistency=linearizable --threads=16 --time=15 $COMMON --out-dir=<out>
```

**契约 1:1 核对（运行前后取差值，leader 上）**：`wal_append_batches`、`wal_append_records`、
`admission_success` 三者增量应**等于**客户端事务数；`unmanaged_write_rejections` 增量必须为 **0**；
`appends_per_fsync = wal_append_records / wal_append_batches`（report.sh 自算）。
报告里另有 `connections with injected identity: N / M`、`request ids issued`、
`autocommit transactions wrapped` 与 5 栏错误分类。

### 6.3 十个原版脚本原样跑（逐个看 exit / 未受管写增量）

```bash
run() { local s=$1; shift
  before=$($MYSQL -h127.0.0.1 -P$LEADER -uroot -Nse \
    "SHOW GLOBAL STATUS LIKE 'Percona_raft_unmanaged_write_rejections'" | awk '{print $2}')
  $SB src/lua/$s --mysql-host=127.0.0.1 --mysql-port=$LEADER --mysql-user=root \
     --mysql-db=perf --tables=1 --table-size=200 --threads=2 --time=3 "$@" run
  rc=$?                                  # 立刻存，别被后面的命令覆盖
  after=$($MYSQL -h127.0.0.1 -P$LEADER -uroot -Nse \
    "SHOW GLOBAL STATUS LIKE 'Percona_raft_unmanaged_write_rejections'" | awk '{print $2}')
  echo "$s exit=$rc unmanaged_rej_delta=$((after-before))"; }

run oltp_point_select.lua
run oltp_read_only.lua
run select_random_points.lua --point-selects=1 --rand-type=uniform
run select_random_ranges.lua
run oltp_read_write.lua
run oltp_write_only.lua
run oltp_update_index.lua
run oltp_update_non_index.lua
run oltp_delete.lua
run oltp_insert.lua
```

> 注意 `--mysql-ignore-errors` 默认 `1213,1020,1205`：`ignored errors` 会把**死锁**吞进去，
> 所以两侧的 `ignored errors` 不能当客户端质量差异读，要连 `write_concurrency` 一起看。

---

## 7. schema transition（往活集群加表）

```bash
# ① 预演实例（Raft 关闭、与集群同一份 schema）上把目标表建出来
$MYSQL -h127.0.0.1 -P43704 -uroot perf -e \
  "CREATE TABLE perf.t2 (id INT PRIMARY KEY, v INT NOT NULL)"
# ② 派生目标 manifest：函数是**两参**（库, 表清单），返回的是 JSON，不是裸十六进制
JSON=$($MYSQL -h127.0.0.1 -P43704 -uroot -Nse \
  "SELECT PERCONA_RAFT_SCHEMA_MANIFEST('perf','t,t2')")
HEX=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['manifest_hex'])" "$JSON")
#    同一个 JSON 里还有 fingerprint（＝目标指纹，提案时用 --expect-fingerprint 对账）
# ③ 对 leader 提案
$MYSQL -h127.0.0.1 -P$LEADER -uroot perf -e \
  "SELECT PERCONA_RAFT_SCHEMA_TRANSITION('CREATE TABLE perf.t2 (id INT PRIMARY KEY, v INT NOT NULL)', '$HEX') AS transition_result\G"
# ④ 三台核对：schema_epoch 都 +1、目标表存在、每台自派生指纹一致
for p in 43701 43702 43703; do $MYSQL -h127.0.0.1 -P$p -uroot -Nse \
  "SHOW STATUS LIKE 'Percona_raft_schema_epoch'"; done
```

或用 §5 的 `percona/raft_schema.sh` 一条命令（含"预演实例与集群在 force 的指纹必须先相同"的预检；
`--schema-tables` 是**目标**清单，新增表必须与 DDL 同名）。

**服务端对 CREATE 类提案的判定**（`sql/raft_consensus/schema_transition_policy.cc:493-513`，
本轮逐条读代码 + 实测确认；`percona/prepare.sh` 的循环就是按这个写的）：

| 规则 | 出处 |
| --- | --- |
| 目标 manifest 的表数必须正好 = 在册 + 1；多一张少一张都以 `the target schema manifest describes a different number of tables than the one in force` 拒绝 | `:326`（appended 分支）、`else` 分支 |
| 在册的每一张都必须仍被描述（`missing == 0`）、且**恰好一张**是新的（`added == 1`）⇒ **一次 transition 只能加一张表**，不能批量；报错原文 `removes N table(s) and adds M table(s)` | `:497-511` |
| 加表只能由 `CREATE TABLE` 完成（`ALTER` 加表被拒：`a table may only be added by a CREATE TABLE`） | `:512-514` |
| **顺序自由**：判定是集合比较（`target_names_match()` 用 `used[]` 逐名抵消，`:129-147`），新表放清单哪儿由该条目自己声明（注释原文 "Where it appears is the entry's declaration to make"）。**实测**：v7 lab 上自动发现的顺序是 `sbtest1,t,…`，而 genesis 声明顺序是 `t,sbtest1` —— 顺序相反仍然 PASS（epoch 4→5→6） | `:493-495` |
| 集群**不可能**"在册 0 张表"：`valid_manifest_structure()` 要求 `!manifest.tables.empty()`（`schema_manifest.cc:207`），`--percona-raft-schema-manifest` 也必须是非空 hex | `schema_manifest.cc:207` |
| manifest 只承载**一个声明库**（`static_cluster_runtime.cc:4488` 取 `tables.front().schema` 作为声明库）⇒ 不能往第二个库里加表 | `:4486-4491` |

---

## 8. 手工受管写与探针

```bash
# 受管写的最小形态：身份 + 显式 BEGIN（缺身份 ⇒ 8109；写到非 leader ⇒ 1402）
$MYSQL -h127.0.0.1 -P$LEADER -uroot perf -e "
SET SESSION percona_raft_request_namespace='probe';
SET SESSION percona_raft_request_id='probe-1';
BEGIN; INSERT INTO perf.t (id,k,note) VALUES (9100,1,'probe'); COMMIT;"
```

> 同一个 `(namespace, request_id)` 复用会被拒（`ERROR 1402 XA_RBROLLBACK`）⇒ 每次工作负载用全新身份。

---

## 9. 集群侧状态与漂移检查

```bash
# 状态速查：角色 / 是否出服务 / leader / epoch / 追平进度
$MYSQL -h127.0.0.1 -P$PORT -uroot -Nse "
SELECT CONCAT('state=',(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Percona_raft_state'),
              ' serving=',(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Percona_raft_serving'),
              ' leader=',(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Percona_raft_leader_id'),
              ' epoch=',(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Percona_raft_schema_epoch'),
              ' applied=',(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Percona_raft_last_applied'))"

# 从派生 GTID 反推 (term,index)，并判断"是不是本集群的"
$MYSQL -h127.0.0.1 -P$LEADER -uroot -Nse "SELECT PERCONA_RAFT_GTID_POSITION('<uuid>:<sequence>')"

# 漂移 E2E：停成员 → 以 Raft 关闭启动 → 改 schema → 带 Raft 重启
kill "$(cat <node2>/mysqld.pid)"; $MYSQLD <原参数> --skip-percona-raft-enabled &
$MYSQL -h127.0.0.1 -P43702 -uroot -e "ALTER TABLE perf.t ADD COLUMN extra INT NULL"
# 再带 Raft 起回来，然后看：
#   成员本地自检：grep "data dictionary" <node2>/error.log
#   leader 的对账轮归因：grep "derives a different schema fingerprint" <leader>/error.log
```

> 漂移成员的心跳修法：修回本地 schema（或把日志追平）后**无需重启 leader**，对账轮下一拍即闭合。

---

## 10. 服务端侧的验收命令（在服务端仓库里）

这些不在本 fork，但验收时要用（`<server repo>/tests/raft_harness/`）：

```bash
# 全量 raft 单测套件（会先把 mysqld/mysql/mysqladmin/mysqlbinlog/raft_snapshot_tool + 所有 raft_*-t 建齐再跑）
tests/raft_harness/run_raft_unit_suite.sh --build-dir <build> --jobs "$(nproc)"

# 崩溃矩阵（每条用例一个进程，可断点续跑；-- 用例清单从机具里读）
P1_CRASH_LEDGER=<ledger> P1_CRASH_LOG_DIR=<logs> \
  tests/raft_harness/run_p1_crash_matrix_resumable.sh <build> [用例名…]

# 跑完必须核对证据（是否有未解释的崩溃/追平超时）
tests/raft_harness/check_run_evidence.sh <ledger> <logs> <用例数>
```

---

## 11. 结果归档

```bash
bash percona/report.sh --scenario=… --out-dir=<out>      # 每次运行写一份 report.json
ls <out>/*.report.json | head
```
`report.json` 段：`run` / `load_shape` / `sysbench` / `contract` / `server_params` /
`server_before` / `server_after` / `delta` / `env`，并**逐字保留** `sysbench_raw`。
`appends_per_fsync` 与 `admission_per_client_txn` 由工具自算，是契约 1:1 的旁证。

---

## 附录 A：常见错误码（工具按数值分 5 栏，实现在 `src/drivers/mysql/drv_mysql.c`）

| 码 | 含义 | 处理 |
| --- | --- | --- |
| `8109` | 未受管写被拒（缺身份 / 语句在受管契约之外） | 设 `percona_raft_request_namespace`/`_request_id` 并用显式 `BEGIN` |
| `1402` | `XA_RBROLLBACK`：BEGIN 预检拒绝（写非 leader、或复用同一请求身份） | 换身份 / 指向 leader |
| `8107` | 不是 leader | 重连到 leader（`temporary` 栏） |
| `3197` | 结果未定（`XA_RETRY`）：提交结果不可知 | 按未定处理，靠对账收敛；不要当"确定的失败" |
| `8100` | 强读只在 leader 上成立（follower 被拒） | 强读档指向 leader |
| `1062` | 唯一键冲突（`pk_secondary` 的唯一索引） | 生成器保证唯一；出现即为数据问题 |

## 附录 B：出处

* 服务端方案与实测记录：`Docs/raft_replication/perf_tool_plan.md`（§5 行长口径、§6.3 强读、§6.4 跨成员核对、
  §8 报告、§9 构建、§10 M0–M2 验收）、`Docs/raft_replication/manual.zh-CN.md`（集群参数、错误码、迁移）、
  `Docs/raft_replication/crash_matrix_run_log.md`（崩溃矩阵的用例与判据）。
* 本文件里的命令行取自实际运行的脚本：`build-linux.sh`、`acceptance-linux.sh`、`m2-acceptance.sh`、
  `stock-scripts-audit.sh`、`v6-verify.sh`、`schema-transition-demo.sh`、`cutover-timeout-test.sh`
  （这些原型脚本在服务端项目外的工作目录里，按需复制进本仓库 `scripts/`）。
