#!/usr/bin/env sysbench
--
-- Percona Raft: **大行只写** workload（BLOB 承载，受管写身份 + 显式 BEGIN）
--
-- 为什么单独写一个：`oltp_common.lua` 的表名硬编码 `sbtest%d`、列固定 `id/k/c/pad`
-- 且 `c` 只有 120 字符，既打不到 `at*`（全类型，主键 `c_long`、载荷在 `c_pad`），
-- 也表达不了 256 KB 这种行。
--
-- 期望的表形状（用 `raft_prepare.lua --types=all --row-length=262144` 建出来的就是它）：
--   <prefix><N>(
--     c_long INT NOT NULL PRIMARY KEY,  ... 31 列全类型 ...,
--     c_pad  MEDIUMBLOB                 -- 大载荷列（默认）
--   )
--
-- 每事务的语句形状（默认，与 oltp_write_only 对齐：BEGIN/COMMIT 也算 query ⇒ 6 条）：
--   BEGIN
--   UPDATE <t> SET <blob> = REPEAT('a', <row-length>) WHERE <pk> = ?   -- 大行：before+after 都进信封
--   UPDATE <t> SET <small> = ?                        WHERE <pk> = ?   -- 小列更新（非索引）
--   DELETE FROM <t> WHERE <pk> = ?                                     -- before 镜像
--   INSERT INTO <t> (<pk>, <blob>, <small>) VALUES (?, REPEAT('a', <row-length>), ?)
--   COMMIT
--
-- ⚠️ 信封体积：一次 UPDATE 大列 = before+after ≈ 2 × row-length；DELETE/INSERT 各 1 份。
--    256 KB 行、默认四条语句 ⇒ 每事务约 1.25 MB 的 ROW 镜像，**超过默认 1 MiB 的
--    --percona-raft-max-envelope-bytes**；跑之前必须把它调到 ≥ 4 MiB：
--      percona_raft_max_envelope_bytes = 4194304
--    只想测"单条大行写"的代价，用 --ops=big_update 把每事务压成一条。
--
-- 受管写契约：每个事务前设 `percona_raft_request_id`，事务内不改；命名空间在
-- thread_init 里设一次。对 Raft **关闭**的实例（预演/自举）用 `--skip-trx` 跳过身份与 BEGIN。
--
-- 用法：
--   SB=./src/sysbench
--   $SB src/lua/raft_bigrow_write.lua \
--     --schema-db=test --mysql-host=<leader> --mysql-port=6606 \
--     --mysql-user=root --mysql-password=root \
--     --table-prefix=at --tables=128 --table-size=1000 \
--     --row-length=262144 --threads=8 --time=600 --report-interval=1 --percentile=99 \
--     run
--
-- 用 monitor.sh 包住（与现有压测同一套观测）：
--   bash percona/monitor.sh --sysbench=$SB "${MEMBER_ARGS[@]}" --user=root --password=root \
--     --interval-ms=500 --ring-interval-ms=250 --host-metrics --node-exporter-port=9100 \
--     --out-dir=/tmp/raft-mon-bigrow --print-full -- \
--     $SB src/lua/raft_bigrow_write.lua --target=raft \
--       --mysql-host=$LEADER --mysql-port=6606 --mysql-user=root --mysql-password=root \
--       --mysql-db=test --schema-db=test --table-prefix=at --tables=128 --table-size=1000 \
--       --row-length=262144 --threads=8 --time=600 --report-interval=1 run
--

if sysbench.cmdline.command == nil then
   error("命令必填：run（这是只写 workload，不负责建表；表由 raft_prepare/prepare.sh 建）", 0)
end

sysbench.cmdline.options = {
   schema_db = {"目标库名（表所在库；--mysql-db 只决定连接默认库）", ""},
   tables = {"表数量（表名 = <prefix>1..N）", 1},
   table_prefix = {"表名前缀，表名 = <prefix>1..N（默认 at，即全类型表）", "at"},
   table_size = {"每张表的行数（主键取值范围 1..N）", 1000},
   row_length = {"载荷列的字节数（BLOB，默认 256 KB）", 262144},
   pk_column = {"定位键列（NOT NULL、通常是主键）", "c_long"},
   blob_column = {"承载大载荷的 BLOB 列", "c_pad"},
   small_column = {"做“非索引小列更新”的列", "c_tiny"},
   ops = {"每事务执行的语句集合：big_update,small_update,delete_insert 的逗号组合", "big_update,small_update,delete_insert"},
   payload_mode = {"载荷生成：sql（服务端 REPEAT，客户端只发小语句）| lua（客户端绑定大字符串）", "sql"},
   request_namespace = {"受管写命名空间（percona_raft_request_namespace）", "bigrow"},
   skip_trx = {"跳过身份与 BEGIN/COMMIT（对 Raft 关闭的实例用）", false},
}

local function has_op(name)
   for item in string.gmatch(sysbench.opt.ops, "[^,%s]+") do
      if item == name then return true end
   end
   return false
end

local con
local stmt_big_update, stmt_small_update, stmt_delete, stmt_insert
local payload
local seq = 0
local db, fq
-- <epoch>-c<CONNECTION_ID>：本连接本运行的唯一令牌
local run_token
local conn_id
local stmt_begin, stmt_commit

local function q(sql) con:query(sql) end

function thread_init()
   local drv = sysbench.sql.driver()
   con = drv:connect()

   db = sysbench.opt.schema_db
   if db == "" then
      -- 没给 --schema-db 就跟连接的默认库（--mysql-db）走
      local current = con:query_row("SELECT DATABASE()")
      if current == nil or current == "" then
         error("必须给 --schema-db（或 --mysql-db）", 0)
      end
      db = current
   end

   local t = sysbench.tid % sysbench.opt.tables + 1
   fq = string.format("%s.%s%d", db, sysbench.opt.table_prefix, t)

   -- 受管写身份：有这两个变量才是 Raft 成员；原生/预演实例上应当 --skip-trx
   if not sysbench.opt.skip_trx then
      local ok, err = pcall(function()
         q(string.format("SET SESSION percona_raft_request_namespace = '%s'",
                         sysbench.opt.request_namespace))
      end)
      if not ok then
         error("这台实例没有 percona_raft_request_namespace（不是 Raft 成员？）：" ..
               tostring(err) .. "；对 Raft 关闭的实例请用 --skip-trx", 0)
      end
      -- 幂等键必须跨运行、跨连接都唯一：XID = f(cluster_uuid, namespace, request_id)，
      -- 光靠 --time 重复跑同一批 id 会撞上上次留下的 XA（XAER_DUPID）。
      -- 这里用 <epoch>-c<CONNECTION_ID()> 做运行令牌，再拼每线程的序号。
      conn_id = tostring(con:query_row("SELECT CONNECTION_ID()"))
      run_token = string.format("%d-c%s", os.time(), conn_id)
      print(string.format("raft_bigrow_write.namespace=%s run_token=%s table=%s",
                          sysbench.opt.request_namespace, run_token, fq))
   end

   if sysbench.opt.payload_mode == "lua" then
      payload = string.rep("a", sysbench.opt.row_length)
   end
end

function prepare_statements()
   if not sysbench.opt.skip_trx then
      stmt_begin = con:prepare("BEGIN")
      stmt_commit = con:prepare("COMMIT")
   end
   local pk = sysbench.opt.pk_column
   local blob = sysbench.opt.blob_column
   local small = sysbench.opt.small_column
   local len = sysbench.opt.row_length

   if has_op("big_update") then
      if sysbench.opt.payload_mode == "lua" then
         stmt_big_update = con:prepare(string.format(
            "UPDATE %s SET %s = ? WHERE %s = ?", fq, blob, pk))
      else
         stmt_big_update = con:prepare(string.format(
            "UPDATE %s SET %s = REPEAT('a', %d) WHERE %s = ?", fq, blob, len, pk))
      end
   end
   if has_op("small_update") then
      stmt_small_update = con:prepare(string.format(
         "UPDATE %s SET %s = ? WHERE %s = ?", fq, small, pk))
   end
   if has_op("delete_insert") then
      stmt_delete = con:prepare(string.format(
         "DELETE FROM %s WHERE %s = ?", fq, pk))
      if sysbench.opt.payload_mode == "lua" then
         stmt_insert = con:prepare(string.format(
            "INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?)", fq, pk, blob, small))
      else
         stmt_insert = con:prepare(string.format(
            "INSERT INTO %s (%s, %s, %s) VALUES (?, REPEAT('a', %d), ?)",
            fq, pk, blob, small, len))
      end
   end
end

function event()
   seq = seq + 1
   local pk = sb_rand(1, sysbench.opt.table_size)
   local small = sb_rand(0, 127)

   if not sysbench.opt.skip_trx then
      local request_id = string.format("%s-n%d", run_token, seq)
      if seq == 1 then
         print(string.format(
            "raft_bigrow_write.first_id tid=%s cid=%s ns=%s id=%s",
            tostring(sysbench.tid), conn_id, sysbench.opt.request_namespace, request_id))
      end
      -- 与 oltp_common 一致：namespace 与 request_id 在**同一条** SET 里给出，紧贴 BEGIN。
      q(string.format("SET SESSION percona_raft_request_namespace = '%s', " ..
                      "SESSION percona_raft_request_id = '%s'",
                      sysbench.opt.request_namespace, request_id))
      stmt_begin:execute()
   end

   if stmt_big_update ~= nil then
      if sysbench.opt.payload_mode == "lua" then
         stmt_big_update:bind(1, payload)
         stmt_big_update:bind(2, pk)
      else
         stmt_big_update:bind(1, pk)
      end
      stmt_big_update:execute()
   end

   if stmt_small_update ~= nil then
      stmt_small_update:bind(1, small)
      stmt_small_update:bind(2, pk)
      stmt_small_update:execute()
   end

   if stmt_delete ~= nil then
      stmt_delete:bind(1, pk)
      stmt_delete:execute()
      -- 删了要补回来，否则表会越跑越空、主键范围落空
      if sysbench.opt.payload_mode == "lua" then
         stmt_insert:bind(1, pk)
         stmt_insert:bind(2, payload)
         stmt_insert:bind(3, small)
      else
         stmt_insert:bind(1, pk)
         stmt_insert:bind(2, small)
      end
      stmt_insert:execute()
   end

   if not sysbench.opt.skip_trx then
      stmt_commit:execute()
   end
end

function thread_done()
   if con ~= nil then con:disconnect() end
end
