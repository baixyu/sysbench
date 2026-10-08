#!/usr/bin/env sysbench
--
-- Percona Raft: 在活着的集群上加/改表（schema transition 辅助）
--
-- 依据：percona-server 的 Docs/raft_replication/perf_tool_plan.md §6.5。
-- 唯一合法的用户 DDL 通道是 SQL 函数：
--     PERCONA_RAFT_SCHEMA_TRANSITION(ddl, target_manifest_hex)
-- 而"目标 manifest"必须由调用方提供（DDL 的结果无法在不执行它的情况下预测），
-- 所以流程是三步：先在**预演实例**（Raft 关闭、schema 与集群一致）上执行 DDL 并派生
-- 目标 manifest，再对 **Leader** 提案，最后逐成员校验 epoch 与指纹。
--
-- 为什么本脚本只有"单端点"命令：Lua 层的 drv:connect() 不接受连接参数，
-- 连接参数只能来自全局 --mysql-*。所以"预演实例 + Leader + 各成员"这三个端点
-- 由外层 wrapper（percona/raft_schema.sh）串起来——本文件不新增任何 C 代码。
--
-- 命令：
--   preview  在预演实例上执行目标 DDL，并打印目标 manifest_hex / fingerprint
--   propose  对 Leader 提案，打印结果 JSON 的关键字段；非 APPLIED 即失败
--   verify   在一台上核对 schema_epoch 与本机自派生的指纹
--

if sysbench.cmdline.command == nil then
   error("命令必填：preview | propose | verify（见文件头注释）", 0)
end

sysbench.cmdline.options = {
   ddl = {"目标 DDL 文本（与 --ddl-file 二选一）", ""},
   ddl_file = {"从文件读目标 DDL（与 --ddl 二选一）", ""},
   schema_db = {"schema（库）名", ""},
   schema_tables =
      {"目标表名，按**目标顺序**逗号分隔（顺序是声明的一部分）", ""},
   manifest_hex = {"preview 派生的目标 manifest（十六进制）", ""},
   expect_fingerprint = {"期望的目标指纹（propose/verify 据此核对）", ""},
   wait_applied = {"提案后等待本机应用完成（按 to_epoch + 指纹核对）", false},
   wait_timeout = {"等待上限（秒）", 30},
   skip_ddl = {"preview 时跳过执行 DDL（目标表在该实例上已存在时用）", false},
}

--
-- 小工具
--

-- sysbench 的 Lua API 没有 sleep（只有 run 命令用的计时器），
-- 而等待"本机应用完"要能小睡，所以直接走 FFI 调 usleep。
local ffi = require("ffi")
ffi.cdef[[int usleep(unsigned int usec);]]
local function sleep_seconds(sec)
   ffi.C.usleep(math.floor(sec * 1000000))
end

local function die(fmt, ...)
   error(string.format(fmt, ...), 0)
end

-- MySQL 字符串字面量：反斜杠与单引号都要转义（DDL 里可能有 DEFAULT '0' 这类）
local function sql_quote(s)
   return "'" .. s:gsub("\\", "\\\\"):gsub("'", "''") .. "'"
end

local function sql_ident(s)
   if s == "" or s:find("[^%w_$]") then
      die("schema 名 '%s' 不适合直接拼进 SQL（只允许字母数字与 _ $）", s)
   end
   return s
end

local function connect()
   local drv = sysbench.sql.driver()
   return drv:connect()
end

-- 服务端返回的 JSON 是最简形状（无嵌套转义），按 key 取字符串值即可
local function json_string(json, key)
   local v = json:match('"' .. key .. '"%s*:%s*"([^"]*)"')
   if v == nil then
      die("无法从 JSON 里取字段 '%s'：%s", key, json)
   end
   return v
end

-- 数字/布尔字段（没有引号），也接受带引号的字符串
local function json_scalar(json, key)
   local v = json:match('"' .. key .. '"%s*:%s*([^,}]+)')
   if v == nil then
      die("无法从 JSON 里取字段 '%s'：%s", key, json)
   end
   v = v:gsub("^%s+", ""):gsub("%s+$", "")
   return (v:gsub('^"(.*)"$', "%1"))
end

local function json_bool(json, key)
   local v = json:match('"' .. key .. '"%s*:%s*(%a+)')
   return v == "true"
end

local function get_ddl()
   local ddl = sysbench.opt.ddl
   if ddl ~= "" then
      return ddl
   end
   local path = sysbench.opt.ddl_file
   if path == "" then
      die("必须给 --ddl 或 --ddl-file")
   end
   local fh, err = io.open(path, "r")
   if fh == nil then
      die("打不开 --ddl-file=%s: %s", path, tostring(err))
   end
   local text = fh:read("*a")
   fh:close()
   -- 去掉行尾换行：DDL 里带换行无害，但会让输出与错误信息难看
   return (text:gsub("%s+$", ""))
end

local function require_schema_args()
   local db = sysbench.opt.schema_db
   local tables = sysbench.opt.schema_tables
   if db == "" then
      die("必须给 --schema-db")
   end
   if tables == "" then
      die("必须给 --schema-tables（目标表名，按目标顺序逗号分隔；" ..
          "顺序是 manifest 声明的一部分，不猜）")
   end
   return sql_ident(db), tables
end

local function derive_manifest(con, db, tables)
   local json = con:query_row(string.format(
      "SELECT PERCONA_RAFT_SCHEMA_MANIFEST(%s, %s)", sql_quote(db),
      sql_quote(tables)))
   if json == nil then
      die("PERCONA_RAFT_SCHEMA_MANIFEST() 没有返回结果")
   end
   local result = json:match('"result"%s*:%s*"([^"]*)"')
   if result ~= nil and result ~= "OK" then
      die("派生 manifest 失败：%s", json)
   end
   return json
end

local function status_value(con, name)
   local _, value = con:query_row(
      "SHOW GLOBAL STATUS LIKE '" .. name .. "'")
   return value
end

--
-- preview：在预演实例上执行 DDL 并派生目标 manifest
--

function cmd_preview()
   local db, tables = require_schema_args()
   local con = connect()

   if not sysbench.opt.skip_ddl then
      local ddl = get_ddl()
      con:query(ddl)
      print("raft_schema.ddl_executed=true")
   else
      print("raft_schema.ddl_executed=false (--skip-ddl)")
   end

   local json = derive_manifest(con, db, tables)
   print("raft_schema.schema_db=" .. db)
   print("raft_schema.schema_tables=" .. tables)
   print("raft_schema.fingerprint=" .. json_string(json, "fingerprint"))
   print("raft_schema.manifest_hex=" .. json_string(json, "manifest_hex"))
end

--
-- propose：对 Leader 提案，并（可选）等待本机应用完成
--

function cmd_propose()
   local db, tables = require_schema_args()
   local hex = sysbench.opt.manifest_hex
   if hex == "" then
      die("必须给 --manifest-hex（由 preview 命令打印）")
   end
   local ddl = get_ddl()
   local con = connect()

   local epoch_before = status_value(con, "Percona_raft_schema_epoch")

   local json = con:query_row(string.format(
      "SELECT PERCONA_RAFT_SCHEMA_TRANSITION(%s, %s)", sql_quote(ddl),
      sql_quote(hex)))
   if json == nil then
      die("PERCONA_RAFT_SCHEMA_TRANSITION() 没有返回结果")
   end

   local result = json_string(json, "result")
   print("raft_schema.result=" .. result)
   print("raft_schema.from_epoch=" .. json_scalar(json, "from_epoch"))
   print("raft_schema.to_epoch=" .. json_scalar(json, "to_epoch"))
   print("raft_schema.index=" .. json_scalar(json, "index"))
   print("raft_schema.ddl_executed=" .. tostring(json_bool(json, "ddl_executed")))
   print("raft_schema.detail=" .. json_string(json, "detail"))
   print("raft_schema.epoch_before=" .. tostring(epoch_before))

   if result ~= "APPLIED" then
      -- REFUSED/INVALID/TIMEOUT/UNAVAILABLE 都是"没提案或没生效"，
      -- detail 里是具名原因（例如 "a CREATE TABLE schema transition must add
      -- exactly one table"），直接把 detail 交给调用方，不再自己解释。
      die("提案未成功：result=%s detail=%s", result,
          json_string(json, "detail"))
   end

   -- 本机校验：epoch 推进到 to_epoch，且本机自派生的指纹 == 目标指纹
   local to_epoch = json_scalar(json, "to_epoch")
   local expect_fp = sysbench.opt.expect_fingerprint

   if sysbench.opt.wait_applied then
      local deadline = os.time() + sysbench.opt.wait_timeout
      while true do
         local now_epoch = status_value(con, "Percona_raft_schema_epoch")
         if tostring(now_epoch) == tostring(to_epoch) then
            break
         end
         if os.time() > deadline then
            die("等待本机应用超时（%d s）：schema_epoch 仍是 %s，期望 %s",
                sysbench.opt.wait_timeout, tostring(now_epoch),
                tostring(to_epoch))
         end
         sleep_seconds(0.2)
      end
   end

   local epoch_after = status_value(con, "Percona_raft_schema_epoch")
   print("raft_schema.epoch_after=" .. tostring(epoch_after))
   if tostring(epoch_after) ~= tostring(to_epoch) then
      die("epoch 没有推进到 %s（本机读到 %s）", tostring(to_epoch),
          tostring(epoch_after))
   end

   local json_after = derive_manifest(con, db, tables)
   local fp_after = json_string(json_after, "fingerprint")
   print("raft_schema.local_fingerprint=" .. fp_after)
   if expect_fp ~= "" and fp_after ~= expect_fp then
      die("本机派生指纹与目标不一致：期望 %s，实得 %s", expect_fp, fp_after)
   end
   print("raft_schema.propose=PASS")
end

--
-- verify：在一台上核对 epoch 与本机自派生指纹（wrapper 会对每个成员各跑一次）
--

function cmd_verify()
   local db, tables = require_schema_args()
   local con = connect()

   local epoch = status_value(con, "Percona_raft_schema_epoch")
   local json = derive_manifest(con, db, tables)
   local fp = json_string(json, "fingerprint")

   print("raft_schema.schema_epoch=" .. tostring(epoch))
   print("raft_schema.local_fingerprint=" .. fp)

   local expect_fp = sysbench.opt.expect_fingerprint
   if expect_fp ~= "" and fp ~= expect_fp then
      die("指纹不一致：期望 %s，实得 %s", expect_fp, fp)
   end
   print("raft_schema.verify=PASS")
end

--
-- fingerprint：只派生并打印指纹（wrapper 用它做"预演实例 vs 集群在 force"的预检）
--

function cmd_fingerprint()
   local db, tables = require_schema_args()
   local con = connect()
   local json = derive_manifest(con, db, tables)
   print("raft_schema.schema_tables=" .. tables)
   print("raft_schema.fingerprint=" .. json_string(json, "fingerprint"))
   print("raft_schema.schema_epoch=" ..
         tostring(status_value(con, "Percona_raft_schema_epoch")))
end

sysbench.cmdline.commands = {
   preview = {cmd_preview},
   propose = {cmd_propose},
   verify = {cmd_verify},
   fingerprint = {cmd_fingerprint},
}
