#!/usr/bin/env sysbench
--
-- Percona Raft: 工具与服务器的接口自检（doctor）+ golden vector 的生成
--
-- 依据：percona-server 的 Docs/raft_replication/perf_tool_plan.md §3.3（doctor 的 7 条）
-- 与 §8.4（golden vector）。
--
-- 为什么要有它：工具是**独立仓库**，服务器改了状态量名/换了 manifest 编码版本，
-- 工具不会自动知道。doctor 是唯一能在"跑压测之前"而不是"跑到一半"发现的地方。
--
-- 命令（每条命令只连一个端点——Lua 层做不到跨端点，端点由 percona/doctor.sh 串）：
--   doctor         对**目标**跑检查 1/2/4/5/6/7；任一 FAIL 即非零退出
--   golden-check   对**预演实例**跑检查 3（它要建表，所以必须 Raft 关闭）
--   freeze-golden  在**预演实例**上建三张哨兵表并冻结 manifest_hex / fingerprint 到 --golden-dir
--
-- golden vector 为什么要"预演实例"：manifest 只能从**活的数据字典**派生，
-- 而受管集群上直接 DDL 会被拒（8109）。所以检查第 3 条需要一个 Raft 关闭的实例。
--

if sysbench.cmdline.command == nil then
   error("命令必填：doctor | freeze-golden", 0)
end

-- golden vector 用的 schema 名是**规范的一部分**（manifest 里含 schema 名），
-- 所以它是常量：freeze 与 check 必须用同一个名字。
local GOLDEN_SCHEMA = "perf_doctor_golden"

sysbench.cmdline.options = {
   golden_dir = {"golden vector 目录", "./schemas/golden"},
   preview_host = {"预演实例 host（Raft 关闭、可建表；用于第 3 条 golden 检查）", ""},
   preview_port = {"预演实例 port（>0 才跑第 3 条）", 0},
   preview_user = {"预演实例 user", "root"},
   require_raft = {"要求目标必须是我们的构建（缺 raft 面即 FAIL）", false},
}

local ffi = require("ffi")

local function die(fmt, ...)
   error(string.format(fmt, ...), 0)
end

local function connect(host, port, user)
   local drv = sysbench.sql.driver()
   -- 说明：Lua 层无法按连接指定端点（连接参数来自全局 --mysql-*），
   --      所以"预演实例"这一项由 wrapper 另起一次进程完成，见 percona/doctor.sh。
   if host ~= nil and host ~= "" then
      die("本命令不支持跨端点连接（见文件头）；请用 percona/doctor.sh")
   end
   return drv:connect()
end

local function sql_quote(s)
   return "'" .. s:gsub("\\", "\\\\"):gsub("'", "''") .. "'"
end

local function json_string(json, key)
   local v = json:match('"' .. key .. '"%s*:%s*"([^"]*)"')
   if v == nil then
      die("无法从 JSON 里取字段 '%s'：%s", key, json)
   end
   return v
end

local function derive_manifest(con, schema, tables)
   local json = con:query_row(string.format(
      "SELECT PERCONA_RAFT_SCHEMA_MANIFEST(%s, %s)", sql_quote(schema),
      sql_quote(tables)))
   if json == nil then
      die("PERCONA_RAFT_SCHEMA_MANIFEST() 没有返回结果")
   end
   return json
end

-- 所有 Percona_raft* 状态量 → map（一次查询）
local function status_var_map(con)
   local map = {}
   local rs = con:query("SHOW GLOBAL STATUS LIKE 'Percona\\_raft%'")
   if rs == nil then
      return map
   end
   for _ = 1, rs.nrows do
      local row = rs:fetch_row()
      if row ~= nil then
         map[row[1]] = row[2]
      end
   end
   return map
end

local function variable_map(con, like)
   local map = {}
   local rs = con:query("SHOW GLOBAL VARIABLES LIKE '" .. like .. "'")
   if rs == nil then
      return map
   end
   for _ = 1, rs.nrows do
      local row = rs:fetch_row()
      if row ~= nil then
         map[row[1]] = row[2]
      end
   end
   return map
end

--
-- 四张哨兵表（§8.4）：全类型（两份）/ 索引形状 / 行长
-- 不含地理类型、分区、临时表、触发器、外键。
-- 订正（2026-10-09，v6）：先前这里写着"不能带列级 DEFAULT（实测被具名拒 UNSUPPORTED_CLAUSE）"，
-- **那是 v5 的事实**；manifest v6 已把列级 DEFAULT 绑进证明机制（`fcdd7563aaa`），
-- 分类器也不再按名拒（`ef2f735da32`）。实测：带 DEFAULT '0' 的 stock 形状 CREATE TABLE
-- 经 SCHEMA_TRANSITION 是 APPLIED 且三台 DEFAULT 保留。所以这条限制不再存在——
-- 但哨兵表的 DDL 保持不变：它们的作用是**冻结字节**，改 DDL 就该重新冻结一次（别偷偷改）。
--

local GOLDEN = {
   {
      name = "alltypes",
      note = "除地理类型外的全部列类型（整数五档含 unsigned、浮点、DECIMAL、时间、字符/二进制、TEXT/BLOB 家族、JSON、BIT、ENUM、SET）",
      ddl = [[CREATE TABLE %SCHEMA%.alltypes (
  c_tiny TINYINT NOT NULL,
  c_utiny TINYINT UNSIGNED NOT NULL,
  c_short SMALLINT NOT NULL,
  c_ushort SMALLINT UNSIGNED NOT NULL,
  c_int24 MEDIUMINT NOT NULL,
  c_uint24 MEDIUMINT UNSIGNED NOT NULL,
  c_long INT NOT NULL,
  c_ulong INT UNSIGNED NOT NULL,
  c_longlong BIGINT NOT NULL,
  c_ulonglong BIGINT UNSIGNED NOT NULL,
  c_float FLOAT NOT NULL,
  c_double DOUBLE NOT NULL,
  c_dec DECIMAL(18,6) NOT NULL,
  c_date DATE NOT NULL,
  c_year YEAR NOT NULL,
  c_time TIME NOT NULL,
  c_dt DATETIME(3) NOT NULL,
  c_ts TIMESTAMP(6) NULL,
  c_char CHAR(10) NOT NULL,
  c_bin BINARY(8) NOT NULL,
  c_vchar VARCHAR(64) NOT NULL,
  c_vbin VARBINARY(64) NOT NULL,
  c_text TEXT NOT NULL,
  c_json JSON NOT NULL,
  c_bit BIT(13) NOT NULL,
  c_enum ENUM('a','b') NOT NULL,
  c_set SET('x','y') NOT NULL,
  PRIMARY KEY (c_long)
) ENGINE=InnoDB]],
   },
   {
      name = "idxshapes",
      note = "索引形状：前缀 / 降序 / 函数 / 多值 / INVISIBLE / UNIQUE（ADR-0024 起全部进 manifest）",
      ddl = [[CREATE TABLE %SCHEMA%.idxshapes (
  id INT NOT NULL,
  v VARCHAR(200) NOT NULL,
  k INT NOT NULL,
  j JSON NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_v (v),
  KEY k_prefix (v(10)),
  KEY k_desc (k DESC),
  KEY k_func ((k + 1)),
  KEY k_multi ((CAST(j->>'$.a' AS UNSIGNED ARRAY))),
  KEY k_invisible (v) INVISIBLE
) ENGINE=InnoDB]],
   },
   {
      -- 与上面 27 列的 alltypes 并存：那份是 M0 起就冻结的老哨兵（形状稳定、字节可比），
      -- 这一份是**现场 correctness harness 用的 31 列矩阵**（照 `app.alltypes` 的
      -- SHOW CREATE TABLE 抄下来，§4.1.1）。两份一起验：
      -- 前者盯"编码/metadata 有没有变"，后者盯"我们声明的类型面服务端是否全收"。
      name = "alltypes31",
      note = "现场 31 列全类型矩阵（照 app.alltypes 的真实定义，§4.1.1）",
      ddl = [[CREATE TABLE %SCHEMA%.alltypes31 (
  c_long INT NOT NULL,
  c_tiny TINYINT,
  c_utiny TINYINT UNSIGNED,
  c_short SMALLINT,
  c_int24 MEDIUMINT,
  c_ulong INT UNSIGNED,
  c_longlong BIGINT,
  c_float FLOAT,
  c_double DOUBLE,
  c_dec DECIMAL(18,6),
  c_date DATE,
  c_year YEAR,
  c_ts TIMESTAMP(6) NULL,
  c_dt DATETIME(3),
  c_time TIME(2),
  c_char CHAR(40),
  c_binary BINARY(20),
  c_varchar VARCHAR(300),
  c_varbinary VARBINARY(100),
  c_tinytext TINYTEXT,
  c_text TEXT,
  c_mediumtext MEDIUMTEXT,
  c_longtext LONGTEXT,
  c_tinyblob TINYBLOB,
  c_blob BLOB,
  c_mediumblob MEDIUMBLOB,
  c_longblob LONGBLOB,
  c_json JSON,
  c_bit BIT(13),
  c_enum ENUM('a','b','c'),
  c_set SET('x','y','z'),
  PRIMARY KEY (c_long)
) ENGINE=InnoDB]],
   },
   {
      name = "rowlen",
      note = "行长表（§5 的平均行长口径）：必需列 + VARCHAR 填充列",
      ddl = [[CREATE TABLE %SCHEMA%.rowlen (
  id INT NOT NULL,
  k INT NOT NULL,
  pad VARCHAR(392) NOT NULL,
  PRIMARY KEY (id),
  KEY k_k (k)
) ENGINE=InnoDB]],
   },
}

local function golden_ddl(entry)
   return (entry.ddl:gsub("%%SCHEMA%%", GOLDEN_SCHEMA))
end

--
-- freeze-golden：在预演实例上建表、派生、冻结成文件
--

function cmd_freeze_golden()
   local dir = sysbench.opt.golden_dir
   local con = connect()

   local ok = os.execute("mkdir -p '" .. dir .. "'")
   if ok ~= true and ok ~= 0 then
      die("无法创建目录 %s", dir)
   end

   con:query("CREATE DATABASE IF NOT EXISTS " .. GOLDEN_SCHEMA)
   print("golden.schema=" .. GOLDEN_SCHEMA)

   for _, entry in ipairs(GOLDEN) do
      local ddl = golden_ddl(entry)
      con:query("DROP TABLE IF EXISTS " .. GOLDEN_SCHEMA .. "." .. entry.name)
      con:query(ddl)
      local json = derive_manifest(con, GOLDEN_SCHEMA, entry.name)
      local hex = json_string(json, "manifest_hex")
      local fp = json_string(json, "fingerprint")

      local sf = assert(io.open(dir .. "/" .. entry.name .. ".sql", "w"))
      sf:write("-- " .. entry.note .. "\n" .. ddl .. "\n")
      sf:close()

      local gf = assert(io.open(dir .. "/" .. entry.name .. ".golden", "w"))
      gf:write("# golden vector：DDL 见同名 .sql；期望值由服务端自己的编码器派生\n")
      gf:write("name=" .. entry.name .. "\n")
      gf:write("schema=" .. GOLDEN_SCHEMA .. "\n")
      gf:write("tables=" .. entry.name .. "\n")
      gf:write("fingerprint=" .. fp .. "\n")
      gf:write("manifest_hex=" .. hex .. "\n")
      gf:close()

      print(string.format("golden.%s.fingerprint=%s", entry.name, fp))
      print(string.format("golden.%s.manifest_bytes=%d", entry.name,
                          #hex / 2))
   end
   print("golden.freeze=OK")
end

--
-- doctor：7 条检查
--

local REQUIRED_STATUS_VARS = {
   "Percona_raft_serving",
   "Percona_raft_state",
   "Percona_raft_leader_id",
   "Percona_raft_applier_ready",
   "Percona_raft_write_permit_waits",
   "Percona_raft_write_permit_wait_timeouts",
   "Percona_raft_write_permit_wait_us",
   "Percona_raft_admission_success",
   "Percona_raft_wal_append_batches",
   "Percona_raft_wal_append_records",
   "Percona_raft_apply_max_in_flight",
   "Percona_raft_read_index_confirmations",
   "Percona_raft_read_index_fallbacks",
   "Percona_raft_catchup_stall",
   "Percona_raft_serving_gap_open_ms",
   "Percona_raft_last_serving_gap_ms",
   "Percona_raft_unmanaged_write_rejections",
   "Percona_raft_metadata_write_rejections",
   "Percona_raft_schema_epoch",
   "Percona_raft_schema_cutover_waits",
   "Percona_raft_schema_cutover_timeouts",
   "Percona_raft_schema_cutover_active",
}

local REQUIRED_SESSION_VARS = {
   "percona_raft_read_consistency",
   "percona_raft_request_namespace",
   "percona_raft_request_id",
}

local function probe_function(con, expr, label)
   local ok, res = pcall(function()
      return con:query_row("SELECT " .. expr)
   end)
   if ok then
      print(string.format("doctor.function.%s=OK", label))
      return true
   end
   print(string.format("doctor.function.%s=FAIL (%s)", label, tostring(res)))
   return false
end

--
-- golden-check：**在预演实例上**跑（连接的就是 --mysql-* 指的那台，必须 Raft 关闭、可建表）
--

function cmd_golden_check()
   local dir = sysbench.opt.golden_dir
   local con = connect()
   con:query("CREATE DATABASE IF NOT EXISTS " .. GOLDEN_SCHEMA)

   local pass, fail = 0, 0
   for _, entry in ipairs(GOLDEN) do
      local f = io.open(dir .. "/" .. entry.name .. ".golden", "r")
      if f == nil then
         print(string.format("golden.%s=SKIP (找不到 %s/%s.golden)", entry.name, dir,
                             entry.name))
      else
         local expect_hex = nil
         for line in f:lines() do
            local k, v = line:match("^([%w_]+)=(.*)$")
            if k == "manifest_hex" then expect_hex = v end
         end
         f:close()

         -- 用 .sql 里的 DDL 建表：保证"冻结时的 DDL"就是"现在检查的 DDL"
         local ddl = nil
         local sf = io.open(dir .. "/" .. entry.name .. ".sql", "r")
         if sf ~= nil then
            local lines = {}
            for line in sf:lines() do
               if line:sub(1, 2) ~= "--" then table.insert(lines, line) end
            end
            sf:close()
            ddl = table.concat(lines, "\n"):gsub("^%s+", ""):gsub("%s+$", "")
         end

         if expect_hex == nil or ddl == nil then
            print(string.format("golden.%s=FAIL (缺 manifest_hex 或 DDL)", entry.name))
            fail = fail + 1
         else
            con:query("DROP TABLE IF EXISTS " .. GOLDEN_SCHEMA .. "." .. entry.name)
            con:query(ddl)
            local json = derive_manifest(con, GOLDEN_SCHEMA, entry.name)
            local hex = json_string(json, "manifest_hex")
            local fp = json_string(json, "fingerprint")
            if hex == expect_hex then
               print(string.format("golden.%s=PASS (fingerprint=%s, %d 字节)", entry.name,
                                   fp, #hex / 2))
               pass = pass + 1
            else
               print(string.format(
                  "golden.%s=FAIL\n  期望 %s…（%d 字节）\n  实得 %s…（%d 字节）\n" ..
                  "  ⇒ manifest 编码版本或编码器变了（别拿 fingerprint 的 domain 常量当哨兵）",
                  entry.name, expect_hex:sub(1, 28), #expect_hex / 2, hex:sub(1, 28),
                  #hex / 2))
               fail = fail + 1
            end
         end
      end
   end

   print(string.format("golden_check.pass=%d fail=%d", pass, fail))
   if fail > 0 then
      print("golden_check.result=FAIL")
      die("golden vector 比对失败 %d 项", fail)
   end
   print("golden_check.result=PASS")
end

local function cmd_doctor()
   local failures = 0
   local con = connect()

   -- 1) 连得上吗
   local version = con:query_row("SELECT VERSION()")
   print("doctor.check1_connect=PASS")
   print("doctor.server_version=" .. tostring(version))

   -- 2) 版本与 build commit（只记录）
   local comment = con:query_row("SELECT @@version_comment")
   print("doctor.version_comment=" .. tostring(comment))
   print("doctor.tool_version=" .. tostring(sysbench.version))

   -- 能力探测：有没有 raft 面
   local status_vars = status_var_map(con)
   local raft_face = false
   for _ in pairs(status_vars) do raft_face = true break end
   print("doctor.raft_face=" .. (raft_face and "PRESENT" or "ABSENT"))
   if not raft_face and sysbench.opt.require_raft then
      print("doctor.check_raft_face=FAIL (--require-raft 但目标没有 Percona_raft* 状态量)")
      failures = failures + 1
   end

   -- 4) 要用的 SQL 函数在吗（3 条只读探针；没 raft 面时控制类函数返回 UNAVAILABLE 而不报错）
   if raft_face then
      if not probe_function(con, "PERCONA_RAFT_STATUS('doctor','probe')", "STATUS") then
         failures = failures + 1
      end
      -- 注意 arity：GTID_POSITION 是 **1 个参数**（GTID 文本），STATUS 是 2 个
      -- （注册表 sql/item_create.cc:1519-1535）。调错参数个数会拿到 1582。
      if not probe_function(con,
             "PERCONA_RAFT_GTID_POSITION('00000000-0000-0000-0000-000000000000:1')",
             "GTID_POSITION") then
         failures = failures + 1
      end
      if not probe_function(con, "PERCONA_RAFT_RECENT_EVENTS()", "RECENT_EVENTS") then
         failures = failures + 1
      end
   else
      print("doctor.function=SKIP (目标没有 raft 面)")
   end

   -- 5) 要用的会话变量在吗
   local session_vars = variable_map(con, "percona\\_raft%")
   if not raft_face then
      print("doctor.session_vars=SKIP (目标没有 raft 面)")
   else
      local missing = {}
      for _, v in ipairs(REQUIRED_SESSION_VARS) do
         if session_vars[v] == nil then table.insert(missing, v) end
      end
      if #missing > 0 then
         print("doctor.session_vars=FAIL 缺：" .. table.concat(missing, ", "))
         failures = failures + 1
      else
         print(string.format("doctor.session_vars=PASS (%d 个)", #REQUIRED_SESSION_VARS))
      end
   end

   -- 6) 要用的状态量在吗
   if raft_face then
      local found, missing = 0, {}
      for _, v in ipairs(REQUIRED_STATUS_VARS) do
         if status_vars[v] ~= nil then found = found + 1
         else table.insert(missing, v) end
      end
      print(string.format("doctor.status_vars=%d/%d", found, #REQUIRED_STATUS_VARS))
      if found == 0 then
         print("doctor.check6_status_vars=FAIL（清单里的量一个都没采到 ⇒ 清单过期，不是版本缺量）")
         failures = failures + 1
      elseif #missing > 0 then
         print("doctor.status_vars_absent=" .. table.concat(missing, ", "))
         print("doctor.check6_status_vars=WARN（缺的量记为\"该版本无此量\"）")
      else
         print("doctor.check6_status_vars=PASS")
      end
   else
      print("doctor.status_vars=0/22 (目标没有 raft 面)")
   end

   -- 3) golden vector：必须在**预演实例**上跑（受管集群上 DDL 被拒），
   --    所以由 percona/doctor.sh 另起一次进程跑 golden-check
   print("doctor.check3_golden=DELEGATED (percona/doctor.sh 会在预演实例上跑 golden-check)")

   -- 7) 记录环境与参数
   local hostname = con:query_row("SELECT @@hostname")
   local port = con:query_row("SELECT @@port")
   print(string.format("doctor.env=hostname:%s port:%s", tostring(hostname),
                       tostring(port)))
   local params = variable_map(con, "percona\\_raft%")
   local n = 0
   for k, v in pairs(params) do
      print(string.format("doctor.param.%s=%s", k, tostring(v)))
      n = n + 1
   end
   print(string.format("doctor.params_recorded=%d（启动项多数不在 SHOW VARIABLES 里，" ..
                       "读不到的要手工填；见方案 §8.2 参数栏）", n))

   if failures > 0 then
      print(string.format("doctor.result=FAIL (%d 项)", failures))
      die("%d 项检查失败", failures)
   end
   print("doctor.result=PASS")
end

sysbench.cmdline.commands = {
   doctor = {cmd_doctor},
   ["golden-check"] = {cmd_golden_check},
   golden_check = {cmd_golden_check},
   ["freeze-golden"] = {cmd_freeze_golden},
   freeze_golden = {cmd_freeze_golden},
}
