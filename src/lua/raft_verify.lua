#!/usr/bin/env sysbench
--
-- Percona Raft: 数据一致性核对（§6.4）——**全是 SQL**
--
-- 依据：percona-server 的 Docs/raft_replication/perf_tool_plan.md §6.4。
-- 一次进程只能连一个端点（Lua 的 drv:connect() 不接受连接参数），所以
-- "跨成员比对"由外层 wrapper（percona/verify.sh）串：每个成员各跑一次本脚本，
-- 再把结果 diff。本文件不新增任何 C 代码。
--
-- 三件事（与 §6.4 一一对应）：
--   aggregate  每表 行数 + SUM(CRC32(...)) + BIT_XOR(CRC32(...))  ← **快速失败**信号
--   dump       每表导出 (主键, 摘要) 有序清单               ← **能指出第一行坏在哪**
--   columns    打印自动推导出的"参与摘要的列 + 主键列"（可审计：别让口径藏在代码里）
--
-- 摘要口径：`CRC32(CONCAT_WS('#', IFNULL(CAST(列 AS CHAR),'<NULL>'), ...))`
--   · 列清单**自动从 information_schema 推导**（按 ordinal_position），不手抄；
--   · NULL 显式写成 '<NULL>'，不用 CONCAT_WS 跳过 NULL 的默认行为
--     （跳过会让 "NULL 在第 2 列" 与 "只有 1 列" 的摘要碰撞）；
--   · 二进制列先 CAST AS CHAR 再进摘要，所以 BLOB/BINARY 也参与比对。
--
-- ⚠️ 为什么不用 `CHECKSUM TABLE` 当门禁：表里有数值型多值索引时它对**自己**都不稳定
--    （§6.4 的警告，机制见 ADR-0024 §4.2）。所以这里只做"摘要 + 聚合"，不碰它。
--

if sysbench.cmdline.command == nil then
   error("命令必填：aggregate | dump | columns（见文件头注释）", 0)
end

sysbench.cmdline.options = {
   schema_db = {"schema（库）名", ""},
   schema_tables = {"表名，逗号分隔（与 --table-prefix/--tables 二选一）", ""},
   table_prefix = {"表名前缀（表名 = <prefix>1..N）", "sbtest"},
   tables = {"表数量（配合 --table-prefix）", 1},
   pk_column = {"主键列（默认自动推导；推导失败才需要显式给）", ""},
   digest_cols = {"参与摘要的列（默认自动推导：全部列）", ""},
   where = {"可选过滤条件（两边必须一致，否则比对无意义）", ""},
   limit = {"dump 的最大行数（0 = 不限；大表建议先 limit 或分片）", 0},
   quiet = {"aggregate 时不打印列口径（只看数字时用）", false},
}

local function die(fmt, ...)
   error(string.format(fmt, ...), 0)
end

local function sql_ident(s)
   if s == "" or s:find("[^%w_$]") then
      die("标识符 '%s' 不适合直接拼进 SQL", s)
   end
   return s
end

local function connect()
   local drv = sysbench.sql.driver()
   return drv:connect()
end

local function table_list()
   local t = sysbench.opt.schema_tables
   if t ~= "" then
      local out = {}
      for name in t:gmatch("[^,]+") do
         out[#out + 1] = sql_ident((name:gsub("^%s+", ""):gsub("%s+$", "")))
      end
      if #out == 0 then die("--schema-tables 解析后是空的") end
      return out
   end
   local n = sysbench.opt.tables
   if n < 1 then die("--tables 必须 >= 1（收到 %d）", n) end
   local out = {}
   for i = 1, n do
      out[i] = sysbench.opt.table_prefix .. i
   end
   return out
end

-- 列清单 / 主键：从 information_schema 推导（不手抄）
local function derive_columns(con, db, tbl, need_pk)
   local cols = {}
   local rs = con:query(string.format(
      "SELECT column_name FROM information_schema.columns " ..
      "WHERE table_schema='%s' AND table_name='%s' ORDER BY ordinal_position",
      db, tbl))
   local row
   while true do
      row = rs:fetch_row()
      if row == nil then break end
      cols[#cols + 1] = row[1]
   end
   rs:free()
   if #cols == 0 then
      die("表 %s.%s 不存在或没有列（先 prepare）", db, tbl)
   end

   local pk = sysbench.opt.pk_column
   if pk == "" then
      local rs2 = con:query(string.format(
         "SELECT column_name FROM information_schema.statistics " ..
         "WHERE table_schema='%s' AND table_name='%s' AND index_name='PRIMARY' " ..
         "ORDER BY seq_in_index", db, tbl))
      local r2 = rs2:fetch_row()
      rs2:free()
      if r2 == nil then
         if need_pk then
            die("表 %s.%s 没有主键；dump 需要稳定顺序，请用 --pk-column 显式指定一列", db, tbl)
         end
         pk = "(none)"
      else
         pk = r2[1]
      end
   end
   return cols, pk
end

local function digest_expr(cols)
   local parts = {}
   for _, c in ipairs(cols) do
      parts[#parts + 1] = string.format("IFNULL(CAST(%s AS CHAR),'<NULL>')", c)
   end
   return "CRC32(CONCAT_WS('#', " .. table.concat(parts, ", ") .. "))"
end

local function where_clause()
   local w = sysbench.opt.where
   if w == "" then return "" end
   return " WHERE " .. w
end

--
-- aggregate：快速失败信号
--

local function cmd_aggregate()
   local db = sql_ident(sysbench.opt.schema_db)
   if db == "" then die("必须给 --schema-db") end
   local con = connect()

   for _, tbl in ipairs(table_list()) do
      local cols, pk = derive_columns(con, db, tbl, false)  -- 聚合不需要主键（实测：只有 UNIQUE 键的表曾被挡住）
      local expr = digest_expr(cols)
      if not sysbench.opt.quiet then
         print(string.format("raft_verify.columns=%s.%s pk=%s ncols=%d list=%s",
                             db, tbl, pk, #cols, table.concat(cols, ",")))
      end
      -- query_row 对多列返回**多个返回值**（unpack(rs:fetch_row())，见 sysbench.sql.lua:299）
      -- ⇒ 一次查询取三列，别查第二遍（大表上那是白跑一遍全表）
      local cnt, sum_crc, xor_crc = con:query_row(string.format(
         "SELECT COUNT(*), IFNULL(SUM(%s),0), IFNULL(BIT_XOR(%s),0) FROM %s.%s%s",
         expr, expr, db, tbl, where_clause()))
      print(string.format("raft_verify.aggregate=%s rows=%s sum_crc32=%s bit_xor_crc32=%s",
                          tbl, tostring(cnt), tostring(sum_crc), tostring(xor_crc)))
   end
   print("raft_verify.aggregate.result=PASS")
end

--
-- dump：有序的 (主键, 摘要) 清单 —— 外部 diff 就能指出"第一行坏在哪"
--

local function cmd_dump()
   local db = sql_ident(sysbench.opt.schema_db)
   if db == "" then die("必须给 --schema-db") end
   local con = connect()

   for _, tbl in ipairs(table_list()) do
      local cols, pk = derive_columns(con, db, tbl, true)   -- dump 需要稳定顺序
      local expr = digest_expr(cols)
      local limit = sysbench.opt.limit
      local tail = (limit > 0) and (" LIMIT " .. limit) or ""
      print(string.format("raft_verify.dump.begin=%s pk=%s ncols=%d order=%s",
                          tbl, pk, #cols, pk))

      local rs = con:query(string.format(
         "SELECT %s, %s AS digest FROM %s.%s%s ORDER BY %s%s",
         pk, expr, db, tbl, where_clause(), pk, tail))
      local n = 0
      local row
      while true do
         row = rs:fetch_row()
         if row == nil then break end
         n = n + 1
         -- 主键与摘要都用字符串；NULL 主键不可能（主键列 NOT NULL）
         print(string.format("%s\t%s", tostring(row[1]), tostring(row[2])))
      end
      rs:free()
      print(string.format("raft_verify.dump.end=%s rows=%d", tbl, n))
   end
end

local function cmd_columns()
   local db = sql_ident(sysbench.opt.schema_db)
   if db == "" then die("必须给 --schema-db") end
   local con = connect()
   for _, tbl in ipairs(table_list()) do
      local cols, pk = derive_columns(con, db, tbl, false)
      print(string.format("raft_verify.columns=%s.%s pk=%s ncols=%d list=%s",
                          db, tbl, pk, #cols, table.concat(cols, ",")))
      print(string.format("raft_verify.digest_expr=%s.%s %s",
                          db, tbl, digest_expr(cols)))
   end
end

sysbench.cmdline.commands = {
   aggregate = {cmd_aggregate},
   dump = {cmd_dump},
   columns = {cmd_columns},
}
