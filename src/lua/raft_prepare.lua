#!/usr/bin/env sysbench
--
-- Percona Raft: schema 生成 + 灌数 + **行长回读校验**
--
-- 依据：percona-server 的 Docs/raft_replication/perf_tool_plan.md §4（参数）、§5（平均行长口径）。
-- 本文件只发 SQL，不起实例、不管集群（工具是纯客户端）。
--
-- 用法（注意 **两个** 库名参数：`--mysql-db` 是"连哪个库"，sysbench 默认连 `sbtest`，
-- 所以必须与 `--schema-db` 一起给，否则会以 `Unknown database 'sbtest'` 失败）：
--   ./src/sysbench ./src/lua/raft_prepare.lua --schema-db=perf --mysql-db=perf \
--       --mysql-host=H --mysql-port=P --mysql-user=U --tables=4 --table-size=100000 \
--       --row-length=400 prepare
--
-- 命令：
--   ddl       只打印将要执行的 DDL（不连库、不执行）——先看再跑
--   prepare   建表（--skip-ddl 可跳过）+ 批量灌数 + **自动跑行长回读**（不通过即失败）
--   rowlen    只跑行长回读（对已灌好的表核对）
--
-- §5 的口径（只有"平均行长"一档）：
--   B = **列数据字节数**的平均值（定宽列按声明宽度 + 变长列按 LENGTH()），
--   **不含** InnoDB 行头/记录头/页开销；information_schema 的 AVG_ROW_LENGTH 是引擎口径，
--   只作旁证、**不与 B 做等值断言**。
--   逐行长度允许有分布（真实业务行就是这样）。为了"平均值**恰好**命中 B"，
--   本生成器让填充列的长度在一个**均值等于目标**的三元多重集里循环
--   （t-w, t, t+w 各一份 ⇒ 均值 = t），w = min(0.3t, n-t)，所以不会因为上界截断而偏低。
--   填充内容是 ASCII（1 字符 = 1 字节）：utf8mb4 多字节内容会让"字符数 ≠ 字节数"，
--   而算错的行长看不出来（AVG 只是偏了）。
--

if sysbench.cmdline.command == nil then
   error("命令必填：ddl | prepare | rowlen（见文件头注释）", 0)
end

sysbench.cmdline.options = {
   schema_db = {"schema（库）名", ""},
   table_prefix = {"表名前缀（表名 = <prefix>1..N）", "sbtest"},
   tables = {"建几张表（与 sysbench 的 --tables 同义）", 1},
   table_size = {"每张表多少行", 10000},
   row_length = {"目标**平均**行长 B（列数据字节数，§5）；0 = 只用必需列", 0},
   types = {"列类型档：builtin（少量常见列）| all（§4.1.1 全类型表）", "builtin"},
   index_profile = {"键结构：pk | pk_secondary", "pk"},
   seed = {"数据生成种子（两侧必须同一个，§4.4）", 42},
   skip_ddl = {"跳过建表（表已存在/已由 schema transition 建好时用）", false},
   batch = {"每条 INSERT 多少行（会再按字节数收敛）", 500},
   rowlen_tolerance = {"行长回读容差（百分比，§5 默认 ±10）", 10},
   quiet_ddl = {"prepare 时不打印 DDL", false},
}

--
-- 小工具
--

local function die(fmt, ...)
   error(string.format(fmt, ...), 0)
end

local function sql_ident(s)
   if s == "" or s:find("[^%w_$]") then
      die("标识符 '%s' 不适合直接拼进 SQL（只允许字母数字与 _ $）", s)
   end
   return s
end

-- 确定性 LCG：同一个 seed ⇒ 两侧同样的字节（不依赖 sysbench 的 rand，跨版本可复现）
local function make_rng(seed)
   local state = (seed % 2147483647)
   if state <= 0 then state = state + 2147483646 end
   return function()
      state = (state * 16807) % 2147483647
      return state
   end
end

local ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"

-- 造一个"看起来像业务数据"的 ASCII 串，长度恰好 len（1 字符 = 1 字节）
local function fill_string(rng, len, tag)
   if len <= 0 then return "" end
   local parts = {}
   local head = tag .. "-"
   if #head >= len then return head:sub(1, len) end
   parts[#parts + 1] = head
   local remain = len - #head
   while remain > 0 do
      local chunk = math.min(remain, 16)
      local buf = {}
      for i = 1, chunk do
         local r = rng()
         buf[i] = ALPHABET:sub((r % #ALPHABET) + 1, (r % #ALPHABET) + 1)
      end
      parts[#parts + 1] = table.concat(buf)
      remain = remain - chunk
   end
   return table.concat(parts)
end

--
-- 表形状
--

-- 定宽列的字节宽度（MySQL 存储宽度）
local FIXED_BYTES = {
   id = 4,     -- INT
   k = 4,      -- INT
}

-- 必需列（业务列）+ 填充列。返回：列定义串数组、定宽总字节、填充列、索引串、目标内容长度
--
-- 宽度怎么定（§5）：判据是 **AVG(列数据字节数)**，而定宽列按声明宽度算、变长列按 LENGTH() 算，
-- 所以"填充列的内容长度"要正好等于 B - 定宽字节数，才能让 AVG **恰好**命中 B。
-- VARCHAR 的容量必须**大于**内容长度：一是要留出分布（§5 要求 min/p50/p99/max），
-- 二是 VARCHAR 的长度前缀（1 或 2 字节）不进 LENGTH()，挤占容量就会让内容被迫截短、AVG 偏低。
-- 先前把容量算成"剩余 - 前缀"⇒ 目标内容长度被容量卡住、分布塌成单值、AVG 差 2 字节，已修。
local function table_shape(b, profile)
   local cols, fixed = {}, 0
   local fill = {}
   local target = 0

   cols[#cols + 1] = "id INT NOT NULL"
   fixed = fixed + FIXED_BYTES.id
   cols[#cols + 1] = "k INT NOT NULL"
   fixed = fixed + FIXED_BYTES.k

   if b > 0 then
      target = b - fixed
      if target < 2 then
         die("目标行长 B=%d 太小：必需列（id INT + k INT）已经要 %d 字节，" ..
             "填充列至少还要 2 字节内容才能形成分布。最小可行 B = %d",
             b, fixed, fixed + 2)
      end
      -- 容量 = 内容目标 + 余量（余量给分布用；0.3t 是 §5 那个 ±30% 分布的宽度）
      local headroom = math.max(16, math.floor(target * 0.3))
      local n = target + headroom
      cols[#cols + 1] = string.format("c VARCHAR(%d) NOT NULL", n)
      fill[#fill + 1] = {name = "c", capacity = n, prefix = (n < 256) and 1 or 2,
                         target = target}
   end

   local idx = "PRIMARY KEY (id)"
   if profile == "pk_secondary" then
      idx = idx .. ", KEY k_k (k)"
   elseif profile ~= "pk" then
      die("--index-profile 只支持 pk | pk_secondary（收到 '%s'）", profile)
   end

   return cols, fixed, fill, idx, target
end

-- 列类型档：目前只有 builtin 落地。**不做静默降级**——all（§4.1.1 全类型表）是 M2 的范围
-- （方案 §10 的 M2 行：多表 + 全类型矩阵 21 种 / 31 列 + 行长分布）。
local function check_types()
   local t = sysbench.opt.types
   if t == "builtin" then return end
   if t == "all" then
      die("--types=all（§4.1.1 全类型表，31 列）尚未落地，属 M2 范围；" ..
          "现在只支持 --types=builtin。**不静默降级成 builtin**：" ..
          "用错了档却拿到一张小表，会让之后的对比全都不可比。")
   end
   die("--types 只支持 builtin | all（收到 '%s'）", t)
end

local function table_ddl(db, name, cols, idx)
   return string.format("CREATE TABLE %s.%s (%s, %s) ENGINE=InnoDB",
                        db, name, table.concat(cols, ", "), idx)
end

local function table_names()
   local n = sysbench.opt.tables
   if n < 1 then die("--tables 必须 >= 1（收到 %d）", n) end
   local t = {}
   for i = 1, n do
      t[i] = sysbench.opt.table_prefix .. i
   end
   return t
end

--
-- 行长算法（§5）
--

-- 返回：每行的填充列长度序列（均值**恰好** = 目标），以及目标填充长度 t
local function fill_lengths(target, capacity)
   if target <= 0 then return {0}, 0 end
   local t = math.min(target, capacity)
   local w = math.floor(t * 0.3)
   if w > capacity - t then w = capacity - t end
   if w < 0 then w = 0 end
   if w == 0 then
      return {t}, t                  -- 没法铺开分布（比如容量刚好等于目标）
   end
   -- 三元多重集，均值 = t（不是"近似"，是恰好）
   return {t - w, t, t + w}, t
end

--
-- 连接
--

local function connect()
   local drv = sysbench.sql.driver()
   return drv:connect()
end

-- 批大小：既看 --batch，也按字节数收敛（别撞 max_allowed_packet）
local function effective_batch(row_bytes)
   local b = sysbench.opt.batch
   if b < 1 then die("--batch 必须 >= 1（收到 %d）", b) end
   local by_bytes = math.floor(1000000 / (row_bytes + 32))
   if by_bytes < 1 then by_bytes = 1 end
   if b > by_bytes then return by_bytes end
   return b
end

local function load_table(con, db, name, fixed, fill, row_bytes)
   local rng = make_rng(sysbench.opt.seed)
   local total = sysbench.opt.table_size
   local batch = effective_batch(row_bytes)
   local inserted = 0

   -- 填充长度序列与目标
   local lens, target = fill_lengths((fill[1] ~= nil) and fill[1].target or 0,
                                     (fill[1] ~= nil) and fill[1].capacity or 0)

   while inserted < total do
      local rows = {}
      local n = math.min(batch, total - inserted)
      for i = 1, n do
         local id = inserted + i
         local vals = {tostring(id), tostring((id * 7) % 100000)}
         if fill[1] ~= nil then
            local len = lens[((id - 1) % #lens) + 1]
            vals[#vals + 1] = "'" .. fill_string(rng, len, "r" .. id) .. "'"
         end
         rows[i] = "(" .. table.concat(vals, ",") .. ")"
      end
      local sql = string.format("INSERT INTO %s.%s (%s) VALUES %s",
                                db, name,
                                (fill[1] ~= nil) and "id,k,c" or "id,k",
                                table.concat(rows, ","))
      con:query(sql)
      inserted = inserted + n
   end

   return inserted, target, #lens
end

--
-- 行长回读（§5：prepare 结束自动跑；不通过即失败）
--

local function row_bytes_expr(fixed, fill)
   local parts = {tostring(fixed)}
   for _, f in ipairs(fill) do
      parts[#parts + 1] = string.format("LENGTH(%s)", f.name)
   end
   return table.concat(parts, " + ")
end

local function percentile(con, db, name, expr, p, total)
   if total <= 0 then return 0 end
   local off = math.floor((total - 1) * p / 100)
   local v = con:query_row(string.format(
      "SELECT %s FROM %s.%s ORDER BY %s LIMIT 1 OFFSET %d",
      expr, db, name, expr, off))
   return tonumber(v) or 0
end

local function check_rowlen(con, db, names, fixed, fill, b)
   local expr = row_bytes_expr(fixed, fill)
   local tolerance = sysbench.opt.rowlen_tolerance
   local failures = 0

   print(string.format("raft_prepare.row_bytes_expr=%s", expr))
   print(string.format("raft_prepare.row_bytes_fixed=%d", fixed))

   for _, name in ipairs(names) do
      local total = tonumber(con:query_row(string.format(
         "SELECT COUNT(*) FROM %s.%s", db, name))) or 0
      if total == 0 then
         die("表 %s.%s 是空的，行长回读无从谈起（先 prepare）", db, name)
      end

      local avg, min, max, p50, p99
      if #fill == 0 then
         -- 没有填充列 ⇒ 表达式是常量（每行都一样）。不能拿常量去 ORDER BY：
         -- MySQL 会把 "ORDER BY 8" 读成列序号（实测 1054 Unknown column '8'）。
         avg, min, max, p50, p99 = fixed, fixed, fixed, fixed, fixed
      else
         avg = tonumber(con:query_row(string.format(
            "SELECT AVG(%s) FROM %s.%s", expr, db, name))) or 0
         min = tonumber(con:query_row(string.format(
            "SELECT MIN(%s) FROM %s.%s", expr, db, name))) or 0
         max = tonumber(con:query_row(string.format(
            "SELECT MAX(%s) FROM %s.%s", expr, db, name))) or 0
         p50 = percentile(con, db, name, expr, 50, total)
         p99 = percentile(con, db, name, expr, 99, total)
      end

      -- 旁证（引擎口径，**不作等值断言**，§5）
      local engine = con:query_row(string.format(
         "SELECT AVG_ROW_LENGTH FROM information_schema.TABLES " ..
         "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='%s'",
         string.format("%q", db), name))
      engine = tonumber(engine) or 0

      -- 引擎口径只作旁证（§5）：它含行头等开销、且**统计刷新前会偏小**，绝不与 B 做等值断言
      print(string.format(
         "raft_prepare.rowlen=%s rows=%d avg=%.1f min=%.0f p50=%.0f p99=%.0f max=%.0f " ..
         "target=%d engine_avg_row_length=%d(引擎口径,仅旁证,统计刷新前偏小)",
         name, total, avg, min, p50, p99, max, b, engine))

      if b > 0 then
         local dev = math.abs(avg - b) / b * 100
         if dev > tolerance then
            failures = failures + 1
            print(string.format(
               "raft_prepare.rowlen.FAIL=%s avg=%.1f target=%d deviation=%.2f%% > %d%%",
               name, avg, b, dev, tolerance))
         else
            print(string.format("raft_prepare.rowlen.PASS=%s deviation=%.2f%%",
                                name, dev))
         end
      end
   end

   if failures > 0 then
      die("行长回读不通过：%d 张表超出 ±%d%%（§5 要求**不静默接受**）",
          failures, tolerance)
   end
end

--
-- 命令
--

local function cmd_ddl()
   local db = sql_ident(sysbench.opt.schema_db)
   if db == "" then die("必须给 --schema-db") end
   local b = sysbench.opt.row_length
   check_types()
   local cols, fixed, fill, idx, target = table_shape(b, sysbench.opt.index_profile)
   for _, name in ipairs(table_names()) do
      print(string.format("raft_prepare.ddl=%s", table_ddl(db, name, cols, idx)))
   end
   print(string.format("raft_prepare.row_bytes_fixed=%d", fixed))
   if b > 0 then
      local lens, t = fill_lengths(target, fill[1].capacity)
      print(string.format("raft_prepare.fill_capacity=%d fill_target=%d fill_multiset=%s",
                          fill[1].capacity, t, table.concat(lens, ",")))
      print(string.format("raft_prepare.avg_row_bytes_expected=%d", fixed + t))
   end
end

local function cmd_prepare()
   local db = sql_ident(sysbench.opt.schema_db)
   if db == "" then die("必须给 --schema-db") end
   check_types()
   local b = sysbench.opt.row_length
   local names = table_names()
   local cols, fixed, fill, idx = table_shape(b, sysbench.opt.index_profile)
   local row_bytes = fixed + ((fill[1] ~= nil) and fill[1].capacity or 0)

   local con = connect()

   if not sysbench.opt.quiet_ddl then
      for _, name in ipairs(names) do
         print(string.format("raft_prepare.ddl=%s", table_ddl(db, name, cols, idx)))
      end
   end

   if not sysbench.opt.skip_ddl then
      for _, name in ipairs(names) do
         con:query(table_ddl(db, name, cols, idx))
         print(string.format("raft_prepare.created=%s", name))
      end
   end

   for _, name in ipairs(names) do
      local inserted = load_table(con, db, name, fixed, fill, row_bytes)
      print(string.format("raft_prepare.loaded=%s rows=%d", name, inserted))
   end

   check_rowlen(con, db, names, fixed, fill, b)
end

local function cmd_rowlen()
   local db = sql_ident(sysbench.opt.schema_db)
   if db == "" then die("必须给 --schema-db") end
   check_types()
   local b = sysbench.opt.row_length
   local cols, fixed, fill, idx = table_shape(b, sysbench.opt.index_profile)
   check_rowlen(connect(), db, table_names(), fixed, fill, b)
end

--
-- 命令注册：必须走 sysbench.cmdline.commands（**不要**在脚本加载时直接分发）。
-- 为什么：`sysbench.opt` 是在 sysbench 调自定义命令**之前**才由 export_options()
-- 填好的（sb_lua.c 的 call_custom_command），脚本加载阶段它是 nil。
--
sysbench.cmdline.commands = {
   ddl = {cmd_ddl},
   prepare = {cmd_prepare},
   rowlen = {cmd_rowlen},
}
