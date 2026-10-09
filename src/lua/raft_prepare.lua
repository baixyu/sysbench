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

-- 定宽列的字节宽度（MySQL 存储宽度；只用于"必需列/数值列"这一部分，
-- 不用于字符/二进制/TEXT/JSON 等——那些一律按 LENGTH() 实测，见 row_bytes_expr）
local FIXED_BYTES = {
   id = 4, k = 4,                                     -- builtin profile
   -- app.alltypes 的 31 列里的定宽部分（照现场那张表的真实定义，见 schemas/golden/alltypes.sql）
   c_long = 4, c_tiny = 1, c_utiny = 1, c_short = 2, c_int24 = 3,
   c_ulong = 4, c_longlong = 8,
   c_float = 4, c_double = 8, c_dec = 9,              -- DECIMAL(18,6) 的 InnoDB 存储宽度
   c_date = 3, c_year = 1, c_ts = 7, c_dt = 7, c_time = 4,
}

-- ---------------------------------------------------------------------------
-- profile：一张表的"形状" = 列定义 + 定宽字节 + 填充列 + 索引 + 逐行取值函数
-- ---------------------------------------------------------------------------

-- 定长 ASCII 内容（1 字符 = 1 字节）：字符/文本列用它，好让"列数据字节数"可预测
local function ascii_of(rng, len, tag)
   local head = tag .. ":"
   if len <= #head then return head:sub(1, len) end
   local out = {head}
   local remain = len - #head
   while remain > 0 do
      local n = math.min(remain, 16)
      local buf = {}
      for i = 1, n do
         local r = rng()
         buf[i] = ALPHABET:sub((r % #ALPHABET) + 1, (r % #ALPHABET) + 1)
      end
      out[#out + 1] = table.concat(buf)
      remain = remain - n
   end
   return table.concat(out)
end

local function hex_of(rng, len, tag)   -- 二进制列：2×len 个十六进制字符
   local a = ascii_of(rng, len, tag)
   return (a:gsub(".", function(c)
      return string.format("%02x", string.byte(c))
   end))
end

-- builtin profile：少量常见列，填充列 c 承载目标行长
local function builtin_shape(b, profile)
   local cols, fixed = {}, 0
   local fill = {}

   cols[#cols + 1] = "id INT NOT NULL"
   fixed = fixed + FIXED_BYTES.id
   cols[#cols + 1] = "k INT NOT NULL"
   fixed = fixed + FIXED_BYTES.k

   -- pk_secondary（§4.2）= 主键 + 二级索引 + **唯一索引**。
   -- 唯一索引必须有天然唯一的列：`k` 取 (id*7) % 100000、>10 万行就会撞，
   -- 所以另加一列 u（= id*1000003，逐行唯一），别拿会撞的列去建 UNIQUE。
   if profile == "pk_secondary" then
      cols[#cols + 1] = "u BIGINT NOT NULL"
      fixed = fixed + 8
   end

   local target = 0
   if b > 0 then
      target = b - fixed
      if target < 2 then
         die("目标行长 B=%d 太小：必需列（id INT + k INT）已经要 %d 字节，" ..
             "填充列至少还要 2 字节内容才能形成分布。最小可行 B = %d",
             b, fixed, fixed + 2)
      end
      local headroom = math.max(16, math.floor(target * 0.3))
      local n = target + headroom
      cols[#cols + 1] = string.format("c VARCHAR(%d) NOT NULL", n)
      fill[#fill + 1] = {name = "c", capacity = n, prefix = (n < 256) and 1 or 2,
                         target = target}
   end

   -- INSERT 的列名表由**形状**决定，别再硬编码（硬编码 "id,k,c" 在加列后会漏列：
   -- 实测踩到过——表建好了、灌数整条语句报错，而外面只看 "created" 还以为成功）
   local names = {"id", "k"}
   if profile == "pk_secondary" then names[#names + 1] = "u" end
   if b > 0 then names[#names + 1] = "c" end

   return {
      cols = cols, fixed = fixed, fill = fill, target = target,
      insert_cols = table.concat(names, ","),
      idx = (profile == "pk_secondary")
            and "PRIMARY KEY (id), KEY k_k (k), UNIQUE KEY u_uniq (u)"
            or "PRIMARY KEY (id)",
      values = function(id, rng, lens, i)
         local v = {tostring(id), tostring((id * 7) % 100000)}
         if profile == "pk_secondary" then
            v[#v + 1] = tostring(id * 1000003)
         end
         if fill[1] ~= nil then
            v[#v + 1] = "'" .. ascii_of(rng, lens[((i - 1) % #lens) + 1], "r" .. id) .. "'"
         end
         return v
      end,
   }
end

-- all-types profile：照现场那张 app.alltypes（**31 列**，`SHOW CREATE TABLE` 抄下来的真实定义），
-- 覆盖除地理空间外的全部列类型。`--row-length` 由 `c_varchar` 承载（把它加宽到装得下目标），
-- **列数与类型面都不变**——只是同一个 VARCHAR 列更长。
local ALLTYPES = {
   {name = "c_long",     ddl = "INT NOT NULL",              gen = function(id) return tostring(id) end},
   {name = "c_tiny",     ddl = "TINYINT",                   gen = function(id) return tostring(id % 100) end},
   {name = "c_utiny",    ddl = "TINYINT UNSIGNED",          gen = function(id) return tostring(id % 200) end},
   {name = "c_short",    ddl = "SMALLINT",                  gen = function(id) return tostring(id % 30000) end},
   {name = "c_int24",    ddl = "MEDIUMINT",                 gen = function(id) return tostring(id % 8000000) end},
   {name = "c_ulong",    ddl = "INT UNSIGNED",              gen = function(id) return tostring(id * 3) end},
   {name = "c_longlong", ddl = "BIGINT",                    gen = function(id) return tostring(id * 1000003) end},
   {name = "c_float",    ddl = "FLOAT",                     gen = function(id) return string.format("%d.5", id % 1000) end},
   {name = "c_double",   ddl = "DOUBLE",                    gen = function(id) return string.format("%d.25", id % 100000) end},
   {name = "c_dec",      ddl = "DECIMAL(18,6)",             gen = function(id) return string.format("%d.250000", id % 1000000) end},
   {name = "c_date",     ddl = "DATE",                      gen = function() return "'2024-01-02'" end},
   {name = "c_year",     ddl = "YEAR",                      gen = function() return "2024" end},
   {name = "c_ts",       ddl = "TIMESTAMP(6) NULL",         gen = function() return "'2024-01-02 03:04:05.123456'" end},
   {name = "c_dt",       ddl = "DATETIME(3)",               gen = function() return "'2024-01-02 03:04:05.678'" end},
   {name = "c_time",     ddl = "TIME(2)",                   gen = function() return "'12:34:56.78'" end},
   {name = "c_char",     ddl = "CHAR(10)",       var = 10,  gen = function(id, rng) return "'" .. ascii_of(rng, 10, "ch") .. "'" end},
   {name = "c_binary",   ddl = "BINARY(8)",      var = 8,   gen = function(id, rng) return "x'" .. hex_of(rng, 8, "bi") .. "'" end},
   -- 填充列：内容长度由 fill_lengths 决定（值函数放在 shape 里覆盖）
   {name = "c_varchar",  ddl = "VARCHAR(%d)",    var = 0,   gen = nil},
   {name = "c_varbinary",ddl = "VARBINARY(100)", var = 20,  gen = function(id, rng) return "x'" .. hex_of(rng, 20, "vb") .. "'" end},
   {name = "c_tinytext", ddl = "TINYTEXT",       var = 12,  gen = function(id, rng) return "'" .. ascii_of(rng, 12, "tt") .. "'" end},
   {name = "c_text",     ddl = "TEXT",           var = 24,  gen = function(id, rng) return "'" .. ascii_of(rng, 24, "tx") .. "'" end},
   {name = "c_mediumtext", ddl = "MEDIUMTEXT",   var = 32,  gen = function(id, rng) return "'" .. ascii_of(rng, 32, "mt") .. "'" end},
   {name = "c_longtext", ddl = "LONGTEXT",       var = 40,  gen = function(id, rng) return "'" .. ascii_of(rng, 40, "lt") .. "'" end},
   {name = "c_tinyblob", ddl = "TINYBLOB",       var = 8,   gen = function(id, rng) return "x'" .. hex_of(rng, 8, "tb") .. "'" end},
   {name = "c_blob",     ddl = "BLOB",           var = 16,  gen = function(id, rng) return "x'" .. hex_of(rng, 16, "bl") .. "'" end},
   {name = "c_mediumblob", ddl = "MEDIUMBLOB",   var = 24,  gen = function(id, rng) return "x'" .. hex_of(rng, 24, "mb") .. "'" end},
   {name = "c_longblob", ddl = "LONGBLOB",       var = 32,  gen = function(id, rng) return "x'" .. hex_of(rng, 32, "lb") .. "'" end},
   -- 下面四列也走 LENGTH() 实测，所以内容长度要**确定**，否则期望值会白偏几个字节：
   -- JSON 用定宽字符串值（JSON 数字不许有前导零，所以放进字符串里）⇒ 恒 13 字符；
   -- BIT(13) 存储 2 字节；ENUM/SET 各 1 字符。
   {name = "c_json",     ddl = "JSON",           var = 13,  gen = function(id) return string.format("'{\"i\":\"%05d\"}'", id) end},
   {name = "c_bit",      ddl = "BIT(13)",        var = 2,   gen = function(id) return "b'1010101010101'" end},
   {name = "c_enum",     ddl = "ENUM('a','b','c')", var = 1, gen = function(id) return "'" .. ({"a","b","c"})[(id % 3) + 1] .. "'" end},
   {name = "c_set",      ddl = "SET('x','y','z')", var = 1,  gen = function(id) return "'" .. ({"x","y","z"})[(id % 3) + 1] .. "'" end},
}

local function alltypes_shape(b, profile)
   -- 定宽部分：ALLTYPES 里没有 var 字段的都是定宽（数值/时间）
   local fixed = 0
   local var_known = 0
   for _, c in ipairs(ALLTYPES) do
      if c.var == nil then
         local w = FIXED_BYTES[c.name]
         if w == nil then
            die("内部错误：列 %s 没有声明宽度（alltypes 的定宽表漏了）", c.name)
         end
         fixed = fixed + w
      elseif c.name ~= "c_varchar" then
         var_known = var_known + c.var
      end
   end

   local target = 0
   local n = 0
   if b > 0 then
      target = b - fixed - var_known
      if target < 2 then
         die("目标行长 B=%d 太小：alltypes 的定宽列（%d 字节）+ 其它变长列" ..
             "（%d 字节）已经占满。最小可行 B ≈ %d",
             b, fixed, var_known, fixed + var_known + 2)
      end
      local headroom = math.max(16, math.floor(target * 0.3))
      n = target + headroom
   end

   -- 其它变长列（不含 c_varchar）也进实测表达式：内容长度固定 ⇒ 可预测
   -- 实测表达式里的变长列：**排除填充列**（它由 fill 单独列出），否则会被算两次
   local var_cols = {}
   for _, c in ipairs(ALLTYPES) do
      if c.var ~= nil and c.name ~= "c_varchar" then
         var_cols[#var_cols + 1] = c.name
      end
   end

   local cols = {}
   for _, c in ipairs(ALLTYPES) do
      if c.name == "c_varchar" then
         cols[#cols + 1] = string.format("c_varchar VARCHAR(%d)", math.max(n, 1))
      else
         cols[#cols + 1] = c.name .. " " .. c.ddl
      end
   end

   local fill = {}
   if b > 0 then
      fill[1] = {name = "c_varchar", capacity = n, prefix = (n < 256) and 1 or 2,
                 target = target}
   end

   local names = {}
   for _, c in ipairs(ALLTYPES) do
      names[#names + 1] = c.name
   end

   return {
      cols = cols, fixed = fixed, fill = fill, target = target,
      var_cols = var_cols, var_bytes = var_known,
      insert_cols = table.concat(names, ","),
      idx = (profile == "pk_secondary")
            and "PRIMARY KEY (c_long), KEY c_tiny_k (c_tiny), UNIQUE KEY c_longlong_u (c_longlong)"
            or "PRIMARY KEY (c_long)",
      values = function(id, rng, lens, i)
         local v = {}
         local len = (b > 0) and lens[((i - 1) % #lens) + 1] or 0
         for _, c in ipairs(ALLTYPES) do
            if c.name == "c_varchar" then
               v[#v + 1] = "'" .. ascii_of(rng, len, "vr") .. "'"
            else
               v[#v + 1] = c.gen(id, rng)
            end
         end
         return v
      end,
   }
end

local function make_shape(b, profile, types)
   if profile ~= "pk" and profile ~= "pk_secondary" then
      die("--index-profile 只支持 pk | pk_secondary（收到 '%s'）", profile)
   end
   if types == "builtin" then
      return builtin_shape(b, profile)
   end
   if types == "all" then
      return alltypes_shape(b, profile)
   end
   die("--types 只支持 builtin | all（收到 '%s'）", types)
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

local function load_table(con, db, name, shape, row_bytes)
   local rng = make_rng(sysbench.opt.seed)
   local total = sysbench.opt.table_size
   local batch = effective_batch(row_bytes)
   local inserted = 0

   -- 填充长度序列与目标
   local lens, target = fill_lengths((shape.fill[1] ~= nil) and shape.fill[1].target or 0,
                                     (shape.fill[1] ~= nil) and shape.fill[1].capacity or 0)

   local col_names = shape.insert_cols
   if col_names == nil then
      die("内部错误：shape 没有 insert_cols（列名表必须由形状给出）")
   end

   while inserted < total do
      local rows = {}
      local n = math.min(batch, total - inserted)
      for i = 1, n do
         local id = inserted + i
         local vals = shape.values(id, rng, lens, i)
         rows[i] = "(" .. table.concat(vals, ",") .. ")"
      end
      local sql = string.format("INSERT INTO %s.%s (%s) VALUES %s",
                                db, name, col_names, table.concat(rows, ","))
      con:query(sql)
      inserted = inserted + n
   end

   return inserted, target, #lens
end

--
-- 行长回读（§5：prepare 结束自动跑；不通过即失败）
--

local function row_bytes_expr(shape)
   local parts = {tostring(shape.fixed)}
   -- 变长列一律按 LENGTH() 实测（内容长度由生成器固定/或由填充列承载）
   for _, name in ipairs(shape.var_cols or {}) do
      parts[#parts + 1] = string.format("LENGTH(%s)", name)
   end
   for _, f in ipairs(shape.fill or {}) do
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

local function check_rowlen(con, db, names, shape, b)
   local expr = row_bytes_expr(shape)
   local fixed, fill = shape.fixed, shape.fill or {}
   local tolerance = sysbench.opt.rowlen_tolerance
   local failures = 0

   print(string.format("raft_prepare.row_bytes_expr=%s", expr))
   print(string.format("raft_prepare.row_bytes_fixed=%d columns=%d types=%s",
                       fixed, #shape.cols, sysbench.opt.types))

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
   local shape = make_shape(b, sysbench.opt.index_profile, sysbench.opt.types)
   for _, name in ipairs(table_names()) do
      print(string.format("raft_prepare.ddl=%s", table_ddl(db, name, shape.cols, shape.idx)))
   end
   print(string.format("raft_prepare.types=%s columns=%d row_bytes_fixed=%d",
                       sysbench.opt.types, #shape.cols, shape.fixed))
   if b > 0 then
      local f = shape.fill[1]
      local lens, t = fill_lengths(f.target, f.capacity)
      print(string.format("raft_prepare.fill_column=%s fill_capacity=%d fill_target=%d fill_multiset=%s",
                          f.name, f.capacity, t, table.concat(lens, ",")))
      -- 这只是**估算**（各类列的声明/内容长度之和）；§5 的主判据是 prepare/rowlen 的**回读实测**，
      -- 两者差几个字节是正常的（例如列的实际内容长度与声明略有出入）。
      print(string.format(
         "raft_prepare.avg_row_bytes_estimate=%d (定宽 %d + 其它变长列 %d + 填充列内容均值 %d; 主判据是回读实测)",
         shape.fixed + (shape.var_bytes or 0) + (t or 0),
         shape.fixed, shape.var_bytes or 0, t or 0))
   end
end

local function cmd_prepare()
   local db = sql_ident(sysbench.opt.schema_db)
   if db == "" then die("必须给 --schema-db") end
   local b = sysbench.opt.row_length
   local names = table_names()
   local shape = make_shape(b, sysbench.opt.index_profile, sysbench.opt.types)
   local row_bytes = shape.fixed + ((shape.fill[1] ~= nil) and shape.fill[1].capacity or 0)
                  + (#shape.cols * 8)

   local con = connect()

   if not sysbench.opt.quiet_ddl then
      for _, name in ipairs(names) do
         print(string.format("raft_prepare.ddl=%s", table_ddl(db, name, shape.cols, shape.idx)))
      end
   end

   if not sysbench.opt.skip_ddl then
      for _, name in ipairs(names) do
         con:query(table_ddl(db, name, shape.cols, shape.idx))
         print(string.format("raft_prepare.created=%s", name))
      end
   end

   for _, name in ipairs(names) do
      local inserted = load_table(con, db, name, shape, row_bytes)
      print(string.format("raft_prepare.loaded=%s rows=%d", name, inserted))
   end

   check_rowlen(con, db, names, shape, b)
end

--
-- 列类型自检（§4.1.1 第 2 步）：对全类型表调 PERCONA_RAFT_SCHEMA_MANIFEST()，
-- 期望**不出现 UNSUPPORTED_COLUMN**。服务端收窄类型面时这里会具名失败并指出是哪一列。
-- 注意它抓不到"服务端**放宽**"（新增类型不改变已有表的指纹）——那只能靠 build commit 比对。
--
local function cmd_typescheck()
   local db = sql_ident(sysbench.opt.schema_db)
   if db == "" then die("必须给 --schema-db") end
   local names = {}
   for _, n in ipairs(table_names()) do names[#names + 1] = n end
   local tables = table.concat(names, ",")

   local con = connect()
   -- 目标上**没有**这个函数（原生发行版/更老的构建）时是"目标差异"，不是失败（§3.3）：
   -- 用 pcall 抓住 SQL 错误，只有 1305 FUNCTION does not exist 才按 SKIP 处理。
   local ok, row = pcall(function()
      return con:query_row(string.format(
         "SELECT PERCONA_RAFT_SCHEMA_MANIFEST('%s', '%s')", db, tables))
   end)
   if not ok then
      local err = tostring(row)
      if err:find("1305") or err:lower():find("does not exist") then
         print(string.format(
            "raft_prepare.typescheck.SKIP=目标上没有 PERCONA_RAFT_SCHEMA_MANIFEST()" ..
            "（不是我们的构建 ⇒ 类型面无从自检，这也**不是**失败）：%s", err))
         return
      end
      die("调用 PERCONA_RAFT_SCHEMA_MANIFEST() 失败：%s", err)
   end
   if row == nil then
      die("PERCONA_RAFT_SCHEMA_MANIFEST() 没有返回结果")
   end

   local result = row:match('"result"%s*:%s*"([^"]*)"')
   print(string.format("raft_prepare.typescheck.schema=%s tables=%s types=%s columns=%d",
                       db, tables, sysbench.opt.types,
                       #make_shape(sysbench.opt.row_length, sysbench.opt.index_profile,
                                   sysbench.opt.types).cols))

   if result == nil then
      die("返回里没有 result 字段，无法判定：%s", row)
   end

   -- 非 Raft 实例上控制类函数返回 UNAVAILABLE ⇒ 按 §3.3"目标差异不是漂移"处理：跳过而**不是**失败
   if result == "UNAVAILABLE" then
      print(string.format("raft_prepare.typescheck.SKIP=%s（该实例没有 Raft 运行时，" ..
                          "无法派生 manifest；这不是类型面的失败）", result))
      return
   end

   if result ~= "OK" then
      -- 具名失败：把整段 JSON 打出来（里面有 UNSUPPORTED_COLUMN 与列名）
      print(string.format("raft_prepare.typescheck.FAIL=%s", result))
      print(string.format("raft_prepare.typescheck.json=%s", row))
      die("列类型自检失败：result=%s（若是 UNSUPPORTED_COLUMN，说明工具这份清单与服务端矩阵不一致，" ..
          "**不许静默跳过那一列**）", result)
   end

   -- 再单独查一次"有没有 UNSUPPORTED_COLUMN 字样"，防止 result 是 OK 但细节里有异常列
   if row:find("UNSUPPORTED_COLUMN") then
      print(string.format("raft_prepare.typescheck.json=%s", row))
      die("返回里出现 UNSUPPORTED_COLUMN：工具的类型清单与服务端不一致")
   end

   print(string.format("raft_prepare.typescheck.PASS=%d 列全部被服务端接受", 
                       #make_shape(sysbench.opt.row_length, sysbench.opt.index_profile,
                                   sysbench.opt.types).cols))
end

local function cmd_rowlen()
   local db = sql_ident(sysbench.opt.schema_db)
   if db == "" then die("必须给 --schema-db") end
   local b = sysbench.opt.row_length
   local shape = make_shape(b, sysbench.opt.index_profile, sysbench.opt.types)
   check_rowlen(connect(), db, table_names(), shape, b)
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
   typescheck = {cmd_typescheck},
}
