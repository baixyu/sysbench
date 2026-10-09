#!/usr/bin/env sysbench
--
-- Percona Raft: 归档报告（§8.2 扩展段 / §8.3 环境段）
--
-- 依据：percona-server 的 Docs/raft_replication/perf_tool_plan.md §8。
-- 本文件只做"采样 + 汇总 + 比较"，**不跑压测**——压测由 percona/report.sh 调 sysbench 跑，
-- 所以输出的第一段永远是 sysbench 原生报告（逐字保留），扩展信息进 report.json。
--
-- 命令：
--   sample    采样一次（状态量 + 可读到的 percona_raft% 变量 + 版本/位置），打印 JSON 到 stdout
--   combine   把"跑前采样 + sysbench 输出 + 跑后采样 + 参数栏 + 环境段"合成 report.json
--   compare   两份 report.json 并排出表（并排的前提是参数栏可比，这一条由 §8.2/§8.4 规定）
--   show      把 report.json 的要点打印成人能看的形状（不装 jq 也能看）
--
-- 为什么 sample 与 combine 分开：状态量差值必须**跨两个时间点**采（跑前/跑后），
-- 而两次采样之间要真正把负载跑完，所以由 wrapper 串起来。
--

if sysbench.cmdline.command == nil then
   error("命令必填：sample | combine | compare | show（见文件头注释）", 0)
end

sysbench.cmdline.options = {
   -- sample
   out = {"把 JSON 写到该文件（默认 stdout）", ""},

   -- combine
   before = {"跑前采样的 JSON 文件", ""},
   after = {"跑后采样的 JSON 文件", ""},
   sysbench_out = {"sysbench 的完整 stdout 文件（原生报告逐字保留）", ""},
   report = {"输出的 report.json 路径", ""},
   -- 参数栏（§8.2：不记参数的两台机器并排不可比）
   scenario = {"场景名/脚本名", ""},
   -- 注意：**不能**叫 mysql_host/mysql_port/mysql_db/target/threads——
   -- 那是 mysql 驱动与 db 层已注册的选项名，脚本再声明一次会让 sysbench 段错误退出
   -- （实测 rc=139）。所以元数据统一带 meta_ 前缀。
   meta_host = {"目标 host", ""},
   meta_port = {"目标 port", ""},
   meta_db = {"目标库", ""},
   meta_target = {"--target 档位", ""},
   meta_read_consistency = {"--read-consistency 档位", ""},
   meta_threads = {"客户端线程数", ""},
   meta_time = {"时长（秒）", ""},
   meta_events = {"事件数", ""},
   tables = {"表数量", ""},
   table_size = {"每表行数", ""},
   row_length = {"目标平均行长 B", ""},
   types = {"列类型档", ""},
   index_profile = {"键结构档", ""},
   seed = {"数据种子", ""},
   rowlen_measured_avg = {"prepare 回读到的实测平均行长（§5）", ""},
   rowlen_measured_deviation = {"回读偏差（%）", ""},
   -- 环境段（§8.3：一条命令能读到的就够）
   env_os = {"uname -a", ""},
   env_cpu = {"CPU 型号", ""},
   env_mem = {"内存总量", ""},

   -- compare / show
   a = {"第一份 report.json（基准）", ""},
   b = {"第二份 report.json（对照）", ""},
   in_file = {"要看的 report.json（CLI：--in-file=…）", ""},
}

local function die(fmt, ...)
   error(string.format(fmt, ...), 0)
end

local function connect()
   local drv = sysbench.sql.driver()
   return drv:connect()
end

--
-- JSON 写：自己拼（不引依赖），key 排序 ⇒ 输出稳定、可 diff
--

local function jesc(s)
   s = tostring(s)
   s = s:gsub("\\", "\\\\"):gsub('"', '\\"'):gsub("\n", "\\n")
        :gsub("\r", "\\r"):gsub("\t", "\\t")
   -- 控制字符（除上面三类）转成 \u00XX，保证 JSON 合法
   s = s:gsub("%c", function(c)
      return string.format("\\u%04x", string.byte(c))
   end)
   return s
end

local function jstr(s) return '"' .. jesc(s) .. '"' end

-- 数值还是字符串？只把"看起来是数字"的写成数字，其余一律当字符串（别猜）
local function jval(v)
   if type(v) == "number" then return tostring(v) end
   local s = tostring(v)
   if s ~= "" and s:match("^%-?%d+$") then return s end
   if s ~= "" and s:match("^%-?%d+%.%d+$") then return s end
   return jstr(s)
end

local function sorted_keys(t)
   local ks = {}
   for k in pairs(t) do ks[#ks + 1] = k end
   table.sort(ks)
   return ks
end

local function jflat(t, indent)
   local parts = {}
   for _, k in ipairs(sorted_keys(t)) do
      parts[#parts + 1] = string.format('%s%s: %s', indent, jstr(k), jval(t[k]))
   end
   return "{\n" .. table.concat(parts, ",\n") .. "\n}"
end

--
-- JSON 读：只读**我们自己写的**这种形状（顶层标量 + 一层扁平对象）
-- 不追求通用；通用解析器不该塞进这个工具里。
--

-- 取出 "key": { ... } 里的对象正文（做花括号配平，跳过字符串内的括号）
local function jobject(text, key)
   local s = text:find('"' .. key .. '"%s*:%s*{')
   if s == nil then return nil end
   local open = text:find("{", s)
   local depth, i, in_str, esc = 0, open, false, false
   while i <= #text do
      local c = text:sub(i, i)
      if in_str then
         if esc then esc = false
         elseif c == "\\" then esc = true
         elseif c == '"' then in_str = false end
      elseif c == '"' then in_str = true
      elseif c == "{" then depth = depth + 1
      elseif c == "}" then
         depth = depth - 1
         if depth == 0 then return text:sub(open + 1, i - 1) end
      end
      i = i + 1
   end
   return nil
end

-- 读一个 JSON 字符串（从 pos 处的引号开始），返回 value, next_pos
local function read_jstring(text, pos)
   local i = pos + 1
   local out = {}
   while i <= #text do
      local c = text:sub(i, i)
      if c == "\\" then
         local n = text:sub(i + 1, i + 1)
         if n == "n" then out[#out + 1] = "\n"
         elseif n == "t" then out[#out + 1] = "\t"
         elseif n == "r" then out[#out + 1] = "\r"
         elseif n == "u" then
            local hex = text:sub(i + 2, i + 5)
            out[#out + 1] = string.char(tonumber(hex, 16) % 256)
            i = i + 4
         else out[#out + 1] = n end
         i = i + 2
      elseif c == '"' then
         return table.concat(out), i + 1
      else
         out[#out + 1] = c
         i = i + 1
      end
   end
   return table.concat(out), i
end

-- 把"一层扁平对象"的正文读成 table。
-- 为什么不用模式匹配：**Lua 的 pattern 没有 "或"**（`|` 是普通字符），
-- 先前写的 `"(\\.|[^"])*"` 想表达"转义或非引号"，实际匹配不到任何数
-- （实测：一份没问题的 JSON 读出来是空表）。手写扫描反而更短、也更可靠。
local function parse_flat(body)
   local t = {}
   local i = 1
   while true do
      local q = body:find('"', i)
      if q == nil then break end
      local key, after = read_jstring(body, q)
      local colon = body:find(":", after)
      if colon == nil then break end
      local v = colon + 1
      while v <= #body and body:sub(v, v):match("%s") do v = v + 1 end
      if body:sub(v, v) == '"' then
         local val, nxt = read_jstring(body, v)
         t[key] = val
         i = nxt
      else
         local raw = body:match("^([^,}]+)", v) or ""
         raw = raw:gsub("^%s+", ""):gsub("%s+$", "")
         t[key] = raw
         i = v + #raw
      end
   end
   return t
end

local function jscalar(text, key)
   local v = text:match('"' .. key .. '"%s*:%s*"((\\.|[^"])*)"')
   if v ~= nil then
      return (v:gsub("\\n", "\n"):gsub("\\t", "\t"):gsub('\\"', '"'):gsub("\\\\", "\\"))
   end
   v = text:match('"' .. key .. '"%s*:%s*([%-%d%.eE]+)')
   return v
end

-- 扁平对象 ⇒ table
local function jflat_read(text, section)
   local body = jobject(text, section)
   if body == nil then return nil end
   return parse_flat(body)
end

local function read_file(path)
   local fh = io.open(path, "r")
   if fh == nil then die("打不开文件：%s", path) end
   local t = fh:read("*a")
   fh:close()
   return t
end

local function write_file(path, text)
   local fh, err = io.open(path, "w")
   if fh == nil then die("写不了 %s：%s", path, tostring(err)) end
   fh:write(text)
   fh:close()
end

--
-- sample：状态量 + 参数 + 位置
--

-- §8.2 要差值的状态量（名字逐个核对过，见方案 §8.2/§3.3）
local STATUS_VARS = {
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
   -- 订正（2026-10-09 实测）：方案 §8.2 里写的 catchup_waits / catchup_refusals /
   -- catchup_max_lag **在服务端不存在**（那三个量一个都没采到就是证据）；真实名字是
   -- caught_up_*（`sql/mysqld.cc:12075/12081/12185/12515`）。这是清单里第二个"名字过期"的坑
   -- （第一个是 fsync_batches → wal_append_batches）。
   "Percona_raft_caught_up_waits",
   "Percona_raft_caught_up_refusals",
   "Percona_raft_caught_up_max_lag",
   "Percona_raft_caught_up_real_lag",
   "Percona_raft_serving_gap_open_ms",
   "Percona_raft_last_serving_gap_ms",
   "Percona_raft_schema_epoch",
   "Percona_raft_unmanaged_write_rejections",
   "Percona_raft_metadata_write_rejections",
   "Percona_raft_applier_apply_count",
}

local function collect_status(con)
   local out, absent = {}, {}
   for _, name in ipairs(STATUS_VARS) do
      local v = con:query_row(string.format(
         "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='%s'",
         name))
      if v ~= nil then out[name] = tostring(v) else absent[#absent + 1] = name end
   end
   -- 缺的量要记下来：报告里"没有这一项"与"这一项是 0"是两件事
   out["__absent__"] = table.concat(absent, ",")
   return out, absent
end

-- 参数块：只收**服务器级/会话默认**能代表配置的量。
-- 排除两个身份变量：它们是**会话**变量，而且我们的驱动一连上就会设 namespace
-- （采样连接自己也被注入），留在参数栏里会被读成"服务器参数"。
local PARAM_EXCLUDE = {
   percona_raft_request_namespace = true,
   percona_raft_request_id = true,
}

local function collect_params(con)
   local out = {}
   local rs = con:query("SHOW VARIABLES LIKE 'percona\\_raft%'")
   local row
   while true do
      row = rs:fetch_row()
      if row == nil then break end
      if not PARAM_EXCLUDE[row[1]] then
         out[row[1]] = tostring(row[2] or "")
      end
   end
   rs:free()
   return out
end

local function cmd_sample()
   local con = connect()
   local meta = {}
   local ok, ver = pcall(function() return con:query_row("SELECT VERSION()") end)
   meta.version = ok and tostring(ver) or ""
   ok, ver = pcall(function() return con:query_row("SELECT @@version_comment") end)
   meta.version_comment = ok and tostring(ver) or ""
   ok, ver = pcall(function() return con:query_row("SELECT @@hostname") end)
   meta.hostname = ok and tostring(ver) or ""
   ok, ver = pcall(function() return con:query_row("SELECT @@port") end)
   meta.port = ok and tostring(ver) or ""
   ok, ver = pcall(function() return con:query_row("SELECT @@log_bin") end)
   meta.log_bin = ok and tostring(ver) or ""

   local status, absent = collect_status(con)
   meta.status_absent = table.concat(absent, ",")
   meta.status_expected = tostring(#STATUS_VARS)

   local text = "{\n"
      .. '  "meta": ' .. jflat(meta, "    ") .. ",\n"
      .. '  "params": ' .. jflat(collect_params(con), "    ") .. ",\n"
      .. '  "status": ' .. jflat(status, "    ") .. "\n"
      .. "}\n"

   if sysbench.opt.out ~= "" then
      write_file(sysbench.opt.out, text)
      print("raft_report.sample=" .. sysbench.opt.out)
   else
      io.write(text)
   end
end

--
-- combine：合成 report.json
--

-- 从 sysbench 原生输出里取数（第一段口径**逐字来自 sysbench**）
local function parse_sysbench(text)
   local o = {}
   local function grab(key, pat)
      local v = text:match(pat)
      if v ~= nil then o[key] = v end
   end
   grab("transactions", "transactions:%s*(%d+)")
   grab("transactions_per_sec", "transactions:%s*%d+%s*%(([%d%.]+) per sec")
   grab("queries", "\n%s*queries:%s*(%d+)")
   grab("queries_per_sec", "queries:%s*%d+%s*%(([%d%.]+) per sec")
   grab("ignored_errors", "ignored errors:%s*(%d+)")
   grab("reconnects", "reconnects:%s*(%d+)")
   grab("latency_min_ms", "min:%s*([%d%.]+)")
   grab("latency_avg_ms", "avg:%s*([%d%.]+)")
   grab("latency_p95_ms", "95th percentile:%s*([%d%.]+)")
   grab("latency_max_ms", "max:%s*([%d%.]+)")
   grab("total_time_s", "total time:%s*([%d%.]+)s")
   grab("total_events", "total number of events:%s*(%d+)")
   grab("threads_fairness_events", "events%s*%(avg/stddev%):%s*([%d%.]+/[%d%.]+)")
   grab("threads_fairness_exec_time", "execution time%s*%(avg/stddev%):%s*([%d%.]+/[%d%.]+)")
   -- 工具扩展段（在原生 SQL statistics 段内）
   grab("target", "\n%s*target:%s*(%S+)")
   grab("read_consistency", "\n%s*read consistency:%s*(.-)\n")
   grab("connections_with_identity", "connections with injected identity:%s*(%d+%s*/%s*%d+)")
   grab("request_ids_issued", "request ids issued:%s*(%d+)")
   grab("autocommit_txns_wrapped", "autocommit txns wrapped by driver:%s*(%d+)")
   grab("refused", "refused:%s*(%d+)")
   grab("ambiguous", "ambiguous:%s*(%d+)")
   grab("temporary", "temporary:%s*(%d+)")
   grab("needs_action", "needs_action:%s*(%d+)")
   grab("rolled_back_xa", "rolled_back_xa:%s*(%d+)")
   return o
end

local function numeric(t)
   local out = {}
   for k, v in pairs(t or {}) do
      local n = tonumber(v)
      out[k] = (n ~= nil) and n or nil
      if n == nil then out[k] = v end
   end
   return out
end

local function cmd_combine()
   if sysbench.opt.before == "" or sysbench.opt.after == "" then
      die("combine 需要 --before 与 --after（跑前/跑后的 sample 文件）")
   end
   if sysbench.opt.report == "" then die("combine 需要 --report=<输出路径>") end

   local before_txt = read_file(sysbench.opt.before)
   local after_txt = read_file(sysbench.opt.after)
   local sb_txt = (sysbench.opt.sysbench_out ~= "") and read_file(sysbench.opt.sysbench_out) or ""

   local b_status = jflat_read(before_txt, "status") or {}
   local a_status = jflat_read(after_txt, "status") or {}
   local b_meta = jflat_read(before_txt, "meta") or {}
   local a_meta = jflat_read(after_txt, "meta") or {}
   local params = jflat_read(after_txt, "params") or jflat_read(before_txt, "params") or {}

   -- 差值（两边都能解析成数字才算差；非数字的（例如 catchup_stall）记为前后值对）
   local delta = {}
   for k, av in pairs(a_status) do
      local bv = b_status[k]
      local an, bn = tonumber(av), tonumber(bv)
      if an ~= nil and bn ~= nil then
         delta[k] = an - bn
      elseif bv ~= nil and bv ~= av then
         delta[k] = tostring(bv) .. " -> " .. tostring(av)
      end
   end
   -- §8.2：appends_per_fsync 这个量**不存在**，要自己算（key 用状态量全名，别用短名）
   local batches = delta["Percona_raft_wal_append_batches"]
   local records = delta["Percona_raft_wal_append_records"]
   if type(batches) == "number" and batches > 0 and type(records) == "number" then
      delta["wal_appends_per_fsync"] = string.format("%.2f", records / batches)
   end
   local admitted = delta["Percona_raft_admission_success"]
   if type(admitted) == "number" then
      local txn = tonumber(parse_sysbench(sb_txt).transactions or "")
      if txn ~= nil and txn > 0 then
         delta["admission_per_client_txn"] = string.format("%.3f", admitted / txn)
      end
   end

   local run = {
      scenario = sysbench.opt.scenario,
      -- 取值也要用 meta_*（先前只改了判断、没改取值 ⇒ 拿到了全局 --target 的默认值 auto）
      target = (sysbench.opt.meta_target ~= "") and sysbench.opt.meta_target
               or (parse_sysbench(sb_txt).target or ""),
      read_consistency = (sysbench.opt.meta_read_consistency ~= "")
               and sysbench.opt.meta_read_consistency
               or (parse_sysbench(sb_txt).read_consistency or ""),
      threads = sysbench.opt.meta_threads,
      time_s = sysbench.opt.meta_time,
      events = sysbench.opt.meta_events,
      mysql_host = sysbench.opt.meta_host,
      mysql_port = sysbench.opt.meta_port,
      mysql_db = sysbench.opt.meta_db,
   }
   local shape = {
      tables = sysbench.opt.tables,
      table_size = sysbench.opt.table_size,
      row_length = sysbench.opt.row_length,
      types = sysbench.opt.types,
      index_profile = sysbench.opt.index_profile,
      seed = sysbench.opt.seed,
      rowlen_measured_avg = sysbench.opt.rowlen_measured_avg,
      rowlen_measured_deviation = sysbench.opt.rowlen_measured_deviation,
   }
   local env = {
      os = sysbench.opt.env_os,
      cpu = sysbench.opt.env_cpu,
      mem = sysbench.opt.env_mem,
      mysqld_version = a_meta.version or b_meta.version or "",
      version_comment = a_meta.version_comment or "",
      log_bin = a_meta.log_bin or "",
      hostname = a_meta.hostname or "",
      port = a_meta.port or "",
   }

   local parsed = parse_sysbench(sb_txt)
   local contract = {}
   for _, k in ipairs({"connections_with_identity", "request_ids_issued",
                       "autocommit_txns_wrapped", "refused", "ambiguous",
                       "temporary", "needs_action", "rolled_back_xa"}) do
      if parsed[k] ~= nil then contract[k] = parsed[k] end
   end

   local json = "{\n"
      .. '  "report_schema": "perf-tool/1",\n'
      .. '  "run": ' .. jflat(run, "    ") .. ",\n"
      .. '  "load_shape": ' .. jflat(shape, "    ") .. ",\n"
      .. '  "sysbench": ' .. jflat(parsed, "    ") .. ",\n"
      .. '  "contract": ' .. jflat(contract, "    ") .. ",\n"
      .. '  "server_params": ' .. jflat(params, "    ") .. ",\n"
      .. '  "server_before": ' .. jflat(b_status, "    ") .. ",\n"
      .. '  "server_after": ' .. jflat(a_status, "    ") .. ",\n"
      .. '  "server_delta": ' .. jflat(delta, "    ") .. ",\n"
      .. '  "env": ' .. jflat(env, "    ") .. ",\n"
      .. '  "sysbench_raw": ' .. jstr(sb_txt) .. "\n"
      .. "}\n"

   write_file(sysbench.opt.report, json)
   print("raft_report.report=" .. sysbench.opt.report)
   print(string.format("raft_report.bytes=%d", #json))
end

--
-- compare：并排（并排的前提是参数栏可比，§8.2/§8.4）
--

local COMPARE_ROWS = {
   {"transactions", "sysbench"},
   {"transactions_per_sec", "sysbench"},
   {"queries_per_sec", "sysbench"},
   {"ignored_errors", "sysbench"},
   {"latency_avg_ms", "sysbench"},
   {"latency_p95_ms", "sysbench"},
   {"latency_max_ms", "sysbench"},
   {"connections_with_identity", "contract"},
   {"request_ids_issued", "contract"},
   {"autocommit_txns_wrapped", "contract"},
   {"refused", "contract"},
   {"ambiguous", "contract"},
   {"temporary", "contract"},
   {"needs_action", "contract"},
   {"rolled_back_xa", "contract"},
   {"Percona_raft_write_permit_waits", "server_delta"},
   {"Percona_raft_write_permit_wait_timeouts", "server_delta"},
   {"Percona_raft_wal_append_batches", "server_delta"},
   {"Percona_raft_wal_append_records", "server_delta"},
   {"wal_appends_per_fsync", "server_delta"},
   {"Percona_raft_admission_success", "server_delta"},
   {"admission_per_client_txn", "server_delta"},
   {"Percona_raft_read_index_confirmations", "server_delta"},
   {"Percona_raft_unmanaged_write_rejections", "server_delta"},
}

local function load_report(path)
   local t = read_file(path)
   local r = { raw = t }
   for _, sec in ipairs({"run", "load_shape", "sysbench", "contract", "server_delta",
                         "server_params", "env"}) do
      r[sec] = jflat_read(t, sec) or {}
   end
   return r
end

local function cmd_compare()
   if sysbench.opt.a == "" or sysbench.opt.b == "" then
      die("compare 需要 --a 与 --b（两份 report.json）")
   end
   local A = load_report(sysbench.opt.a)
   local B = load_report(sysbench.opt.b)

   -- 表头带**负载参数**（线程/时长/target/强读档）：并排的人要能一眼看出"负载是不是同一份"
   local function head(tag, path, R)
      local r = R.run
      local dur = (r.time_s ~= "" and r.time_s) and (r.time_s .. "s") or (r.events .. " events")
      print(string.format("%s: %s", tag, path))
      print(string.format("   目标 %s:%s/%s  target=%s  强读=%s  场景=%s  线程=%s  时长=%s",
         r.mysql_host or "?", r.mysql_port or "?", r.mysql_db or "?",
         r.target or "?", r.read_consistency or "?", r.scenario or "?",
         r.threads or "?", dur))
   end
   head("A", sysbench.opt.a, A)
   head("B", sysbench.opt.b, B)

   -- 参数栏差异：并排不可比时**先说清楚**。
   -- 比什么：load_shape（数据形状）+ 可读到的 percona_raft% 变量 + **负载侧参数**
   -- （线程/时长/事件数/场景/强读档）。**不比** target/host/port/db——
   -- 那几项正是"两条臂"的定义差异（§4.4：一个工具两个 target），放表头看就行。
   local diffs = {}
   for _, k in ipairs({"threads", "time_s", "events", "scenario", "read_consistency"}) do
      local av, bv = A.run[k], B.run[k]
      if tostring(av) ~= tostring(bv) then
         diffs[#diffs + 1] = string.format("  run.%s:  A=%s  B=%s", k,
                                           tostring(av), tostring(bv))
      end
   end
   local all_keys = {}
   for k in pairs(A.load_shape) do all_keys[k] = true end
   for k in pairs(B.load_shape) do all_keys[k] = true end
   for _, sec in ipairs({"load_shape", "server_params"}) do
      for k in pairs(A[sec]) do all_keys["\1" .. sec .. "." .. k] = true end
      for k in pairs(B[sec]) do all_keys["\1" .. sec .. "." .. k] = true end
   end
   for k in pairs(all_keys) do
      local sec, key = "load_shape", k
      if k:sub(1, 1) == "\1" then
         sec, key = k:match("^\1([^%.]+)%.(.+)$")
      end
      local av, bv = A[sec][key], B[sec][key]
      if tostring(av) ~= tostring(bv) then
         diffs[#diffs + 1] = string.format("  %s.%s:  A=%s  B=%s", sec, key,
                                           tostring(av), tostring(bv))
      end
   end
   if #diffs > 0 then
      print("参数差异（这些差异会让并排不可比，先看这里）：")
      table.sort(diffs)
      for _, d in ipairs(diffs) do print(d) end
      print("")
   else
      print("参数栏一致（load_shape + 负载侧参数 + 可读到的 percona_raft% 变量逐项相同）")
      print("")
   end

   print(string.format("%-32s %-18s %-18s %s", "指标", "A", "B", "B/A"))
   for _, row in ipairs(COMPARE_ROWS) do
      local key, sec = row[1], row[2]
      local av, bv = A[sec][key], B[sec][key]
      if av ~= nil or bv ~= nil then
         local ratio = ""
         local an, bn = tonumber(av), tonumber(bv)
         if an ~= nil and bn ~= nil and bn ~= 0 then
            ratio = string.format("%.3f", an / bn)
         end
         print(string.format("%-32s %-18s %-18s %s", key,
                             tostring(av or "-"), tostring(bv or "-"), ratio))
      end
   end
end

--
-- show：不装 jq 也能看要点
--

local function cmd_show()
   local path = sysbench.opt.in_file
   if path == "" then die("show 需要 --in-file=<report.json>") end
   local R = load_report(path)
   print("== run ==")
   for _, k in ipairs(sorted_keys(R.run)) do
      print(string.format("  %-20s %s", k, tostring(R.run[k])))
   end
   print("== load_shape ==")
   for _, k in ipairs(sorted_keys(R.load_shape)) do
      print(string.format("  %-24s %s", k, tostring(R.load_shape[k])))
   end
   print("== sysbench ==")
   for _, k in ipairs(sorted_keys(R.sysbench)) do
      print(string.format("  %-28s %s", k, tostring(R.sysbench[k])))
   end
   print("== contract ==")
   for _, k in ipairs(sorted_keys(R.contract)) do
      print(string.format("  %-28s %s", k, tostring(R.contract[k])))
   end
   print("== server_delta ==")
   for _, k in ipairs(sorted_keys(R.server_delta)) do
      print(string.format("  %-36s %s", k, tostring(R.server_delta[k])))
   end
   print("== env ==")
   for _, k in ipairs(sorted_keys(R.env)) do
      print(string.format("  %-18s %s", k, tostring(R.env[k])))
   end
end

sysbench.cmdline.commands = {
   sample = {cmd_sample},
   combine = {cmd_combine},
   compare = {cmd_compare},
   show = {cmd_show},
}
