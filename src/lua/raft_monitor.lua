#!/usr/bin/env sysbench
--
-- Percona Raft: 边跑负载边采指标，然后把"慢在哪一条腿"直接判出来
--
-- 为什么需要它：灌数/压测慢的时候，服务端的 recent_events 只给每笔事务的分阶段耗时，
-- 而"提交完成 → 本机开始 apply"这一段空档要靠**状态量在时间轴上的形状**才能定性
-- （apply 滞后 / 本机日志持久化滞后 / commit gate 排队 / 许可排队 / 冻结推迟 / 装快照）。
-- 所以：load 期间每 N 毫秒采一行，跑完自动汇总出判读。
--
-- 命令：
--   sample     按间隔把状态量追加到 TSV（同名的 <out>.stop 文件出现即停；--duration-s 也可停）
--   ring       把 PERCONA_RAFT_RECENT_EVENTS() 的 JSON 落到文件（供 summarize 算分阶段分布）
--   ring-watch 压测期间持续轮询有界事件环，追加去重事件到 JSONL
--   summarize  读 TSV（可带 ring JSON），打印汇总 + 判读（verdict）
--
-- 用法（一般用 percona/monitor.sh 包住一条命令，不用手写这三个命令）：
--   ./src/sysbench ./src/lua/raft_monitor.lua \
--     --mysql-host=127.0.0.1 --mysql-port=49602 --mysql-user=root \
--     sample --out=/tmp/m.tsv --interval-ms=1000
--
-- 采样一律用**服务端时钟**（UNIX_TIMESTAMP(NOW(6))）打时间戳，三个成员的样本可以直接对齐。
--

if sysbench.cmdline.command == nil then
   error("命令必填：sample | ring | ring-watch | summarize（见文件头注释）", 0)
end

sysbench.cmdline.options = {
   out = {"sample：TSV 输出路径（同名 .stop 文件 = 停止开关）；ring：JSON 输出路径", ""},
   in_file = {"summarize：要分析的 TSV", ""},
   ring_file = {"summarize：可选的 recent_events JSON/JSONL 文件", ""},
   client_file = {"summarize：可选的 monitor.sh 客户端区间输出（用于 freeze 前后 p99）", ""},
   interval_ms = {"sample：采样间隔（毫秒）", 1000},
   duration_s = {"sample：最长采样秒数（0 = 采到 <out>.stop 出现）", 0},
   lag_warn = {"summarize：apply 滞后告警阈值（条目数）", 200},
}

local function die(fmt, ...)
   error(string.format(fmt, ...), 0)
end

-- 有序变量表：{名字, 类型}；c=只增计数器，g=瞬时量，s=字符串。
-- 顺序 = TSV 列顺序，改动会让旧 TSV 不可比，所以只在必要时追加。
local VARS = {
   {"Percona_raft_state", "s"}, {"Percona_raft_serving", "s"},
   {"Percona_raft_reconciliation_required", "s"}, {"Percona_raft_applier_ready", "s"},
   {"Percona_raft_term", "g"}, {"Percona_raft_leader_id", "g"}, {"Percona_raft_leader_generation", "c"},
   {"Percona_raft_voter_count", "g"}, {"Percona_raft_schema_epoch", "g"},
   {"Percona_raft_local_raft_state", "s"},   -- 内含 is_leader/term/last_log/committed/durable/log_start
   -- 位点：commit 与四个"跟到哪儿"的游标
   {"Percona_raft_commit_index", "c"}, {"Percona_raft_committed_payload_index", "c"},
   {"Percona_raft_last_applied", "c"}, {"Percona_raft_applier_progress_index", "c"},
   {"Percona_raft_applier_applying_index", "c"}, {"Percona_raft_local_durable_index", "c"},
   {"Percona_raft_durable_checkpoint_index", "c"}, {"Percona_raft_durable_quorum_index", "c"},
   {"Percona_raft_durable_quorum_members", "g"}, {"Percona_raft_durable_quorum_gate_failures", "c"},
   -- 等待类
   {"Percona_raft_apply_barrier_entries", "c"}, {"Percona_raft_apply_dependency_waits", "c"},
   {"Percona_raft_apply_max_in_flight", "g"}, {"Percona_raft_apply_parallel_dispatches", "c"},
   {"Percona_raft_write_permit_waits", "c"}, {"Percona_raft_write_permit_wait_us", "c"},
   {"Percona_raft_admission_hold_timeouts", "c"}, {"Percona_raft_caught_up_waits", "c"},
   {"Percona_raft_caught_up_wait_us", "c"}, {"Percona_raft_caught_up_real_lag", "g"},
   {"Percona_raft_catchup_stall", "s"},
   -- 结果未定
   {"Percona_raft_client_delivery_unknown", "c"}, {"Percona_raft_client_proposal_uncertain", "c"},
   {"Percona_raft_proposal_uncertain", "c"}, {"Percona_raft_proposal_unknown", "c"},
   {"Percona_raft_client_apply_failed_unknown", "c"}, {"Percona_raft_client_wait_timeout", "c"},
   -- 本机日志
   {"Percona_raft_journal_write_us", "c"}, {"Percona_raft_journal_fsync_us", "c"},
   {"Percona_raft_journal_batches", "c"}, {"Percona_raft_journal_applied_batch_fsync_us", "c"},
   {"Percona_raft_journal_applied_batches", "c"}, {"Percona_raft_journal_scan_calls", "c"},
   {"Percona_raft_journal_scan_us", "c"}, {"Percona_raft_journal_retained_records", "g"},
   -- 引擎 apply
   {"Percona_raft_applier_apply_count", "c"}, {"Percona_raft_applier_apply_engine_us", "c"},
   -- applying_us 是"当前这笔 apply 已经跑了多久，空闲时为 0"的瞬时量（raft_applier.h:
   -- applying_microseconds()），不是累加器：当计数器算会出现"回退"，而且首末相减恒为 0。
   {"Percona_raft_applier_applying_us", "g"}, {"Percona_raft_applier_slow_applies", "c"},
   -- 冻结 / binlog / 快照
   {"Percona_raft_freeze_deferred_by_lagging_member", "c"},
   {"Percona_raft_freeze_live_member_floor", "g"},
   {"Percona_raft_binlog_freeze_deferrals", "c"}, {"Percona_raft_binlog_watermark_writes", "c"},
   {"Percona_raft_binlog_derived_writes", "c"}, {"Percona_raft_binlog_derivation_prefix_losses", "c"},
   {"Percona_raft_admission_success", "c"},
   {"Percona_raft_wal_append_batches", "c"}, {"Percona_raft_wal_append_records", "c"},
   {"Percona_raft_binlog_derived_through_index", "g"},
   {"Percona_raft_binlog_derived_syncs", "c"}, {"Percona_raft_binlog_backfilled_entries", "c"},
   {"Percona_raft_binlog_backfill_active", "g"}, {"Percona_raft_binlog_backfill_from_index", "g"},
   {"Percona_raft_binlog_backfill_through_index", "g"},
   {"Percona_raft_binlog_backfill_scanned_entries", "c"},
   {"Percona_raft_binlog_backfill_scanned_bytes", "c"},
   {"Percona_raft_binlog_backfill_max_batch_bytes", "g"},
   {"Percona_raft_binlog_backfill_last_duration_us", "g"},
   {"Percona_raft_binlog_derivation_failures", "c"},
   {"Percona_raft_binlog_derivation_paused", "g"},
   {"Percona_raft_wal_snapshot_copy_calls", "c"}, {"Percona_raft_wal_snapshot_copy_bytes", "c"},
   {"Percona_raft_peer_below_compacted_start", "g"},
   -- 捕获/编码（本机成本）
   {"Percona_raft_captured_cache_calls", "c"}, {"Percona_raft_captured_cache_bytes", "c"},
   {"Percona_raft_envelope_sha256_calls", "c"}, {"Percona_raft_envelope_sha256_bytes", "c"},
   {"Percona_raft_metadata_statements_applied", "c"}, {"Percona_raft_metadata_statements_failed", "c"},
   -- MySQL 侧旁证
   {"Threads_running", "g"}, {"Innodb_data_fsyncs", "c"}, {"Innodb_os_log_written", "c"},
   {"Innodb_log_waits", "c"}, {"Innodb_buffer_pool_read_requests", "c"},
   {"Innodb_buffer_pool_reads", "c"}, {"Innodb_buffer_pool_pages_dirty", "g"},
   {"Innodb_buffer_pool_pages_data", "g"}, {"Innodb_rows_inserted", "c"},
   {"Innodb_rows_updated", "c"}, {"Innodb_rows_deleted", "c"}, {"Innodb_rows_read", "c"},
   {"Innodb_data_read", "c"}, {"Innodb_data_written", "c"},
   {"Queries", "c"},
   -- 新指标只追加，保留现有 TSV 的列顺序兼容性。
   {"Percona_raft_journal_sync_calls", "c"}, {"Percona_raft_journal_sync_us", "c"},
   {"Percona_raft_journal_open_us", "c"},
   {"Percona_raft_journal_close_us", "c"}, {"Percona_raft_journal_dir_sync_us", "c"},
   {"Percona_raft_journal_batches", "c"}, {"Percona_raft_journal_batch_fsync_us", "c"},
   {"Percona_raft_journal_batch_records", "c"},
   {"Percona_raft_journal_applied_batch_records", "c"},
   {"Percona_raft_apply_slot_waits", "c"}, {"Percona_raft_apply_slot_wait_us", "c"},
   {"Percona_raft_apply_dependency_wait_us", "c"},
   {"Percona_raft_apply_dependency_wait_samples", "c"},
   {"Percona_raft_apply_worker_queue_wait_samples", "c"},
   {"Percona_raft_apply_worker_queue_wait_us", "c"},
   {"Percona_raft_apply_worker_queue_max", "g"},
   {"Percona_raft_journal_compaction_successes", "c"},
   {"Percona_raft_journal_compaction_scan_bytes", "c"},
   {"Percona_raft_journal_compaction_scan_us", "c"},
   {"Percona_raft_journal_compaction_rewrite_bytes", "c"},
   {"Percona_raft_journal_compaction_rewrite_us", "c"},
   {"Percona_raft_journal_compaction_total_us", "c"},
   {"Percona_raft_journal_compaction_last_scan_bytes", "g"},
   {"Percona_raft_journal_compaction_last_scan_us", "g"},
   {"Percona_raft_journal_compaction_last_rewrite_bytes", "g"},
   {"Percona_raft_journal_compaction_last_rewrite_us", "g"},
   {"Percona_raft_journal_compaction_last_total_us", "g"},
   {"Percona_raft_journal_compaction_last_start_epoch_us", "g"},
   {"Percona_raft_journal_compaction_last_end_epoch_us", "g"},
   {"Percona_raft_observability_events_evicted", "c"},
   {"Percona_raft_wal_writer_queue_wait_us", "c"},
   {"Percona_raft_wal_writer_io_us", "c"},
   {"Percona_raft_wal_fsync_us", "c"},
   {"Percona_raft_wal_fsync_calls", "c"},
   {"Percona_raft_wal_durability_wait_us", "c"},
   {"Percona_raft_wal_durability_wait_calls", "c"},
}

local TYPE_OF = {}
local IS_GAUGE = {}
for _, v in ipairs(VARS) do
   TYPE_OF[v[1]] = v[2]
   if v[2] == "g" then IS_GAUGE[v[1]] = true end
end

local function connect()
   local drv = sysbench.sql.driver()
   return drv:connect()
end

-- 一个采样点：一次往返取回全部状态量（performance_schema.global_status）
local function sample_once(con)
   local names = {}
   for _, v in ipairs(VARS) do names[#names + 1] = "'" .. v[1] .. "'" end
   local rs = con:query(
      "SELECT VARIABLE_NAME, VARIABLE_VALUE, UNIX_TIMESTAMP(NOW(6)) "
      .. "FROM performance_schema.global_status WHERE VARIABLE_NAME IN (" .. table.concat(names, ",") .. ")")
   local m, ts = {}, "0"
   local row
   while true do
      row = rs:fetch_row()
      if row == nil then break end
      m[row[1]] = tostring(row[2] or "")
      ts = tostring(row[3] or ts)
   end
   rs:free()
   local cells = { ts }
   for _, v in ipairs(VARS) do cells[#cells + 1] = m[v[1]] or "" end
   -- Append the local timestamp to preserve the established status-column positions.
   -- monitor.sh uses it to map server freeze epochs to client report intervals.
   cells[#cells + 1] = tostring(os.time())
   return table.concat(cells, "\t")
end

local function write_file(path, text)
   local fh, err = io.open(path, "w")
   if fh == nil then die("写不了 %s：%s", path, tostring(err)) end
   fh:write(text)
   fh:close()
end

-- 精确按制表符切列：`gmatch("[^\t]*")` 会在每个制表符处多吐一个空单元（实测导致整行错位）
local function split_tabs(line)
   local cells, pos = {}, 1
   while true do
      local s, e = line:find("\t", pos, true)
      if s == nil then cells[#cells + 1] = line:sub(pos); break end
      cells[#cells + 1] = line:sub(pos, s - 1)
      pos = e + 1
   end
   return cells
end

local function read_file(path)
   local fh, err = io.open(path, "r")
   if fh == nil then die("打不开 %s：%s", path, tostring(err)) end
   local text = fh:read("*a")
   fh:close()
   return text
end

local function env_of(con)
   local host = con:query_row("SELECT @@hostname") or "?"
   local port = con:query_row("SELECT @@port") or "?"
   return tostring(host), tostring(port)
end

--
-- sample
--
local function cmd_sample()
   if sysbench.opt.out == "" then die("sample 需要 --out=<TSV 路径>") end
   local con = connect()
   local host, port = env_of(con)
   local stop_path = sysbench.opt.out .. ".stop"
   local interval = tonumber(sysbench.opt.interval_ms) or 1000
   if interval < 1 then interval = 1 end
   local duration = tonumber(sysbench.opt.duration_s) or 0

   -- 防串台：目标文件已存在、且头里记的 host/port 与本机不同 ⇒ 拒绝。
   -- （实测踩过：三台都用 6606 端口，wrapper 按"端口"命名 ⇒ 三个采样进程写同一个文件，
   --  两份数据交错、表头被覆盖，最后 summarize 直接解析不出来。）
   local pre = io.open(sysbench.opt.out, "r")
   if pre ~= nil then
      local first = pre:read("*l") or ""
      pre:close()
      local eh, ep = first:match("host=([^\t]+)"), first:match("port=([^\t]+)")
      if eh ~= nil and (eh ~= host or tostring(ep) ~= port) then
         die("目标文件 %s 已经是另一台写的（host=%s port=%s；本机 host=%s port=%s）：换 --out，或先删掉那个文件",
             sysbench.opt.out, eh, tostring(ep), host, port)
      end
   end

   local names = {}
   for _, v in ipairs(VARS) do names[#names + 1] = v[1] end
   local fh, err = io.open(sysbench.opt.out, "w")
   if fh == nil then die("写不了 %s：%s", sysbench.opt.out, tostring(err)) end
   -- 头也**全用制表符**分隔：混用空格会让下游按 \t 切列时把元数据与第一个名字粘在一起
   fh:write(string.format("#\traft_monitor.v1\thost=%s\tport=%s\tinterval_ms=%d\tts_epoch\t%s\tclient_epoch\n",
                          host, port, interval, table.concat(names, "\t")))
   fh:flush()

   local started = os.time()
   local ticks = 0
   while true do
      local line = sample_once(con)
      fh:write(line .. "\n")
      fh:flush()
      ticks = ticks + 1
      local f = io.open(stop_path, "r")
      if f ~= nil then f:close(); break end
      if duration > 0 and (os.time() - started) >= duration then break end
      -- 用服务端 SLEEP 控节奏：不 spawn 进程，也不会因为本机时钟跳变而漂
      pcall(function() con:query_row(string.format("SELECT SLEEP(%.3f)", interval / 1000)) end)
   end
   fh:close()
   print(string.format("raft_monitor.samples=%d host=%s port=%s out=%s", ticks, host, port, sysbench.opt.out))
end

--
-- ring
--
local parse_ring -- ring-watch 在定义时前向引用

local function ring_snapshot(con)
   local s = con:query_row("SELECT PERCONA_RAFT_RECENT_EVENTS()")
   return s and tostring(s) or "[]"
end

local function cmd_ring()
   if sysbench.opt.out == "" then die("ring 需要 --out=<JSON 路径>") end
   local s = ring_snapshot(connect())
   write_file(sysbench.opt.out, s .. "\n")
   print(string.format("raft_monitor.ring_bytes=%d out=%s", #s, sysbench.opt.out))
end

-- recent_events 是有界环形缓冲区。采样期间持续轮询，把新事件追加为 JSONL，避免只拿到
-- 压测结束时最近的 256 条。启动时先做基线，不把压测前事件计入本次结果。
local function cmd_ring_watch()
   if sysbench.opt.out == "" then die("ring-watch 需要 --out=<JSONL 路径>") end
   local con, seen = connect(), {}
   local interval = math.max(tonumber(sysbench.opt.interval_ms) or 1000, 50)
   local stop_path = sysbench.opt.out .. ".stop"
   local last_seq, last_epoch, sequence_supported = nil, nil, true
   local missed_events, sequence_resets, observed_events = 0, 0, 0
   local poll_errors = 0
   local ignore_first_gap = false
   local function identity(e)
      return table.concat({e.cluster_txn_id or "", e.stage or "", e.outcome or "", e.reason or "",
                           e.event_seq or e.start_us or "", e.end_us or ""}, "|")
   end
   local function write_event(e, fh)
      if fh == nil then return end
      local fields = {"event_epoch", "event_seq", "cluster_txn_id", "stage", "outcome", "reason", "node_id", "term", "log_index",
                      "leader_generation", "duration_us", "start_us", "end_us"}
      local out = {}
      for _, k in ipairs(fields) do
         local v = e[k]
         if v ~= nil then
            v = tostring(v):gsub("\\", "\\\\"):gsub('"', '\\"'):gsub("\n", "\\n")
            out[#out + 1] = string.format('"%s":"%s"', k, v)
         end
      end
      fh:write("{" .. table.concat(out, ",") .. "}\n")
   end
   local function remember(snapshot, fh, baseline)
      local events = parse_ring(snapshot)
      local max_seq = nil
      for _, e in ipairs(events) do
         local epoch = tonumber(e.event_epoch)
         if epoch ~= nil then
            if last_epoch ~= nil and epoch ~= last_epoch then
               -- The sequence is process-local; a changed epoch proves mysqld restarted,
               -- even if the new sequence has already passed the previous value.
               sequence_resets = sequence_resets + 1
               last_seq = nil
               ignore_first_gap = true
            end
            last_epoch = epoch
         end
         local seq = tonumber(e.event_seq)
         if seq == nil then
            sequence_supported = false
         elseif max_seq == nil or seq > max_seq then
            max_seq = seq
         end
      end
      if sequence_supported and max_seq ~= nil then
         if last_seq ~= nil and max_seq < last_seq then
            -- A mysqld restart resets the process-local sequence. Coverage across
            -- that boundary cannot be proven, but collection can continue.
            sequence_resets = sequence_resets + 1
            last_seq = nil
            ignore_first_gap = true
         end
         for _, e in ipairs(events) do
            local seq = tonumber(e.event_seq)
            if seq ~= nil and (last_seq == nil or seq > last_seq) then
               if not baseline and last_seq ~= nil and seq > last_seq + 1 then
                  missed_events = missed_events + seq - last_seq - 1
               elseif last_seq == nil and not baseline and not ignore_first_gap and seq > 1 then
                  missed_events = missed_events + seq - 1
               end
               if not baseline then
                  write_event(e, fh)
                  observed_events = observed_events + 1
               end
               last_seq = seq
               ignore_first_gap = false
            end
         end
      else
         -- Compatibility with servers predating event_seq. The old identity
         -- de-duplication still collects events, while completeness is unknown.
         for _, e in ipairs(events) do
            local id = identity(e)
            if not seen[id] then
               seen[id] = true
               if not baseline then
                  write_event(e, fh)
                  observed_events = observed_events + 1
               end
            end
         end
      end
      if fh then fh:flush() end
   end
   remember(ring_snapshot(con), nil, true) -- 压测前基线
   local fh, err = io.open(sysbench.opt.out, "w")
   if fh == nil then die("写不了 %s：%s", sysbench.opt.out, tostring(err)) end
   local count = 0
   while true do
      local poll_ok = pcall(function() remember(ring_snapshot(con), fh, false) end)
      if not poll_ok then poll_errors = poll_errors + 1 end
      count = count + 1
      local f = io.open(stop_path, "r")
      if f then
         f:close()
         local final_ok = pcall(function() remember(ring_snapshot(con), fh, false) end)
         if not final_ok then poll_errors = poll_errors + 1 end
         break
      end
      pcall(function() con:query_row(string.format("SELECT SLEEP(%.3f)", interval / 1000)) end)
   end
   local coverage_status = "unknown"
   if sequence_supported and sequence_resets == 0 and missed_events == 0 and poll_errors == 0 then
      coverage_status = "complete"
   elseif sequence_supported and missed_events > 0 then
      coverage_status = "incomplete"
   end
   fh:write(string.format('{"kind":"ring_coverage","status":"%s","sequence_supported":%s,"observed_events":%d,"missed_events":%d,"sequence_resets":%d,"poll_errors":%d}\n',
                          coverage_status, sequence_supported and "true" or "false",
                          observed_events, missed_events, sequence_resets, poll_errors))
   fh:close()
   print(string.format("raft_monitor.ring_polls=%d coverage=%s observed=%d missed=%d resets=%d poll_errors=%d out=%s",
                       count, coverage_status, observed_events, missed_events,
                       sequence_resets, poll_errors, sysbench.opt.out))
end

--
-- summarize
--

-- 极简 ring 解析：它是 [{ "k":v, ... }, ...]，值都是标量（没有嵌套），所以按 "},{" 切开逐个取键值。
parse_ring = function(text)
   local events, coverage = {}, nil
   local function quoted(s, pos)
      if s:sub(pos, pos) ~= '"' then return nil, pos end
      local out, i = {}, pos + 1
      while i <= #s do
         local c = s:sub(i, i)
         if c == '"' then return table.concat(out), i + 1 end
         if c == "\\" and i < #s then
            i = i + 1
            local esc = s:sub(i, i)
            local map = {n="\n", r="\r", t="\t", ['"']='"', ['\\']='\\', ['/']="/"}
            out[#out + 1] = map[esc] or esc
         else
            out[#out + 1] = c
         end
         i = i + 1
      end
      return table.concat(out), i
   end
   for obj in text:gmatch("{([^{}]*)}") do
      local e, pos = {}, 1
      while pos <= #obj do
         while pos <= #obj and (obj:sub(pos, pos):match("[%s,]") ~= nil) do pos = pos + 1 end
         if pos > #obj then break end
         local key
         key, pos = quoted(obj, pos)
         if key == nil then break end
         while obj:sub(pos, pos):match("%s") do pos = pos + 1 end
         if obj:sub(pos, pos) ~= ":" then break end
         pos = pos + 1
         while obj:sub(pos, pos):match("%s") do pos = pos + 1 end
         local value
         if obj:sub(pos, pos) == '"' then
            value, pos = quoted(obj, pos)
         else
            local stop = obj:find(",", pos, true) or (#obj + 1)
            value = obj:sub(pos, stop - 1):match("^%s*(.-)%s*$")
            pos = stop
         end
         if value ~= nil and value ~= "null" then e[key] = value end
      end
      if e.stage ~= nil then
         events[#events + 1] = e
      elseif e.kind == "ring_coverage" then
         coverage = e
      end
   end
   return events, coverage
end

local function pct(sorted, p)
   if #sorted == 0 then return 0 end
   local i = math.floor(#sorted * p / 100)
   if i < 1 then i = 1 end
   if i > #sorted then i = #sorted end
   return sorted[i]
end

local function stage_stats(events)
   local by_stage, txn, seen = {}, {}, {}
   for _, e in ipairs(events) do
      local key = table.concat({e.event_epoch or "", e.event_seq or "", e.cluster_txn_id or "", e.stage or "", e.outcome or "", e.reason or "",
                                e.start_us or "", e.end_us or ""}, "|")
      if not seen[key] then
         seen[key] = true
         local d = tonumber(e.duration_us)
         if d ~= nil then
            by_stage[e.stage] = by_stage[e.stage] or {}
            table.insert(by_stage[e.stage], d)
         end
         local id = e.cluster_txn_id
         if id ~= nil then
            txn[id] = txn[id] or {}
            txn[id][e.stage] = e
         end
      end
   end
   -- proposal 结束 → local_apply 开始 的空档（"提交后等本机 apply"）
   local gaps, deliveries = {}, {}
   for _, t in pairs(txn) do
      if t.proposal and t.local_apply then
         local g = tonumber(t.local_apply.start_us) - tonumber(t.proposal.end_us)
         if g ~= nil then table.insert(gaps, g) end
      end
      if t.client_delivery then
         local d = tonumber(t.client_delivery.duration_us)
         if d ~= nil then table.insert(deliveries, d) end
      end
   end
   return by_stage, gaps, deliveries, txn
end

local function sum_of(list)
   local s = 0
   for _, v in ipairs(list) do s = s + v end
   return s
end

local function read_client_p99(path)
   local samples = {}
   if path == "" then return samples end
   for line in read_file(path):gmatch("[^\n]+") do
      local stamp, report = line:match("^([%d%.]+)\t(.*)$")
      if stamp and report then
         -- 一行里会有多个 `lat (ms,NN%): x`（实测 sysbench 默认就打 95% 和 99%）：
         -- 必须遍历所有匹配，只认 99% 那一个；取第一个匹配会拿到 95%。
         for percentile, latency in report:gmatch("lat %(ms,%s*(%d+)%%%s*%)%s*:%s*([%d%.]+)") do
            if tonumber(percentile) == 99 and tonumber(latency) then
               samples[#samples + 1] = {ts = tonumber(stamp), p99_ms = tonumber(latency)}
               break
            end
         end
      end
   end
   return samples
end

local function p99_window(samples, boundary, before)
   local candidates, selected = {}, {}
   for _, sample in ipairs(samples) do
      if (before and sample.ts < boundary) or (not before and sample.ts > boundary) then
         candidates[#candidates + 1] = sample
      end
   end
   table.sort(candidates, function(a, b)
      if before then return a.ts > b.ts else return a.ts < b.ts end
   end)
   for i = 1, math.min(5, #candidates) do
      selected[#selected + 1] = candidates[i].p99_ms
   end
   table.sort(selected)
   if #selected == 0 then return nil end
   return {n = #selected, median = selected[math.floor((#selected + 1) / 2)], max = selected[#selected]}
end

local function cmd_summarize()
   if sysbench.opt.in_file == "" then die("summarize 需要 --in-file=<TSV>") end
   local text = read_file(sysbench.opt.in_file)
   local lines = {}
   for l in text:gmatch("[^\n]+") do lines[#lines + 1] = l end
   if #lines < 2 then die("%s 里没有采样数据", sysbench.opt.in_file) end
   local header = {}
   for cell in lines[1]:gmatch("[^\t]+") do header[#header + 1] = cell end
   -- 头是 "# raft_monitor.v1 host=… port=… interval_ms=… ts_epoch <名字…>"：
   -- 用 ts_epoch 这个标记定位名字列（不要写死下标，元数据列以后可能增加）
   local names, marker, client_epoch_column = {}, nil, nil
   for i, cell in ipairs(header) do
      if cell == "ts_epoch" then marker = i end
      if cell == "client_epoch" then client_epoch_column = i end
   end
   if marker == nil then die("这不像 raft_monitor 的 TSV（头里没有 ts_epoch 标记）：%s", sysbench.opt.in_file) end
   for i = marker + 1, #header do
      if header[i] ~= "client_epoch" then names[#names + 1] = header[i] end
   end
   local has_client_epoch = client_epoch_column ~= nil
   local series, ts_all, client_ts_all = {}, {}, {}
   for _, n in ipairs(names) do series[n] = {} end
   for i = 2, #lines do
      local cells = split_tabs(lines[i])
      if #cells >= #names + 1 then
         ts_all[#ts_all + 1] = tonumber(cells[1]) or 0
         if has_client_epoch then client_ts_all[#client_ts_all + 1] = tonumber(cells[#names + 2]) end
         for j, n in ipairs(names) do series[n][#series[n] + 1] = cells[j + 1] end
      end
   end
   local n = #ts_all
   if n == 0 then die("没解析出采样行") end

   local function num(nm, i) return tonumber(series[nm] and series[nm][i] or "") end
   local function nums(nm)
      local out = {}
      if not series[nm] then return out end
      for i = 1, n do if num(nm, i) ~= nil then out[#out + 1] = num(nm, i) end end
      return out
   end
   local function delta(nm)
      local a, b = num(nm, 1), num(nm, n)
      if a == nil or b == nil then return nil end
      return b - a
   end
   local function maxlag(a, b)
      local best, at = nil, nil
      for i = 1, n do
         local x, y = num(a, i), num(b, i)
         if x ~= nil and y ~= nil then
            if best == nil or (x - y) > best then best = x - y; at = i end
         end
      end
      return best, at
   end
   local function monotonic_violations(nm)
      local v = 0
      for i = 2, n do
         local a, b = num(nm, i - 1), num(nm, i)
         if a ~= nil and b ~= nil and b < a then v = v + 1 end
      end
      return v
   end
   -- commit_index 不前进的最长连续区间（≈ 完全卡住的时长）
   local stall_max, run = 0, 0
   for i = 2, n do
      local a, b = num("Percona_raft_commit_index", i - 1), num("Percona_raft_commit_index", i)
      if a ~= nil and b ~= nil and b == a then run = run + 1; if run > stall_max then stall_max = run end
      else run = 0 end
   end
   local span = (ts_all[n] - ts_all[1])
   local client_p99 = read_client_p99(sysbench.opt.client_file)
   local clock_offsets = {}
   for i, client_ts in ipairs(client_ts_all) do
      if client_ts then clock_offsets[#clock_offsets + 1] = ts_all[i] - client_ts end
   end
   table.sort(clock_offsets)
   local client_server_offset = #clock_offsets > 0 and pct(clock_offsets, 50) or nil

   print("=== raft_monitor 汇总 ===")
   print(string.format("samples=%d span=%.1fs (%.2fs/样本) commit_index %s→%s",
                       n, span, span / math.max(n - 1, 1),
                       tostring(num("Percona_raft_commit_index", 1)),
                       tostring(num("Percona_raft_commit_index", n))))
   if series["Percona_raft_state"] then
      local ar = series["Percona_raft_applier_ready"]
      print(string.format("state: 首=%s 末=%s   serving 末=%s   applier_ready 末=%s",
                          series["Percona_raft_state"][1] or "", series["Percona_raft_state"][n] or "",
                          series["Percona_raft_serving"] and series["Percona_raft_serving"][n] or "?",
                          ar and ar[n] or "?"))
      -- applier_ready 是"该瞬间 applied == committed"（nuraft_adapter.h），是瞬时判断不是状态：
      -- 末=OFF 只说明最后一个采样点差一点点，不代表没在 apply——要和 apply_lag 一起读。
      if ar and ar[n] == "OFF" then
         print("  注：applier_ready=OFF 表示最后一个采样点 applied 尚未追平 committed（见下方 apply 滞后峰值），不是\"没在 apply\"")
      end
   end

   -- ① 三条"跟到哪儿"的滞后
   local la, la_at = maxlag("Percona_raft_commit_index", "Percona_raft_applier_progress_index")
   local ld, ld_at = maxlag("Percona_raft_commit_index", "Percona_raft_local_durable_index")
   local lq, lq_at = maxlag("Percona_raft_commit_index", "Percona_raft_durable_quorum_index")
   print("\n-- 滞后峰值（commit - 游标，条目数）--")
   local function shown(v, at) return v == nil and "n/a（该状态量在这台机器上不存在）" or tostring(v) end
   print(string.format("  applier_progress : max=%s（第 %s 个样本）", shown(la), tostring(la_at)))
   print(string.format("  local_durable    : max=%s（第 %s 个样本）", shown(ld), tostring(ld_at)))
   -- durable_quorum_index 是 leader 侧的量：follower 上恒 0，直接算 commit-0 会打出接近
   -- commit_index 的"滞后"，读起来像严重落后。这里按角色标 n/a，并说明它不是滞后。
   local dq = series["Percona_raft_durable_quorum_index"]
   local dq_flat_zero = dq ~= nil
   if dq_flat_zero then
      for i = 1, n do
         if (dq[i] or 0) ~= 0 then dq_flat_zero = false; break end
      end
   end
   if dq == nil then
      print(string.format("  durable_quorum   : %s", shown(nil, nil)))
   elseif dq_flat_zero then
      print("  durable_quorum   : n/a（本机不是 leader：该项恒为 0，不是滞后）")
   else
      print(string.format("  durable_quorum   : max=%s（第 %s 个样本）", shown(lq), tostring(lq_at)))
   end

   -- ② 等待/未定/冻结 类的增量
   -- 瞬时量的峰值另列：applying_us 的 max 才是"最长一笔 apply 用了多久"
   local au = series["Percona_raft_applier_applying_us"]
   if au ~= nil then
      local peak, at = 0, 0
      for i = 1, n do
         local v = au[i] or 0
         if v > peak then peak, at = v, i end
      end
      if peak > 0 then
         print(string.format("\n-- 瞬时峰值 --\n  applier_applying_us : max=%d µs（第 %d 个样本；应计入 apply 侧成本）",
                             peak, at))
      end
   end

   print("\n-- 计数增量（首→末）--")
   local interesting = {
      "Percona_raft_apply_barrier_entries", "Percona_raft_apply_dependency_waits",
      "Percona_raft_apply_parallel_dispatches",
      "Percona_raft_apply_slot_waits", "Percona_raft_apply_slot_wait_us",
      "Percona_raft_apply_dependency_wait_us",
      "Percona_raft_apply_dependency_wait_samples",
      "Percona_raft_apply_worker_queue_wait_samples", "Percona_raft_apply_worker_queue_wait_us",
      "Percona_raft_write_permit_waits", "Percona_raft_write_permit_wait_us",
      "Percona_raft_admission_hold_timeouts", "Percona_raft_caught_up_waits",
      "Percona_raft_caught_up_wait_us",
      "Percona_raft_client_delivery_unknown", "Percona_raft_client_proposal_uncertain",
      "Percona_raft_proposal_uncertain", "Percona_raft_proposal_unknown",
      "Percona_raft_client_apply_failed_unknown", "Percona_raft_client_wait_timeout",
      "Percona_raft_journal_write_us", "Percona_raft_journal_fsync_us",
      "Percona_raft_journal_sync_us", "Percona_raft_journal_sync_calls",
      "Percona_raft_journal_open_us", "Percona_raft_journal_close_us",
      "Percona_raft_journal_dir_sync_us", "Percona_raft_journal_batches",
      "Percona_raft_journal_batch_fsync_us", "Percona_raft_journal_batch_records",
      "Percona_raft_journal_applied_batch_records",
      "Percona_raft_journal_applied_batch_fsync_us", "Percona_raft_journal_applied_batches",
      "Percona_raft_journal_scan_us", "Percona_raft_journal_scan_calls",
      "Percona_raft_applier_apply_count", "Percona_raft_applier_apply_engine_us",
      "Percona_raft_applier_applying_us", "Percona_raft_applier_slow_applies",
      "Percona_raft_journal_compaction_successes", "Percona_raft_journal_compaction_scan_bytes",
      "Percona_raft_journal_compaction_scan_us", "Percona_raft_journal_compaction_rewrite_bytes",
      "Percona_raft_journal_compaction_rewrite_us", "Percona_raft_journal_compaction_total_us",
      "Percona_raft_freeze_deferred_by_lagging_member", "Percona_raft_binlog_freeze_deferrals",
      "Percona_raft_binlog_watermark_writes", "Percona_raft_binlog_derived_writes",
      "Percona_raft_binlog_derived_syncs", "Percona_raft_binlog_backfilled_entries",
      "Percona_raft_binlog_backfill_scanned_entries", "Percona_raft_binlog_backfill_scanned_bytes",
      "Percona_raft_binlog_derivation_prefix_losses", "Percona_raft_binlog_derivation_failures",
      "Percona_raft_wal_append_batches", "Percona_raft_wal_append_records", "Percona_raft_admission_success",
      "Percona_raft_observability_events_evicted",
      "Percona_raft_wal_writer_queue_wait_us", "Percona_raft_wal_writer_io_us",
      "Percona_raft_wal_fsync_us", "Percona_raft_wal_fsync_calls",
      "Percona_raft_wal_durability_wait_us", "Percona_raft_wal_durability_wait_calls",
      "Percona_raft_wal_snapshot_copy_calls", "Percona_raft_wal_snapshot_copy_bytes",
      "Percona_raft_durable_quorum_gate_failures",
      "Percona_raft_captured_cache_calls", "Percona_raft_envelope_sha256_calls",
      "Innodb_data_fsyncs", "Innodb_os_log_written", "Innodb_log_waits",
      "Innodb_rows_inserted", "Innodb_rows_updated", "Innodb_rows_deleted", "Innodb_rows_read",
      "Innodb_data_read", "Innodb_data_written",
   }
   for _, nm in ipairs(interesting) do
      local d = delta(nm)
      local mv = monotonic_violations(nm)
      -- 瞬时量（g）本来就会上下走（例如 applying_us 空闲时归 0），既不能首末相减，也不能
      -- 报"回退"。它们单独按峰值呈现。
      if d ~= nil and not IS_GAUGE[nm] and (d ~= 0 or mv > 0) then
         local extra = mv > 0 and string.format("  [回退 %d 次！]", mv) or ""
         print(string.format("  %-52s %+d%s", nm, d, extra))
      end
   end

   local journal_syncs = delta("Percona_raft_journal_sync_calls")
   local journal_sync_us = delta("Percona_raft_journal_sync_us")
   if journal_syncs and journal_syncs > 0 then
      local function per_sync(name)
         local value = delta(name)
         return value and string.format("%.3fms", value / journal_syncs / 1000) or "n/a"
      end
      print(string.format("\n-- journal 耐久等待链（窗口累计 / 每次 sync 均值）--\n  syncs=%d total=%s open=%s write=%s fsync=%s close=%s",
         journal_syncs, per_sync("Percona_raft_journal_sync_us"),
         per_sync("Percona_raft_journal_open_us"), per_sync("Percona_raft_journal_write_us"),
         per_sync("Percona_raft_journal_fsync_us"), per_sync("Percona_raft_journal_close_us")))
      local dir_sync = delta("Percona_raft_journal_dir_sync_us")
      if dir_sync and dir_sync > 0 then
         print(string.format("  first-create directory sync total=%.3fms (不计入每事务 sync 均值)", dir_sync / 1000))
      end
   end
   local journal_batches = delta("Percona_raft_journal_batches")
   local journal_batch_fsync = delta("Percona_raft_journal_batch_fsync_us")
   local applied_batches = delta("Percona_raft_journal_applied_batches")
   local applied_records = delta("Percona_raft_journal_applied_batch_records")
   if (journal_batches and journal_batches > 0) or (applied_batches and applied_batches > 0) then
      print(string.format("  group-commit batches=%s records/batch=%s fsync_total=%.3fms; APPLIED batches=%s records/batch=%s fsync_total=%.3fms",
         tostring(journal_batches or 0),
         journal_batches and journal_batches > 0 and string.format("%.2f", (delta("Percona_raft_journal_batch_records") or 0) / journal_batches) or "n/a",
         (journal_batch_fsync or 0) / 1000,
         tostring(applied_batches or 0),
         applied_batches and applied_batches > 0 and string.format("%.2f", (applied_records or 0) / applied_batches) or "n/a",
         (delta("Percona_raft_journal_applied_batch_fsync_us") or 0) / 1000))
   end
   local wal_records = delta("Percona_raft_wal_append_records")
   local wal_batches = delta("Percona_raft_wal_append_batches")
   local wal_queue_us = delta("Percona_raft_wal_writer_queue_wait_us")
   local wal_io_us = delta("Percona_raft_wal_writer_io_us")
   local wal_fsync_us = delta("Percona_raft_wal_fsync_us")
   local wal_fsync_calls = delta("Percona_raft_wal_fsync_calls")
   local wal_wait_us = delta("Percona_raft_wal_durability_wait_us")
   local wal_wait_calls = delta("Percona_raft_wal_durability_wait_calls")
   if (wal_records or wal_batches or wal_queue_us or wal_io_us or wal_fsync_us) then
      local function mean_ms(total_us, calls)
         if total_us == nil or calls == nil or calls <= 0 then return "n/a" end
         return string.format("%.3f", total_us / calls / 1000)
      end
      local write_only_us = nil
      if wal_io_us ~= nil and wal_fsync_us ~= nil then
         write_only_us = math.max(0, wal_io_us - wal_fsync_us)
      end
      print(string.format(
         "\n-- Raft WAL 耐久链（窗口均值）--\n  records=%s batches=%s queue/record=%sms writer-I/O/batch=%sms I/O-excl-fsync/batch≈%sms fsync/call=%sms append-durability-wait/batch=%sms",
         tostring(wal_records or 0), tostring(wal_batches or 0),
         mean_ms(wal_queue_us, wal_records), mean_ms(wal_io_us, wal_batches),
         mean_ms(write_only_us, wal_batches), mean_ms(wal_fsync_us, wal_fsync_calls),
         mean_ms(wal_wait_us, wal_wait_calls)))
   end
   local slot_waits = delta("Percona_raft_apply_slot_waits")
   local slot_wait_us = delta("Percona_raft_apply_slot_wait_us")
   local dependency_waits = delta("Percona_raft_apply_dependency_wait_samples")
   local dependency_wait_us = delta("Percona_raft_apply_dependency_wait_us")
   local queue_samples = delta("Percona_raft_apply_worker_queue_wait_samples")
   local queue_wait_us = delta("Percona_raft_apply_worker_queue_wait_us")
   if (slot_waits and slot_waits > 0) or (dependency_waits and dependency_waits > 0) or
      (queue_samples and queue_samples > 0) then
      print(string.format("\n-- applier 调度等待（窗口均值）--\n  slot: n=%s mean=%.3fms; dependency: n=%s mean=%.3fms; worker queue: n=%s mean=%.3fms max_depth=%s",
         tostring(slot_waits or 0), slot_waits and slot_waits > 0 and (slot_wait_us or 0) / slot_waits / 1000 or 0,
         tostring(dependency_waits or 0), dependency_waits and dependency_waits > 0 and (dependency_wait_us or 0) / dependency_waits / 1000 or 0,
         tostring(queue_samples or 0), queue_samples and queue_samples > 0 and (queue_wait_us or 0) / queue_samples / 1000 or 0,
         tostring(num("Percona_raft_apply_worker_queue_max", n) or 0)))
   end

   -- 按窗口秒数归一化，方便和 sysbench 的 report-interval 对齐比较。
   if span > 0 then
      local rate_names = {
         {"Queries", "queries/s"}, {"Percona_raft_wal_append_records", "WAL records/s"},
         {"Percona_raft_wal_append_batches", "WAL batches/s"}, {"Innodb_rows_inserted", "InnoDB inserts/s"},
         {"Innodb_rows_updated", "InnoDB updates/s"}, {"Innodb_rows_deleted", "InnoDB deletes/s"},
         {"Innodb_rows_read", "InnoDB rows read/s"}, {"Innodb_data_read", "InnoDB bytes read/s"},
         {"Innodb_data_written", "InnoDB bytes written/s"}, {"Innodb_data_fsyncs", "InnoDB fsync/s"},
      }
      local shown_rates = {}
      for _, item in ipairs(rate_names) do
         local d = delta(item[1])
         if d ~= nil then shown_rates[#shown_rates + 1] = string.format("%s=%.1f", item[2], d / span) end
      end
      local records, batches = delta("Percona_raft_wal_append_records"), delta("Percona_raft_wal_append_batches")
      if records and batches and batches > 0 then
         shown_rates[#shown_rates + 1] = string.format("WAL records/batch=%.2f", records / batches)
      end
      local requests, reads = delta("Innodb_buffer_pool_read_requests"), delta("Innodb_buffer_pool_reads")
      if requests and reads and requests > 0 then
         local hit = math.max(0, math.min(100, 100 * (1 - reads / requests)))
         shown_rates[#shown_rates + 1] = string.format("buffer-pool hit=%.2f%%", hit)
      end
      if #shown_rates > 0 then print("\n-- 窗口速率（首末计数差 / 采样秒数）--\n  " .. table.concat(shown_rates, "  ")) end
   end

   local cdelta = delta("Percona_raft_commit_index")
   -- ②' 瞬时量：打印首/末/最大（非零才打）——留存条目数、peer_below_compacted_start 这类"状态"
   -- 没有"增量"，只看增量会把它们全漏掉（实测踩过：journal_retained_records 一直没出现）。
   local gauges = {
      "Percona_raft_journal_retained_records", "Percona_raft_peer_below_compacted_start",
      "Percona_raft_apply_max_in_flight", "Percona_raft_caught_up_real_lag",
      "Percona_raft_apply_worker_queue_max",
      "Percona_raft_journal_compaction_last_scan_bytes",
      "Percona_raft_journal_compaction_last_scan_us",
      "Percona_raft_journal_compaction_last_rewrite_bytes",
      "Percona_raft_journal_compaction_last_rewrite_us",
      "Percona_raft_journal_compaction_last_total_us",
      "Percona_raft_journal_compaction_last_start_epoch_us",
      "Percona_raft_journal_compaction_last_end_epoch_us",
      "Percona_raft_freeze_live_member_floor", "Percona_raft_durable_quorum_members",
      "Percona_raft_leader_generation", "Percona_raft_caught_up_max_lag",
      "Innodb_buffer_pool_pages_dirty", "Innodb_buffer_pool_pages_data",
      "Percona_raft_binlog_derived_through_index", "Percona_raft_binlog_backfill_active",
      "Percona_raft_binlog_backfill_from_index", "Percona_raft_binlog_backfill_through_index",
      "Percona_raft_binlog_backfill_max_batch_bytes", "Percona_raft_binlog_backfill_last_duration_us",
      "Percona_raft_binlog_derivation_paused",
   }
   local gl = {}
   for _, nm in ipairs(gauges) do
      local v = nums(nm)
      if #v > 0 then
         local mx = v[1]
         for _, x in ipairs(v) do if x > mx then mx = x end end
         if mx ~= 0 then gl[#gl + 1] = string.format("  %-46s 首=%s 末=%s 最大=%s", nm, tostring(v[1]), tostring(v[#v]), tostring(mx)) end
      end
   end
   if #gl > 0 then
      print("\n-- 瞬时量（首/末/最大）--")
      for _, l in ipairs(gl) do print(l) end
   end
   if series["Percona_raft_local_raft_state"] then
      print("\n-- local_raft_state（含 log_start / last_log / committed / durable）--")
      print("  首: " .. (series["Percona_raft_local_raft_state"][1] or ""))
      print("  末: " .. (series["Percona_raft_local_raft_state"][n] or ""))
   end

   -- ③ ring：分阶段分布
   local gaps, deliveries = {}, {}
   if sysbench.opt.ring_file ~= "" then
      local events, ring_coverage = parse_ring(read_file(sysbench.opt.ring_file))
      local by_stage, g, d, txn = stage_stats(events)
      gaps, deliveries = g, d
      print(string.format("\n-- ring 分阶段（%d 条事件，%d 笔事务）--", #events, (function() local c = 0 for _ in pairs(txn) do c = c + 1 end return c end)()))
      if ring_coverage ~= nil then
         print(string.format("  事件覆盖：%s，已收集=%s，轮询间漏失=%s，服务重启=%s，轮询错误=%s",
                             tostring(ring_coverage.status or "unknown"),
                             tostring(ring_coverage.observed_events or #events),
                             tostring(ring_coverage.missed_events or "unknown"),
                             tostring(ring_coverage.sequence_resets or 0),
                             tostring(ring_coverage.poll_errors or 0)))
         if ring_coverage.status == "incomplete" then
            print("  警告：下列分位数只代表已收集样本；recent_events 环覆盖过旧事件，p99 可能偏低或偏高。")
         elseif ring_coverage.status == "unknown" then
            print("  注意：当前文件无法证明阶段样本完整；用新服务端和 monitor.sh ring-watch 重新采集可获得覆盖状态。")
         end
      else
         print("  事件覆盖：unknown（文件没有 ring-watch 覆盖记录）")
      end
      local stage_order = {"proposal", "proposal_batch_wait", "proposal_append_entries",
                           "proposed_sync", "committed_sync", "local_apply",
                           "applied_sync", "client_delivery", "cache_capture", "admission", "prepare"}
      for _, st in ipairs(stage_order) do
         local v = by_stage[st]
         if v ~= nil and #v > 0 then
            table.sort(v)
            print(string.format("  %-16s n=%-4d p50=%-9d p95=%-9d p99=%-9d max=%d µs", st, #v, pct(v, 50), pct(v, 95), pct(v, 99), v[#v]))
         end
      end
      if #gaps > 0 then
         table.sort(gaps)
         print(string.format("  proposal→local_apply 空档: n=%d p50=%d p95=%d p99=%d max=%d µs",
                             #gaps, pct(gaps, 50), pct(gaps, 95), pct(gaps, 99), gaps[#gaps]))
      elseif by_stage["client_delivery"] == nil and by_stage["proposal"] == nil then
         print("  注意：这份 ring 只有 apply 侧阶段（采样时本机是 follower）⇒ 看不到 proposal/client_delivery；")
         print("        要定位\"提交完成→本机 apply\"那段空档，必须看 **leader** 的那一份 ring。")
      end
   end

   -- Each row captures the most recent completed freeze. Compactions are serialized by the journal
   -- mutex; if more than one completed between samples, the counter delta shows how many occurred,
   -- while the last-* fields describe the newest one. Client p99 uses the nearest five report intervals.
   local freeze_count = delta("Percona_raft_journal_compaction_successes") or 0
   if freeze_count > 0 then
      print(string.format("\n-- Freeze journal 折叠（成功 %d 次）--", freeze_count))
      local events, missed = {}, 0
      local successes = series["Percona_raft_journal_compaction_successes"]
      for i = 2, n do
         local old, current = tonumber(successes[i - 1]), tonumber(successes[i])
         if old and current and current > old then
            local count = current - old
            if count > 1 then missed = missed + count - 1 end
            events[#events + 1] = {
               count = count,
               start_us = num("Percona_raft_journal_compaction_last_start_epoch_us", i),
               end_us = num("Percona_raft_journal_compaction_last_end_epoch_us", i),
               scan_bytes = num("Percona_raft_journal_compaction_last_scan_bytes", i),
               scan_us = num("Percona_raft_journal_compaction_last_scan_us", i),
               rewrite_bytes = num("Percona_raft_journal_compaction_last_rewrite_bytes", i),
               rewrite_us = num("Percona_raft_journal_compaction_last_rewrite_us", i),
               total_us = num("Percona_raft_journal_compaction_last_total_us", i),
            }
         end
      end
      for i, event in ipairs(events) do
         local pre, post
         if #client_p99 > 0 and event.start_us and event.end_us and client_server_offset then
            pre = p99_window(client_p99, event.start_us / 1e6 - client_server_offset, true)
            post = p99_window(client_p99, event.end_us / 1e6 - client_server_offset, false)
         end
         local function shown_window(window)
            if not window then return "n/a" end
            return string.format("median=%.3fms,max=%.3fms,n=%d", window.median, window.max, window.n)
         end
         print(string.format("  freeze#%d%s scan=%sB/%.3fms rewrite=%sB/%.3fms total=%.3fms p99_before[%s] p99_after[%s]",
            i, event.count > 1 and string.format(" (间隔内共%d次，仅最近一次有明细)", event.count) or "",
            tostring(event.scan_bytes or "n/a"), (event.scan_us or 0) / 1000,
            tostring(event.rewrite_bytes or "n/a"), (event.rewrite_us or 0) / 1000,
            (event.total_us or 0) / 1000, shown_window(pre), shown_window(post)))
      end
      if missed > 0 then
         print(string.format("  注意：采样间隔内还有 %d 次成功折叠没有单独的 last-* 明细；缩短 --interval-ms 可避免遗漏。", missed))
      end
      if #client_p99 == 0 then
         print("  客户端 p99 未采到；压测命令请带 --report-interval=1 和 --percentile=99，并把 monitor.sh 的 client-intervals.tsv 传给 --client-file。")
      elseif #events == 0 then
         print("  p99 区间不可关联：monitor 采样没有观察到成功折叠计数变化。")
      elseif not client_server_offset then
         print("  p99 区间不可关联：采样文件没有 client_epoch；请重新采集 monitor TSV。")
      end
   end

   -- ④ 判读
   print("\n=== 判读（verdict）===")
   local verdicts = {}
   local notes = {}
   -- 日志扫描占了多少墙钟时间：这个比值高说明 applier 的时间被 txn store 的线性扫描吃掉了
   -- （has_applied_record 是全量 find_if + 逐条比 envelope；read_unapplied_committed_* 还会拷 payload）
   local scan_us, scan_calls = delta("Percona_raft_journal_scan_us"), delta("Percona_raft_journal_scan_calls")
   if scan_us ~= nil and span > 0 then
      local frac = scan_us / (span * 1e6)
      local per_call = (scan_calls ~= nil and scan_calls > 0) and (scan_us / scan_calls) or nil
      print(string.format("\n-- 日志扫描开销 --\n  journal_scan_us=%d（占本样本窗口墙钟 %.1f%%），调用 %s 次%s",
                          scan_us, frac * 100, tostring(scan_calls),
                          per_call and string.format("（≈%.1fms/次）", per_call / 1000) or ""))
      if frac > 0.3 then
         verdicts[#verdicts + 1] = string.format(
            "JOURNAL_SCAN_DOMINANT：journal_scan 吃掉本窗口 %.0f%% 的墙钟（%s 次调用，≈%.1fms/次）" ..
            " ⇒ txn store 的线性扫描/拷贝是主因，不是 commit gate、也不是磁盘 fsync。" ..
            " 缓解方向：缩小条目（--batch 调小）、让留存条目变少（推进回收 / 修好留存最多的那台），" ..
            "再看 --percona-raft-snapshot-reserved-log-items",
            frac * 100, tostring(scan_calls), per_call and per_call / 1000 or 0)
      end
   end
   if delta("Percona_raft_client_delivery_unknown") and delta("Percona_raft_client_delivery_unknown") > 0 then
      verdicts[#verdicts + 1] = "UNKNOWN_OUTCOMES：采样期间出现「结果未定」的交付（客户端会拿到 3197），" ..
         "按 ring 里 client_delivery=UNKNOWN 的那几笔反查 local_apply 是否 SUCCESS（SUCCESS = 已落盘，别重灌）"
   end
   if delta("Percona_raft_apply_barrier_entries") and delta("Percona_raft_apply_barrier_entries") > 0 then
      verdicts[#verdicts + 1] = "COMMIT_GATE_QUEUE：applier 在等提交轮次（apply_barrier_entries 在涨）"
   end
   if delta("Percona_raft_apply_dependency_waits") and delta("Percona_raft_apply_dependency_waits") > 0 then
      verdicts[#verdicts + 1] = "APPLY_DEPENDENCY_WAIT：并行 apply 在等更低索引（apply_dependency_waits 在涨）"
   end
   if la ~= nil and la > tonumber(sysbench.opt.lag_warn) then
      verdicts[#verdicts + 1] = string.format("APPLIER_LAG：commit 领先 applier_progress 峰值 %d 条" ..
         "（本机 apply 跟不上 ⇒ 看 applier_apply_engine_us 与磁盘）", la)
   end
   if ld ~= nil and ld > tonumber(sysbench.opt.lag_warn) then
      verdicts[#verdicts + 1] = string.format("DURABLE_LAG：commit 领先 local_durable 峰值 %d 条" ..
         "（本机日志写/fsync 跟不上 ⇒ 看 journal_* 与磁盘）", ld)
   end
   -- durable_quorum 是 leader 侧的量：follower 上它一直是 0，拿它算滞后就是误报（本工具第一版
   -- 就在自己的 lab 上报出过 "QUORUM_LAG 348" 这种假警）。所以只在本机是 leader 时判。
   local is_leader = series["Percona_raft_state"] and series["Percona_raft_state"][n] == "LEADER"
   if is_leader and lq ~= nil and lq > tonumber(sysbench.opt.lag_warn) then
      verdicts[#verdicts + 1] = string.format("QUORUM_LAG：commit 领先 durable_quorum 峰值 %d 条" ..
         "（有成员的持久化确认慢 ⇒ 看那台的磁盘/网络）", lq)
   end
   if delta("Percona_raft_write_permit_waits") and delta("Percona_raft_write_permit_waits") > 0 then
      verdicts[#verdicts + 1] = "PERMIT_QUEUE：写许可在排队（write_permit_waits 在涨 ⇒ 并发数不够或写太慢）"
   end
   if delta("Percona_raft_wal_snapshot_copy_calls") and delta("Percona_raft_wal_snapshot_copy_calls") > 0 then
      notes[#notes + 1] = "SNAPSHOT_COPY：采样期间发生了快照拷贝（回收超过某成员所需；刚启动过集群时属正常）"
   end
   if delta("Percona_raft_freeze_deferred_by_lagging_member") and delta("Percona_raft_freeze_deferred_by_lagging_member") > 0 then
      notes[#notes + 1] = "RECLAIM_DEFERRED：日志回收被落后成员推迟（不挡 apply，只拖回收）"
   end
   if #gaps > 0 then
      table.sort(gaps)
      local g95 = pct(gaps, 95)
      if g95 > 1000000 then
         verdicts[#verdicts + 1] = string.format("POST_COMMIT_WAIT：proposal→local_apply 空档 p95=%.1fs" ..
            "（提交已完成、本机迟迟不 apply ⇒ 这条腿是主因）", g95 / 1e6)
      elseif g95 > 100000 then
         verdicts[#verdicts + 1] = string.format("POST_COMMIT_WAIT(轻)：proposal→local_apply 空档 p95=%.0fms", g95 / 1e3)
      end
   end
   -- 只有"确实提交过条目"时才把"commit_index 长时间不动"当异常：空载时它当然不动
   -- （本工具第一版就在空载下误报过 COMMIT_STALLED）。
   if stall_max >= 3 and cdelta ~= nil and cdelta > 0 then
      verdicts[#verdicts + 1] = string.format("COMMIT_STALLED：commit_index 连续 %d 个采样点没前进（≈%.1fs），" ..
         "期间集群整体没提交新条目", stall_max, stall_max * (span / math.max(n - 1, 1)))
   elseif stall_max >= 3 then
      notes[#notes + 1] = string.format("采样窗口内 commit_index 一直没变（%d 个采样点）——这次没跑出提交量，判读仅供参考",
                                        stall_max)
   end
   if #verdicts == 0 then
      print("  NOTHING_SEEN：采样期间本机侧没有异常（滞后峰值 < 阈值、等待/未定类无增量）。")
      print("  下一步：把三台的报告并排看（尤其 follower 的 APPLIER_LAG），")
      print("          并在 leader 上同时跑 iostat -x 1 看磁盘；网络侧看 ping/重传。")
   else
      for i, v in ipairs(verdicts) do
         print(string.format("  [%d] %s", i, v))
      end
   end
   if #notes > 0 then
      print("  （提示项，不是判据）")
      for _, v in ipairs(notes) do print("   - " .. v) end
   end

   -- 机器可读的一行：wrapper 用它把三台并排打出来（放在最后，免得插进判读段）
   local host, port = "?", "?"
   for i = 2, #header do
      if header[i]:match("^host=") then host = header[i]:sub(6) end
      if header[i]:match("^port=") then port = header[i]:sub(6) end
      end
   local cdelta = delta("Percona_raft_commit_index")
   -- durable_quorum 是 leader 侧的量：follower 上恒 0，标成 n/a(follower) 免得被误读成"滞后"
   local quorum_shown = is_leader and tostring(lq) or "n/a(follower)"
   print(string.format("\nraft_monitor.result host=%s port=%s samples=%d commit_delta=%s max_apply_lag=%s max_durable_lag=%s max_quorum_lag=%s post_commit_gap_p95_us=%s unknown_delta=%s verdicts=%d",
                          host, port, n, tostring(cdelta), tostring(la), tostring(ld), quorum_shown,
                          #gaps > 0 and tostring(pct(gaps, 95)) or "n/a",
                          tostring(delta("Percona_raft_client_delivery_unknown")), #verdicts))
end

-- 自定义命令必须**注册**在这里由 sysbench 调用：加载时直接调用的话 sysbench.opt 还没填
-- （实测 "attempt to index field 'opt' (a nil value)"）。
sysbench.cmdline.commands = {
   sample = {cmd_sample},
   ring = {cmd_ring},
   ["ring-watch"] = {cmd_ring_watch},
   summarize = {cmd_summarize},
}

   -- 机器可读的一行：wrapper 用它把三台并排打出来


