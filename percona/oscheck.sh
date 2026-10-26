#!/usr/bin/env bash
#
# percona/oscheck.sh —— 压测前/后核对 OS 层配置（只读，不改任何东西）
#
# 为什么需要它：受管写路径的尾延迟由三样东西决定 —— 单个事务的 fsync、
# 成员之间的网络 RTT、以及 CPU 是否真的被让给了 mysqld/asio。这些都在 OS 层，
# MySQL 状态量看不到；而 host-metrics（Node Exporter）只给运行期曲线，不给配置。
#
# 用法：
#   bash percona/oscheck.sh [--json]        # 在每台成员、以及压测机上各跑一次
#
# 输出按"能不能影响结论"排序，末尾给 WARN 列表。全部只读；部分 /sys 项需要 root
# （缺权限时会打印 n/a，不报错）。
#
set -uo pipefail

JSON=0
[[ "${1:-}" == "--json" ]] && JSON=1

warn=()
note() { printf '  %-34s %s\n' "$1" "$2"; }
section() { printf '\n== %s ==\n' "$1"; }
have() { command -v "$1" >/dev/null 2>&1; }
rd() { cat "$1" 2>/dev/null || echo n/a; }

printf '# oscheck  %s  on %s\n' "$(date '+%F %T')" "$(hostname)"

if [[ ! -d /proc ]] || ! have lscpu; then
  echo
  echo "注意：本脚本面向 Linux（读 /proc、/sys、sysctl、lsblk、ethtool）。"
  echo "      当前不是 Linux（或缺少 lscpu），大量条目会是 n/a —— 请在成员机与压测机上各跑一次。"
fi

section "1. 机器与虚拟化"
note "kernel" "$(uname -r)"
note "virtualization" "$(systemd-detect-virt 2>/dev/null || echo n/a)"
note "cpu model" "$(awk -F: '/model name/{gsub(/^ +/,"",$2); print $2; exit}' /proc/cpuinfo 2>/dev/null)"
note "sockets/cores/threads" "$(lscpu 2>/dev/null | awk -F: '/Socket|Core\(s\) per socket|Thread\(s\) per core/{gsub(/^ +/,"",$2); printf "%s ", $2}'; echo)"
note "numa nodes" "$(lscpu 2>/dev/null | awk -F: '/NUMA node\(s\)/{gsub(/^ +/,"",$2); print $2}')"
note "load / steal" "$(awk '{printf "1m=%.2f ", $1}' /proc/loadavg 2>/dev/null)$(awk '/^cpu /{printf "steal=%.2f%% ", $9*100/($2+$3+$4+$5+$6+$7+$8+$9+$10+$11)}' /proc/stat 2>/dev/null)"
[[ "${HOSTNAME:-$(hostname)}" == "" ]] || true

section "2. CPU 频率 / 调度 / 隔离（Raft 心跳与 asio 线程最怕被饿死）"
gov="$(rd /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"
note "governor (cpu0)" "$gov"
[[ "$gov" == "performance" ]] || warn+=("cpufreq governor=${gov}（建议 performance，避免频率抖动拉长 fsync 尾延迟）")
note "energy_perf_policy" "$(rd /sys/devices/system/cpu/cpu0/cpufreq/energy_performance_preference)"
note "isolcpus" "$(grep -o 'isolcpus=[^ ]*' /proc/cmdline 2>/dev/null || echo none)"
note "nohz_full" "$(grep -o 'nohz_full=[^ ]*' /proc/cmdline 2>/dev/null || echo none)"
if compgen -G "/sys/devices/system/cpu/cpu0/cpuidle/state*/disable" >/dev/null; then
  note "cpuidle disabled?" "$(for s in /sys/devices/system/cpu/cpu0/cpuidle/state*/disable; do printf '%s ' "$(cat "$s" 2>/dev/null)"; done)"
else
  note "cpuidle disabled?" "n/a（容器/VM 里通常没有；裸机上全 1 = 已关 C-state）"
fi
note "IRQ count" "$(grep -c '^ *[0-9]\+:' /proc/interrupts 2>/dev/null || echo n/a)"
echo "  （mysqld 的 asio 线程可用 --percona-raft-asio-affinity 钉核；钉了之后这里应能看到 isolcpus/nohz_full 与它不是同一批核）"

section "3. 内存"
note "MemTotal / MemAvailable" "$(awk '/MemTotal|MemAvailable/{printf "%s=%dMiB ", $1, $2/1024}' /proc/meminfo 2>/dev/null)"
note "vm.swappiness" "$(sysctl -n vm.swappiness 2>/dev/null || echo n/a)"
sw="$(sysctl -n vm.swappiness 2>/dev/null || echo 0)"
[[ "${sw:-0}" -le 10 ]] || warn+=("vm.swappiness=${sw}（建议 1-10；换出会直接放大 fsync 与 apply 尾延迟）")
note "vm.overcommit_memory" "$(sysctl -n vm.overcommit_memory 2>/dev/null || echo n/a)"
note "vm.dirty_ratio / bg" "$(sysctl -n vm.dirty_ratio 2>/dev/null)/$(sysctl -n vm.dirty_background_ratio 2>/dev/null)"
note "vm.dirty_expire_centisecs" "$(sysctl -n vm.dirty_expire_centisecs 2>/dev/null || echo n/a)"
note "THP enabled" "$(rd /sys/kernel/mm/transparent_hugepage/enabled)"
note "THP defrag" "$(rd /sys/kernel/mm/transparent_hugepage/defrag)"
thp="$(rd /sys/kernel/mm/transparent_hugepage/enabled)"
[[ "$thp" == *"[never]"* ]] || warn+=("THP=${thp}（建议 never；khugepaged/compaction 会造成毫秒级卡顿，落在 apply 上就是长尾）")
note "zone_reclaim_mode" "$(rd /proc/sys/vm/zone_reclaim_mode)"

section "4. 存储（datadir 的 fsync 决定单事务下限）"
note "datadir" "$(mysql -Nse 'SELECT @@datadir' 2>/dev/null || echo 'n/a（本机没有 mysql 客户端；可直接看 my.cnf 的 datadir）')"
echo "  -- 块设备 --"
lsblk -d -o NAME,ROTA,SCHED,SIZE,MODEL 2>/dev/null | sed 's/^/  /'
for d in /sys/block/*/queue/scheduler; do
  [[ -w "$d" || -r "$d" ]] || continue
  dev=$(echo "$d" | awk -F/ '{print $(NF-2)}')
  case "$dev" in loop*|ram*|dm-*|md*) continue;; esac
  note "scheduler[$dev]" "$(cat "$d" 2>/dev/null)"
  note "nr_requests[$dev]" "$(cat "/sys/block/$dev/queue/nr_requests" 2>/dev/null)"
  note "read_ahead_kb[$dev]" "$(cat "/sys/block/$dev/queue/read_ahead_kb" 2>/dev/null)"
done
sch="$(cat /sys/block/sda/queue/scheduler 2>/dev/null || true)"
[[ "$sch" == *"[none]"* || "$sch" == *"[mq-deadline]"* ]] || warn+=("block scheduler=${sch}（NVMe/SSD 建议 none/mq-deadline；cfq/bfq 会引入不确定延迟）")
echo "  -- 挂载 --"
findmnt -no TARGET,SOURCE,FSTYPE,OPTIONS -T "$(mysql -Nse 'SELECT @@datadir' 2>/dev/null || echo /)" 2>/dev/null | sed 's/^/  /'
have xfs_info && note "xfs_info" "$(xfs_info "$(mysql -Nse 'SELECT @@datadir' 2>/dev/null || echo /)" 2>/dev/null | head -3 | tr '\n' ' ')"
note "fs 空间/ inode" "$(df -h / | awk 'NR==2{printf "use=%s avail=%s ", $5, $4}')$(df -i / | awk 'NR==2{printf "inode_use=%s", $5}')"

section "5. 网络（成员间 RTT 与重传直接进提交尾延迟）"
iface="$(ip route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1); exit}}')"
note "default iface" "${iface:-n/a}"
[[ -n "${iface:-}" ]] && have ethtool && {
  note "speed/duplex" "$(ethtool "$iface" 2>/dev/null | awk -F: '/Speed|Duplex/{gsub(/^ +/,"",$2); printf "%s ", $2}')"
  note "MTU" "$(cat "/sys/class/net/$iface/mtu" 2>/dev/null)"
  note "rx/tx queues" "$(ls "/sys/class/net/$iface/queues" 2>/dev/null | grep -c '^rx-')/$(ls "/sys/class/net/$iface/queues" 2>/dev/null | grep -c '^tx-')"
  note "offloads(tso/gso/gro)" "$(ethtool -k "$iface" 2>/dev/null | awk -F: '/^(tcp-segmentation-offload|generic-segmentation-offload|generic-receive-offload)/{gsub(/^ +/,"",$2); printf "%s=%s ", $1, $2}')"
}
note "tcp_congestion_control" "$(sysctl -n net.ipv4.tcp_congestion_control 2>/dev/null || echo n/a)"
note "tcp_slow_start_after_idle" "$(sysctl -n net.ipv4.tcp_slow_start_after_idle 2>/dev/null || echo n/a)"
note "tcp_notsent_lowat" "$(sysctl -n net.ipv4.tcp_notsent_lowat 2>/dev/null || echo n/a)"
note "somaxconn / backlog" "$(sysctl -n net.core.somaxconn 2>/dev/null)/$(sysctl -n net.core.netdev_max_backlog 2>/dev/null)"
note "tcp_rmem/wmem" "$(sysctl -n net.ipv4.tcp_rmem 2>/dev/null) | $(sysctl -n net.ipv4.tcp_wmem 2>/dev/null)"
retr="$(awk '/^Tcp:/{print $12}' /proc/net/snmp 2>/dev/null | tail -1)"
note "tcp RetransSegs (total)" "${retr:-n/a}"
note "tc qdisc" "$(tc qdisc show dev "${iface:-lo}" 2>/dev/null | head -2 | tr '\n' ' ')"
echo "  -- 成员间 RTT（在每台上对另两台各 ping 20 次，看 p99/max）--"
echo "     ping -c 20 -i 0.2 <member2> | tail -2 ; ping -c 20 -i 0.2 <member3> | tail -2"

section "6. 进程/文件限制"
note "nofile (this shell)" "$(ulimit -n)"
note "fs.file-max" "$(sysctl -n fs.file-max 2>/dev/null || echo n/a)"
note "pid_max / threads-max" "$(cat /proc/sys/kernel/pid_max 2>/dev/null)/$(cat /proc/sys/kernel/threads-max 2>/dev/null)"
echo "  mysqld 的实际限制：cat /proc/\$(pgrep -o mysqld)/limits | grep -E 'open files|max processes'"

section "7. 时间同步（monitor 的 client_epoch 与 ts_epoch 对齐、以及结论可信度）"
note "timedatectl" "$(timedatectl show -p NTPSynchronized --value 2>/dev/null || echo n/a)"
note "chrony tracking" "$(chronyc tracking 2>/dev/null | awk -F: '/System time|Last offset|RMS offset/{gsub(/^ +/,"",$2); printf "%s ", $2}' || echo n/a)"

section "8. WARN"
if [[ ${#warn[@]} -eq 0 ]]; then
  echo "  （无）"
else
  for w in "${warn[@]}"; do echo "  - $w"; done
fi

if [[ "$JSON" -eq 1 ]]; then
  printf '\n# json: {"host":"%s","warnings":%d}\n' "$(hostname)" "${#warn[@]}"
fi
