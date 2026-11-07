#!/usr/bin/env python3
"""Poll Node Exporter or Prometheus for host metrics during a benchmark."""
import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

METRIC_NAMES = (
    "node_cpu_seconds_total|node_memory_MemAvailable_bytes|"
    "node_load1|node_procs_running|node_procs_blocked|node_context_switches_total|"
    "node_schedstat_waiting_seconds_total|node_pressure_cpu_waiting_seconds_total|"
    "node_disk_read_bytes_total|node_disk_written_bytes_total|"
    "node_disk_reads_completed_total|node_disk_writes_completed_total|"
    "node_disk_io_time_seconds_total|node_disk_read_time_seconds_total|"
    "node_disk_write_time_seconds_total|node_network_receive_bytes_total|"
    "node_network_transmit_bytes_total|node_network_receive_packets_total|"
    "node_network_transmit_packets_total|node_network_receive_errs_total|"
    "node_network_receive_fifo_total|node_network_receive_frame_total|"
    "node_network_transmit_errs_total|node_network_receive_drop_total|"
    "node_network_transmit_fifo_total|node_network_transmit_carrier_total|"
    "node_network_transmit_colls_total|"
    "node_network_transmit_drop_total|node_network_carrier|"
    "node_network_carrier_changes_total|node_network_carrier_up_changes_total|"
    "node_network_carrier_down_changes_total|node_network_speed_bytes|"
    "node_network_mtu_bytes|node_network_transmit_queue_length|"
    "node_netstat_Tcp_RetransSegs|node_netstat_Tcp_ActiveOpens|"
    "node_netstat_Tcp_PassiveOpens|node_netstat_Tcp_AttemptFails|"
    "node_netstat_Tcp_EstabResets|node_netstat_Tcp_OutRsts|"
    "node_netstat_Tcp_InErrs|node_netstat_Tcp_InSegs|node_netstat_Tcp_OutSegs|"
    "node_netstat_Tcp_CurrEstab|node_netstat_TcpExt_ListenDrops|"
    "node_netstat_TcpExt_ListenOverflows|node_netstat_TcpExt_TCPSynRetrans|"
    "node_netstat_TcpExt_TCPRcvQDrop|node_netstat_TcpExt_TCPBacklogDrop|"
    "node_netstat_TcpExt_SyncookiesSent|node_netstat_TcpExt_SyncookiesRecv|"
    "node_netstat_TcpExt_SyncookiesFailed|node_netstat_TcpExt_TCPReqQFullDoCookies|"
    "node_netstat_TcpExt_TCPReqQFullDrop|"
    "node_netstat_TcpExt_TCPTimeouts|node_netstat_TcpExt_TCPAbortOnData|"
    "node_netstat_TcpExt_TCPAbortOnClose|node_netstat_TcpExt_TCPAbortOnMemory|"
    "node_netstat_TcpExt_TCPAbortOnTimeout|node_netstat_TcpExt_TCPMemoryPressures|"
    "node_sockstat_TCP_inuse|node_sockstat_TCP_orphan|node_sockstat_TCP_tw|"
    "node_sockstat_TCP_alloc|node_sockstat_TCP_mem|"
    "node_softnet_processed_total|node_softnet_dropped_total|"
    "node_softnet_times_squeezed_total|node_softnet_cpu_collision_total|"
    "node_softnet_flow_limit_count_total|node_softnet_received_rps_total|"
    "node_softnet_backlog_len|"
    "node_nf_conntrack_entries|node_nf_conntrack_entries_limit"
)
LINE_RE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+([^\s]+)(?:\s+\d+)?$')
LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"')


def unescape_label(value):
    return re.sub(r'\\([\\"n])', lambda m: {"n": "\n", "\\": "\\", '"': '"'}[m.group(1)], value)


def parse_exposition(body):
    result = []
    for line in body.splitlines():
        match = LINE_RE.match(line)
        if not match:
            continue
        name, labels_text, raw_value = match.groups()
        try:
            value = float(raw_value)
        except ValueError:
            continue
        labels = {}
        if labels_text:
            labels = {key: unescape_label(value) for key, value in LABEL_RE.findall(labels_text)}
        result.append((name, labels, value))
    return result


def parse_prometheus(body):
    document = json.loads(body)
    if document.get("status") != "success":
        raise RuntimeError("Prometheus API returned a non-success status")
    result = []
    for sample in document.get("data", {}).get("result", []):
        labels = sample.get("metric", {})
        name = labels.get("__name__")
        value = sample.get("value", [None, None])
        if not name or len(value) < 2:
            continue
        try:
            result.append((name, labels, float(value[1])))
        except (TypeError, ValueError):
            continue
    return result


def fetch(args):
    if args.source == "node-exporter":
        request_url = args.url.rstrip("/") + "/metrics"
        with urllib.request.urlopen(request_url, timeout=args.timeout) as response:
            samples = parse_exposition(response.read().decode("utf-8", "replace"))
        if not samples:
            raise RuntimeError("endpoint returned no Prometheus metrics")
        return samples, time.time()
    escaped_instance = args.instance.replace("\\", "\\\\").replace('"', '\\"')
    query = '{__name__=~"' + METRIC_NAMES + '",instance="' + escaped_instance + '"}'
    request_url = args.url.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(request_url, timeout=args.timeout) as response:
        body = response.read().decode("utf-8", "replace")
    samples = parse_prometheus(body)
    if not samples:
        raise RuntimeError("Prometheus returned no series for instance=" + args.instance)
    source_times = []
    try:
        source_times = [float(x["value"][0]) for x in json.loads(body).get("data", {}).get("result", [])]
    except (ValueError, TypeError, KeyError):
        pass
    return samples, max(source_times) if source_times else time.time()


def grouped(samples, metric):
    return [(labels, value) for name, labels, value in samples if name == metric]


def counter_delta(current, previous):
    if previous is None:
        return 0.0
    return max(0.0, current - previous)


def rate_sum(samples, old_samples, metric, label_name, elapsed, excluded=()):
    now = {labels.get(label_name, ""): value for labels, value in grouped(samples, metric)
           if labels.get(label_name, "") not in excluded}
    old = {labels.get(label_name, ""): value for labels, value in grouped(old_samples or [], metric)
           if labels.get(label_name, "") not in excluded}
    return sum(counter_delta(value, old.get(key)) for key, value in now.items()) / elapsed if elapsed > 0 else 0.0


def counter_delta_sum(samples, old_samples, metric):
    current = {tuple(sorted(labels.items())): value for labels, value in grouped(samples, metric)}
    previous = {tuple(sorted(labels.items())): value for labels, value in grouped(old_samples or [], metric)}
    if not current or not previous:
        return None
    return sum(counter_delta(value, previous.get(key)) for key, value in current.items())


def gauge_sum(samples, metric):
    values = grouped(samples, metric)
    return sum(value for _, value in values) if values else None


def summarize(samples, previous, elapsed):
    previous = previous or []
    cpu_now = {(labels.get("cpu", ""), labels.get("mode", "")): value
               for labels, value in grouped(samples, "node_cpu_seconds_total")}
    cpu_old = {(labels.get("cpu", ""), labels.get("mode", "")): value
               for labels, value in grouped(previous, "node_cpu_seconds_total")}
    cpu_delta = {key: counter_delta(value, cpu_old.get(key)) for key, value in cpu_now.items()}
    cpu_total = sum(value for (cpu, mode), value in cpu_delta.items() if mode not in ("guest", "guest_nice"))
    idle = sum(value for (cpu, mode), value in cpu_delta.items() if mode == "idle")
    iowait = sum(value for (cpu, mode), value in cpu_delta.items() if mode == "iowait")
    busy_pct = max(0.0, min(100.0, 100.0 * (cpu_total - idle - iowait) / cpu_total)) if cpu_total else 0.0
    iowait_pct = max(0.0, min(100.0, 100.0 * iowait / cpu_total)) if cpu_total else 0.0

    mem = grouped(samples, "node_memory_MemAvailable_bytes")
    mem_available = mem[0][1] if mem else None

    def gauge(metric):
        values = grouped(samples, metric)
        return values[0][1] if values else None

    load1 = gauge("node_load1")
    procs_running = gauge("node_procs_running")
    procs_blocked = gauge("node_procs_blocked")
    context_switch_rate = (rate_sum(samples, previous, "node_context_switches_total", "", elapsed)
                           if grouped(samples, "node_context_switches_total") else None)
    sched_wait_rate = (rate_sum(samples, previous, "node_schedstat_waiting_seconds_total", "cpu", elapsed)
                       if grouped(samples, "node_schedstat_waiting_seconds_total") else None)
    cpu_psi_rate = (rate_sum(samples, previous, "node_pressure_cpu_waiting_seconds_total", "", elapsed)
                    if grouped(samples, "node_pressure_cpu_waiting_seconds_total") else None)

    skipped_disks = ("loop", "ram", "dm-", "md", "zram", "sr")
    disk_devices = set()
    disk_read_bytes = disk_write_bytes = read_ops = write_ops = 0.0
    disk_util_max = disk_await_max = 0.0
    metrics = (
        ("node_disk_read_bytes_total", "node_disk_written_bytes_total"),
        ("node_disk_reads_completed_total", "node_disk_writes_completed_total"),
    )
    now_by_name = {name: {labels.get("device", ""): value for labels, value in grouped(samples, name)}
                   for pair in metrics for name in pair}
    old_by_name = {name: {labels.get("device", ""): value for labels, value in grouped(previous, name)}
                   for pair in metrics for name in pair}
    io_names = ("node_disk_io_time_seconds_total", "node_disk_read_time_seconds_total", "node_disk_write_time_seconds_total")
    for name in io_names:
        now_by_name[name] = {labels.get("device", ""): value for labels, value in grouped(samples, name)}
        old_by_name[name] = {labels.get("device", ""): value for labels, value in grouped(previous, name)}
    devices = set()
    for values in now_by_name.values():
        devices.update(values)
    def whole_disk(device):
        if not device or device.startswith(skipped_disks):
            return False
        if re.match(r"^(sd[a-z]+|vd[a-z]+|xvd[a-z]+)\d+$", device):
            return False
        if re.match(r"^(nvme\d+n\d+p|mmcblk\d+p)\d+$", device):
            return False
        return True

    devices = {d for d in devices if whole_disk(d)}
    disk_devices.update(devices)
    for device in devices:
        read_bytes = now_by_name.get("node_disk_read_bytes_total", {}).get(device)
        write_bytes = now_by_name.get("node_disk_written_bytes_total", {}).get(device)
        if read_bytes is not None:
            disk_read_bytes += counter_delta(read_bytes, old_by_name.get("node_disk_read_bytes_total", {}).get(device))
        if write_bytes is not None:
            disk_write_bytes += counter_delta(write_bytes, old_by_name.get("node_disk_written_bytes_total", {}).get(device))
        reads = counter_delta(now_by_name.get("node_disk_reads_completed_total", {}).get(device, 0.0),
                              old_by_name.get("node_disk_reads_completed_total", {}).get(device))
        writes = counter_delta(now_by_name.get("node_disk_writes_completed_total", {}).get(device, 0.0),
                               old_by_name.get("node_disk_writes_completed_total", {}).get(device))
        read_ops += reads
        write_ops += writes
        io_delta = counter_delta(now_by_name.get("node_disk_io_time_seconds_total", {}).get(device, 0.0),
                                 old_by_name.get("node_disk_io_time_seconds_total", {}).get(device))
        disk_util_max = max(disk_util_max, 100.0 * io_delta / elapsed if elapsed > 0 else 0.0)
        read_time = counter_delta(now_by_name.get("node_disk_read_time_seconds_total", {}).get(device, 0.0),
                                  old_by_name.get("node_disk_read_time_seconds_total", {}).get(device))
        write_time = counter_delta(now_by_name.get("node_disk_write_time_seconds_total", {}).get(device, 0.0),
                                   old_by_name.get("node_disk_write_time_seconds_total", {}).get(device))
        if reads + writes > 0:
            disk_await_max = max(disk_await_max, 1000.0 * (read_time + write_time) / (reads + writes))
    disk_read_rate = disk_read_bytes / elapsed if elapsed > 0 else 0.0
    disk_write_rate = disk_write_bytes / elapsed if elapsed > 0 else 0.0
    if not now_by_name.get("node_disk_read_bytes_total"):
        disk_read_rate = None
    if not now_by_name.get("node_disk_written_bytes_total"):
        disk_write_rate = None

    def include_interface(name):
        return name != "lo" and not name.startswith(("veth", "docker", "br-", "cni", "flannel", "cali", "virbr", "tun", "tap"))

    net_rx = rate_sum(samples, previous, "node_network_receive_bytes_total", "device", elapsed,
                      tuple(name for labels, _ in grouped(samples, "node_network_receive_bytes_total")
                            for name in [labels.get("device", "")] if not include_interface(name)))
    net_tx = rate_sum(samples, previous, "node_network_transmit_bytes_total", "device", elapsed,
                      tuple(name for labels, _ in grouped(samples, "node_network_transmit_bytes_total")
                            for name in [labels.get("device", "")] if not include_interface(name)))
    drops = 0.0
    for metric in ("node_network_receive_drop_total", "node_network_transmit_drop_total"):
        current = {labels.get("device", ""): value for labels, value in grouped(samples, metric)
                   if include_interface(labels.get("device", ""))}
        old = {labels.get("device", ""): value for labels, value in grouped(previous, metric)
               if include_interface(labels.get("device", ""))}
        drops += sum(counter_delta(value, old.get(device)) for device, value in current.items())
    retrans_delta = counter_delta_sum(samples, previous, "node_netstat_Tcp_RetransSegs")

    tcp_counter_metrics = (
        "node_netstat_Tcp_ActiveOpens", "node_netstat_Tcp_PassiveOpens",
        "node_netstat_Tcp_AttemptFails", "node_netstat_Tcp_EstabResets",
        "node_netstat_Tcp_OutRsts", "node_netstat_Tcp_InErrs",
        "node_netstat_Tcp_InSegs", "node_netstat_Tcp_OutSegs",
        "node_netstat_TcpExt_ListenDrops", "node_netstat_TcpExt_ListenOverflows",
        "node_netstat_TcpExt_TCPRcvQDrop", "node_netstat_TcpExt_TCPBacklogDrop",
        "node_netstat_TcpExt_SyncookiesSent", "node_netstat_TcpExt_SyncookiesRecv",
        "node_netstat_TcpExt_SyncookiesFailed", "node_netstat_TcpExt_TCPReqQFullDoCookies",
        "node_netstat_TcpExt_TCPReqQFullDrop",
        "node_netstat_TcpExt_TCPSynRetrans", "node_netstat_TcpExt_TCPTimeouts",
        "node_netstat_TcpExt_TCPAbortOnData", "node_netstat_TcpExt_TCPAbortOnClose",
        "node_netstat_TcpExt_TCPAbortOnMemory", "node_netstat_TcpExt_TCPAbortOnTimeout",
        "node_netstat_TcpExt_TCPMemoryPressures",
    )
    tcp_counter_deltas = [counter_delta_sum(samples, previous, metric) for metric in tcp_counter_metrics]
    softnet_counter_metrics = (
        "node_softnet_processed_total", "node_softnet_dropped_total",
        "node_softnet_times_squeezed_total", "node_softnet_cpu_collision_total",
        "node_softnet_flow_limit_count_total", "node_softnet_received_rps_total",
    )
    softnet_counter_deltas = [counter_delta_sum(samples, previous, metric)
                              for metric in softnet_counter_metrics]
    tcp_gauge_metrics = (
        "node_netstat_Tcp_CurrEstab", "node_sockstat_TCP_inuse",
        "node_sockstat_TCP_orphan", "node_sockstat_TCP_tw", "node_sockstat_TCP_alloc",
        "node_sockstat_TCP_mem", "node_nf_conntrack_entries",
        "node_nf_conntrack_entries_limit",
    )
    tcp_gauges = [gauge_sum(samples, metric) for metric in tcp_gauge_metrics]
    softnet_backlog = gauge_sum(samples, "node_softnet_backlog_len")

    # Keep the per-interface counters instead of collapsing them into one host total.
    # A total drop count cannot identify a bad NIC, and packet/error deltas expose
    # link problems that byte rates and Tcp_RetransSegs alone miss.
    network_metrics = (
        ("node_network_receive_bytes_total", "rx_bytes_delta", "delta"),
        ("node_network_receive_bytes_total", "rx_Bps", "rate"),
        ("node_network_transmit_bytes_total", "tx_bytes_delta", "delta"),
        ("node_network_transmit_bytes_total", "tx_Bps", "rate"),
        ("node_network_receive_packets_total", "rx_packets_delta", "delta"),
        ("node_network_receive_packets_total", "rx_pps", "rate"),
        ("node_network_transmit_packets_total", "tx_packets_delta", "delta"),
        ("node_network_transmit_packets_total", "tx_pps", "rate"),
        ("node_network_receive_errs_total", "rx_errors_delta", "delta"),
        ("node_network_receive_fifo_total", "rx_fifo_delta", "delta"),
        ("node_network_receive_frame_total", "rx_frame_delta", "delta"),
        ("node_network_transmit_errs_total", "tx_errors_delta", "delta"),
        ("node_network_transmit_fifo_total", "tx_fifo_delta", "delta"),
        ("node_network_transmit_carrier_total", "tx_carrier_delta", "delta"),
        ("node_network_transmit_colls_total", "tx_collisions_delta", "delta"),
        ("node_network_receive_drop_total", "rx_drops_delta", "delta"),
        ("node_network_transmit_drop_total", "tx_drops_delta", "delta"),
        ("node_network_carrier_changes_total", "carrier_changes_delta", "delta"),
        ("node_network_carrier_up_changes_total", "carrier_up_changes_delta", "delta"),
        ("node_network_carrier_down_changes_total", "carrier_down_changes_delta", "delta"),
    )
    interface_names = set()
    for metric, _, _ in network_metrics:
        interface_names.update(labels.get("device", "") for labels, _ in grouped(samples, metric))
    for metric in ("node_network_carrier", "node_network_speed_bytes", "node_network_mtu_bytes",
                   "node_network_transmit_queue_length"):
        interface_names.update(labels.get("device", "") for labels, _ in grouped(samples, metric))
    interface_stats = {}
    for device in sorted(name for name in interface_names if include_interface(name)):
        item = {}
        for metric, key, kind in network_metrics:
            now = {labels.get("device", ""): value for labels, value in grouped(samples, metric)}
            old = {labels.get("device", ""): value for labels, value in grouped(previous, metric)}
            if device not in now or device not in old:
                continue
            change = counter_delta(now[device], old.get(device))
            item[key] = change / elapsed if kind == "rate" and elapsed > 0 else change
        for metric, key in (("node_network_carrier", "carrier"),
                            ("node_network_speed_bytes", "speed_Bps"),
                            ("node_network_mtu_bytes", "mtu_bytes"),
                            ("node_network_transmit_queue_length", "tx_queue_length")):
            values = {labels.get("device", ""): value for labels, value in grouped(samples, metric)}
            if device in values:
                item[key] = values[device]
        interface_stats[device] = item

    return [busy_pct, iowait_pct, mem_available,
            disk_read_rate, disk_write_rate, read_ops / elapsed if elapsed > 0 else 0.0,
            write_ops / elapsed if elapsed > 0 else 0.0, disk_util_max, disk_await_max,
            ",".join(sorted(disk_devices)), net_rx, net_tx, drops, retrans_delta,
            load1, procs_running, procs_blocked, context_switch_rate,
            sched_wait_rate, 100.0 * cpu_psi_rate if cpu_psi_rate is not None else None,
            *tcp_counter_deltas, *softnet_counter_deltas, *tcp_gauges, softnet_backlog,
            json.dumps(interface_stats, separators=(",", ":"), sort_keys=True)]


def summarize_host_file(path):
    import csv
    with open(path, encoding="utf-8", newline="") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if not rows:
        print("network summary: no samples")
        return
    tcp_columns = (
        "tcp_active_opens_delta", "tcp_passive_opens_delta", "tcp_attempt_fails_delta",
        "tcp_estab_resets_delta", "tcp_out_rsts_delta", "tcp_in_errors_delta",
        "tcp_retrans_delta", "tcp_listen_drops_delta", "tcp_listen_overflows_delta",
        "tcp_rcv_queue_drops_delta", "tcp_backlog_drops_delta", "tcp_syncookies_sent_delta",
        "tcp_syncookies_recv_delta", "tcp_syncookies_failed_delta",
        "tcp_request_queue_cookies_delta", "tcp_request_queue_drops_delta",
        "tcp_syn_retrans_delta", "tcp_timeouts_delta", "tcp_abort_on_data_delta",
        "tcp_abort_on_close_delta", "tcp_abort_on_memory_delta", "tcp_abort_on_timeout_delta",
        "tcp_memory_pressures_delta",
    )
    softnet_columns = (
        "softnet_processed_delta", "softnet_dropped_delta",
        "softnet_times_squeezed_delta", "softnet_cpu_collision_delta",
        "softnet_flow_limit_count_delta", "softnet_received_rps_delta",
    )
    gauge_columns = (
        "tcp_curr_estab", "tcp_sockstat_inuse", "tcp_sockstat_orphan",
        "tcp_sockstat_time_wait", "tcp_sockstat_alloc", "tcp_sockstat_mem_pages",
        "conntrack_entries", "conntrack_entries_limit", "softnet_backlog_len",
    )
    totals = {column: 0.0 for column in tcp_columns}
    softnet_totals = {column: 0.0 for column in softnet_columns}
    gauge_max = {column: None for column in gauge_columns}
    present = set()
    softnet_present = set()
    interfaces = {}
    for row in rows:
        for column in tcp_columns:
            value = row.get(column, "")
            if value != "":
                totals[column] += float(value)
                present.add(column)
        for column in softnet_columns:
            value = row.get(column, "")
            if value != "":
                softnet_totals[column] += float(value)
                softnet_present.add(column)
        for column in gauge_columns:
            value = row.get(column, "")
            if value != "":
                value = float(value)
                gauge_max[column] = value if gauge_max[column] is None else max(gauge_max[column], value)
        raw = row.get("net_interfaces_json", "")
        if not raw:
            continue
        try:
            snapshot = json.loads(raw)
        except (TypeError, ValueError):
            continue
        for name, item in snapshot.items():
            agg = interfaces.setdefault(name, {
                "rx_bytes": 0.0, "tx_bytes": 0.0, "rx_packets": 0.0, "tx_packets": 0.0,
                "rx_errors": 0.0, "tx_errors": 0.0, "rx_drops": 0.0, "tx_drops": 0.0,
                "rx_fifo": 0.0, "rx_frame": 0.0, "tx_fifo": 0.0,
                "tx_carrier": 0.0, "tx_collisions": 0.0,
                "carrier_changes": 0.0, "carrier_down_changes": 0.0,
                "rx_Bps_max": 0.0, "tx_Bps_max": 0.0, "carrier": None,
                "speed_Bps": None, "mtu_bytes": None, "tx_queue_length": None,
            })
            agg["rx_bytes"] += float(item.get("rx_bytes_delta", 0.0))
            agg["tx_bytes"] += float(item.get("tx_bytes_delta", 0.0))
            agg["rx_packets"] += float(item.get("rx_packets_delta", 0.0))
            agg["tx_packets"] += float(item.get("tx_packets_delta", 0.0))
            agg["rx_errors"] += float(item.get("rx_errors_delta", 0.0))
            agg["tx_errors"] += float(item.get("tx_errors_delta", 0.0))
            agg["rx_fifo"] += float(item.get("rx_fifo_delta", 0.0))
            agg["rx_frame"] += float(item.get("rx_frame_delta", 0.0))
            agg["tx_fifo"] += float(item.get("tx_fifo_delta", 0.0))
            agg["tx_carrier"] += float(item.get("tx_carrier_delta", 0.0))
            agg["tx_collisions"] += float(item.get("tx_collisions_delta", 0.0))
            agg["rx_drops"] += float(item.get("rx_drops_delta", 0.0))
            agg["tx_drops"] += float(item.get("tx_drops_delta", 0.0))
            agg["carrier_changes"] += float(item.get("carrier_changes_delta", 0.0))
            agg["carrier_down_changes"] += float(item.get("carrier_down_changes_delta", 0.0))
            agg["rx_Bps_max"] = max(agg["rx_Bps_max"], float(item.get("rx_Bps", 0.0)))
            agg["tx_Bps_max"] = max(agg["tx_Bps_max"], float(item.get("tx_Bps", 0.0)))
            if "carrier" in item:
                agg["carrier"] = item["carrier"]
            for key in ("speed_Bps", "mtu_bytes", "tx_queue_length"):
                if key in item:
                    agg[key] = item[key]
    if present:
        print("network.tcp " + " ".join(f"{key}={totals[key]:.0f}" for key in tcp_columns if key in present))
    if softnet_present:
        print("network.softnet " + " ".join(
            f"{key}={softnet_totals[key]:.0f}" for key in softnet_columns if key in softnet_present))
    present_gauges = [column for column in gauge_columns if gauge_max[column] is not None]
    if present_gauges:
        print("network.gauges " + " ".join(
            f"{column}_max={gauge_max[column]:.0f}" for column in present_gauges))
    for name in sorted(interfaces):
        item = interfaces[name]
        print("network.interface " + name +
              " rx_MiB=%.1f tx_MiB=%.1f rx_packets=%.0f tx_packets=%.0f"
              " rx_errors=%.0f tx_errors=%.0f rx_drops=%.0f tx_drops=%.0f"
              " rx_fifo=%.0f rx_frame=%.0f tx_fifo=%.0f tx_carrier=%.0f tx_collisions=%.0f"
              " carrier_changes=%.0f carrier_down_changes=%.0f carrier_last=%s speed_Mbps=%s mtu=%s tx_queue=%s"
              " rx_MiBps_max=%.1f tx_MiBps_max=%.1f" %
              (item["rx_bytes"] / 1024**2, item["tx_bytes"] / 1024**2,
               item["rx_packets"], item["tx_packets"],
               item["rx_errors"], item["tx_errors"], item["rx_drops"], item["tx_drops"],
               item["rx_fifo"], item["rx_frame"], item["tx_fifo"],
               item["tx_carrier"], item["tx_collisions"],
               item["carrier_changes"], item["carrier_down_changes"],
               "n/a" if item["carrier"] is None else str(item["carrier"]),
               "n/a" if item["speed_Bps"] is None else "%.1f" % (float(item["speed_Bps"]) * 8 / 1e6),
               "n/a" if item["mtu_bytes"] is None else str(int(float(item["mtu_bytes"]))),
               "n/a" if item["tx_queue_length"] is None else str(int(float(item["tx_queue_length"]))),
               item["rx_Bps_max"] / 1024**2, item["tx_Bps_max"] / 1024**2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=("node-exporter", "prometheus"))
    parser.add_argument("--url", default="")
    parser.add_argument("--instance", default="")
    parser.add_argument("--out", default="")
    parser.add_argument("--summarize", default="", help="print network counters summarized from a host TSV")
    parser.add_argument("--interval-ms", type=int, default=1000)
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()
    if args.summarize:
        summarize_host_file(args.summarize)
        return
    if not args.source or not args.url or not args.out:
        parser.error("--source, --url, and --out are required unless --summarize is used")
    args.interval_ms = max(args.interval_ms, 250)
    stop_path = args.out + ".stop"
    columns = ["epoch", "source_epoch", "cpu_busy_pct", "cpu_iowait_pct", "memavailable_bytes",
               "disk_read_Bps", "disk_write_Bps", "disk_read_iops", "disk_write_iops", "disk_util_max_pct",
               "disk_await_max_ms", "disk_devices", "net_rx_Bps", "net_tx_Bps", "net_drops_delta",
               "tcp_retrans_delta", "load1", "procs_running", "procs_blocked",
               "context_switches_per_s", "sched_wait_cpu_s_per_s", "cpu_psi_some_pct",
               "tcp_active_opens_delta", "tcp_passive_opens_delta", "tcp_attempt_fails_delta",
               "tcp_estab_resets_delta", "tcp_out_rsts_delta", "tcp_in_errors_delta",
               "tcp_in_segments_delta", "tcp_out_segments_delta", "tcp_listen_drops_delta",
               "tcp_listen_overflows_delta", "tcp_rcv_queue_drops_delta", "tcp_backlog_drops_delta",
               "tcp_syncookies_sent_delta", "tcp_syncookies_recv_delta", "tcp_syncookies_failed_delta",
               "tcp_request_queue_cookies_delta", "tcp_request_queue_drops_delta",
               "tcp_syn_retrans_delta", "tcp_timeouts_delta",
               "tcp_abort_on_data_delta", "tcp_abort_on_close_delta", "tcp_abort_on_memory_delta",
               "tcp_abort_on_timeout_delta", "tcp_memory_pressures_delta",
               "softnet_processed_delta", "softnet_dropped_delta", "softnet_times_squeezed_delta",
               "softnet_cpu_collision_delta", "softnet_flow_limit_count_delta", "softnet_received_rps_delta",
               "tcp_curr_estab",
               "tcp_sockstat_inuse", "tcp_sockstat_orphan", "tcp_sockstat_time_wait",
               "tcp_sockstat_alloc", "tcp_sockstat_mem_pages", "conntrack_entries",
               "conntrack_entries_limit", "softnet_backlog_len", "net_interfaces_json"]
    previous = None
    previous_source_epoch = None
    last_error = None
    with open(args.out, "w", encoding="utf-8") as output:
        output.write("\t".join(columns) + "\n")
        output.flush()
        while not os.path.exists(stop_path):
            started = time.monotonic()
            try:
                samples, source_epoch = fetch(args)
                now = time.monotonic()
                if previous_source_epoch is None:
                    # Establish a counter baseline; do not report fake zero rates for the first scrape.
                    previous, previous_source_epoch = samples, source_epoch
                    remaining = args.interval_ms / 1000.0 - (time.monotonic() - started)
                    if remaining > 0:
                        time.sleep(remaining)
                    continue
                if source_epoch <= previous_source_epoch:
                    # Prometheus returns the latest scraped sample repeatedly between scrapes.
                    # Do not turn stale counters into misleading zero-rate rows.
                    remaining = args.interval_ms / 1000.0 - (time.monotonic() - started)
                    if remaining > 0:
                        time.sleep(remaining)
                    continue
                elapsed = source_epoch - previous_source_epoch
                values = summarize(samples, previous, elapsed)
                row = [time.time(), source_epoch] + values
                output.write("\t".join("" if value is None else str(value) for value in row) + "\n")
                output.flush()
                previous, previous_source_epoch = samples, source_epoch
                if last_error is not None:
                    print("host metrics endpoint recovered", file=sys.stderr, flush=True)
                    last_error = None
            except Exception as exc:  # keep the benchmark running if monitoring is unavailable
                message = str(exc)
                if message != last_error:
                    print("host metrics scrape failed: " + message, file=sys.stderr, flush=True)
                    last_error = message
            remaining = args.interval_ms / 1000.0 - (time.monotonic() - started)
            if remaining > 0:
                time.sleep(remaining)


if __name__ == "__main__":
    main()
