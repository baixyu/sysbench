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
    "node_disk_read_bytes_total|node_disk_written_bytes_total|"
    "node_disk_reads_completed_total|node_disk_writes_completed_total|"
    "node_disk_io_time_seconds_total|node_disk_read_time_seconds_total|"
    "node_disk_write_time_seconds_total|node_network_receive_bytes_total|"
    "node_network_transmit_bytes_total|node_network_receive_drop_total|"
    "node_network_transmit_drop_total|node_netstat_Tcp_RetransSegs"
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
    retrans = grouped(samples, "node_netstat_Tcp_RetransSegs")
    old_retrans = grouped(previous, "node_netstat_Tcp_RetransSegs")
    retrans_delta = sum(counter_delta(value, old_retrans[0][1] if old_retrans else None) for _, value in retrans[:1])

    return [busy_pct, iowait_pct, mem_available,
            disk_read_rate, disk_write_rate, read_ops / elapsed if elapsed > 0 else 0.0,
            write_ops / elapsed if elapsed > 0 else 0.0, disk_util_max, disk_await_max,
            ",".join(sorted(disk_devices)), net_rx, net_tx, drops, retrans_delta]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=("node-exporter", "prometheus"), required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--instance", default="")
    parser.add_argument("--out", required=True)
    parser.add_argument("--interval-ms", type=int, default=1000)
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()
    args.interval_ms = max(args.interval_ms, 250)
    stop_path = args.out + ".stop"
    columns = ["epoch", "source_epoch", "cpu_busy_pct", "cpu_iowait_pct", "memavailable_bytes",
               "disk_read_Bps", "disk_write_Bps", "disk_read_iops", "disk_write_iops", "disk_util_max_pct",
               "disk_await_max_ms", "disk_devices", "net_rx_Bps", "net_tx_Bps", "net_drops_delta",
               "tcp_retrans_delta"]
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
