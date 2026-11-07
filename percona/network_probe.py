#!/usr/bin/env python3
"""Record TCP connect outcomes from the benchmark host to Raft members."""
import argparse
import concurrent.futures
import datetime
import errno
import json
import os
import socket
import time
from collections import Counter, defaultdict


def parse_member(value):
    try:
        host, raw_port = value.rsplit(":", 1)
        port = int(raw_port)
    except (AttributeError, ValueError):
        raise argparse.ArgumentTypeError("endpoint must be HOST:PORT")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    if not host or not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("endpoint port must be in 1..65535")
    return host, port


def parse_ports(value):
    ports = []
    for raw in value.split(","):
        if not raw.strip():
            continue
        port = int(raw.strip())
        if not 1 <= port <= 65535:
            raise argparse.ArgumentTypeError("port must be in 1..65535")
        if port not in ports:
            ports.append(port)
    return ports


def targets_from_args(args):
    targets = set()
    raft_ports = parse_ports(args.raft_ports)
    for member in args.member:
        host, sql_port = parse_member(member)
        for port in [sql_port] + raft_ports:
            targets.add((host, port))
    for member in args.raft_member:
        host, port = parse_member(member)
        targets.add((host, port))
    return sorted(targets)


def probe(target, timeout_s):
    host, port = target
    started = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            elapsed_ms = (time.perf_counter() - started) * 1000.0
        return {"host": host, "port": port, "result": "ok", "connect_ms": elapsed_ms,
                "errno": None, "error": ""}
    except socket.timeout as error:
        result = "timeout"
        error_number = getattr(error, "errno", None)
        error_text = str(error)
    except OSError as error:
        error_number = getattr(error, "errno", None)
        error_text = str(error)
        if error_number == errno.ECONNREFUSED:
            result = "refused"
        elif error_number in (errno.ETIMEDOUT, errno.EWOULDBLOCK):
            result = "timeout"
        elif error_number in (errno.EHOSTUNREACH, errno.ENETUNREACH):
            result = "unreachable"
        else:
            result = "error"
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return {"host": host, "port": port, "result": result, "connect_ms": elapsed_ms,
            "errno": error_number, "error": error_text}


def run(args):
    targets = targets_from_args(args)
    if not targets:
        raise SystemExit("no TCP probe targets")
    stop_path = args.out + ".stop"
    source_host = socket.getfqdn()
    timeout_s = args.timeout_ms / 1000.0
    interval_s = args.interval_ms / 1000.0
    workers = min(max(1, len(targets)), 32)
    with open(args.out, "a", encoding="utf-8") as output, concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        while not os.path.exists(stop_path):
            started = time.monotonic()
            sample_epoch = time.time()
            futures = [pool.submit(probe, target, timeout_s) for target in targets]
            for future in concurrent.futures.as_completed(futures):
                record = future.result()
                record["epoch"] = time.time()
                record["sample_epoch"] = sample_epoch
                record["source_host"] = source_host
                output.write(json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n")
            output.flush()
            remaining = interval_s - (time.monotonic() - started)
            if remaining > 0:
                time.sleep(remaining)


def summarize(path):
    groups = defaultdict(list)
    with open(path, encoding="utf-8") as source:
        for line in source:
            try:
                row = json.loads(line)
                groups[(row["host"], int(row["port"]))].append(row)
            except (ValueError, KeyError, TypeError):
                continue
    if not groups:
        print("tcp_probe: no probe records")
        return
    for (host, port), rows in sorted(groups.items()):
        counts = Counter(row.get("result", "unknown") for row in rows)
        latency = sorted(float(row["connect_ms"]) for row in rows if row.get("result") == "ok")
        p95 = latency[min(len(latency) - 1, int((len(latency) - 1) * 0.95))] if latency else None
        maximum = latency[-1] if latency else None
        last = rows[-1]
        failures = [row for row in rows if row.get("result") != "ok"]
        first_failure = failures[0].get("epoch") if failures else None
        last_failure = failures[-1].get("epoch") if failures else None
        source_host = last.get("source_host", "unknown")
        details = [f"target={host}:{port}", f"source={source_host}", f"samples={len(rows)}"]
        details.extend(f"{name}={counts[name]}" for name in ("ok", "refused", "timeout", "unreachable", "error") if counts[name])
        details.append(f"last={last.get('result', 'unknown')}")
        if p95 is not None:
            details.append(f"connect_ms_p95={p95:.2f}")
            details.append(f"connect_ms_max={maximum:.2f}")
        if first_failure is not None:
            details.append("first_failure_utc=" + datetime.datetime.fromtimestamp(first_failure, datetime.timezone.utc).isoformat())
            details.append("last_failure_utc=" + datetime.datetime.fromtimestamp(last_failure, datetime.timezone.utc).isoformat())
            details.append("last_error=" + str(failures[-1].get("error", "")).replace(" ", "_"))
        print("tcp_probe " + " ".join(details))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--member", action="append", default=[])
    parser.add_argument("--raft-member", action="append", default=[], help="explicit Raft endpoint HOST:PORT")
    parser.add_argument("--raft-ports", default="", help="Raft port(s) shared by all --member hosts")
    parser.add_argument("--out", default="")
    parser.add_argument("--interval-ms", type=int, default=1000)
    parser.add_argument("--timeout-ms", type=int, default=300)
    parser.add_argument("--summarize", default="")
    args = parser.parse_args()
    try:
        parse_ports(args.raft_ports)
        for member in args.member + args.raft_member:
            parse_member(member)
    except (ValueError, argparse.ArgumentTypeError) as error:
        parser.error(str(error))
    if args.summarize:
        summarize(args.summarize)
        return
    if not args.member or not args.out:
        parser.error("--member and --out are required unless --summarize is used")
    if args.interval_ms < 100 or args.timeout_ms < 1:
        parser.error("--interval-ms must be >= 100 and --timeout-ms must be positive")
    run(args)


if __name__ == "__main__":
    main()
