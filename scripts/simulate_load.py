#!/usr/bin/env python3
"""
Load simulator for the 5v5 matchmaker.

It can start the Rust service for you, then inject thousands of concurrent
enqueue requests and poll metrics until the pool drains or a timeout is hit.
"""

import argparse
import concurrent.futures
import json
import os
import random
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request


REGIONS = ("na", "eu", "apac", "sa")
MODES = ("ranked", "ranked", "ranked", "casual")


def parse_args():
    parser = argparse.ArgumentParser(description="Stress the 5v5 matchmaker service.")
    parser.add_argument("--url", default="http://127.0.0.1:8080", help="service base URL")
    parser.add_argument("--players", type=int, default=10000, help="number of players to enqueue")
    parser.add_argument("--concurrency", type=int, default=256, help="parallel request workers")
    parser.add_argument("--timeout", type=float, default=30.0, help="seconds to wait for matching")
    parser.add_argument("--seed", type=int, default=42, help="random seed")
    parser.add_argument(
        "--no-start-server",
        action="store_true",
        help="assume the Rust service is already running",
    )
    parser.add_argument(
        "--server-workers",
        type=int,
        default=max(2, os.cpu_count() or 4),
        help="matching worker threads when starting the service",
    )
    return parser.parse_args()


def post_json(url, payload, timeout=5.0):
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.status, response.read()


def get_json(url, timeout=5.0):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for_service(base_url, timeout=10.0):
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        try:
            health = get_json(base_url + "/health", timeout=1.0)
            if health.get("status") == "ok":
                return True
        except Exception:
            time.sleep(0.1)
    return False


def start_server(base_url, worker_count):
    if wait_for_service(base_url, timeout=0.5):
        return None

    host_port = base_url.removeprefix("http://").removeprefix("https://")
    cmd = [
        "cargo",
        "run",
        "--release",
        "--",
        "--addr",
        host_port,
        "--workers",
        str(worker_count),
    ]
    print("Starting service:", " ".join(cmd), flush=True)
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if not wait_for_service(base_url, timeout=20.0):
        if process.stdout:
            print(process.stdout.read(), file=sys.stderr)
        raise RuntimeError("service did not become healthy")
    return process


def stop_server(process):
    if process is None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()


def make_player(idx, rng):
    if idx % 100 == 0:
        skill = rng.choice((100, 4900))
    else:
        skill = int(rng.gauss(2500, 650))
    skill = max(0, min(5000, skill))
    return {
        "player_id": "sim-{0:07d}".format(idx),
        "skill": skill,
        "region": rng.choice(REGIONS),
        "mode": rng.choice(MODES),
    }


def enqueue_one(base_url, payload):
    start = time.perf_counter()
    try:
        status, _ = post_json(base_url + "/enqueue", payload)
        latency_ms = (time.perf_counter() - start) * 1000.0
        return status, latency_ms, None
    except urllib.error.HTTPError as error:
        latency_ms = (time.perf_counter() - start) * 1000.0
        return error.code, latency_ms, error.read().decode("utf-8", errors="replace")
    except Exception as error:
        latency_ms = (time.perf_counter() - start) * 1000.0
        return 0, latency_ms, str(error)


def percentile(values, pct):
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[idx]


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    server = None

    try:
        if not args.no_start_server:
            server = start_server(args.url, args.server_workers)
        elif not wait_for_service(args.url, timeout=10.0):
            raise RuntimeError("service is not healthy at {}".format(args.url))

        players = [make_player(idx, rng) for idx in range(args.players)]
        print(
            "Injecting {} players with concurrency {}...".format(args.players, args.concurrency),
            flush=True,
        )
        start = time.perf_counter()
        statuses = {}
        latencies = []
        failures = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = [executor.submit(enqueue_one, args.url, payload) for payload in players]
            for future in concurrent.futures.as_completed(futures):
                status, latency_ms, error = future.result()
                statuses[status] = statuses.get(status, 0) + 1
                latencies.append(latency_ms)
                if error:
                    failures.append(error)

        inject_seconds = time.perf_counter() - start
        deadline = time.perf_counter() + args.timeout
        metrics = {}
        while time.perf_counter() < deadline:
            metrics = get_json(args.url + "/metrics")
            if metrics.get("waiting_players", 0) == 0:
                break
            time.sleep(0.25)

        metrics = get_json(args.url + "/metrics")
        print("")
        print("Request status counts:", statuses)
        print("Injection time: {:.3f}s".format(inject_seconds))
        print("Throughput: {:.1f} enqueue req/s".format(args.players / max(inject_seconds, 0.001)))
        print(
            "Enqueue latency ms: p50={:.2f} p95={:.2f} p99={:.2f}".format(
                percentile(latencies, 50),
                percentile(latencies, 95),
                percentile(latencies, 99),
            )
        )
        print("Final metrics:")
        print(json.dumps(metrics, indent=2, sort_keys=True))
        if failures:
            print("Sample errors:")
            for error in failures[:5]:
                print(error)

    finally:
        stop_server(server)


if __name__ == "__main__":
    main()
