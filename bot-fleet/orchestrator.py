"""
orchestrator.py — control plane for a coordinated bot-fleet attack.

One command instead of hand-set env vars. It:
  1. assigns a unique run_id to this attack,
  2. publishes the attack config to Redis (key "attack:config") so EVERY
     bot-fleet replica reads identical settings at startup,
  3. (optionally) launches the fleet at a chosen scale via docker compose,
  4. waits for the replicas to finish, then prints a summary for THIS run only
     — filtered by run_id so results from earlier runs don't blend in.

Bots stamp run_id on every telemetry event, so you can also watch a single run
live with:   python metrics.py --run <run_id>

Examples:
  # full coordinated attack: 4 replicas × 500 bots for 30s, then summary
  python orchestrator.py --scale 4 --n-bots 500 --duration 30

  # just publish config (don't launch); start the fleet yourself afterwards
  python orchestrator.py --n-bots 1000 --no-launch
  #   then: docker compose up --scale bot-fleet=8 bot-fleet

  # attack a real submission instead of the mock
  python orchestrator.py --target http://172.18.0.9:7000 --team-id 42 --scale 6
"""

import os
import sys
import json
import time
import uuid
import math
import argparse
import subprocess

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CONFIG_KEY = "attack:config"
STREAM = "telemetry_events"


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * (p / 100.0)
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return sorted_vals[int(k)]
    return sorted_vals[lo] * (hi - k) + sorted_vals[hi] * (k - lo)


def summarize(r, run_id):
    """Read the whole stream once and report stats for this run_id only."""
    last_id = "0-0"
    lats, total, correct = [], 0, 0
    first_ms = last_ms = None
    while True:
        resp = r.xread({STREAM: last_id}, count=5000, block=300)
        if not resp:
            break
        for _stream, entries in resp:
            for entry_id, fields in entries:
                last_id = entry_id
                if fields.get("run_id") != run_id:
                    continue
                total += 1
                lats.append(float(fields.get("latency_ms", 0.0)))
                if fields.get("is_correct") in ("1", 1, "true", "True"):
                    correct += 1
                t_ms = int(entry_id.split("-")[0])
                first_ms = t_ms if first_ms is None else first_ms
                last_ms = t_ms

    lats.sort()
    elapsed = ((last_ms - first_ms) / 1000.0) if (first_ms and last_ms) else 0.0
    tps = (total / elapsed) if elapsed else 0.0
    correct_pct = (100.0 * correct / total) if total else 0.0

    print("\n" + "═" * 56)
    print(f"  RUN SUMMARY — {run_id}")
    print("═" * 56)
    print(f"  events     : {total:,}")
    print(f"  duration   : {elapsed:6.1f} s")
    print(f"  avg TPS    : {tps:8.0f}")
    print(f"  latency    : p50 {percentile(lats,50):7.2f} ms | "
          f"p90 {percentile(lats,90):7.2f} ms | "
          f"p99 {percentile(lats,99):7.2f} ms | "
          f"max {(lats[-1] if lats else 0):7.2f} ms")
    print(f"  correctness: {correct_pct:6.2f} %  ({correct:,}/{total:,})")
    print("═" * 56)


def compose(*args):
    """Run a docker compose subcommand, surfacing a clean error if it fails."""
    cmd = ["docker", "compose", *args]
    print("  $", " ".join(cmd))
    try:
        return subprocess.run(cmd, check=False).returncode
    except FileNotFoundError:
        print("\n  docker not found. Is Docker Desktop installed and running?")
        sys.exit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="http://mock-engine:9100",
                    help="endpoint to attack (service name inside compose)")
    ap.add_argument("--team-id", type=int, default=1)
    ap.add_argument("--n-bots", type=int, default=500, help="bots PER replica")
    ap.add_argument("--duration", type=int, default=30, help="seconds (0 = run until stopped)")
    ap.add_argument("--ramp-up", type=int, default=5)
    ap.add_argument("--think-ms", type=int, default=0)
    ap.add_argument("--scale", type=int, default=4, help="number of fleet replicas")
    ap.add_argument("--no-launch", action="store_true",
                    help="only publish config; don't start the fleet")
    ap.add_argument("--keep", action="store_true",
                    help="keep existing stream events (default clears them)")
    args = ap.parse_args()

    r = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        r.ping()
    except Exception as e:
        print(f"Cannot reach Redis at {REDIS_URL}: {e}")
        print("Start it first:  docker compose up -d redis")
        sys.exit(1)

    run_id = f"{int(time.time())}-{uuid.uuid4().hex[:6]}"
    cfg = {
        "target": args.target,
        "team_id": args.team_id,
        "n_bots": args.n_bots,
        "duration": args.duration,
        "ramp_up": args.ramp_up,
        "think_ms": args.think_ms,
        "run_id": run_id,
    }

    if not args.keep:
        r.delete(STREAM)
        print(f"cleared stream '{STREAM}'")

    r.set(CONFIG_KEY, json.dumps(cfg))
    print(f"published attack:config  (run_id={run_id})")
    print(json.dumps(cfg, indent=2))

    if args.no_launch:
        total = args.scale * args.n_bots
        print(f"\nConfig is live. Launch the fleet yourself ({total} bots total):")
        print(f"  docker compose up --build --scale bot-fleet={args.scale} bot-fleet")
        print(f"Then watch this run live:")
        print(f"  python metrics.py --run {run_id}")
        return

    # Make sure redis + target are up (detached), then run the fleet attached
    # with --no-deps so `up` returns when the replicas exit (they would not if
    # it also managed the always-on redis/mock-engine containers).
    print("\nbringing up redis + mock-engine...")
    compose("up", "-d", "--build", "redis", "mock-engine")

    total = args.scale * args.n_bots
    print(f"\nlaunching {args.scale} replicas × {args.n_bots} = {total} bots "
          f"for {args.duration}s...\n")
    compose("up", "--build", "--no-deps", "--scale", f"bot-fleet={args.scale}", "bot-fleet")

    if args.duration > 0:
        # Give the emitter a moment to flush its final batch to Redis.
        time.sleep(1.5)
        summarize(r, run_id)
    else:
        print(f"\nFleet running until stopped. Watch live:  python metrics.py --run {run_id}")


if __name__ == "__main__":
    main()