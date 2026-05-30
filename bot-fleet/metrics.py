"""
metrics.py — telemetry consumer & live dashboard.

Reads the "telemetry_events" Redis stream that the bot fleet writes to and
computes the metrics the hackathon scores on:

  • Latency   — p50 / p90 / p99 / max (the spec asks for exactly these)
  • Throughput— current TPS (events/sec, windowed) and peak TPS
  • Correctness — % of responses the target answered correctly

This is YOUR sanity check on the load test. The telemetry_service owns the
official numbers (it persists to Postgres); this just lets you see, live,
what your swarm is producing and catch bad data early.

Usage:
  python metrics.py                 # live dashboard, tails new + existing events
  python metrics.py --summary       # read everything currently in the stream, print once, exit
  python metrics.py --reset         # DELETE the stream (clear between runs), then exit
  python metrics.py --window 5      # rolling window in seconds for live stats (default 10)
"""

import os
import sys
import math
import time
import argparse
from collections import deque, defaultdict

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM = "telemetry_events"


def percentile(sorted_vals, p):
    """Linear-interpolated percentile over a pre-sorted list."""
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * (p / 100.0)
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return sorted_vals[int(k)]
    return sorted_vals[lo] * (hi - k) + sorted_vals[hi] * (k - lo)


class Stats:
    """Cumulative totals + a rolling time window for 'live' latency/TPS."""

    def __init__(self, window_s):
        self.window_ms = window_s * 1000
        self.window_s = window_s
        self.run_filter = None   # if set, only events with this run_id count
        # rolling window: (t_ms, latency_ms, correct, order_type)
        self.window = deque()
        # cumulative
        self.total = 0
        self.correct = 0
        self.by_type = defaultdict(lambda: [0, 0])  # type -> [count, correct]
        self.first_ms = None
        self.last_ms = None
        self.peak_tps = 0.0

    def ingest(self, entry_id, fields):
        if self.run_filter is not None and fields.get("run_id") != self.run_filter:
            return
        # Stream IDs look like "1716998400123-0"; the prefix is epoch ms.
        t_ms = int(entry_id.split("-")[0])
        latency = float(fields.get("latency_ms", 0.0))
        correct = fields.get("is_correct", "0") in ("1", 1, "true", "True")
        otype = fields.get("order_type", "unknown")

        self.total += 1
        if correct:
            self.correct += 1
        self.by_type[otype][0] += 1
        if correct:
            self.by_type[otype][1] += 1

        if self.first_ms is None:
            self.first_ms = t_ms
        self.last_ms = t_ms

        self.window.append((t_ms, latency, correct, otype))
        self._evict(t_ms)

    def _evict(self, now_ms):
        cutoff = now_ms - self.window_ms
        while self.window and self.window[0][0] < cutoff:
            self.window.popleft()

    def render(self):
        # Window-based latency percentiles (reflects current load).
        lats = sorted(x[1] for x in self.window)
        w_count = len(self.window)
        w_correct = sum(1 for x in self.window if x[2])

        # Current TPS: events in the window / window seconds.
        tps = (w_count / self.window_s) if self.window_s else 0.0
        self.peak_tps = max(self.peak_tps, tps)

        overall_correct_pct = (100.0 * self.correct / self.total) if self.total else 0.0
        window_correct_pct = (100.0 * w_correct / w_count) if w_count else 0.0
        elapsed = ((self.last_ms - self.first_ms) / 1000.0) if self.first_ms else 0.0

        lines = []
        lines.append("═" * 56)
        lines.append("  BOT FLEET — LIVE TELEMETRY")
        lines.append("═" * 56)
        lines.append(f"  events total   : {self.total:,}")
        lines.append(f"  elapsed        : {elapsed:6.1f} s")
        lines.append(f"  TPS (now/peak) : {tps:8.0f} / {self.peak_tps:.0f}")
        lines.append("")
        lines.append(f"  LATENCY (last {self.window_s}s window)")
        lines.append(f"    p50 : {percentile(lats, 50):8.2f} ms")
        lines.append(f"    p90 : {percentile(lats, 90):8.2f} ms")
        lines.append(f"    p99 : {percentile(lats, 99):8.2f} ms")
        lines.append(f"    max : {(lats[-1] if lats else 0):8.2f} ms")
        lines.append("")
        lines.append(f"  CORRECTNESS")
        lines.append(f"    overall : {overall_correct_pct:6.2f} %  ({self.correct:,}/{self.total:,})")
        lines.append(f"    window  : {window_correct_pct:6.2f} %")
        lines.append("")
        lines.append(f"  BY ORDER TYPE")
        for otype in sorted(self.by_type):
            cnt, ok = self.by_type[otype]
            pct = (100.0 * ok / cnt) if cnt else 0.0
            lines.append(f"    {otype:8s}: {cnt:8,}  ({pct:5.1f}% correct)")
        lines.append("═" * 56)
        return "\n".join(lines)


def clear_screen():
    sys.stdout.write("\033[2J\033[H")  # ANSI clear + cursor home


def run_live(r, stats, refresh=1.0):
    last_id = "0-0"  # start from the beginning so you see the whole run
    last_print = 0.0
    print("Reading telemetry... (Ctrl-C to stop)\n")
    try:
        while True:
            resp = r.xread({STREAM: last_id}, count=5000, block=500)
            if resp:
                for _stream, entries in resp:
                    for entry_id, fields in entries:
                        last_id = entry_id
                        stats.ingest(entry_id, fields)
            now = time.time()
            if now - last_print >= refresh:
                clear_screen()
                print(stats.render())
                last_print = now
    except KeyboardInterrupt:
        clear_screen()
        print(stats.render())
        print("\n(stopped)")


def run_summary(r, stats):
    last_id = "0-0"
    while True:
        resp = r.xread({STREAM: last_id}, count=5000, block=300)
        if not resp:
            break
        for _stream, entries in resp:
            for entry_id, fields in entries:
                last_id = entry_id
                stats.ingest(entry_id, fields)
    # For a one-shot summary, widen the window to the whole run so the
    # percentiles cover every event, not just the last few seconds.
    stats.window_s = max(1, (stats.last_ms - stats.first_ms) / 1000.0) if stats.first_ms else 1
    print(stats.render())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", action="store_true", help="read all, print once, exit")
    ap.add_argument("--reset", action="store_true", help="delete the stream and exit")
    ap.add_argument("--window", type=int, default=10, help="rolling window seconds (live mode)")
    ap.add_argument("--run", default=None, help="only count events with this run_id")
    args = ap.parse_args()

    r = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        r.ping()
    except Exception as e:
        print(f"Cannot reach Redis at {REDIS_URL}: {e}")
        print("Is Redis running?  docker compose up -d redis   (or  brew services start redis)")
        sys.exit(1)

    if args.reset:
        deleted = r.delete(STREAM)
        print(f"stream '{STREAM}' {'deleted' if deleted else 'was already empty'}")
        return

    stats = Stats(window_s=args.window)
    stats.run_filter = args.run
    if args.summary:
        run_summary(r, stats)
    else:
        run_live(r, stats)


if __name__ == "__main__":
    main()