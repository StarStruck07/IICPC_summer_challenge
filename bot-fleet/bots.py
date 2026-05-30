"""
Bot Fleet — distributed load generator.

Spawns N concurrent async "trading bots" that bombard a target matching engine
with a realistic mix of limit / market / cancel orders, measures the latency of
every request, and streams one telemetry event per request to Redis.

────────────────────────────────────────────────────────────────────────────
TELEMETRY CONTRACT  (what the telemetry ingester consumes)
────────────────────────────────────────────────────────────────────────────
Redis Stream  "telemetry_events"   (XADD)
  fields per event:
    team_id     : int    — which submission this load is hitting
    latency_ms  : float  — client-measured round-trip time
    is_correct  : 0|1    — did the target return a valid, expected response
  (order_type and status are attached too; the ingester can ignore them — the
   telemetry table in telemetry_service only needs the three above.)

────────────────────────────────────────────────────────────────────────────
ATTACK CONFIG
────────────────────────────────────────────────────────────────────────────
On startup each replica reads JSON from the Redis key "attack:config" if it
exists, otherwise falls back to env vars. This lets an orchestrator publish one
config and have every replica pick it up:

  {
    "target":    "http://localhost:9100",
    "team_id":   1,
    "n_bots":    500,        # bots THIS replica runs
    "duration":  30,         # seconds of attack (0 = run until stopped)
    "ramp_up":   5,          # seconds to spin all bots up gradually
    "think_ms":  0           # optional pause between a bot's requests
  }

Run a single replica:
  python bots.py
Scale horizontally:
  docker compose up --scale bot-fleet=8
────────────────────────────────────────────────────────────────────────────
"""

import os
import json
import time
import random
import signal
import asyncio
import logging

import aiohttp
import redis.asyncio as redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("bot-fleet")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM = "telemetry_events"
CONFIG_KEY = "attack:config"

# Batch telemetry writes so a high-RPS swarm doesn't make one round-trip to
# Redis per request. Events accumulate in a queue and are flushed in pipelines.
FLUSH_BATCH = 500
FLUSH_INTERVAL = 0.25  # seconds


# ──────────────────────────────────────────────────────────────────────────
# Order generation — matches the submission API contract
# ──────────────────────────────────────────────────────────────────────────
def make_order():
    """Realistic market mix: mostly limits, some markets, a few cancels."""
    r = random.random()
    side = random.choice(["buy", "sell"])
    if r < 0.60:
        return {
            "type": "limit",
            "side": side,
            "price": round(random.uniform(99.0, 101.0), 2),
            "qty": random.randint(1, 50),
        }
    if r < 0.85:
        return {"type": "market", "side": side, "qty": random.randint(1, 50)}
    return {"type": "cancel", "order_id": random.randint(1, 2000)}


VALID_STATUSES = {"accepted", "partial", "filled", "rejected", "cancelled", "not_found"}


def is_response_correct(http_status: int, body: dict) -> bool:
    """A response is 'correct' if the target replied 200 with a well-formed,
    expected payload. A dead/slow/garbage target counts as incorrect."""
    if http_status != 200 or not isinstance(body, dict):
        return False
    if "order_id" not in body or "fills" not in body:
        return False
    return body.get("status") in VALID_STATUSES


# ──────────────────────────────────────────────────────────────────────────
# A single bot: loop sending orders until told to stop
# ──────────────────────────────────────────────────────────────────────────
async def bot(session, target, team_id, queue, stop, think_ms, run_id):
    url = f"{target}/order"
    timeout = aiohttp.ClientTimeout(total=2.0)
    while not stop.is_set():
        order = make_order()
        t0 = time.perf_counter()
        correct = False
        try:
            async with session.post(url, json=order, timeout=timeout) as resp:
                body = await resp.json(content_type=None)
                correct = is_response_correct(resp.status, body)
        except Exception:
            correct = False  # timeout / connection refused / target crashed
        latency_ms = (time.perf_counter() - t0) * 1000.0

        # Hand off to the emitter; never block the bot on Redis.
        queue.put_nowait({
            "team_id": team_id,
            "latency_ms": round(latency_ms, 3),
            "is_correct": 1 if correct else 0,
            "order_type": order["type"],
            "run_id": run_id,
        })

        if think_ms:
            await asyncio.sleep(think_ms / 1000.0)


# ──────────────────────────────────────────────────────────────────────────
# Emitter: drain the queue and batch-write events to the Redis stream
# ──────────────────────────────────────────────────────────────────────────
async def emitter(r, queue, stop):
    sent = 0
    while not (stop.is_set() and queue.empty()):
        batch = []
        try:
            # Wait for at least one event, then greedily grab more.
            first = await asyncio.wait_for(queue.get(), timeout=FLUSH_INTERVAL)
            batch.append(first)
            while len(batch) < FLUSH_BATCH:
                batch.append(queue.get_nowait())
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            pass

        if batch:
            pipe = r.pipeline(transaction=False)
            for ev in batch:
                pipe.xadd(STREAM, ev)
            try:
                await pipe.execute()
                sent += len(batch)
            except Exception as e:
                log.warning(f"telemetry flush failed ({len(batch)} events): {e}")
    log.info(f"emitter done, {sent} events sent")


# ──────────────────────────────────────────────────────────────────────────
# Swarm orchestration: ramp up, run, tear down
# ──────────────────────────────────────────────────────────────────────────
async def load_config(r):
    cfg = {
        "target": os.getenv("TARGET", "http://localhost:9100"),
        "team_id": int(os.getenv("TEAM_ID", "1")),
        "n_bots": int(os.getenv("N_BOTS", "500")),
        "duration": int(os.getenv("DURATION", "30")),
        "ramp_up": int(os.getenv("RAMP_UP", "5")),
        "think_ms": int(os.getenv("THINK_MS", "0")),
        "run_id": os.getenv("RUN_ID", "adhoc"),
    }
    try:
        raw = await r.get(CONFIG_KEY)
        if raw:
            cfg.update(json.loads(raw))
    except Exception as e:
        log.warning(f"couldn't read {CONFIG_KEY}, using env defaults: {e}")
    return cfg


async def run_swarm():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    cfg = await load_config(r)
    log.info(f"config: {cfg}")

    stop = asyncio.Event()

    # Wire SIGINT/SIGTERM to graceful shutdown (Ctrl-C, `docker stop`).
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # not supported on some platforms (e.g. Windows)

    queue: asyncio.Queue = asyncio.Queue(maxsize=100_000)

    # One shared connection pool for the whole swarm; limit=0 => unbounded.
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        emit_task = asyncio.create_task(emitter(r, queue, stop))

        # Ramp up: stagger bot starts over `ramp_up` seconds so we can watch
        # the target degrade and find the TPS at which it breaks.
        bots = []
        n = cfg["n_bots"]
        delay = (cfg["ramp_up"] / n) if (cfg["ramp_up"] and n) else 0
        for i in range(n):
            bots.append(asyncio.create_task(
                bot(session, cfg["target"], cfg["team_id"],
                    queue, stop, cfg["think_ms"], cfg["run_id"])
            ))
            if delay:
                await asyncio.sleep(delay)
        log.info(f"{n} bots live, attacking {cfg['target']} (run_id={cfg['run_id']})")

        # Run for the configured duration (or until a signal stops us).
        if cfg["duration"] > 0:
            try:
                await asyncio.wait_for(stop.wait(), timeout=cfg["duration"])
            except asyncio.TimeoutError:
                pass
        else:
            await stop.wait()

        log.info("stopping bots...")
        stop.set()
        await asyncio.gather(*bots, return_exceptions=True)
        await emit_task

    await r.aclose()
    log.info("swarm shut down cleanly")


if __name__ == "__main__":
    asyncio.run(run_swarm())