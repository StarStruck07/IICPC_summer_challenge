from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncpg
import redis.asyncio as redis
import asyncio
import json
import logging
import os
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LeaderboardAPI")

app = FastAPI(title="IICPC Distributed Telemetry & Leaderboard")

DB_DSN = os.getenv("DB_DSN", "postgres://hackathon_admin:super_secure_password_123@postgres:5432/iicpc_telemetry")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

db_pool: Optional[asyncpg.Pool] = None
redis_client: Optional[redis.Redis] = None
active_websockets: list[WebSocket] = []


@app.on_event("startup")
async def startup():
    global db_pool, redis_client
    
    # 1. Connect to Redis
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    
    # 2. Connect to PostgreSQL (With Retry Loop)
    retries = 5
    while retries > 0:
        try:
            db_pool = await asyncpg.create_pool(DB_DSN, min_size=5, max_size=20)
            logger.info("✅ PostgreSQL connected successfully.")
            break
        except Exception as e:
            retries -= 1
            logger.warning(f"⏳ Waiting for PostgreSQL to boot... ({retries} retries left)")
            await asyncio.sleep(3)
            
    if db_pool is None:
        logger.error(" Could not connect to PostgreSQL. Crashing gracefully.")
        return

    # 3. Create tables safely
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS telemetry (
                id SERIAL PRIMARY KEY,
                team_id VARCHAR(50) NOT NULL,
                latency_ms FLOAT NOT NULL,
                is_correct BOOLEAN NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    
    # 4. Start Background Tasks
    asyncio.create_task(redis_stream_consumer())
    asyncio.create_task(broadcast_leaderboard())
    logger.info("✅ System Online: DB Pool, Redis, and Background Tasks running.")


# THE REDIS CONSUMER
async def redis_stream_consumer():
    """Reads from the 'telemetry_events' stream written by bot-fleet/bots.py"""
    last_id = "0-0"
    while True:
        if redis_client is None or db_pool is None:
            await asyncio.sleep(1)
            continue

        try:
            streams = await redis_client.xread({"telemetry_events": last_id}, count=1000, block=1000)
            if not streams:
                continue
                
            _, messages = streams[0] # type: ignore
            batch_data = []
            message_ids = []
            
            for msg_id, data in messages: # type: ignore
                if not isinstance(data, dict):
                    continue

                is_correct = data.get("is_correct") in ("1", 1, "true", "True")
                batch_data.append((
                    str(data.get("team_id", "unknown")), 
                    float(data.get("latency_ms", 0.0)), 
                    is_correct
                ))
                
                # Ensure msg_id is treated as a string for xdel
                msg_id_str = str(msg_id)
                message_ids.append(msg_id_str)
                last_id = msg_id_str

            if batch_data:
                async with db_pool.acquire() as conn:
                    await conn.executemany(
                        "INSERT INTO telemetry (team_id, latency_ms, is_correct) VALUES ($1, $2, $3)",
                        batch_data
                    )
                # Unpack the list of IDs into the xdel function
                await redis_client.xdel("telemetry_events", *message_ids)
                logger.info(f"💾 Batched {len(batch_data)} records to PostgreSQL.")
                
        except Exception as e:
            logger.error(f"Consumer Error: {e}")
            await asyncio.sleep(1)


# THE ANALYTICS CALCULATOR
async def calculate_leaderboard():
    if db_pool is None:
        return []
        
    query = """
        SELECT 
            team_id,
            COUNT(id) as total_requests,
            ROUND(COUNT(id) / 60.0, 2) as tps,
            ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) as p50,
            ROUND(percentile_cont(0.9) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) as p90,
            ROUND(percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) as p99,
            ROUND((COUNT(id) FILTER (WHERE is_correct = true) * 100.0 / NULLIF(COUNT(id), 0))::numeric, 2) as accuracy
        FROM telemetry
        GROUP BY team_id;
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query)
        
    leaderboard = []
    for r in rows:
        score = (float(r["accuracy"] or 0) * 10) + float(r["tps"]) - (float(r["p90"] or 0) * 0.5)
        leaderboard.append({
            "team_id": r["team_id"],
            "tps": float(r["tps"]),
            "p50": float(r["p50"] or 0),
            "p90": float(r["p90"] or 0),
            "p99": float(r["p99"] or 0),
            "accuracy": float(r["accuracy"] or 0),
            "score": round(max(0, score), 2)
        })
    return sorted(leaderboard, key=lambda x: x["score"], reverse=True)


@app.get("/api/leaderboard")
async def get_leaderboard():
    return await calculate_leaderboard()


# WEBSOCKETS FOR REACT FRONTEND
@app.websocket("/ws/leaderboard")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_websockets.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_websockets.remove(websocket)


async def broadcast_leaderboard():
    while True:
        if active_websockets:
            try:
                data = await calculate_leaderboard()
                for ws in active_websockets:
                    await ws.send_text(json.dumps(data))
            except Exception as e:
                logger.error(f"Broadcast Error: {e}")
        await asyncio.sleep(1)