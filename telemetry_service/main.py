from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncpg
import redis
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TelemetryDay3")

app = FastAPI(title="IICPC Telemetry Engine - Day 3")

# custom port 5433!
DB_DSN = "postgres://hackathon_admin:super_secure_password_123@localhost:5433/iicpc_telemetry"
REDIS_URL = "redis://localhost:6379"

db_pool = None
redis_client = None

# Data Validation Schema
class TelemetryPayload(BaseModel):
    team_id: str
    latency_ms: float
    is_correct: bool

@app.on_event("startup")
async def startup_event():
    global db_pool, redis_client
    
    # PostgreSQL Connection Pool
    try:
        db_pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=20)
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
        logger.info("Database pools connected and tables verified.")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")

    # Redis Connection
    try:
        redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        logger.info("Redis connection established successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    if db_pool:
        await db_pool.close()
    logger.info("Database pools closed.")

# Ingestion Endpoint
@app.post("/api/telemetry", status_code=202)
async def ingest_telemetry(payload: TelemetryPayload):
    """
    Catches bot data and queues it into PostgreSQL.
    asyncpg ensures the event loop never blocks.
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database connection lost")
        
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO telemetry (team_id, latency_ms, is_correct)
                VALUES ($1, $2, $3)
                """,
                payload.team_id, payload.latency_ms, payload.is_correct
            )
        return {"status": "queued"}
    except Exception as e:
        logger.error(f"Insert failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to save metric")

# Day 2 Health Check
@app.get("/health")
async def health_check():
    """Simple endpoint to verify infrastructure health."""
    postgres_healthy = False
    redis_healthy = False
    
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("SELECT 1;")
                postgres_healthy = True
        except Exception:
            pass
            
    if redis_client:
        try:
            redis_healthy = redis_client.ping()
        except Exception:
            pass

    return {
        "status": "online",
        "systems": {
            "postgresql": "CONNECTED" if postgres_healthy else "OFFLINE",
            "redis": "CONNECTED" if redis_healthy else "OFFLINE"
        }
    }