from fastapi import FastAPI
import asyncpg
import redis
import logging

# Set up logging so you can see connection statuses in your terminal
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TelemetryDay2")

app = FastAPI(title="IICPC Telemetry Engine - Day 2 Core")

# These credentials match your docker-compose file perfectly
DB_DSN = "postgres://hackathon_admin:super_secure_password_123@localhost:5433/iicpc_telemetry"
REDIS_URL = "redis://localhost:6379"

# Global placeholders for your database connections
db_pool = None
redis_client = None

@app.on_event("startup")
async def startup_event():
    global db_pool, redis_client
    
    # 1. Initialize PostgreSQL Connection Pool
    try:
        db_pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=10)
        logger.info("✅ PostgreSQL Connection Pool established successfully.")
        
        # Automatically create the telemetry table for storing bot data
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
            logger.info("✅ Telemetry database tables verified/created.")
    except Exception as e:
        logger.error(f"❌ Failed to connect to PostgreSQL: {e}")

    # 2. Initialize Redis Connection
    try:
        redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        logger.info("✅ Redis connection established successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Redis: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    # Clean up connections when shutting down the app
    if db_pool:
        await db_pool.close()
    logger.info("Database pools closed.")

@app.get("/health")
async def health_check():
    """Simple endpoint to verify infrastructure health at a glance"""
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