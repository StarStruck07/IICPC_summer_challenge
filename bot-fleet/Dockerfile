# Single image for the load-generation side: runs the bot fleet by default,
# and the mock matching engine when the command is overridden (see compose).
FROM python:3.13-slim

WORKDIR /app

# Deps installed inline (req.txt intentionally skipped for now).
#   bots.py        -> aiohttp, redis (>=4.2 for redis.asyncio)
#   mock_engine.py -> fastapi, uvicorn, pydantic
RUN pip install --no-cache-dir aiohttp redis fastapi uvicorn pydantic

# Expects bots.py, metrics.py, mock_engine.py to live in bot-fleet/
COPY *.py .

# Default role: a load-generating replica. The mock-engine service in
# docker-compose.bot-fleet.yml overrides this command to run uvicorn instead.
CMD ["python", "bots.py"]