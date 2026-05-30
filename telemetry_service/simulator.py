import asyncio
import httpx
import random
import time

URL = "http://localhost:8000/api/telemetry"
TEAMS = ["team_alpha", "team_beta", "team_gamma"]

async def fire_bot_request(client, bot_id):
    """Simulates a single bot sending a metric payload"""
    payload = {
        "team_id": random.choice(TEAMS),
        "latency_ms": round(random.uniform(2.5, 45.0), 2),
        "is_correct": random.choices([True, False], weights=[0.9, 0.1])[0]
    }
    
    try:
        response = await client.post(URL, json=payload)
        if response.status_code == 202:
            print(f"Bot {bot_id} hit target successfully.")
        else:
            print(f" Bot {bot_id} failed: {response.status_code}")
    except Exception as e:
        print(f" Bot {bot_id} connection error: {e}")

async def main():
    print(" Initiating 100 concurrent bot requests...")
    start_time = time.time()
    
    # created async HTTP client and launched 100 requests simultaneously
    async with httpx.AsyncClient() as client:
        tasks = [fire_bot_request(client, i) for i in range(1, 101)]
        await asyncio.gather(*tasks)
        
    duration = time.time() - start_time
    print(f"\n Simulation complete. 100 requests handled in {duration:.2f} seconds.")

if __name__ == "__main__":
    asyncio.run(main())