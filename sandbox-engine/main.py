from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uuid
import os
import io
import json
import redis
from minio import Minio

app = FastAPI(title="IICPC Sandbox Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

minio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False,
)

redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))

BUCKET_NAME = "submissions"
ALLOWED_EXTENSIONS = {".cpp", ".rs", ".go", ".zip", ".tar.gz"}


@app.on_event("startup")
def startup():
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)


@app.post("/submit")
async def submit(file: UploadFile = File(...)):
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")

    submission_id = str(uuid.uuid4())
    object_name = f"{submission_id}/{file.filename}"

    contents = await file.read()
    minio_client.put_object(
        BUCKET_NAME,
        object_name,
        io.BytesIO(contents),
        length=len(contents),
        content_type=file.content_type,
    )

    redis_client.set(f"submission:{submission_id}:status", json.dumps({
        "submission_id": submission_id,
        "status": "queued",
        "filename": file.filename,
    }))

    redis_client.lpush("sandbox_jobs", json.dumps({
        "submission_id": submission_id,
        "object_name": object_name,
        "filename": file.filename,
        "ext": ext,
    }))

    return {"submission_id": submission_id, "status": "queued"}


@app.get("/submission/{submission_id}/status")
def get_status(submission_id: str):
    raw = redis_client.get(f"submission:{submission_id}:status")
    if not raw:
        raise HTTPException(status_code=404, detail="Submission not found")
    return json.loads(raw)


@app.get("/health")
def health():
    return {"status": "ok"}