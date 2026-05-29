import os
import json
import time
import redis
import docker
import tempfile
from minio import Minio

redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
docker_client = docker.from_env()
minio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False,
)

BUCKET_NAME = "submissions"

BASE_IMAGES = {
    ".cpp":    "gcc:latest",
    ".rs":     "rust:slim",
    ".go":     "golang:alpine",
    ".zip":    "python:3.13-slim",
    ".tar.gz": "python:3.13-slim",
}


def update_status(submission_id, status, detail=None):
    data = {"submission_id": submission_id, "status": status}
    if detail:
        data["detail"] = detail
    redis_client.set(f"submission:{submission_id}:status", json.dumps(data))
    print(f"[{submission_id[:8]}] {status}" + (f" — {detail}" if detail else ""))


def process_job(job):
    submission_id = job["submission_id"]
    object_name   = job["object_name"]
    filename      = job["filename"]
    ext           = job["ext"]

    update_status(submission_id, "downloading")

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, filename)
        minio_client.fget_object(BUCKET_NAME, object_name, local_path)

        update_status(submission_id, "building")

        base_image = BASE_IMAGES.get(ext, "ubuntu:22.04")

        dockerfile = f"FROM {base_image}\nWORKDIR /submission\nCOPY {filename} .\n"
        with open(os.path.join(tmpdir, "Dockerfile"), "w") as f:
            f.write(dockerfile)

        try:
            image, logs = docker_client.images.build(
                path=tmpdir,
                tag=f"submission-{submission_id}:latest",
                rm=True,
            )
            for log in logs:
                if "stream" in log:
                    print(log["stream"].strip())
        except docker.errors.BuildError as e:
            update_status(submission_id, "failed", str(e))
            return

        update_status(submission_id, "deploying")

        try:
            container = docker_client.containers.run(
                f"submission-{submission_id}:latest",
                detach=True,
                mem_limit="512m",
                cpu_period=100000,
                cpu_quota=50000,
                network_mode="bridge",
                cap_drop=["ALL"],
                labels={"submission_id": submission_id},
                name=f"sub-{submission_id[:8]}",
            )
            update_status(submission_id, "ready", f"container:{container.short_id}")
        except docker.errors.ContainerError as e:
            update_status(submission_id, "failed", str(e))


def main():
    print("Runner started, waiting for jobs...")
    while True:
        try:
            result = redis_client.brpop("sandbox_jobs", timeout=0)
            if result:
                _, raw = result
                job = json.loads(raw)
                print(f"Got job: {job['submission_id'][:8]}")
                try:
                    process_job(job)
                except Exception as e:
                    update_status(job["submission_id"], "failed", str(e))
        except Exception as e:
            print(f"Redis error: {e}, retrying in 3s...")
            time.sleep(3)


if __name__ == "__main__":
    main()