FROM python:3.13-slim
WORKDIR /app
COPY req.txt .
RUN pip install --no-cache-dir fastapi uvicorn redis minio python-multipart docker
COPY main.py .
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]