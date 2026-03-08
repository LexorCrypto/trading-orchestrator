FROM python:3.12-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY orchestrator/ ./orchestrator/

EXPOSE 8002

CMD ["uvicorn", "orchestrator.main:app", "--host", "0.0.0.0", "--port", "8002", "--workers", "1"]
