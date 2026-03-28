FROM python:3.11-slim

# bust cache: 2026-03-28
# Install system deps for chromadb / lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# HF Spaces runs as non-root user 1000
RUN chown -R 1000:1000 /app
USER 1000

EXPOSE 7860

CMD ["gunicorn", "app:app", "-w", "1", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:7860", "--timeout", "120"]
