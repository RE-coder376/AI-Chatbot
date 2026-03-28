FROM python:3.11-slim

# Install system deps (cmake needed if any wheel must compile from source)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ cmake libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv via pip, then use uv for fast installs
# --prefer-binary avoids C++ source compilation for hnswlib/chromadb
COPY requirements.txt .
RUN pip install uv && uv pip install --system -r requirements.txt

# Copy app code
COPY . .

# HF Spaces runs as non-root user 1000
RUN chown -R 1000:1000 /app
USER 1000

EXPOSE 7860

CMD ["gunicorn", "app:app", "-w", "1", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:7860", "--timeout", "120"]
