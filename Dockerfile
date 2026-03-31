FROM python:3.11-slim

# Install system deps (cmake needed if any wheel must compile from source)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ cmake libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install packages
COPY requirements.txt .
RUN pip install uv && uv pip install --system -r requirements.txt

# Set HOME so fastembed/HF cache goes to /app/.cache (writable by user 1000)
ENV HOME=/app
# Pre-download fastembed ONNX model so DB loading is instant at runtime
RUN python -c "from langchain_community.embeddings.fastembed import FastEmbedEmbeddings; FastEmbedEmbeddings(model_name='BAAI/bge-small-en-v1.5')" || true

# Copy app code
COPY . .

# Install Playwright to /app/.playwright so user 1000 can access it at runtime
ENV PLAYWRIGHT_BROWSERS_PATH=/app/.playwright
RUN playwright install --with-deps chromium

# HF Spaces runs as non-root user 1000
RUN chown -R 1000:1000 /app
USER 1000

EXPOSE 7860

CMD ["gunicorn", "app:app", "-w", "1", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:7860", "--timeout", "120"]
