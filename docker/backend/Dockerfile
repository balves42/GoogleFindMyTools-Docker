FROM python:3.11-slim

WORKDIR /app

# System deps (some python wheels need these)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy backend + vendor
COPY . /app

# Install backend deps
RUN pip install --no-cache-dir -r /app/requirements.txt

# Install vendor deps (GoogleFindMyTools)
RUN pip install --no-cache-dir -r /app/vendor/GoogleFindMyTools-main/requirements.txt

EXPOSE 8000

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
