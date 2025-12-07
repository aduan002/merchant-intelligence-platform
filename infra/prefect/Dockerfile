FROM python:3.12.10

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PREFECT_LOGGING_LEVEL=INFO

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY flows/ flows/
COPY postgres_queries/ postgres_queries/
COPY infra/prefect/entrypoint.sh entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
