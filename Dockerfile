FROM python:3.12-slim

WORKDIR /app

# 시스템 의존성 (lxml 빌드용)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libxml2-dev libxslt1-dev && \
    rm -rf /var/lib/apt/lists/*

# 의존성 먼저 설치 (캐시 활용)
COPY pyproject.toml ./
RUN pip install --no-cache-dir .

# 소스 복사
COPY . .

# data 디렉토리 (duckdb, portfolio.json 저장)
RUN mkdir -p /app/data
VOLUME /app/data

CMD ["python", "main.py"]
