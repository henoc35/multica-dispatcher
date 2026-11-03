FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STATE_FILE=/data/state.json \
    HEALTH_PORT=8080

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt \
 && useradd --system --uid 10001 --home /app dispatcher \
 && mkdir -p /data && chown dispatcher /data

COPY dispatcher.py .
USER dispatcher

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request,sys; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=4); sys.exit(0)"

CMD ["python", "dispatcher.py"]
