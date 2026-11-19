# FADE ml-scorer image (Phase 0 skeleton).
# Build context is the repository root (see deploy/docker-compose.yml).
FROM python:3.12.8-slim-bookworm AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build
COPY ml/serve/requirements.txt ./
RUN pip install --timeout 120 --retries 5 --prefix=/install -r requirements.txt

FROM python:3.12.8-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 fade
COPY --from=builder /install /usr/local
COPY ml /app/ml
COPY ml/serve/app.py /app/app.py

ENV PYTHONPATH=/app \
    OMP_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1

USER fade
WORKDIR /app
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).status == 200 else 1)"

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
