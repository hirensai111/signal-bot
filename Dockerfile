# syntax=docker/dockerfile:1
FROM python:3.12-slim AS build
WORKDIR /src
COPY pyproject.toml README.md ./
COPY signal_bot ./signal_bot
RUN pip install --no-cache-dir build && python -m build --wheel --outdir /dist

FROM python:3.12-slim
ARG SIGNAL_BOT_COMMIT=unknown
ENV PYTHONUNBUFFERED=1 \
    SIGNAL_BOT_COMMIT=${SIGNAL_BOT_COMMIT} \
    LOG_FORMAT=json \
    OPS_HOST=0.0.0.0
RUN useradd --create-home --uid 10001 bot
COPY --from=build /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl
USER bot
WORKDIR /home/bot
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=3)"
CMD ["signal-bot"]
