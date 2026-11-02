FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN addgroup --system --gid 10001 emoda \
    && adduser --system --uid 10001 --ingroup emoda --home /app emoda

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY alembic.ini ./
COPY alembic ./alembic
RUN pip install .

RUN mkdir -p /data/media /data/session && chown -R emoda:emoda /app /data
USER emoda

CMD ["emoda", "run"]

