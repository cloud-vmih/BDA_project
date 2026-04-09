FROM apache/superset:latest
USER root

RUN pip install --no-cache-dir --target /app/.venv/lib/python3.10/site-packages sqlalchemy-trino trino

# Copy entrypoint
COPY superset_entrypoint.sh /app/superset-entrypoint.sh
RUN chmod +x /app/superset-entrypoint.sh

USER superset

ENTRYPOINT ["/app/superset-entrypoint.sh"]