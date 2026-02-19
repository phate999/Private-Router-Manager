"""Gunicorn configuration for Private Router Manager production deployment."""

import multiprocessing
import os

# Bind address and port
bind = os.environ.get("GUNICORN_BIND", "0.0.0.0:9000")

# Use single worker: routers_data is in-memory and process-local.
# Multi-worker would require shared storage (Redis, DB) for router state.
workers = int(os.environ.get("GUNICORN_WORKERS", 1))

worker_class = "sync"
timeout = 120
keepalive = 5

# Logging
accesslog = os.environ.get("GUNICORN_ACCESS_LOG", "-")  # stdout
errorlog = os.environ.get("GUNICORN_ERROR_LOG", "-")  # stderr
loglevel = os.environ.get("GUNICORN_LOG_LEVEL", "info")
