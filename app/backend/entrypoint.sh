#!/bin/bash

# Read GUNICORN_TIMEOUT from environment, default to 300 seconds
GUNICORN_TIMEOUT=${GUNICORN_TIMEOUT:-300}

# Restart the worker after this many requests; 0, the default, never does.
# The one worker holds every session in memory, so a restart loses them and
# ends any request still running, such as an annotation.
GUNICORN_MAX_REQUESTS=${GUNICORN_MAX_REQUESTS:-0}

exec gunicorn \
    --workers 1 \
    --worker-class gevent \
    --worker-connections 1000 \
    --timeout "$GUNICORN_TIMEOUT" \
    --keep-alive 5 \
    --max-requests "$GUNICORN_MAX_REQUESTS" \
    --log-level info \
    --access-logfile - \
    --access-logformat "%(t)s [ACCESS] %(m)s %(U)s %(s)s request by %(h)s" \
    --error-logfile - \
    --bind 0.0.0.0:5000 \
    --preload \
    --env GEVENT_SUPPORT=True \
    data_descriptor.data_descriptor_main:app
