#!/bin/bash

# Read GUNICORN_TIMEOUT from environment, default to 300 seconds
GUNICORN_TIMEOUT=${GUNICORN_TIMEOUT:-300}

exec gunicorn \
    --workers 1 \
    --worker-class gevent \
    --worker-connections 1000 \
    --timeout "$GUNICORN_TIMEOUT" \
    --keep-alive 5 \
    --max-requests 1000 \
    --max-requests-jitter 50 \
    --log-level info \
    --access-logfile - \
    --access-logformat "%(t)s [ACCESS] %(m)s %(U)s %(s)s request by %(h)s" \
    --error-logfile - \
    --bind 0.0.0.0:5000 \
    --preload \
    --env GEVENT_SUPPORT=True \
    data_descriptor.data_descriptor_main:app
