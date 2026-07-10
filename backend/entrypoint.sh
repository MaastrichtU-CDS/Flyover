#!/bin/bash

# Read GUNICORN_TIMEOUT from environment, default to 300 seconds
GUNICORN_TIMEOUT=${GUNICORN_TIMEOUT:-300}

# Worker recycling wipes the in-memory session cache (the app's entire
# session state), silently destroying a user's in-progress flow — and the
# suggestion polling endpoints make reaching any request cap realistic.
# Default it off; set GUNICORN_MAX_REQUESTS to re-enable recycling.
GUNICORN_MAX_REQUESTS=${GUNICORN_MAX_REQUESTS:-0}

exec gunicorn \
    --workers 1 \
    --worker-class gevent \
    --worker-connections 1000 \
    --timeout "$GUNICORN_TIMEOUT" \
    --keep-alive 5 \
    --max-requests "$GUNICORN_MAX_REQUESTS" \
    --max-requests-jitter 50 \
    --log-level info \
    --access-logfile - \
    --access-logformat "%(t)s [ACCESS] %(m)s %(U)s %(s)s request by %(h)s" \
    --error-logfile - \
    --bind 0.0.0.0:5000 \
    --preload \
    --env GEVENT_SUPPORT=True \
    flyover.main:app
