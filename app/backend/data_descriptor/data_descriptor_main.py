# Apply gevent monkey patching as early as possible
from gevent import monkey

monkey.patch_all()

import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def setup_logging() -> None:
    """Setup centralised logging with timestamp format"""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %z",
        handlers=[logging.StreamHandler()],
    )


# Initialize logging immediately
setup_logging()
logger = logging.getLogger(__name__)

from app_factory import AppSettings, create_app

# The blueprints, the SPA route, the session cache, the services and the
# APP_CONTEXT the controllers read are all built by the factory (see
# app_factory.py); this module only adds what gunicorn needs on top: gevent's
# monkey patching and logging, both applied before anything else is imported.
app = create_app(AppSettings.from_env())

if __name__ == "__main__":
    # Use 0.0.0.0 in Docker (safe within container network), 127.0.0.1 for local dev
    is_docker = (
        os.getenv("FLYOVER_RDF_STORE_URL") or os.getenv("FLYOVER_GRAPHDB_URL")
    ) is not None
    default_host = "0.0.0.0" if is_docker else "127.0.0.1"

    host = os.getenv("FLASK_HOST", default_host)
    port = int(os.getenv("FLASK_PORT", "5000"))

    app.run(host=host, port=port)
