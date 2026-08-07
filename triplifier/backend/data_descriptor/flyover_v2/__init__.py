"""Flyover v2 application package.

The v2 package deliberately lives beside the legacy application.  It owns its
state, API, and converter contracts, while the legacy blueprints keep serving
the existing workflow until the migration is complete.
"""

def create_api_blueprint(data_root):
    """Import the HTTP layer lazily so domain modules stay lightweight."""
    from .api import create_api_blueprint as create

    return create(data_root)


__all__ = ["create_api_blueprint"]
