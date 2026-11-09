"""
Response compression with Flask-Compress.

(Not named ``compression``: Python 3.14 adds a standard-library package of
that name, which Flask-Compress imports, and this folder is sometimes on
``sys.path`` itself.)

Flask-Compress compresses a response whose type is listed in
``COMPRESS_MIMETYPES`` once it is at least ``COMPRESS_MIN_SIZE`` bytes,
choosing zstd, Brotli, gzip or deflate from the client's ``Accept-Encoding``
and adding ``Vary: Accept-Encoding``. Two of its behaviours do not suit the
files Flyover serves, so :class:`FlyoverCompress` adjusts them:

- A partial response (206) is never compressed. Its ``Content-Range`` counts
  bytes of the uncompressed file, so a compressed body would corrupt a
  ranged or resumed download.
- Flask-Compress marks a compressed response's ETag with the encoding
  (``"abc"`` becomes ``"abc:gzip"``), and the browser revalidates with that
  value. The marker is removed from ``If-None-Match`` before Flask compares
  it with the file's own ETag, so a cached asset gets its 304 instead of the
  whole file again.
"""

import re

from flask import Flask, request
from flask_compress import Compress

# The types Flyover serves that compress well. Already-compressed formats
# (zip downloads, woff2 fonts, PNG) are left out on purpose.
COMPRESS_MIMETYPES = [
    "text/html",
    "text/css",
    "text/plain",
    "text/javascript",
    "application/javascript",
    "application/json",
    "application/ld+json",
    "application/n-triples",
    "image/svg+xml",
    "image/vnd.microsoft.icon",
    "image/x-icon",
]

_ENCODING_MARKER = re.compile(r':(?:zstd|br|gzip|deflate)"')


def _strip_encoding_markers() -> None:
    """Map ETags from compressed responses back to the file's own ETag."""
    validators = request.environ.get("HTTP_IF_NONE_MATCH")
    if validators:
        request.environ["HTTP_IF_NONE_MATCH"] = _ENCODING_MARKER.sub('"', validators)


class FlyoverCompress(Compress):
    """Flask-Compress that leaves partial responses alone and keeps
    conditional requests working on compressed validators."""

    def init_app(self, app: Flask) -> None:
        app.config.setdefault("COMPRESS_MIMETYPES", COMPRESS_MIMETYPES)
        super().init_app(app)
        app.before_request(_strip_encoding_markers)

    def after_request(self, response):
        if response.status_code == 206 or "Content-Range" in response.headers:
            return response
        return super().after_request(response)
