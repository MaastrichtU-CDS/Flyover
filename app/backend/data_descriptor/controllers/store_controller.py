"""
Store controller: the datasets in the RDF store Flyover works on.

The status bar's store opens a panel that lists them and deletes them.
Routes are the ``/api/v1/store`` surface:

- ``GET /api/v1/store/datasets``: every dataset, with its rows and whether
  it was described and annotated;
- ``DELETE /api/v1/store/datasets/<name>``: one dataset, everything Flyover
  made for it;
- ``DELETE /api/v1/store/datasets``: every dataset (Flyover's graphs only);
- ``DELETE /api/v1/store/annotations``: every annotation and materialised
  inference, keeping the data and the describe answers;
- ``GET /api/v1/store/datasets/<name>/export``, ``GET /api/v1/store/export``
  (every dataset) and ``GET /api/v1/store/annotations/export``: what the
  matching delete removes, as a TriG file to keep or import again.

They work on the store the current request works on: an open project's, or
the configured one. After a delete, the session forgets what it knew of the
deleted datasets.
"""

import logging
from typing import Iterable, Optional

from flask import Blueprint, Response, current_app, jsonify
from werkzeug.utils import secure_filename

from services.store_datasets import StoreDatasetError, valid_name
from services.store_selection import pin_store

logger = logging.getLogger(__name__)

store_bp = Blueprint("store", __name__)


def _context() -> dict:
    return current_app.config.get("APP_CONTEXT", {})


def _datasets():
    ctx = _context()
    return ctx["store_datasets"](pin_store(ctx["rdf_store_service"]))


def _failed(error: StoreDatasetError):
    return jsonify({"error": error.message}), error.status


def _trig(content: str, name: str) -> Response:
    filename = f"{secure_filename(name) or 'flyover'}.trig"
    return Response(
        content,
        mimetype="application/trig",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _checked(name: str, datasets) -> Optional[tuple]:
    """A 400 or 404 for a name that is not a dataset of this store."""
    if not valid_name(name):
        return jsonify({"error": f"{name!r} cannot be a dataset's name."}), 400
    if not datasets.has(name):
        return jsonify({"error": f"The store has no dataset named {name!r}."}), 404
    return None


def _forget(names: Optional[Iterable[str]] = None) -> None:
    """What the session knew of deleted datasets (None: of all of them)."""
    session_cache = _context().get("session_cache")
    if session_cache is None:
        return
    gone = None if names is None else set(names)
    session_cache.databases = None
    session_cache.existing_graph = False
    for attribute in ("descriptive_info", "DescriptiveInfoDetails"):
        value = getattr(session_cache, attribute, None)
        if gone is None or not isinstance(value, dict):
            setattr(session_cache, attribute, None if gone is None else value)
        else:
            setattr(
                session_cache,
                attribute,
                {k: v for k, v in value.items() if k not in gone},
            )
    status = getattr(session_cache, "annotation_status", None)
    if gone is None or not isinstance(status, dict):
        session_cache.annotation_status = None if gone is None else status
    else:
        session_cache.annotation_status = {
            key: value
            for key, value in status.items()
            if key.split(".", 1)[0] not in gone
        }
    # Suggestions were worked out over the datasets' columns.
    session_cache.suggestion_jobs = None
    session_cache.suggestion_ingested = None


@store_bp.route("/api/v1/store/datasets", methods=["GET"])
def list_datasets():
    """Every dataset in the store, with its rows and progress."""
    try:
        return jsonify({"datasets": _datasets().list()})
    except StoreDatasetError as error:
        return _failed(error)


@store_bp.route("/api/v1/store/datasets/<name>", methods=["DELETE"])
def delete_dataset(name: str):
    """One dataset: its data, ontology, annotations, inferences and links."""
    datasets = _datasets()
    try:
        refused = _checked(name, datasets)
        if refused:
            return refused
        datasets.delete_dataset(name)
        _forget([name])
        logger.info("Deleted the dataset %r", name)
        return jsonify({"deleted": [name], "datasets": datasets.list()})
    except StoreDatasetError as error:
        return _failed(error)


@store_bp.route("/api/v1/store/datasets/<name>/export", methods=["GET"])
def export_dataset(name: str):
    """One dataset, its links included, as a TriG file."""
    datasets = _datasets()
    try:
        refused = _checked(name, datasets)
        if refused:
            return refused
        return _trig(datasets.export_dataset(name), name)
    except StoreDatasetError as error:
        return _failed(error)


@store_bp.route("/api/v1/store/datasets", methods=["DELETE"])
def delete_all_datasets():
    """Every graph Flyover writes; anything else in the repository stays."""
    datasets = _datasets()
    try:
        names = [dataset["name"] for dataset in datasets.list()]
        datasets.delete_all()
        _forget()
        logger.info("Deleted every dataset (%d)", len(names))
        return jsonify({"deleted": names, "datasets": datasets.list()})
    except StoreDatasetError as error:
        return _failed(error)


# Not under /datasets/: a dataset may be called "export".
@store_bp.route("/api/v1/store/export", methods=["GET"])
def export_all_datasets():
    """Every graph Flyover writes, as a TriG file."""
    try:
        return _trig(_datasets().export_all(), "flyover-datasets")
    except StoreDatasetError as error:
        return _failed(error)


@store_bp.route("/api/v1/store/annotations", methods=["DELETE"])
def delete_annotations():
    """Every annotation and materialised inference; data and descriptions stay."""
    datasets = _datasets()
    try:
        graphs = datasets.delete_annotations()
        session_cache = _context().get("session_cache")
        if session_cache is not None:
            session_cache.annotation_status = None
        logger.info("Deleted every annotation (%d graphs)", len(graphs))
        return jsonify({"deleted": graphs, "datasets": datasets.list()})
    except StoreDatasetError as error:
        return _failed(error)


@store_bp.route("/api/v1/store/annotations/export", methods=["GET"])
def export_annotations():
    """Every annotation and materialised inference, as a TriG file."""
    try:
        return _trig(_datasets().export_annotations(), "flyover-annotations")
    except StoreDatasetError as error:
        return _failed(error)
