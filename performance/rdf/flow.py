"""
The RDF flow's requests, sent the way a browser sends them.

Only routes that have been there since v3.5.0 are used, with the same
fields, so the same steps can be timed on an old release and on today's
branch. Each step is timed at the client; nothing is asked of the app that
a user's session would not ask.
"""

import json
import time
from typing import Any, Dict, Optional

import requests

from ..harness.measure import Step, timed
from ..harness.stack import Stack
from .workload.generator import Table

DBO = "http://um-cds/ontologies/databaseontology/"
RELATIONSHIPS_GRAPH = "http://relationships.local/"
# The triplifier's names: http://data.local/rdf/ontology/<table>.<column>
COLUMN_CLASS = "http://data.local/rdf/ontology/{table}.{column}"

# How long one request may take. Annotation of a large table runs for many
# minutes, all of it inside one request.
REQUEST_TIMEOUT = 3 * 3600


class FlowError(RuntimeError):
    """A step of the flow that did not do its work: the run is void."""


class RdfFlow:
    def __init__(self, stack: Stack):
        self.stack = stack
        self.app = stack.app_url
        self.session = requests.Session()
        # Flyover refuses requests that change something unless they come
        # from its own pages.
        self.session.headers["Origin"] = self.app

    def enter(self) -> None:
        """Choose the standard RDF flow, as the start-up pop-up does on the
        versions that have projects. Older versions have no such route and
        are in the RDF flow already."""
        try:
            self.session.delete(f"{self.app}/api/v1/project", timeout=120)
        except requests.RequestException as error:
            raise FlowError(f"Flyover did not answer: {error}") from error

    # -- ingest ---------------------------------------------------------------

    def upload(self, table: Table, link_to: Optional[Table] = None) -> Step:
        """POST /upload: parse the CSV, triplify it, load it into the store.

        With ``link_to``, the upload also asks for the new table's identifier
        to be linked to that table's, as the ingest page's link form does.
        """
        form = {"fileType": "CSV"}
        if link_to is not None:
            form["crossGraphLinkData"] = json.dumps(
                {
                    "newTableName": table.name,
                    "newColumnName": table.columns[0].name,
                    "existingTableName": link_to.name,
                    "existingColumnName": link_to.columns[0].name,
                }
            )
        step = timed(
            f"upload {table.name}",
            lambda: self.session.post(
                f"{self.app}/upload",
                data=form,
                files={"csvFile": (table.file_name, table.csv(), "text/csv")},
                allow_redirects=False,
                timeout=REQUEST_TIMEOUT,
            ),
        )
        response = step.detail.pop("response", None)
        if step.ok:
            location = response.headers.get("Location", "")
            step.detail["location"] = location
            if step.status != 302 or "error" in location.lower():
                step.error = f"the upload was refused: {step.status} {location[:300]}"
        return step

    def wait_for_link(self, table: Table, timeout: float = 1800) -> Step:
        """Wait until the new table's identifiers refer to the first table's.

        Flyover makes the links in the background, a few seconds after the
        upload has answered. A user is on the next page by then; the
        benchmark waits, so the link job never runs into the next step.
        """
        column = COLUMN_CLASS.format(table=table.name, column=table.columns[0].name)
        query = (
            f"ASK {{ GRAPH <{RELATIONSHIPS_GRAPH}> {{ ?new <{DBO}fk_refers_to> ?old }} "
            f"GRAPH ?data {{ ?new a <{column}> }} }}"
        )
        started = time.perf_counter()
        deadline = started + timeout
        while True:
            try:
                answer = requests.get(
                    self.stack.repository_url,
                    params={"query": query},
                    headers={"Accept": "application/sparql-results+json"},
                    timeout=600,
                )
                if answer.status_code == 200 and answer.json().get("boolean"):
                    return Step(
                        f"link {table.name}", round(time.perf_counter() - started, 4)
                    )
            except (requests.RequestException, ValueError):
                pass
            if time.perf_counter() > deadline:
                return Step(
                    f"link {table.name}",
                    round(time.perf_counter() - started, 4),
                    error="the link to the first table never appeared in the store",
                )
            time.sleep(0.25)

    # -- describe -------------------------------------------------------------

    def load_map(self, mapping: bytes) -> Step:
        """POST /upload-semantic-map: the describe page's map upload."""
        step = timed(
            "load map",
            lambda: self.session.post(
                f"{self.app}/upload-semantic-map",
                files={
                    "semanticMapFile": ("map.jsonld", mapping, "application/ld+json")
                },
                timeout=REQUEST_TIMEOUT,
            ),
        )
        body = _json(step)
        if step.ok and not (step.status == 200 and body.get("success") is True):
            step.error = f"the map was refused: {step.status} {str(body)[:300]}"
        return step

    # -- annotate -------------------------------------------------------------

    def annotate(self, name: str = "annotate") -> Step:
        """POST /start-annotation: annotate every table the loaded map names."""
        step = timed(
            name,
            lambda: self.session.post(
                f"{self.app}/start-annotation", timeout=REQUEST_TIMEOUT
            ),
        )
        body = _json(step)
        if step.ok:
            step.detail["message"] = str(body.get("message") or body.get("error"))[:200]
            if not (step.status == 200 and body.get("success") is True):
                step.error = f"annotation failed: {step.status} {str(body)[:300]}"
        return step

    def annotation_outcome(self) -> Dict[str, Any]:
        """What the last annotation run did, variable by variable, as the
        verify page is told."""
        try:
            answer = self.session.get(
                f"{self.app}/api/v1/annotation-verify-state", timeout=600
            )
            status = answer.json().get("annotation_status") or {}
        except (requests.RequestException, ValueError) as error:
            return {"error": str(error)[:200]}
        failed = sorted(
            key
            for key, entry in status.items()
            if not (isinstance(entry, dict) and entry.get("success"))
        )
        return {"variables": len(status), "failed": failed}


def _json(step: Step) -> Dict[str, Any]:
    response = step.detail.pop("response", None)
    if response is None:
        return {}
    try:
        body = response.json()
    except ValueError:
        return {"body": response.text[:300]}
    return body if isinstance(body, dict) else {"body": body}
