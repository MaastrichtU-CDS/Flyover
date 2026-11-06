"""
Helpers for normalising RDF store base URLs and building repository endpoints.
"""

import ipaddress
from typing import Tuple
from urllib.parse import quote, urlsplit, urlunsplit

# Every store Flyover talks to (GraphDB, RDF4J and the QLever adapter) is
# reached over HTTP; anything else in a map's endpoint is refused.
STORE_URL_SCHEMES = ("http", "https")


def normalise_rdf_store_base_url(rdf_store_url: str, repo: str | None = None) -> str:
    """
    Normalise an RDF store URL to the server base URL.

    Flyover internally expects a base server URL and appends
    `/repositories/{repo}` itself. Older configurations may provide either:
    - `.../repositories`
    - `.../repositories/{repo}`

    This helper strips those suffixes so endpoint construction remains valid
    for both GraphDB and RDF4J deployments.
    """
    if not rdf_store_url:
        return rdf_store_url

    base_url = rdf_store_url.rstrip("/")

    if repo and base_url.endswith(f"/repositories/{repo}"):
        return base_url[: -(len(f"/repositories/{repo}"))]

    if base_url.endswith("/repositories"):
        return base_url[: -len("/repositories")]

    return base_url


def build_repository_endpoint(
    rdf_store_url: str, repo: str, endpoint_suffix: str = ""
) -> str:
    """
    Build a repository endpoint from a server base URL and repository name.
    """
    base_url = normalise_rdf_store_base_url(rdf_store_url, repo).rstrip("/")
    suffix = (
        endpoint_suffix
        if endpoint_suffix.startswith("/") or not endpoint_suffix
        else f"/{endpoint_suffix}"
    )
    return f"{base_url}/repositories/{repo}{suffix}"


def build_graph_store_endpoint(rdf_store_url: str, repo: str, graph_uri: str) -> str:
    """
    Build a Graph Store Protocol endpoint for a named graph.
    """
    repository_endpoint = build_repository_endpoint(
        rdf_store_url, repo, "/rdf-graphs/service"
    )
    return f"{repository_endpoint}?graph={quote(graph_uri, safe='')}"


def repository_of_endpoint(endpoint: str) -> Tuple[str, str]:
    """
    Split a repository endpoint into the store's base URL and the repository.

    Flyover reaches every store through ``<base>/repositories/<repository>``,
    so an endpoint can be used when its path holds ``/repositories/<name>``.
    Whatever follows the name, such as the ``/statements`` of an update
    endpoint, is dropped, and so are a query and a fragment. Credentials in
    the URL stay in the base URL, as the store needs them.

    Raises:
        ValueError: when the endpoint is not an http(s) URL with a host and a
            repository in its path; the message says what is missing.
    """
    parts = urlsplit((endpoint or "").strip())
    if parts.scheme.lower() not in STORE_URL_SCHEMES:
        raise ValueError("it must start with http:// or https://")
    if not parts.hostname:
        raise ValueError("it names no host")
    try:
        parts.port
    except ValueError:
        raise ValueError("its port is not a number from 0 to 65535") from None
    segments = parts.path.split("/")
    if "repositories" in segments:
        place = segments.index("repositories")
        repository = segments[place + 1] if place + 1 < len(segments) else ""
        if repository:
            base_path = "/".join(segments[:place])
            base_url = urlunsplit(
                (parts.scheme.lower(), parts.netloc, base_path, "", "")
            )
            return base_url, repository
    raise ValueError("its path has no /repositories/<name> part")


def without_credentials(url: str) -> str:
    """
    The URL with any user name and password taken out, for showing it.
    """
    parts = urlsplit(url or "")
    if parts.username is None and parts.password is None:
        return url
    host = parts.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    try:
        port = parts.port
    except ValueError:
        port = None
    netloc = f"{host}:{port}" if port else host
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def names_this_machine(url: str) -> bool:
    """
    Whether the URL's host is the machine it is used on (localhost or a
    loopback address such as 127.0.0.1 or ::1).
    """
    host = (urlsplit(url or "").hostname or "").lower()
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False
