"""
Helpers for normalising RDF store base URLs and building repository endpoints.
"""

from urllib.parse import quote


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
