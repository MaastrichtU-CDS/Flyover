"""
The file behind a page that shows it: a code hosting site's page for a file,
and a Zenodo record.

A link to a file on Bitbucket, Forgejo, Gitea, GitHub, GitLab or SourceHut is
usually the address of the page that shows the file, not of the file:
fetched, it gives HTML. Each of these sites serves the file itself at an
address that follows from the page's, and ``raw_file_address`` works it out,
so a user may give the link they have open in their browser.

Forgejo, Gitea and GitLab are recognised on any host, by the shape of their
addresses: code.europa.eu, codeberg.org, framagit.org, gitlab.com and the
instances universities and hospitals run alike. Bitbucket, GitHub and
SourceHut are recognised on their own hosts. GitHub Enterprise, and GitLab's
older links, are recognised by their ``/blob/`` part on any host.

Zenodo serves a record's file at its own address already; only its preview
page is a page around the file, which ``raw_file_address`` works out too. A
record's page, its API address and its DOI name the record, not a file:
``zenodo_record`` reads which record, and only Zenodo's API lists its files.
"""

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote, unquote, urlsplit, urlunsplit

# github.com/<owner>/<repo>/blob/<ref>/<path>
GITHUB_HOSTS = ("github.com", "www.github.com")
GITHUB_RAW_HOST = "raw.githubusercontent.com"
GITHUB_PAGE = re.compile(r"^/([^/]+)/([^/]+)/blob/(.+)$")
# <host>/<group>[/<subgroup>...]/<project>/-/blob/<ref>/<path> (GitLab)
GITLAB_PAGE = "/-/blob/"
GITLAB_RAW = "/-/raw/"
# <host>/<owner>/<repo>/src/{branch,tag,commit}/<ref>/<path> (Gitea, Forgejo)
GITEA_PAGE = re.compile(r"^(/[^/]+/[^/]+)/src/(branch|tag|commit)/(.+)$")
# bitbucket.org/<workspace>/<repo>/src/<ref>/<path>
BITBUCKET_HOST = "bitbucket.org"
BITBUCKET_PAGE = re.compile(r"^(/[^/]+/[^/]+)/src/(.+)$")
# git.sr.ht/~<user>/<repo>/tree/<ref>/item/<path>
SOURCEHUT_HOST = "git.sr.ht"
SOURCEHUT_PAGE = re.compile(r"^(/~[^/]+/[^/]+)/tree/([^/]+)/item/(.+)$")
# <host>/<owner>/<repo>/blob/<ref>/<path> (GitHub Enterprise; GitLab's links
# before its /-/ part, under any number of groups)
BLOB_PAGE = re.compile(r"^(/[^/]+(?:/[^/]+)+?)/blob/(.+)$")

# zenodo.org/records/<id>/preview/<name> (and /record/, the older form)
ZENODO_HOSTS = ("zenodo.org", "www.zenodo.org", "sandbox.zenodo.org")
ZENODO_PREVIEW = re.compile(r"^/records?/(\d+)/preview/(.+)$")
# A record: zenodo.org/records/<id>, /record/<id>, /api/records/<id>, and
# /doi/<doi>; doi.org/10.5281/zenodo.<id> (10.5072 is the sandbox's prefix).
ZENODO_RECORD_PAGE = re.compile(r"^(?:/api)?/records?/(\d+)/?$")
ZENODO_DOI_PAGE = re.compile(r"^/doi/10\.(?:5281|5072)/zenodo\.(\d+)/?$", re.I)
DOI_HOSTS = ("doi.org", "dx.doi.org", "www.doi.org")
ZENODO_DOI = re.compile(r"^/10\.5281/zenodo\.(\d+)/?$", re.I)
ZENODO_DOI_HOST = "zenodo.org"


@dataclass(frozen=True)
class ZenodoRecord:
    """A record on a Zenodo host, by its number."""

    host: str
    record_id: str

    @property
    def api_address(self) -> str:
        return f"https://{self.host}/api/records/{self.record_id}"

    def file_address(self, record_id: str, name: str) -> str:
        """The address of the file ``name`` in the record ``record_id`` (a
        version of this one), which serves the file itself."""
        return f"https://{self.host}/records/{record_id}/files/{quote(name)}"


def raw_file_address(url: str) -> Optional[str]:
    """The address of the raw file whose page ``url`` is, or None.

    None means ``url`` is not a file's page on a site this recognises (an
    address of a raw file among them). The page's query and fragment are
    display options and go, except on GitLab, whose raw files take the same
    query as its pages (``ref_type``, an access token). Credentials in the
    address stay with it on its own host, and never go to another.
    """
    parts = urlsplit((url or "").strip())
    if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
        return None
    host = parts.hostname.lower()
    path = parts.path

    def on_this_host(new_path: str, query: str = "") -> str:
        return urlunsplit((parts.scheme, parts.netloc, new_path, query, ""))

    if host in GITHUB_HOSTS:
        match = GITHUB_PAGE.match(path)
        if not match:
            return None
        owner, repo, rest = match.groups()
        return urlunsplit(("https", GITHUB_RAW_HOST, f"/{owner}/{repo}/{rest}", "", ""))
    if host in ZENODO_HOSTS:
        match = ZENODO_PREVIEW.match(path)
        return (
            on_this_host("/records/{}/files/{}".format(*match.groups()))
            if match
            else None
        )
    if host == SOURCEHUT_HOST:
        match = SOURCEHUT_PAGE.match(path)
        return on_this_host("{}/blob/{}/{}".format(*match.groups())) if match else None
    if host == BITBUCKET_HOST:
        match = BITBUCKET_PAGE.match(path)
        return on_this_host("{}/raw/{}".format(*match.groups())) if match else None
    if GITLAB_PAGE in path:
        return on_this_host(path.replace(GITLAB_PAGE, GITLAB_RAW, 1), parts.query)
    match = GITEA_PAGE.match(path)
    if match:
        return on_this_host("{}/raw/{}/{}".format(*match.groups()))
    match = BLOB_PAGE.match(path)
    if match:
        return on_this_host("{}/raw/{}".format(*match.groups()))
    return None


def zenodo_record(url: str) -> Optional[ZenodoRecord]:
    """The Zenodo record ``url`` names, or None.

    A record's page (``zenodo.org/records/<id>``, or the older ``/record/``),
    its API address and its DOI (``doi.org/10.5281/zenodo.<id>``) name it. A
    concept DOI, which names every version of a record, is a record number
    too: Zenodo's API answers for it with the latest version. A link to one
    of a record's files is not a record: it serves the file itself.
    """
    parts = urlsplit((url or "").strip())
    if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
        return None
    host = parts.hostname.lower()
    if host in ZENODO_HOSTS:
        match = ZENODO_RECORD_PAGE.match(parts.path) or ZENODO_DOI_PAGE.match(
            unquote(parts.path)
        )
        # www.zenodo.org only sends visitors on to zenodo.org.
        host = ZENODO_DOI_HOST if host == "www.zenodo.org" else host
        return ZenodoRecord(host, match.group(1)) if match else None
    if host in DOI_HOSTS:
        match = ZENODO_DOI.match(unquote(parts.path))
        return ZenodoRecord(ZENODO_DOI_HOST, match.group(1)) if match else None
    return None


def shown_address(url: str) -> str:
    """``url`` as a message may show it: without credentials or query."""
    parts = urlsplit(url)
    host = parts.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    try:
        port = parts.port
    except ValueError:
        port = None
    if port:
        host = f"{host}:{port}"
    return urlunsplit((parts.scheme, host, parts.path, "", ""))
