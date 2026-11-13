"""
The read queries a consumer of the store sends, as finished ``.rq`` files.

A file is named after the variables it asks for:

    single_column__<variable>.rq
    multi_column__<variable>__<variable>.rq

The files in this folder are STRONG AYA's own queries (see ``render.py`` and
``UPSTREAM.md``). A side of a comparison may bring another folder with the
same file names, to compare rewritten queries with these: the answers are
checked against the generated data either way (``oracle.py``).
"""

from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

DEFAULT_FOLDER = Path(__file__).resolve().parent


def single_name(variable: str) -> str:
    return f"single_column__{variable}.rq"


def pair_name(first: str, second: str) -> str:
    return f"multi_column__{first}__{second}.rq"


def variables_of(file_name: str) -> Tuple[str, ...]:
    """The variables a query file asks for, from its name."""
    stem = file_name[:-3] if file_name.endswith(".rq") else file_name
    kind, _, rest = stem.partition("__")
    names = tuple(rest.split("__")) if rest else ()
    if (kind, len(names)) not in (("single_column", 1), ("multi_column", 2)):
        raise ValueError(
            f"'{file_name}' is not named single_column__<variable>.rq or "
            "multi_column__<variable>__<variable>.rq"
        )
    return names


def load(
    folder: Optional[Path] = None, holding: Optional[Iterable[str]] = None
) -> Dict[str, str]:
    """The queries of ``folder`` (this one by default), by file name. With
    ``holding``, only those whose variables are all among these."""
    folder = folder or DEFAULT_FOLDER
    available = None if holding is None else set(holding)
    queries = {}
    for path in sorted(folder.glob("*.rq")):
        if available is None or set(variables_of(path.name)) <= available:
            queries[path.name] = path.read_text(encoding="utf-8")
    return queries
