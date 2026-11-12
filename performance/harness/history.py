"""
The history of a workflow's runs: a page with one row per run, newest
first, and beside it every run's result file.

The folder is meant to live in the project's wiki (``performance/<workflow>``
there), where the job that runs after a merge adds to it. Nothing here
knows about git or the wiki: it only writes the page and the file.
"""

import re
from pathlib import Path
from typing import Any, Mapping

from . import report
from . import result as results

PAGE = "{workflow}-performance.md"
RESULTS = "results"

_INTRODUCTION = """\
# {title} performance

How long Flyover's {title} workflow takes, measured after every change to
`main`. Each run puts `main` (the candidate) next to the last release (the
baseline) on the same machine and does the same work on both, several
times over. A figure shows the candidate's median and, in brackets, how it
differs from the baseline: `36.6 s (+30 %)` is 30 % slower than the last
release. ⚠️ marks a figure that got worse by the threshold or more, 🟢 one
that got better.

The seconds of two rows are not comparable: the runs were on different
machines. The percentages are, because both sides of a run shared one.

What is measured, and how to run it yourself, is in
[`performance/README.md`](https://github.com/MaastrichtU-CDS/Flyover/blob/main/performance/README.md).
Every run's full result is kept beside this page, in the `results` folder
of the wiki's repository.

## Runs on main

"""


def add(
    result: Mapping[str, Any],
    folder: Path,
    candidate: str = "",
    link: str = "",
) -> Path:
    """Add a run to the history in ``folder``. Returns the page."""
    folder.mkdir(parents=True, exist_ok=True)
    stamp = re.sub(r"[^0-9TZ-]", "", result["created"])
    results.write(dict(result), folder / RESULTS / f"{stamp}.json")

    workflow = result["workflow"]
    page = folder / PAGE.format(workflow=workflow.upper())
    header = report.history_header(result)
    rows = report.history_rows(result, candidate, link)
    if page.is_file():
        text = page.read_text(encoding="utf-8")
    else:
        text = _INTRODUCTION.format(title=workflow.upper()) + header + "\n"

    lines = text.rstrip("\n").split("\n")
    separator = header.split("\n")[1]
    if separator in lines:
        # Newest first: right under the table's heading.
        at = lines.index(separator) + 1
        lines[at:at] = rows.split("\n")
    else:
        # The figures have changed since the table was begun: a new table.
        lines += ["", header, rows]
    page.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return page
