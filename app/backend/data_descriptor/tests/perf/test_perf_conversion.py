"""
How the conversion to RDF grows with the table: the triplifier itself, on a
CSV of a chosen size, with no store involved.

The other checks in this folder time the HTTP layer with the services
doubled; this one runs the real conversion service, because that is where
an upload's time goes before the store is reached. It answers one question
a machine's speed cannot blur: does the time grow in step with the table
(about 1), or faster (2 for a quadratic loop)? What a conversion costs in
seconds on a real stack, next to annotation and the store, is measured by
the benchmark in ``performance/``.

Every run of the conversion carries the same fixed cost (starting the
triplifier, and a pause it takes before it removes its working file), so
the growth is fitted to what a table costs beyond an almost empty one.
"""

import polars as pl
import pytest

from services.conversion_service import run_triplifier
from tests.perf.support import best_of, growth_exponent

COLUMNS = 8
ROWS = 500


def _table(rows: int, columns: int) -> pl.DataFrame:
    """A table the way an uploaded CSV arrives: every cell a string, a
    column of identifiers, then categories and numbers by turns."""
    data = {"id": [f"P{row:06d}" for row in range(rows)]}
    for column in range(1, columns):
        if column % 2:
            data[f"category_{column}"] = [
                str((row + column) % 5) for row in range(rows)
            ]
        else:
            data[f"number_{column}"] = [str((row * column) % 97) for row in range(rows)]
    return pl.DataFrame(data)


@pytest.fixture
def convert(tmp_path):
    """Seconds one conversion of a table takes, the best of two runs."""

    def seconds(rows: int, columns: int) -> float:
        table = _table(rows, columns)

        def run():
            done, message, files = run_triplifier(
                "triplifierCSV.properties",
                str(tmp_path),
                ".",
                csv_data_list=[table],
                csv_table_names=[f"table_{rows}_{columns}"],
            )
            assert done, message
            assert len(files) == 1

        return best_of(run, repeat=2, warmup=0)

    return seconds


@pytest.fixture
def fixed_cost(convert):
    """What a conversion costs whatever the table holds."""
    return convert(2, 2)


def _beyond(seconds: float, fixed: float) -> float:
    return max(seconds - fixed, 1e-3)


def test_conversion_grows_in_step_with_the_rows(convert, fixed_cost, perf):
    sizes = [ROWS, ROWS * 2, ROWS * 4]
    times = [_beyond(convert(rows, COLUMNS), fixed_cost) for rows in sizes]

    exponent = growth_exponent(sizes, times)
    per_cell = times[-1] / (sizes[-1] * COLUMNS) * 1e6
    perf.record(
        f"conversion: time vs rows ({sizes[0]}-{sizes[-1]} x {COLUMNS} columns)",
        exponent,
        "growth exponent",
        "< 1.5",
        f"{times[-1]:.2f} s beyond the fixed {fixed_cost:.2f} s for {sizes[-1]} rows",
    )
    perf.record("conversion, per cell", per_cell, "µs", "< 1000 µs")
    assert exponent < 1.5
    assert per_cell < 1000


def test_conversion_grows_in_step_with_the_columns(convert, fixed_cost, perf):
    sizes = [COLUMNS, COLUMNS * 2, COLUMNS * 4]
    times = [_beyond(convert(ROWS, columns), fixed_cost) for columns in sizes]

    exponent = growth_exponent(sizes, times)
    perf.record(
        f"conversion: time vs columns ({sizes[0]}-{sizes[-1]} x {ROWS} rows)",
        exponent,
        "growth exponent",
        "< 1.5",
        f"{times[-1]:.2f} s beyond the fixed {fixed_cost:.2f} s for {sizes[-1]} columns",
    )
    assert exponent < 1.5
