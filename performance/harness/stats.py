"""
The statistics behind a verdict: is one side slower than the other?

Both sides run the same work on the same machine, one after the other, once
per repetition. Each repetition therefore gives a pair of values, and what
is compared is the ratio within each pair (candidate over baseline): the
machine's speed, which differs from run to run and from runner to runner,
is in both values and cancels out.

The ratios are averaged on the log scale (a geometric mean, so that twice
as slow and twice as fast weigh the same), and a 95 % interval says how well
the repetitions agree. A metric is flagged only when the change is both
large (at least the threshold) and clear (the interval excludes no change).
"""

import math
import statistics
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Sequence

# Fewer pairs than this give no usable interval: one pair has none, and two
# give one so wide that nothing could ever be flagged.
MINIMUM_PAIRS = 3

REGRESSED = "regressed"
IMPROVED = "improved"
NO_FLAG = "no flag"

# Two-sided 95 % critical values of Student's t, by degrees of freedom.
_T_95 = (
    12.706,
    4.303,
    3.182,
    2.776,
    2.571,
    2.447,
    2.365,
    2.306,
    2.262,
    2.228,
    2.201,
    2.179,
    2.160,
    2.145,
    2.131,
    2.120,
    2.110,
    2.101,
    2.093,
    2.086,
    2.080,
    2.074,
    2.069,
    2.064,
    2.060,
    2.056,
    2.052,
    2.048,
    2.045,
    2.042,
)
_Z_95 = 1.959964
_ROUNDING = 1e-9


def t_critical(degrees_of_freedom: int) -> float:
    """The two-sided 95 % critical value of Student's t."""
    if degrees_of_freedom < 1:
        raise ValueError("an interval needs at least two values")
    if degrees_of_freedom <= len(_T_95):
        return _T_95[degrees_of_freedom - 1]
    # Beyond the table, the normal value with its first two corrections is
    # accurate to three decimals.
    z, n = _Z_95, degrees_of_freedom
    return z + (z**3 + z) / (4 * n) + (5 * z**5 + 16 * z**3 + 3 * z) / (96 * n**2)


@dataclass(frozen=True)
class Comparison:
    """One metric, compared between the two sides."""

    metric: str
    pairs: int
    baseline: Optional[float]  # median of the baseline's values
    candidate: Optional[float]  # median of the candidate's values
    ratio: Optional[float]  # candidate over baseline, geometric mean of the pairs
    low: Optional[float]  # the ratio's 95 % interval
    high: Optional[float]
    verdict: str
    note: str = ""

    @property
    def flagged(self) -> bool:
        return self.verdict in (REGRESSED, IMPROVED)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def compare(
    metric: str,
    baseline: Sequence[Optional[float]],
    candidate: Sequence[Optional[float]],
    threshold: float,
) -> Comparison:
    """Compare a metric for which a higher value is worse (time, memory).

    ``baseline[i]`` and ``candidate[i]`` are the two sides' values of
    repetition ``i``. A repetition in which either side has no value, or a
    value that is not positive, is left out.

    ``threshold`` is the smallest change worth flagging, as a fraction: 0.2
    flags a candidate at least 20 % above the baseline as regressed, and one
    that makes the baseline at least 20 % higher than itself as improved.
    """
    if len(baseline) != len(candidate):
        raise ValueError("the two sides need a value for the same repetitions")
    if threshold <= 0:
        raise ValueError("the threshold is a positive fraction, such as 0.2")

    pairs = [
        (b, c)
        for b, c in zip(baseline, candidate)
        if _is_positive(b) and _is_positive(c)
    ]
    if not pairs:
        return Comparison(
            metric, 0, None, None, None, None, None, NO_FLAG, "no values to compare"
        )

    base_median = statistics.median(b for b, _ in pairs)
    cand_median = statistics.median(c for _, c in pairs)
    logs = [math.log(c / b) for b, c in pairs]
    mean = statistics.fmean(logs)
    ratio = math.exp(mean)

    if len(pairs) < MINIMUM_PAIRS:
        return Comparison(
            metric,
            len(pairs),
            base_median,
            cand_median,
            ratio,
            None,
            None,
            NO_FLAG,
            f"fewer than {MINIMUM_PAIRS} repetitions: no interval, so no flag",
        )

    half_width = (
        t_critical(len(logs) - 1) * statistics.stdev(logs) / math.sqrt(len(logs))
    )
    low, high = math.exp(mean - half_width), math.exp(mean + half_width)

    # A change of exactly the threshold is flagged; the tolerance keeps the
    # rounding of the logarithms from deciding that.
    verdict = NO_FLAG
    if ratio >= (1 + threshold) * (1 - _ROUNDING) and low > 1:
        verdict = REGRESSED
    elif ratio <= (1 + _ROUNDING) / (1 + threshold) and high < 1:
        verdict = IMPROVED
    return Comparison(
        metric, len(pairs), base_median, cand_median, ratio, low, high, verdict
    )


def spread(values: Sequence[float]) -> Optional[float]:
    """The coefficient of variation of ``values``: how much one side's own
    repetitions differ, as a fraction of their mean."""
    usable: List[float] = [v for v in values if _is_positive(v)]
    if len(usable) < 2:
        return None
    return statistics.stdev(usable) / statistics.fmean(usable)


def _is_positive(value: Optional[float]) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )
