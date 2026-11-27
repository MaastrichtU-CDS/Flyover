"""The verdict has to be right: a wrong one is worse than none."""

import math
import random

import pytest

from performance.harness.stats import (
    IMPROVED,
    MINIMUM_PAIRS,
    NO_FLAG,
    REGRESSED,
    compare,
    spread,
    t_critical,
)

THRESHOLD = 0.2


def noisy(base, factor, repetitions, noise, seed):
    """Paired values: every repetition has its own machine speed, which both
    sides share, and each side has a little noise of its own."""
    rng = random.Random(seed)
    baseline, candidate = [], []
    for _ in range(repetitions):
        machine = rng.uniform(0.6, 1.6)
        baseline.append(base * machine * rng.gauss(1, noise))
        candidate.append(base * factor * machine * rng.gauss(1, noise))
    return baseline, candidate


class TestCriticalValues:
    @pytest.mark.parametrize(
        "degrees_of_freedom, expected",
        [(1, 12.706), (2, 4.303), (4, 2.776), (9, 2.262), (30, 2.042)],
    )
    def test_the_table_holds_the_textbook_values(self, degrees_of_freedom, expected):
        assert t_critical(degrees_of_freedom) == expected

    @pytest.mark.parametrize(
        "degrees_of_freedom, expected", [(40, 2.021), (60, 2.000), (120, 1.980)]
    )
    def test_beyond_the_table_the_value_is_close_to_the_textbook_one(
        self, degrees_of_freedom, expected
    ):
        assert t_critical(degrees_of_freedom) == pytest.approx(expected, abs=0.001)

    def test_the_values_fall_towards_the_normal_one(self):
        values = [t_critical(n) for n in range(1, 200)]
        assert values == sorted(values, reverse=True)
        assert values[-1] > 1.96

    def test_no_degrees_of_freedom_is_an_error(self):
        with pytest.raises(ValueError):
            t_critical(0)


class TestTheVerdict:
    def test_a_clear_slowdown_is_flagged(self):
        baseline, candidate = noisy(10, 1.5, 5, 0.02, seed=1)
        result = compare("conversion", baseline, candidate, THRESHOLD)
        assert result.verdict == REGRESSED
        assert result.flagged
        assert result.low < 1.5 < result.high
        assert result.ratio == pytest.approx(1.5, rel=0.05)

    def test_a_clear_speed_up_is_flagged_as_improved(self):
        baseline, candidate = noisy(10, 1 / 1.5, 5, 0.02, seed=2)
        result = compare("conversion", baseline, candidate, THRESHOLD)
        assert result.verdict == IMPROVED
        assert result.high < 1

    def test_the_same_version_twice_is_not_flagged(self):
        baseline, candidate = noisy(10, 1.0, 5, 0.05, seed=3)
        result = compare("conversion", baseline, candidate, THRESHOLD)
        assert result.verdict == NO_FLAG
        assert not result.flagged

    def test_a_small_but_clear_change_is_not_flagged(self):
        # 8 % slower in every repetition: clear, but below the threshold.
        baseline = [10.0, 12.0, 9.0, 11.0, 10.5]
        candidate = [value * 1.08 for value in baseline]
        result = compare("conversion", baseline, candidate, THRESHOLD)
        assert result.verdict == NO_FLAG
        assert result.low == pytest.approx(1.08)
        assert result.high == pytest.approx(1.08)

    def test_a_large_but_unclear_change_is_not_flagged(self):
        # 40 % slower on average, but the repetitions disagree on the sign.
        baseline = [10.0, 10.0, 10.0, 10.0]
        candidate = [30.0, 6.0, 28.0, 7.0]
        result = compare("conversion", baseline, candidate, THRESHOLD)
        assert result.ratio > 1 + THRESHOLD
        assert result.low < 1 < result.high
        assert result.verdict == NO_FLAG

    def test_the_machine_speed_cancels_out(self):
        # Each repetition ran on a machine up to five times faster or slower
        # than the last; within a repetition both sides share it.
        speeds = [1.0, 5.0, 0.4, 2.5, 0.8]
        baseline = [10.0 * speed for speed in speeds]
        candidate = [13.0 * speed for speed in speeds]
        result = compare("annotation", baseline, candidate, THRESHOLD)
        assert result.ratio == pytest.approx(1.3)
        assert result.verdict == REGRESSED

    def test_slower_and_faster_weigh_the_same(self):
        slower = compare("x", [10.0] * 4, [20.0, 20.0, 20.0, 5.0], THRESHOLD)
        faster = compare("x", [10.0] * 4, [5.0, 5.0, 5.0, 20.0], THRESHOLD)
        assert slower.ratio == pytest.approx(1 / faster.ratio)
        assert slower.low == pytest.approx(1 / faster.high)

    @pytest.mark.parametrize(
        "factor, verdict",
        [(1.2, REGRESSED), (1.19, NO_FLAG), (1 / 1.2, IMPROVED), (1 / 1.19, NO_FLAG)],
    )
    def test_the_threshold_itself_is_flagged(self, factor, verdict):
        baseline = [10.0, 11.0, 12.0]
        candidate = [value * factor for value in baseline]
        assert compare("x", baseline, candidate, THRESHOLD).verdict == verdict

    def test_the_medians_are_reported_for_reading(self):
        result = compare("x", [1.0, 2.0, 9.0], [2.0, 4.0, 18.0], THRESHOLD)
        assert (result.baseline, result.candidate) == (2.0, 4.0)
        assert result.pairs == 3


class TestTooLittleToJudge:
    def test_one_or_two_repetitions_are_never_flagged(self):
        for repetitions in range(1, MINIMUM_PAIRS):
            result = compare(
                "x", [10.0] * repetitions, [100.0] * repetitions, THRESHOLD
            )
            assert result.verdict == NO_FLAG
            assert result.ratio == pytest.approx(10)
            assert result.low is None and result.high is None
            assert "repetitions" in result.note

    def test_a_repetition_without_a_usable_value_is_left_out(self):
        baseline = [10.0, None, 10.0, 0.0, 10.0, 10.0, math.nan]
        candidate = [15.0, 15.0, 15.0, 15.0, None, 15.0, 15.0]
        result = compare("x", baseline, candidate, THRESHOLD)
        assert result.pairs == 3
        assert result.verdict == REGRESSED

    def test_no_usable_values_at_all(self):
        result = compare("x", [None, 0.0], [1.0, 1.0], THRESHOLD)
        assert result.pairs == 0
        assert result.ratio is None
        assert result.verdict == NO_FLAG

    def test_the_sides_must_cover_the_same_repetitions(self):
        with pytest.raises(ValueError):
            compare("x", [1.0, 2.0], [1.0], THRESHOLD)

    def test_the_threshold_must_be_positive(self):
        with pytest.raises(ValueError):
            compare("x", [1.0] * 3, [1.0] * 3, 0)


class TestFalseFlags:
    """With no real difference, a flag must be rare whatever the noise."""

    @pytest.mark.parametrize(
        "noise, at_most",
        # The interval alone flags 5 % of the time, however noisy the
        # measurement (0.065 allows for 2,000 trials). The threshold removes
        # nearly all of those once the measurement is quiet.
        [(0.02, 0.001), (0.05, 0.01), (0.10, 0.065), (0.20, 0.065)],
    )
    def test_the_same_version_is_rarely_flagged(self, noise, at_most):
        flags = 0
        for seed in range(2000):
            baseline, candidate = noisy(10, 1.0, 5, noise, seed)
            flags += compare("x", baseline, candidate, THRESHOLD).flagged
        assert flags / 2000 <= at_most

    def test_a_real_slowdown_of_half_is_nearly_always_flagged(self):
        flags = 0
        for seed in range(2000):
            baseline, candidate = noisy(10, 1.5, 5, 0.05, seed)
            flags += compare("x", baseline, candidate, THRESHOLD).verdict == REGRESSED
        assert flags / 2000 > 0.99


class TestSpread:
    def test_the_spread_is_the_coefficient_of_variation(self):
        assert spread([10.0, 10.0, 10.0]) == 0
        assert spread([9.0, 10.0, 11.0]) == pytest.approx(0.1)

    def test_one_value_has_no_spread(self):
        assert spread([10.0]) is None
        assert spread([]) is None
