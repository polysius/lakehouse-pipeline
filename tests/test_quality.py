from datetime import date, datetime

import pytest

from lakehouse.quality import thresholds

MONTH = date(2024, 3, 1)


def test_expected_row_range_uses_the_median_of_recent_months():
    low, high = thresholds.expected_row_range([3_000_000, 3_200_000, 3_100_000])
    assert low == int(3_100_000 * (1 - thresholds.VOLUME_TOLERANCE))
    assert high == int(3_100_000 * (1 + thresholds.VOLUME_TOLERANCE))


def test_expected_row_range_needs_history():
    assert thresholds.expected_row_range([]) is None


def test_expected_row_range_ignores_months_beyond_the_history_window():
    history = [100] * thresholds.HISTORY_MONTHS + [1_000_000] * 10
    assert thresholds.expected_row_range(history) == thresholds.expected_row_range([100])


def test_coverage_accepts_a_full_month():
    first, last = datetime(2024, 3, 1, 0, 5), datetime(2024, 3, 31, 23, 50)
    assert thresholds.coverage_problem(first, last, MONTH) is None


def test_coverage_rejects_a_month_that_stops_early():
    problem = thresholds.coverage_problem(datetime(2024, 3, 1), datetime(2024, 3, 15), MONTH)
    assert "before month end" in problem


def test_coverage_rejects_a_month_that_starts_late():
    problem = thresholds.coverage_problem(datetime(2024, 3, 10), datetime(2024, 3, 31, 23), MONTH)
    assert "after month start" in problem


def test_coverage_rejects_an_empty_month():
    assert thresholds.coverage_problem(None, None, MONTH) is not None


# The gate itself runs Great Expectations on pandas, so it needs no Spark.
pd = pytest.importorskip("pandas")
pytest.importorskip("great_expectations")

from lakehouse.quality import alert, gate


def good_batch(rows: int = 2000):
    return pd.DataFrame(
        {
            "pickup_datetime": pd.date_range("2024-03-01 00:05", "2024-03-31 23:50", periods=rows),
            "dropoff_datetime": pd.date_range("2024-03-01 00:25", "2024-04-01 00:10", periods=rows),
            "trip_distance": [3.0] * rows,
            "fare_amount": [15.0] * rows,
            "pu_location_id": [100] * rows,
            "do_location_id": [200] * rows,
            "payment_type": [1] * rows,
        }
    )


def failed_names(result):
    return {check.name for check in result.failed_checks}


def test_good_batch_passes():
    result = gate.evaluate_batch(good_batch(), MONTH, history=[2000, 2100])
    assert result.passed, result.failed_checks


def test_negative_fares_fail_the_fare_check():
    frame = good_batch()
    frame.loc[:400, "fare_amount"] = -5.0
    result = gate.evaluate_batch(frame, MONTH, history=[])
    assert failed_names(result) == {"fare_amount is within 0 to 500"}


def test_pickups_outside_the_month_fail():
    frame = good_batch()
    frame.loc[:100, "pickup_datetime"] = pd.Timestamp("2002-12-31")
    result = gate.evaluate_batch(frame, MONTH, history=[])
    assert "pickup_datetime is inside the month" in failed_names(result)


def test_unknown_payment_codes_fail():
    frame = good_batch()
    frame.loc[:400, "payment_type"] = 99
    result = gate.evaluate_batch(frame, MONTH, history=[])
    assert failed_names(result) == {"payment_type is a known code"}


def test_missing_timestamps_fail():
    frame = good_batch()
    frame.loc[:100, "dropoff_datetime"] = pd.NaT
    result = gate.evaluate_batch(frame, MONTH, history=[])
    assert failed_names(result) == {"dropoff_datetime is present"}


def test_a_small_share_of_bad_rows_is_tolerated():
    frame = good_batch()
    frame.loc[:20, "fare_amount"] = -5.0  # about 1% of rows, under the 5% allowance
    assert gate.evaluate_batch(frame, MONTH, history=[]).passed


def test_volume_anomaly_fails_against_history():
    result = gate.evaluate_batch(good_batch(rows=500), MONTH, history=[2000, 2000, 2000])
    assert any(name.startswith("row count") for name in failed_names(result))


def test_batch_that_stops_early_fails_coverage():
    frame = good_batch()
    frame["pickup_datetime"] = pd.date_range("2024-03-01", "2024-03-12", periods=len(frame))
    result = gate.evaluate_batch(frame, MONTH, history=[])
    assert "data covers the whole month" in failed_names(result)


def test_alert_names_every_failed_check():
    frame = good_batch()
    frame.loc[:400, "fare_amount"] = -5.0
    frame.loc[:400, "payment_type"] = 99
    text = alert.format_alert(gate.evaluate_batch(frame, MONTH, history=[]), "demo.quarantine")
    assert "batch 2024-03 failed 2 of" in text
    assert "fare_amount is within 0 to 500" in text
    assert "payment_type is a known code" in text
    assert "demo.quarantine" in text
