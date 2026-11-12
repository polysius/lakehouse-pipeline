"""Spark tests. They run on Databricks serverless and skip when no workspace is configured."""

import os
from datetime import datetime

import pytest

pytest.importorskip("databricks.connect")

from pyspark.sql import functions as F

from lakehouse.silver import transform


@pytest.fixture(scope="module")
def spark():
    if not (os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN")):
        pytest.skip("DATABRICKS_HOST and DATABRICKS_TOKEN are not set")
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.serverless(True).getOrCreate()


def make_trip(**overrides):
    trip = {
        "vendor_id": 1,
        "pickup_datetime": datetime(2024, 1, 5, 10, 0),
        "dropoff_datetime": datetime(2024, 1, 5, 10, 20),
        "trip_distance": 3.0,
        "pu_location_id": 100,
        "do_location_id": 200,
        "fare_amount": 15.0,
        "total_amount": 20.0,
        "source_month": "2024-01",
    }
    trip.update(overrides)
    return trip


def reason_for(spark, **overrides):
    # Spark cannot infer a type for a column that is only null, so nulls are applied after.
    null_columns = [name for name, value in overrides.items() if value is None]
    present = {name: value for name, value in overrides.items() if value is not None}
    trips = spark.createDataFrame([make_trip(**present)])
    for name in null_columns:
        trips = trips.withColumn(name, F.lit(None).cast(dict(trips.dtypes)[name]))
    return transform.flag_rejects(trips).collect()[0]["reject_reason"]


def test_good_trip_has_no_reason(spark):
    assert reason_for(spark) is None


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({"pickup_datetime": datetime(2002, 12, 31, 1, 0)}, "pickup_outside_source_month"),
        ({"dropoff_datetime": datetime(2024, 1, 5, 9, 0)}, "dropoff_not_after_pickup"),
        ({"dropoff_datetime": datetime(2024, 1, 5, 17, 0)}, "duration_too_long"),
        ({"trip_distance": 0.0}, "non_positive_distance"),
        ({"trip_distance": 250.0}, "distance_too_long"),
        ({"fare_amount": -5.0}, "invalid_amount"),
        ({"fare_amount": 900.0}, "fare_too_high"),
    ],
)
def test_each_rule_labels_its_row(spark, overrides, expected):
    assert reason_for(spark, **overrides) == expected


def test_null_amounts_are_rejected(spark):
    assert reason_for(spark, fare_amount=None, total_amount=None) == "invalid_amount"
    assert reason_for(spark, fare_amount=None, total_amount=10.0) == "invalid_amount"


@pytest.mark.parametrize(
    "column, expected",
    [
        ("trip_distance", "non_positive_distance"),
        ("pickup_datetime", "missing_timestamp"),
        ("dropoff_datetime", "missing_timestamp"),
    ],
)
def test_null_values_are_rejected(spark, column, expected):
    assert reason_for(spark, **{column: None}) == expected


def test_limits_are_inclusive(spark):
    # Exactly 100 miles, exactly 6 hours and exactly 500 dollars are still allowed.
    assert reason_for(spark, trip_distance=100.0) is None
    assert reason_for(spark, dropoff_datetime=datetime(2024, 1, 5, 16, 0)) is None
    assert reason_for(spark, fare_amount=500.0, total_amount=510.0) is None


def test_first_broken_rule_wins(spark):
    # Outside the month and a negative fare: the month rule comes first.
    reason = reason_for(spark, pickup_datetime=datetime(2009, 1, 1), fare_amount=-1.0)
    assert reason == "pickup_outside_source_month"


def test_deduplicate_removes_identical_trips(spark):
    trips = spark.createDataFrame([make_trip(), make_trip(), make_trip(total_amount=21.0)])
    assert transform.deduplicate(trips).count() == 2


def test_labels_map_codes_to_names(spark):
    trips = spark.createDataFrame([{"payment_type": 2, "rate_code_id": 2, "vendor_id": 2}])
    row = transform.add_labels(trips).collect()[0]
    assert (row["payment_type_name"], row["rate_code_name"]) == ("Cash", "JFK")
    assert row["vendor_name"] == "VeriFone"
