from datetime import date

import pytest

from lakehouse.ingest import tlc


def test_months_between_crosses_year_boundary():
    months = tlc.months_between(date(2023, 11, 1), date(2024, 2, 1))
    assert months == [date(2023, 11, 1), date(2023, 12, 1), date(2024, 1, 1), date(2024, 2, 1)]


def test_months_between_twelve_months():
    assert len(tlc.months_between(tlc.parse_month("2024-01"), tlc.parse_month("2024-12"))) == 12


def test_source_url():
    assert tlc.source_url("yellow", date(2024, 3, 1)) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet"
    )


def test_bronze_key_is_partitioned_by_month():
    assert tlc.bronze_key("green", date(2024, 3, 1)) == (
        "tlc/green/year=2024/month=03/green_tripdata_2024-03.parquet"
    )


def test_unknown_dataset_is_rejected():
    with pytest.raises(ValueError):
        tlc.file_name("purple", date(2024, 1, 1))
