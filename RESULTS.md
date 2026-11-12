# Results

## M1: bronze backfill

Yellow taxi, January to December 2024, local storage in WSL2.

| Month | Rows |
|-------|-----:|
| 2024-01 | 2,964,624 |
| 2024-02 | 3,007,526 |
| 2024-03 | 3,582,628 |
| 2024-04 | 3,514,289 |
| 2024-05 | 3,723,833 |
| 2024-06 | 3,539,193 |
| 2024-07 | 3,076,903 |
| 2024-08 | 2,979,183 |
| 2024-09 | 3,633,030 |
| 2024-10 | 3,833,771 |
| 2024-11 | 3,646,369 |
| 2024-12 | 3,668,371 |
| Total | 41,169,720 |

First run: 12 months loaded in about 61 seconds.
Second run: all 12 skipped in under 1 second, and the bronze folder still holds 12 files.

The Databricks volume backend loaded the same 12 months in about 2 minutes 46 seconds, with identical row counts.

## M2: silver

Yellow taxi 2024, built on Databricks serverless through Databricks Connect.

| Step | Rows |
|------|-----:|
| Bronze | 41,169,720 |
| Rejected by quality rules | 1,481,846 (3.6%) |
| Duplicates removed | 5 |
| Silver | 39,687,869 |

Each month takes 11 to 16 seconds. Rerunning a month leaves its row counts unchanged.

Rejected rows by first broken rule:

| Reason | Rows |
|--------|-----:|
| non_positive_distance | 764,825 |
| invalid_amount | 679,274 |
| duration_too_long | 21,994 |
| dropoff_not_after_pickup | 13,510 |
| distance_too_long | 1,590 |
| pickup_outside_source_month | 420 |
| fare_too_high | 233 |

About 10% of bronze rows have a null passenger_count, rate_code_id and airport_fee. They are kept, because a null there is a missing optional field and not bad data. Every silver row matched a pickup and dropoff zone.

### Layout benchmark

Four layouts of the same 39.7 million rows, five queries, five repeats each, variants interleaved.
Cells show the first run and the median, in seconds. Run `scripts/benchmark_layout.py` to reproduce.

| Variant | Files | Size (MB) |
|---------|------:|----------:|
| flat | 6 | 763 |
| partitioned by pickup_date | 366 | 746 |
| liquid clustering on pickup_date, pu_location_id | 16 | 773 |
| partitioned plus ZORDER BY pu_location_id | 366 | 745 |

| Query | flat | partitioned | liquid | partitioned + zorder |
|-------|-----:|------------:|-------:|---------------------:|
| one day | 1.8 / 0.8 | 1.2 / 0.7 | 1.3 / 0.8 | 0.8 / 0.7 |
| one week by zone | 1.2 / 0.9 | 1.4 / 0.9 | 1.3 / 0.9 | 0.9 / 0.9 |
| one zone, one month | 1.4 / 0.9 | 1.1 / 0.8 | 1.4 / 0.8 | 0.8 / 0.8 |
| one zone, full year | 1.6 / 0.9 | 1.7 / 1.0 | 1.3 / 0.9 | 1.1 / 1.0 |
| full year (control) | 1.1 / 0.9 | 1.0 / 0.9 | 1.4 / 0.9 | 1.0 / 1.0 |

Reading the table: no layout is measurably faster at this size. The medians sit between 0.7 and
1.0 seconds for every variant, including the full-year control, which suggests a per-query floor
from Databricks Connect and serverless start-up that hides any difference in data scanned. The
first runs of the partitioned and Z-ordered layouts are a little quicker on the day and month
queries, but the gap is about half a second and the repeats overlap. Partitioning by day also
produced 366 files of about 2 MB each, which is the small-file pattern that usually hurts. At
roughly 750 MB, 40 million rows is too small for layout to matter on this compute. A larger
dataset, or a measure of bytes scanned from the query profile, would be needed to show a
difference.
