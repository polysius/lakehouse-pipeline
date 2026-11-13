"""Quality limits and the checks that need no Great Expectations. Plain Python, easy to test."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from statistics import median

from lakehouse.ingest import tlc

# A month's row count may differ from the recent median by this fraction.
# The 2024 months range from 0.83 to 1.07 of their median, so 0.35 leaves wide margin.
VOLUME_TOLERANCE = 0.35
HISTORY_MONTHS = 6

# Trips must reach to within this many days of both ends of the month.
COVERAGE_DAYS = 3


@dataclass(frozen=True)
class ValueRule:
    """A column must stay inside limits for at least `mostly` of its rows.

    Source data always has some bad rows, so each rule allows a share of them. The silver job
    rejects those rows one by one. This gate only fails the batch when too many are bad.
    """

    column: str
    minimum: float | None = None
    maximum: float | None = None
    allowed_values: tuple | None = None
    mostly: float = 0.99


# Calibrated on the twelve 2024 months, where the worst month still passes with margin.
VALUE_RULES = (
    ValueRule("trip_distance", minimum=0, maximum=100, mostly=0.95),
    ValueRule("fare_amount", minimum=0, maximum=500, mostly=0.95),
    ValueRule("pu_location_id", minimum=1, maximum=265, mostly=0.999),
    ValueRule("do_location_id", minimum=1, maximum=265, mostly=0.999),
    ValueRule("payment_type", allowed_values=(0, 1, 2, 3, 4, 5, 6), mostly=0.99),
)

# Timestamps must exist for nearly every row.
REQUIRED_COLUMNS = ("pickup_datetime", "dropoff_datetime")
REQUIRED_MOSTLY = 0.999
# Pickups must fall inside the batch's month for nearly every row.
IN_MONTH_MOSTLY = 0.995


def expected_row_range(history: list[int]) -> tuple[int, int] | None:
    """Row counts a new month may have, from the recent months that passed. None without history."""
    if not history:
        return None
    centre = median(history[:HISTORY_MONTHS])
    return int(centre * (1 - VOLUME_TOLERANCE)), int(centre * (1 + VOLUME_TOLERANCE))


def coverage_problem(first_pickup: datetime | None, last_pickup: datetime | None, month: date):
    """Describe why the data does not cover the month, or return None when it does.

    Rows dated outside the month are ignored by the caller, so stray years do not matter.
    A file that stops early, for example a truncated download, fails here.
    """
    if first_pickup is None or last_pickup is None:
        return "no pickups fall inside the month"
    month_end = datetime.combine(tlc.next_month(month), datetime.min.time())
    month_start = datetime.combine(month, datetime.min.time())
    window = timedelta(days=COVERAGE_DAYS)
    if last_pickup < month_end - window:
        return f"latest pickup is {last_pickup:%Y-%m-%d %H:%M}, more than {COVERAGE_DAYS} days before month end"
    if first_pickup > month_start + window:
        return f"earliest pickup is {first_pickup:%Y-%m-%d %H:%M}, more than {COVERAGE_DAYS} days after month start"
    return None
