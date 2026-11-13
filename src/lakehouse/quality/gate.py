"""Batch quality gate. Great Expectations checks one month of trips before it is published."""

from dataclasses import dataclass
from datetime import date, datetime

import great_expectations as gx
import pandas as pd

from lakehouse.ingest import tlc
from lakehouse.quality import thresholds

# Columns the gate reads. The silver job selects only these when it builds the pandas frame.
GATE_COLUMNS = (
    "pickup_datetime",
    "dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "pu_location_id",
    "do_location_id",
    "payment_type",
)


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class GateResult:
    source_month: str
    row_count: int
    checks: tuple[CheckResult, ...]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failed_checks(self) -> tuple[CheckResult, ...]:
        return tuple(check for check in self.checks if not check.passed)


class QualityGateFailed(Exception):
    def __init__(self, result: GateResult):
        self.result = result
        super().__init__(f"Quality gate failed for {result.source_month}")


def _expectations(month: date, history: list[int]) -> list[tuple[str, object]]:
    """The Great Expectations checks for one month, each with a readable name."""
    month_start = datetime.combine(month, datetime.min.time())
    month_end = datetime.combine(tlc.next_month(month), datetime.min.time())
    expect = gx.expectations
    checks = []

    for column in thresholds.REQUIRED_COLUMNS:
        checks.append(
            (
                f"{column} is present",
                expect.ExpectColumnValuesToNotBeNull(
                    column=column, mostly=thresholds.REQUIRED_MOSTLY
                ),
            )
        )
    checks.append(
        (
            "pickup_datetime is inside the month",
            expect.ExpectColumnValuesToBeBetween(
                column="pickup_datetime",
                min_value=month_start,
                max_value=month_end,
                mostly=thresholds.IN_MONTH_MOSTLY,
            ),
        )
    )
    for rule in thresholds.VALUE_RULES:
        if rule.allowed_values is not None:
            checks.append(
                (
                    f"{rule.column} is a known code",
                    expect.ExpectColumnValuesToBeInSet(
                        column=rule.column,
                        value_set=list(rule.allowed_values),
                        mostly=rule.mostly,
                    ),
                )
            )
        else:
            checks.append(
                (
                    f"{rule.column} is within {rule.minimum} to {rule.maximum}",
                    expect.ExpectColumnValuesToBeBetween(
                        column=rule.column,
                        min_value=rule.minimum,
                        max_value=rule.maximum,
                        mostly=rule.mostly,
                    ),
                )
            )

    row_range = thresholds.expected_row_range(history)
    if row_range is not None:
        low, high = row_range
        checks.append(
            (
                f"row count is within {low:,} to {high:,}",
                expect.ExpectTableRowCountToBeBetween(min_value=low, max_value=high),
            )
        )
    return checks


def _describe(name: str, result) -> CheckResult:
    values = result.result
    # Value checks report the share of failing rows. The row count check reports the count.
    failing_percent = values.get("unexpected_percent_total", values.get("unexpected_percent"))
    if failing_percent is not None:
        detail = f"{failing_percent:.2f}% of rows fail"
    else:
        detail = f"observed {values.get('observed_value'):,}"
    return CheckResult(name, bool(result.success), detail)


def evaluate_batch(frame: pd.DataFrame, month: date, history: list[int]) -> GateResult:
    """Run every check on one month. `history` holds row counts of recent passed months."""
    context = gx.get_context(mode="ephemeral")
    asset = context.data_sources.add_pandas("trips").add_dataframe_asset("batch")
    batch = asset.add_batch_definition_whole_dataframe("month").get_batch(
        batch_parameters={"dataframe": frame}
    )

    checks = [
        _describe(name, batch.validate(expectation))
        for name, expectation in _expectations(month, history)
    ]

    in_month = frame.loc[
        (frame["pickup_datetime"] >= pd.Timestamp(month))
        & (frame["pickup_datetime"] < pd.Timestamp(tlc.next_month(month))),
        "pickup_datetime",
    ]
    problem = thresholds.coverage_problem(
        in_month.min().to_pydatetime() if len(in_month) else None,
        in_month.max().to_pydatetime() if len(in_month) else None,
        month,
    )
    checks.append(CheckResult("data covers the whole month", problem is None, problem or "ok"))

    return GateResult(f"{month:%Y-%m}", len(frame), tuple(checks))
