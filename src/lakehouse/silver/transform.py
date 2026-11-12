"""Silver transformations. Each step takes a DataFrame and returns a DataFrame."""

from itertools import chain

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lakehouse.silver import rules


def standardise(bronze: DataFrame, source_month: str) -> DataFrame:
    """Rename to snake_case and cast every column to one type, whatever the file used.

    A column missing from the file becomes null, so files from different years line up.
    """
    columns = []
    for bronze_name, (silver_name, sql_type) in rules.COLUMNS.items():
        if bronze_name in bronze.columns:
            value = F.col(bronze_name).cast(sql_type)
        else:
            value = F.lit(None).cast(sql_type)
        columns.append(value.alias(silver_name))
    return bronze.select(*columns).withColumn("source_month", F.lit(source_month))


def flag_rejects(trips: DataFrame) -> DataFrame:
    """Add reject_reason: the first broken rule, or null when the row is good."""
    reason = F.lit(None).cast("string")
    # Build the CASE from the last rule to the first so the first rule ends up outermost.
    for rule in reversed(rules.REJECT_RULES):
        reason = F.when(F.expr(rule.condition), F.lit(rule.reason)).otherwise(reason)
    return trips.withColumn("reject_reason", reason)


def split_rejects(flagged: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (good rows without the reason column, rejected rows with it)."""
    good = flagged.where(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = flagged.where(F.col("reject_reason").isNotNull())
    return good, rejected


def deduplicate(trips: DataFrame) -> DataFrame:
    return trips.dropDuplicates(list(rules.DEDUP_KEY))


def _label(column: str, names: dict[int, str]) -> Column:
    lookup = F.create_map(*chain.from_iterable((F.lit(k), F.lit(v)) for k, v in names.items()))
    return lookup[F.col(column)]


def add_labels(trips: DataFrame) -> DataFrame:
    """Add readable names for the coded columns, using the TLC data dictionary."""
    return (
        trips.withColumn("payment_type_name", _label("payment_type", rules.PAYMENT_TYPES))
        .withColumn("rate_code_name", _label("rate_code_id", rules.RATE_CODES))
        .withColumn("vendor_name", _label("vendor_id", rules.VENDORS))
    )


def prepare_zones(zones_csv: DataFrame) -> DataFrame:
    return zones_csv.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone"),
    )


def add_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Left join the zone lookup twice, once for pickup and once for dropoff."""
    pickup = zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("borough").alias("pu_borough"),
        F.col("zone").alias("pu_zone"),
    )
    dropoff = zones.select(
        F.col("location_id").alias("do_location_id"),
        F.col("borough").alias("do_borough"),
        F.col("zone").alias("do_zone"),
    )
    return trips.join(F.broadcast(pickup), "pu_location_id", "left").join(
        F.broadcast(dropoff), "do_location_id", "left"
    )


def add_pickup_date(trips: DataFrame) -> DataFrame:
    return trips.withColumn("pickup_date", F.to_date("pickup_datetime"))
