"""Silver layer definitions kept as plain data, so they can be tested without Spark."""

from dataclasses import dataclass

MAX_TRIP_HOURS = 6
MAX_TRIP_MILES = 100
MAX_FARE_DOLLARS = 500

# bronze column -> (silver column, Spark SQL type). Bronze files differ in integer widths
# between months, so every column is cast to one type.
COLUMNS = {
    "VendorID": ("vendor_id", "int"),
    "tpep_pickup_datetime": ("pickup_datetime", "timestamp"),
    "tpep_dropoff_datetime": ("dropoff_datetime", "timestamp"),
    "passenger_count": ("passenger_count", "int"),
    "trip_distance": ("trip_distance", "double"),
    "RatecodeID": ("rate_code_id", "int"),
    "store_and_fwd_flag": ("store_and_fwd_flag", "string"),
    "PULocationID": ("pu_location_id", "int"),
    "DOLocationID": ("do_location_id", "int"),
    "payment_type": ("payment_type", "int"),
    "fare_amount": ("fare_amount", "double"),
    "extra": ("extra", "double"),
    "mta_tax": ("mta_tax", "double"),
    "tip_amount": ("tip_amount", "double"),
    "tolls_amount": ("tolls_amount", "double"),
    "improvement_surcharge": ("improvement_surcharge", "double"),
    "total_amount": ("total_amount", "double"),
    "congestion_surcharge": ("congestion_surcharge", "double"),
    "Airport_fee": ("airport_fee", "double"),
}

# There is no trip id in the source, so a trip is identified by these fields together.
DEDUP_KEY = (
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pu_location_id",
    "do_location_id",
    "total_amount",
)

# Values from the TLC yellow taxi data dictionary.
PAYMENT_TYPES = {
    0: "Flex fare",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}
RATE_CODES = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
    99: "Unknown",
}
VENDORS = {
    1: "Creative Mobile Technologies",
    2: "VeriFone",
    6: "Myle",
    7: "Helix",
}


@dataclass(frozen=True)
class RejectRule:
    reason: str
    # Spark SQL condition that is true when the row is bad.
    condition: str


# Order matters: a row is labelled with the first rule it breaks.
REJECT_RULES = (
    RejectRule("missing_timestamp", "pickup_datetime IS NULL OR dropoff_datetime IS NULL"),
    RejectRule(
        "pickup_outside_source_month",
        "date_format(pickup_datetime, 'yyyy-MM') <> source_month",
    ),
    RejectRule("dropoff_not_after_pickup", "dropoff_datetime <= pickup_datetime"),
    RejectRule(
        "duration_too_long",
        f"unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime) "
        f"> {MAX_TRIP_HOURS * 3600}",
    ),
    RejectRule("non_positive_distance", "trip_distance IS NULL OR trip_distance <= 0"),
    RejectRule("distance_too_long", f"trip_distance > {MAX_TRIP_MILES}"),
    RejectRule(
        "invalid_amount",
        "fare_amount IS NULL OR total_amount IS NULL OR fare_amount < 0 OR total_amount <= 0",
    ),
    RejectRule("fare_too_high", f"fare_amount > {MAX_FARE_DOLLARS}"),
)
