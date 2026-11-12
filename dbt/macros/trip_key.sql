{#
  The source has no trip id. A trip is identified by the same fields the silver job
  deduplicates on, hashed into one key. Nulls become empty text so they hash the same way twice.
#}
{% macro trip_key() %}
    md5(concat_ws('|',
        coalesce(cast(vendor_id as string), ''),
        coalesce(cast(pickup_datetime as string), ''),
        coalesce(cast(dropoff_datetime as string), ''),
        coalesce(cast(pu_location_id as string), ''),
        coalesce(cast(do_location_id as string), ''),
        coalesce(cast(total_amount as string), '')
    ))
{% endmacro %}
