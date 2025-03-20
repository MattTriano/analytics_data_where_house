{{ config(materialized='view') }}
{% set ck_cols = ["segmentid"] %}
{% set record_id = "segmentid" %}

WITH records_with_basic_cleaning AS (
    SELECT
        segmentid::bigint                 AS segmentid,
        upper(street::text)               AS street,
        upper(direction::text)            AS direction,
        upper(from_street::text)          AS from_street,
        upper(to_street::text)            AS to_street,
        length::double precision          AS length,
        upper(_street_heading::text)      AS street_heading,
        upper(_comments::text)            AS comments,
        start_longitude::double precision AS start_longitude,
        _start_latitude::double precision AS start_latitude,
        end_longitude::double precision   AS end_longitude,
        _end_latitude::double precision   AS end_latitude,
        _current_speed::bigint            AS current_speed,
        _last_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS last_updated,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_traffic_congestion') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
