{{ config(materialized='view') }}
{% set ck_cols = ["address", "request_date", "completion_date"] %}
{% set record_id = "pothole_repair_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
        request_date::timestamp                         AS request_date,
        completion_date::timestamp                      AS completion_date,
        upper(address::text)                            AS address,
        number_of_potholes_filled_on_block::smallint    AS number_of_potholes_filled_on_block,
        latitude::double precision                      AS latitude,
        longitude::double precision                     AS longitude,
        geometry::GEOMETRY(GEOMETRY,4326)               AS geometry,
        source_data_updated::timestamptz                AS source_data_updated,
        ingestion_check_time::timestamptz               AS ingestion_check_time
    FROM {{ ref('chicago_potholes_patched') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
