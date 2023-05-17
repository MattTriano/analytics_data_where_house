{{ config(materialized='view') }}
{% set ck_cols = ["address", "request_date", "completion_date"] %}
{% set record_id = "pothole_repair_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        request_date::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS request_date,
        completion_date::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS completion_date,
        upper(address::text)                                  AS address,
        number_of_potholes_filled_on_block::smallint          AS number_of_potholes_filled_on_block,
        latitude::double precision                            AS latitude,
        longitude::double precision                           AS longitude,
        geometry::GEOMETRY(GEOMETRY,4326)                     AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_potholes_patched') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
