{{ config(materialized='view') }}
{% set ck_cols = ["stop_id"] %}
{% set record_id = "stop_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(stop_id::text)                  AS stop_id,
        upper(station_name::text)             AS station_name,
        upper(station_descriptive_name::text) AS station_descriptive_name,
        upper(direction_id::char(1))          AS direction_id,
        map_id::int                           AS map_id,
        upper(stop_name::text)                AS stop_name,
        red::boolean                          AS red_line,
        o::boolean                            AS orange_line,
        pnk::boolean                          AS pink_line,
        y::boolean                            AS yellow_line,
        g::boolean                            AS green_line,
        blue::boolean                         AS blue_line,
        p::boolean                            AS purple_line,
        pexp::boolean                         AS purple_express,
        brn::boolean                          AS brown_line,
        ada::boolean                          AS ada,
        geometry::GEOMETRY(POINT,4326)        AS geometry,
        source_data_updated::timestamptz      AS source_data_updated,
        ingestion_check_time::timestamptz     AS ingestion_check_time
    FROM {{ ref('chicago_cta_train_stations') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
