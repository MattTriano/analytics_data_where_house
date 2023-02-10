{{ config(materialized='view') }}
{% set ck_cols = ["dist_num"] %}
{% set record_id = "dist_num" %}

WITH records_with_basic_cleaning AS (
    SELECT
        lpad(upper(dist_num::char(2)), 2, '0') AS dist_num,
        upper(dist_label::text)                AS dist_label,        
        geometry::GEOMETRY(MULTIPOLYGON, 4326) AS geometry,
        source_data_updated::timestamptz       AS source_data_updated,
        ingestion_check_time::timestamptz      AS ingestion_check_time
    FROM {{ ref('chicago_police_district_boundaries') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
WHERE {{ record_id }} <> '31'
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated
